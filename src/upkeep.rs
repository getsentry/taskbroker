use chrono::{Timelike, Utc};
use prost::Message;
use prost_types::Timestamp;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use sentry_protos::taskbroker::v1::TaskActivation;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{select, time};
use tracing::{error, info, info_span, instrument, Instrument};
use uuid::Uuid;

use crate::{
    config::Config,
    inflight_activation_store::{
        InflightActivation, InflightActivationStatus, InflightActivationStore,
    },
};

/// The upkeep task that periodically performs upkeep
/// on the inflight store
pub async fn upkeep(config: Arc<Config>, store: Arc<InflightActivationStore>) {
    let kafka_config = config.kafka_producer_config();
    let producer: FutureProducer = kafka_config
        .create()
        .expect("Could not create kafka producer in upkeep");
    let producer_arc = Arc::new(producer);

    let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    let mut timer = time::interval(Duration::from_millis(config.upkeep_task_interval_ms));
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    loop {
        select! {
            _ = timer.tick() => {
                let _ = do_upkeep(config.clone(), store.clone(), producer_arc.clone()).await;
            }
            _ = guard.wait() => {
                info!("Cancellation token received, shutting down upkeep");
                break;
            }
        }
    }
}

// Debugging context
#[derive(Debug)]
struct UpkeepResults {
    retried: u64,
    processing_deadline_reset: u64,
    remove_at_expired: u64,
    deadlettered: u64,
    completed: u64,
    pending: u32,
    processing: u32,
    discarded: u64,
}

impl UpkeepResults {
    fn empty(&self) -> bool {
        self.retried == 0
            && self.processing_deadline_reset == 0
            && self.remove_at_expired == 0
            && self.deadlettered == 0
            && self.completed == 0
            && self.pending == 0
            && self.processing == 0
            && self.discarded == 0
    }
}

#[instrument(name = "consumer::do_upkeep", skip(store, config, producer))]
pub async fn do_upkeep(
    config: Arc<Config>,
    store: Arc<InflightActivationStore>,
    producer: Arc<FutureProducer>,
) -> UpkeepResults {
    let upkeep_start = Instant::now();
    let mut result_context = UpkeepResults {
        retried: 0,
        processing_deadline_reset: 0,
        remove_at_expired: 0,
        deadlettered: 0,
        completed: 0,
        pending: 0,
        processing: 0,
        discarded: 0,
    };

    // 1. Handle retry tasks
    if let Ok(retries) = store
        .get_retry_activations()
        .instrument(info_span!("get_retry_activations"))
        .await
    {
        // 2. Append retries to kafka
        let mut ids: Vec<String> = vec![];
        for inflight in retries {
            let retry_activation = create_retry_activation(&inflight);
            let payload = retry_activation.encode_to_vec();
            let message = FutureRecord::<(), _>::to(&config.kafka_topic).payload(&payload);
            let send_result = producer
                .send(
                    message,
                    Timeout::After(Duration::from_millis(config.kafka_send_timeout_ms)),
                )
                .await;
            if let Err((err, _msg)) = send_result {
                error!("retry.publish.failure {}", err);
            }

            ids.push(inflight.activation.id);
        }

        // 3. Update retry tasks to complete
        if let Ok(retried_count) = store.mark_completed(ids).await {
            result_context.retried = retried_count;
        }
    }

    // 4. Handle processing deadlines
    if let Ok(processing_count) = store
        .handle_processing_deadline()
        .instrument(info_span!("handle_processing_deadline"))
        .await
    {
        result_context.processing_deadline_reset = processing_count;
    }

    // 5. Advance state on tasks past remove_at
    if let Ok(remove_count) = store
        .handle_remove_at()
        .instrument(info_span!("handle_remove_at"))
        .await
    {
        result_context.remove_at_expired = remove_count;
    }

    // 6. Handle failure state tasks
    if let Ok(failed_tasks_forwarder) = store
        .handle_failed_tasks()
        .instrument(info_span!("handle_failed_tasks"))
        .await
    {
        result_context.discarded = failed_tasks_forwarder.to_discard.len() as u64;
        let mut ids: Vec<String> = vec![];
        // Submit deadlettered tasks to dlq.
        for activation in failed_tasks_forwarder.to_deadletter {
            let payload = activation.encode_to_vec();
            let message =
                FutureRecord::<(), _>::to(&config.kafka_deadletter_topic).payload(&payload);

            let send_result = producer
                .send(
                    message,
                    Timeout::After(Duration::from_millis(config.kafka_send_timeout_ms)),
                )
                .await;
            if let Err((err, _msg)) = send_result {
                error!("deadletter.publish.failure {}", err);
            }
            ids.push(activation.id);
        }
        // 7. Update deadlettered tasks to complete
        if let Ok(deadletter_count) = store.mark_completed(ids).await {
            result_context.deadlettered = deadletter_count;
        }
    }

    // 8. Cleanup completed tasks
    if let Ok(count) = store
        .remove_completed()
        .instrument(info_span!("remove_completed"))
        .await
    {
        result_context.completed = count;
    }

    if let Ok(pending_count) = store
        .count_by_status(InflightActivationStatus::Pending)
        .await
    {
        result_context.pending = pending_count as u32;
    }
    if let Ok(processing_count) = store
        .count_by_status(InflightActivationStatus::Processing)
        .await
    {
        result_context.processing = processing_count as u32;
    }

    if !result_context.empty() {
        info!(
            result_context.completed,
            result_context.deadlettered,
            result_context.remove_at_expired,
            result_context.retried,
            result_context.pending,
            result_context.processing,
            "upkeep.complete",
        );
    }
    metrics::histogram!("upkeep.duration").record(upkeep_start.elapsed());

    metrics::counter!("upkeep.completed").increment(result_context.completed);
    metrics::counter!("upkeep.deadlettered").increment(result_context.deadlettered);
    metrics::counter!("upkeep.remove_at_expired").increment(result_context.remove_at_expired);
    metrics::counter!("upkeep.retried").increment(result_context.retried);

    metrics::counter!("upkeep.pending_count").increment(result_context.pending.into());
    metrics::gauge!("upkeep.processing_count").set(result_context.processing);

    result_context
}

/// Create a new activation that is a 'retry' of the passed inflight_activation
/// The retry_state.attempts is advanced as part of the retry state machine.
#[instrument(skip_all)]
fn create_retry_activation(inflight_activation: &InflightActivation) -> TaskActivation {
    let mut new_activation = inflight_activation.activation.clone();

    let now = Utc::now();
    new_activation.id = Uuid::new_v4().into();
    new_activation.received_at = Some(Timestamp {
        seconds: now.timestamp(),
        nanos: now.nanosecond() as i32,
    });
    if new_activation.retry_state.is_some() {
        new_activation.retry_state.as_mut().unwrap().attempts += 1;
    }

    new_activation
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::sync::Arc;

    use chrono::{TimeDelta, TimeZone, Utc};
    use prost_types::Timestamp;
    use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, RetryState};

    use crate::{
        inflight_activation_store::{InflightActivationStatus, InflightActivationStore},
        test_utils::{
            consume_topic, create_config, create_integration_config, create_producer,
            generate_temp_filename, make_activations, reset_topic,
        },
        upkeep::do_upkeep,
    };

    async fn create_inflight_store() -> Arc<InflightActivationStore> {
        let url = generate_temp_filename();

        Arc::new(InflightActivationStore::new(&url).await.unwrap())
    }

    #[tokio::test]
    async fn test_retry_activation_is_appended_to_kafka() {
        let config = create_integration_config();
        reset_topic(config.clone()).await;

        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());
        let mut records = make_activations(2);

        let old = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();
        records[0].activation.received_at = Some(Timestamp {
            seconds: old.timestamp(),
            nanos: 0,
        });
        records[0].activation.parameters = r#"{"a":"b"}"#.into();
        records[0].status = InflightActivationStatus::Retry;
        records[0].activation.retry_state = Some(RetryState {
            attempts: 1,
            max_attempts: 2,
            on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
            at_most_once: None,
        });
        assert!(store.store(records.clone()).await.is_ok());

        let result_context = do_upkeep(config.clone(), store.clone(), producer).await;

        // Only 1 record left as the retry task should be appended as a new task
        assert_eq!(store.count().await.unwrap(), 1);
        assert_eq!(result_context.retried, 1);

        let messages = consume_topic(config.clone(), config.kafka_topic.as_ref(), 1).await;
        assert_eq!(messages.len(), 1);
        let activation = &messages[0];

        // Should spawn a new task
        assert_ne!(activation.id, records[0].activation.id);
        // Should increment the attempt counter
        assert_eq!(activation.retry_state.as_ref().unwrap().attempts, 2);

        // Retry should retain task and parameters of original task
        assert_eq!(activation.taskname, records[0].activation.taskname);
        assert_eq!(activation.namespace, records[0].activation.namespace);
        assert_eq!(activation.parameters, records[0].activation.parameters);
        // received_at should be set be later than the original activation
        assert!(
            activation.received_at.unwrap().seconds
                > records[0].activation.received_at.unwrap().seconds,
            "retry activation should have a later timestamp"
        );
    }

    #[tokio::test]
    async fn test_processing_deadline_retains_future_deadline() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(2);
        // Make a task past with a future processing deadline
        batch[1].status = InflightActivationStatus::Processing;
        batch[1].processing_deadline = Some(Utc::now().add(TimeDelta::minutes(5)));
        assert!(store.store(batch.clone()).await.is_ok());

        let _ = do_upkeep(config, store.clone(), producer).await;

        // Should retain the processing record
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Processing)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_processing_deadline_updates_past_deadline() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(2);
        // Make a task past with a processing deadline in the past
        batch[1].status = InflightActivationStatus::Processing;
        batch[1].processing_deadline =
            Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
        assert!(store.store(batch.clone()).await.is_ok());

        // Should start off with one in processing
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Processing)
                .await
                .unwrap(),
            1
        );

        let result_context = do_upkeep(config, store.clone(), producer).await;

        // 0 processing, 2 pending now
        assert_eq!(result_context.processing_deadline_reset, 1);
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Processing)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Pending)
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn test_processing_deadline_discard_at_most_once() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(2);
        // Make a task past with a processing deadline in the past
        batch[1].status = InflightActivationStatus::Processing;
        batch[1].processing_deadline =
            Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
        batch[1].at_most_once = true;
        batch[1].activation.retry_state = Some(RetryState {
            attempts: 0,
            max_attempts: 1,
            on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
            at_most_once: Some(true),
        });
        assert!(store.store(batch.clone()).await.is_ok());

        let result_context = do_upkeep(config, store.clone(), producer).await;

        // 0 processing, 1 pending, 1 discarded
        assert_eq!(result_context.discarded, 1);
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Processing)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Pending)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_past_remove_at_discard() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(3);
        // Because 1 is complete and has a higher offset than 0, index 2 can be discarded
        batch[0].remove_at = Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap();
        batch[1].status = InflightActivationStatus::Complete;
        batch[2].remove_at = Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap();

        assert!(store.store(batch.clone()).await.is_ok());
        let result_context = do_upkeep(config, store.clone(), producer).await;

        assert_eq!(result_context.remove_at_expired, 1); // batch[0] is removed due to remove_at deadline
        assert_eq!(result_context.discarded, 1); // batch[0] is discarded
        assert_eq!(result_context.completed, 2); // batch[1] and batch[2] are removed as completed
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Pending)
                .await
                .unwrap(),
            1,
            "one pending task should remain"
        );
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Complete)
                .await
                .unwrap(),
            0,
            "complete tasks were removed"
        );

        assert!(
            store
                .get_by_id(&batch[0].activation.id)
                .await
                .unwrap()
                .is_none(),
            "first task should be removed"
        );
        assert!(
            store
                .get_by_id(&batch[1].activation.id)
                .await
                .unwrap()
                .is_none(),
            "second task should be removed"
        );
        assert!(
            store
                .get_by_id(&batch[2].activation.id)
                .await
                .unwrap()
                .is_some(),
            "third task should be kept"
        );
    }

    #[tokio::test]
    async fn test_remove_at_remove_failed_publish_to_kafka() {
        let config = create_integration_config();
        reset_topic(config.clone()).await;

        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());
        let mut records = make_activations(2);
        records[0].activation.parameters = r#"{"a":"b"}"#.into();
        records[0].status = InflightActivationStatus::Failure;
        records[0].activation.retry_state = Some(RetryState {
            attempts: 1,
            max_attempts: 1,
            on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
            at_most_once: None,
        });
        assert!(store.store(records.clone()).await.is_ok());

        let result_context = do_upkeep(config.clone(), store.clone(), producer).await;

        // Only 1 record left as the failure task should be appended to dlq
        assert_eq!(result_context.deadlettered, 1);
        assert_eq!(store.count().await.unwrap(), 1);

        let messages =
            consume_topic(config.clone(), config.kafka_deadletter_topic.as_ref(), 1).await;
        assert_eq!(messages.len(), 1);
        let activation = &messages[0];

        // Should move the task without changing the id
        assert_eq!(activation.id, records[0].activation.id);
        // DLQ should retain parameters of original task
        assert_eq!(activation.parameters, records[0].activation.parameters);
    }

    #[tokio::test]
    async fn test_remove_failed_discard() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(2);
        batch[0].status = InflightActivationStatus::Failure;
        assert!(store.store(batch).await.is_ok());

        let result_context = do_upkeep(config, store.clone(), producer).await;

        assert_eq!(result_context.discarded, 1);
        assert_eq!(
            store.count().await.unwrap(),
            1,
            "failed task should be removed"
        );
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Pending)
                .await
                .unwrap(),
            1,
            "pending task should remain"
        );
    }
}
