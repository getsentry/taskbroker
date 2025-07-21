use chrono::{Timelike, Utc};
use futures::{StreamExt, stream::FuturesUnordered};
use prost::Message;
use prost_types::Timestamp;
use rdkafka::error::KafkaError;
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
use tracing::{debug, error, info, instrument};
use uuid::Uuid;

use crate::{
    config::Config,
    store::inflight_activation::{InflightActivationStatus, InflightActivationStore},
};

/// The upkeep task that periodically performs upkeep
/// on the inflight store
pub async fn upkeep(config: Arc<Config>, store: Arc<InflightActivationStore>) {
    let kafka_config = config.kafka_producer_config();
    let producer: Arc<FutureProducer> = Arc::new(
        kafka_config
            .create()
            .expect("Could not create kafka producer in upkeep"),
    );

    let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    let mut timer = time::interval(Duration::from_millis(config.upkeep_task_interval_ms));
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    loop {
        select! {
            _ = timer.tick() => {
                let _ = do_upkeep(config.clone(), store.clone(), producer.clone()).await;
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
    processing_attempts_exceeded: u64,
    delay_elapsed: u64,
    expired: u64,
    completed: u64,
    failed: u64,
    pending: u32,
    processing: u32,
    delay: u32,
    deadlettered: u64,
    discarded: u64,
}

impl UpkeepResults {
    fn empty(&self) -> bool {
        self.retried == 0
            && self.processing_deadline_reset == 0
            && self.processing_attempts_exceeded == 0
            && self.expired == 0
            && self.completed == 0
            && self.failed == 0
            && self.pending == 0
            && self.processing == 0
            && self.delay == 0
            && self.discarded == 0
            && self.deadlettered == 0
    }
}

#[instrument(name = "upkeep::do_upkeep", skip(store, config, producer))]
pub async fn do_upkeep(
    config: Arc<Config>,
    store: Arc<InflightActivationStore>,
    producer: Arc<FutureProducer>,
) -> UpkeepResults {
    let upkeep_start = Instant::now();
    let mut result_context = UpkeepResults {
        retried: 0,
        processing_deadline_reset: 0,
        processing_attempts_exceeded: 0,
        delay_elapsed: 0,
        expired: 0,
        completed: 0,
        failed: 0,
        pending: 0,
        processing: 0,
        delay: 0,
        deadlettered: 0,
        discarded: 0,
    };

    // 1. Handle retry tasks
    let handle_retries_start = Instant::now();
    if let Ok(retries) = store.get_retry_activations().await {
        // 2. Append retries to kafka
        let deliveries = retries
            .into_iter()
            .map(|inflight| {
                let producer = producer.clone();
                let config = config.clone();

                async move {
                    let activation = TaskActivation::decode(&inflight.activation as &[u8]).unwrap();
                    let serialized = create_retry_activation(&activation).encode_to_vec();
                    let delivery = producer
                        .send(
                            FutureRecord::<(), Vec<u8>>::to(&config.kafka_topic)
                                .payload(&serialized),
                            Timeout::After(Duration::from_millis(config.kafka_send_timeout_ms)),
                        )
                        .await;
                    match delivery {
                        Ok(_) => Ok(inflight.id),
                        Err((err, _msg)) => Err(err),
                    }
                }
            })
            .collect::<FuturesUnordered<_>>();

        let ids = deliveries
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result: Result<String, KafkaError>| match result {
                Ok(id) => Some(id),
                Err(err) => {
                    error!("retry.publish.failure {}", err);
                    None
                }
            })
            .collect();

        // 3. Update retry tasks to complete
        if let Ok(retried_count) = store.mark_completed(ids).await {
            result_context.retried = retried_count;
        }
    }
    metrics::histogram!("upkeep.handle_retries").record(handle_retries_start.elapsed());

    // 4. Handle processing deadlines
    let handle_processing_deadline_start = Instant::now();
    if let Ok(processing_deadline_reset) = store.handle_processing_deadline().await {
        result_context.processing_deadline_reset = processing_deadline_reset;
    }
    metrics::histogram!("upkeep.handle_processing_deadline")
        .record(handle_processing_deadline_start.elapsed());

    // 5. Handle processing attempts exceeded
    let handle_processing_attempts_exceeded_start = Instant::now();
    if let Ok(processing_attempts_exceeded) = store.handle_processing_attempts().await {
        result_context.processing_attempts_exceeded = processing_attempts_exceeded;
    }
    metrics::histogram!("upkeep.handle_processing_attempts_exceeded")
        .record(handle_processing_attempts_exceeded_start.elapsed());

    // 6. Remove tasks that are past their expires_at deadline
    let handle_expires_at_start = Instant::now();
    if let Ok(expired_count) = store.handle_expires_at().await {
        result_context.expired = expired_count;
    }
    metrics::histogram!("upkeep.handle_expires_at").record(handle_expires_at_start.elapsed());

    // 7. Handle tasks that are past their delay_until deadline
    let handle_delay_until_start = Instant::now();
    if let Ok(delay_elapsed) = store.handle_delay_until().await {
        result_context.delay_elapsed = delay_elapsed;
    }
    metrics::histogram!("upkeep.handle_delay_until").record(handle_delay_until_start.elapsed());

    // 8. Handle failure state tasks
    let handle_failed_tasks_start = Instant::now();
    if let Ok(failed_tasks_forwarder) = store.handle_failed_tasks().await {
        result_context.discarded = failed_tasks_forwarder.to_discard.len() as u64;
        result_context.failed =
            result_context.discarded + failed_tasks_forwarder.to_deadletter.len() as u64;

        let deadletters = failed_tasks_forwarder
            .to_deadletter
            .into_iter()
            .map(|(id, activation_data)| {
                let producer = producer.clone();
                let config = config.clone();
                async move {
                    metrics::histogram!("upkeep.dlq.message_size")
                        .record(activation_data.len() as f64);
                    let delivery = producer
                        .send(
                            FutureRecord::<(), Vec<u8>>::to(&config.kafka_deadletter_topic)
                                .payload(&activation_data),
                            Timeout::After(Duration::from_millis(config.kafka_send_timeout_ms)),
                        )
                        .await;

                    if let Err((err, _msg)) = delivery {
                        error!(
                            "deadletter.publish.failure: {}, message: {:?}",
                            err, activation_data
                        );
                    }
                    id
                }
            })
            .collect::<FuturesUnordered<_>>();

        // Submit deadlettered tasks to dlq.
        let ids = deadletters.collect::<Vec<_>>().await.into_iter().collect();

        // 9. Update deadlettered tasks to complete
        if let Ok(deadletter_count) = store.mark_completed(ids).await {
            result_context.deadlettered = deadletter_count;
        }
    }
    metrics::histogram!("upkeep.handle_failed_tasks").record(handle_failed_tasks_start.elapsed());

    // 10. Cleanup completed tasks
    let remove_completed_start = Instant::now();
    if let Ok(count) = store.remove_completed().await {
        result_context.completed = count;
    }
    metrics::histogram!("upkeep.remove_completed").record(remove_completed_start.elapsed());

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
    if let Ok(delay_count) = store.count_by_status(InflightActivationStatus::Delay).await {
        result_context.delay = delay_count as u32;
    }
    let max_lag = store.pending_activation_max_lag(&Utc::now()).await;

    if !result_context.empty() {
        debug!(
            result_context.completed,
            result_context.deadlettered,
            result_context.discarded,
            result_context.processing_deadline_reset,
            result_context.processing_attempts_exceeded,
            result_context.expired,
            result_context.retried,
            result_context.pending,
            result_context.processing,
            result_context.delay,
            result_context.delay_elapsed,
            "upkeep.complete",
        );
    }
    metrics::histogram!("upkeep.duration").record(upkeep_start.elapsed());

    // Task statuses
    metrics::counter!("upkeep.task.state_transition", "state" => "completed")
        .increment(result_context.completed);
    metrics::counter!("upkeep.task.state_transition", "state" => "failed")
        .increment(result_context.failed);
    metrics::counter!("upkeep.task.state_transition", "state" => "retried")
        .increment(result_context.retried);

    // Upkeep cleanup actions
    metrics::counter!("upkeep.cleanup_action", "kind" => "publish_deadlettered")
        .increment(result_context.deadlettered);
    metrics::counter!("upkeep.cleanup_action", "kind" => "removed_expired")
        .increment(result_context.expired);
    metrics::counter!("upkeep.cleanup_action", "kind" => "delete_discarded")
        .increment(result_context.discarded);
    metrics::counter!("upkeep.cleanup_action", "kind" => "mark_processing_attempts_exceeded_as_failure")
        .increment(result_context.processing_attempts_exceeded);
    metrics::counter!("upkeep.cleanup_action", "kind" => "mark_processing_deadline_exceeded_as_failure")
        .increment(result_context.processing_deadline_reset);
    metrics::counter!("upkeep.cleanup_action", "kind" => "mark_delay_elapsed_as_pending")
        .increment(result_context.delay_elapsed);

    // State of inflight tasks
    metrics::gauge!("upkeep.current_pending_tasks").set(result_context.pending);
    metrics::gauge!("upkeep.current_processing_tasks").set(result_context.processing);
    metrics::gauge!("upkeep.current_delayed_tasks").set(result_context.delay);
    metrics::gauge!("upkeep.pending_activation.max_lag").set(max_lag as f64);

    result_context
}

/// Create a new activation that is a 'retry' of the passed inflight_activation
/// The retry_state.attempts is advanced as part of the retry state machine.
#[instrument(skip_all)]
fn create_retry_activation(activation: &TaskActivation) -> TaskActivation {
    let mut new_activation = activation.clone();

    let now = Utc::now();
    new_activation.id = Uuid::new_v4().into();
    new_activation.received_at = Some(Timestamp {
        seconds: now.timestamp(),
        nanos: now.nanosecond() as i32,
    });
    new_activation.delay = new_activation
        .retry_state
        .and_then(|retry_state| retry_state.delay_on_retry);

    if new_activation.retry_state.is_some() {
        new_activation.retry_state.as_mut().unwrap().attempts += 1;
    }

    new_activation
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeDelta, TimeZone, Utc};
    use prost::Message;
    use prost_types::Timestamp;
    use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, RetryState, TaskActivation};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    use crate::{
        store::inflight_activation::{
            InflightActivationStatus, InflightActivationStore, InflightActivationStoreConfig,
        },
        test_utils::{
            consume_topic, create_config, create_integration_config, create_producer,
            generate_temp_filename, make_activations, replace_retry_state, reset_topic,
        },
        upkeep::{create_retry_activation, do_upkeep},
    };

    async fn create_inflight_store() -> Arc<InflightActivationStore> {
        let url = generate_temp_filename();
        let config = create_integration_config();

        Arc::new(
            InflightActivationStore::new(&url, InflightActivationStoreConfig::from_config(&config))
                .await
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_retry_activation_sets_delay_with_delay_on_retry() {
        let inflight = make_activations(1).remove(0);
        let mut activation = TaskActivation::decode(&inflight.activation as &[u8]).unwrap();
        activation.delay = None;
        activation.retry_state = Some(RetryState {
            attempts: 0,
            max_attempts: 3,
            on_attempts_exceeded: OnAttemptsExceeded::Discard.into(),
            at_most_once: Some(false),
            delay_on_retry: Some(60),
        });

        let retry = create_retry_activation(&activation);
        assert_eq!(retry.delay, Some(60));
        assert_eq!(
            retry.retry_state,
            Some(RetryState {
                attempts: 1,
                max_attempts: 3,
                on_attempts_exceeded: OnAttemptsExceeded::Discard.into(),
                at_most_once: Some(false),
                delay_on_retry: Some(60),
            })
        );
    }

    #[tokio::test]
    async fn test_retry_activation_updates_delay_with_delay_on_retry() {
        let inflight = make_activations(1).remove(0);
        let mut activation = TaskActivation::decode(&inflight.activation as &[u8]).unwrap();
        activation.delay = Some(100);
        activation.retry_state = Some(RetryState {
            attempts: 0,
            max_attempts: 3,
            on_attempts_exceeded: OnAttemptsExceeded::Discard.into(),
            at_most_once: Some(false),
            delay_on_retry: Some(60),
        });

        let retry = create_retry_activation(&activation);
        assert_eq!(retry.delay, Some(60));
        assert_eq!(
            retry.retry_state,
            Some(RetryState {
                attempts: 1,
                max_attempts: 3,
                on_attempts_exceeded: OnAttemptsExceeded::Discard.into(),
                at_most_once: Some(false),
                delay_on_retry: Some(60),
            })
        );
    }

    #[tokio::test]
    async fn test_retry_activation_clears_delay_without_delay_on_retry() {
        let inflight = make_activations(1).remove(0);
        let mut activation = TaskActivation::decode(&inflight.activation as &[u8]).unwrap();
        activation.delay = Some(60);
        activation.retry_state = Some(RetryState {
            attempts: 0,
            max_attempts: 3,
            on_attempts_exceeded: OnAttemptsExceeded::Discard.into(),
            at_most_once: Some(false),
            delay_on_retry: None,
        });

        let retry = create_retry_activation(&activation);
        assert_eq!(retry.delay, None);
        assert_eq!(
            retry.retry_state,
            Some(RetryState {
                attempts: 1,
                max_attempts: 3,
                on_attempts_exceeded: OnAttemptsExceeded::Discard.into(),
                at_most_once: Some(false),
                delay_on_retry: None,
            })
        );
    }

    #[tokio::test]
    async fn test_retry_activation_is_appended_to_kafka() {
        let config = create_integration_config();
        reset_topic(config.clone()).await;

        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());
        let mut records = make_activations(2);

        let old = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();
        replace_retry_state(
            &mut records[0],
            Some(RetryState {
                attempts: 1,
                max_attempts: 2,
                on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
                at_most_once: None,
                delay_on_retry: None,
            }),
        );
        let mut activation = TaskActivation::decode(&records[0].activation as &[u8]).unwrap();
        activation.received_at = Some(Timestamp {
            seconds: old.timestamp(),
            nanos: 0,
        });
        records[0].received_at = DateTime::from_timestamp(
            activation.received_at.unwrap().seconds,
            activation.received_at.unwrap().nanos as u32,
        )
        .expect("");
        activation.parameters = r#"{"a":"b"}"#.into();
        activation.delay = Some(30);
        records[0].status = InflightActivationStatus::Retry;
        records[0].delay_until = Some(Utc::now() + Duration::from_secs(30));
        records[0].activation = activation.encode_to_vec();

        records[1].added_at += Duration::from_secs(1);
        assert!(store.store(records.clone()).await.is_ok());

        let result_context = do_upkeep(config.clone(), store.clone(), producer).await;

        // Only 1 record left as the retry task should be appended as a new task
        assert_eq!(store.count().await.unwrap(), 1);
        assert_eq!(result_context.retried, 1);

        let messages = consume_topic(config.clone(), config.kafka_topic.as_ref(), 1).await;
        assert_eq!(messages.len(), 1);
        let activation = &messages[0];

        // Should spawn a new task
        let activation_to_check = TaskActivation::decode(&records[0].activation as &[u8]).unwrap();
        assert_ne!(activation.id, activation_to_check.id);
        // Should increment the attempt counter
        assert_eq!(activation.retry_state.as_ref().unwrap().attempts, 2);

        // Retry should retain task and parameters of original task
        let activation_to_check = TaskActivation::decode(&records[0].activation as &[u8]).unwrap();
        assert_eq!(activation.taskname, activation_to_check.taskname);
        assert_eq!(activation.namespace, activation_to_check.namespace);
        assert_eq!(activation.parameters, activation_to_check.parameters);
        // received_at should be set be later than the original activation
        assert!(
            activation.received_at.unwrap().seconds
                > activation_to_check.received_at.unwrap().seconds,
            "retry activation should have a later timestamp"
        );
        // The delay_until of a retry task should be set to None
        assert!(activation.delay.is_none());
    }

    #[tokio::test]
    async fn test_processing_deadline_retains_future_deadline() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(2);
        // Make a task past with a future processing deadline
        batch[1].status = InflightActivationStatus::Processing;
        batch[1].processing_deadline = Some(Utc::now() + TimeDelta::minutes(5));
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
        replace_retry_state(
            &mut batch[1],
            Some(RetryState {
                attempts: 0,
                max_attempts: 1,
                on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
                at_most_once: Some(true),
                delay_on_retry: None,
            }),
        );
        batch[1].status = InflightActivationStatus::Processing;
        batch[1].processing_deadline =
            Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
        batch[1].at_most_once = true;
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
    async fn test_processing_attempts_exceeded_discard() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(3);
        // Because 1 is complete and has a higher offset than 0, index 2 can be discarded
        batch[0].processing_attempts = config.max_processing_attempts as i32;

        batch[1].status = InflightActivationStatus::Complete;
        batch[1].added_at += Duration::from_secs(1);

        batch[2].processing_attempts = config.max_processing_attempts as i32;
        batch[2].added_at += Duration::from_secs(2);

        assert!(store.store(batch.clone()).await.is_ok());
        let result_context = do_upkeep(config, store.clone(), producer).await;

        assert_eq!(result_context.processing_attempts_exceeded, 2); // batch[0] and batch[2] are removed due to max processing_attempts exceeded
        assert_eq!(result_context.discarded, 2); // batch[0] and batch[2] are discarded
        assert_eq!(result_context.completed, 3); // all three are removed as completed
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Pending)
                .await
                .unwrap(),
            0,
            "zero pending task should remain"
        );
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Complete)
                .await
                .unwrap(),
            0,
            "complete tasks were removed"
        );
    }

    #[tokio::test]
    async fn test_remove_at_remove_failed_publish_to_kafka() {
        let config = create_integration_config();
        reset_topic(config.clone()).await;

        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());
        let mut records = make_activations(2);
        replace_retry_state(
            &mut records[0],
            Some(RetryState {
                attempts: 1,
                max_attempts: 1,
                on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
                at_most_once: None,
                delay_on_retry: None,
            }),
        );
        records[0].status = InflightActivationStatus::Failure;
        records[1].added_at += Duration::from_secs(1);
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
        let activation_to_check = TaskActivation::decode(&records[0].activation as &[u8]).unwrap();
        assert_eq!(activation.id, activation_to_check.id);
        // DLQ should retain parameters of original task
        assert_eq!(activation.parameters, activation_to_check.parameters);
    }

    #[tokio::test]
    async fn test_remove_failed_discard() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(2);
        batch[0].status = InflightActivationStatus::Failure;
        batch[1].added_at += Duration::from_secs(1);
        assert!(store.store(batch).await.is_ok());

        let result_context = do_upkeep(config, store.clone(), producer).await;

        assert_eq!(result_context.discarded, 1);
        assert_eq!(result_context.completed, 1);
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

    #[tokio::test]
    async fn test_expired_discard() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(4);

        batch[0].expires_at = Some(Utc::now() - Duration::from_secs(100));
        batch[1].status = InflightActivationStatus::Complete;
        batch[2].expires_at = Some(Utc::now() - Duration::from_secs(100));

        // Ensure the fourth task is in the future
        batch[3].expires_at = Some(Utc::now() + Duration::from_secs(100));
        batch[3].added_at += Duration::from_secs(1);

        assert!(store.store(batch.clone()).await.is_ok());
        let result_context = do_upkeep(config, store.clone(), producer).await;

        assert_eq!(result_context.expired, 2); // 0/2 removed as expired
        assert_eq!(result_context.completed, 1); // 1 complete
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
            store.get_by_id(&batch[0].id).await.unwrap().is_none(),
            "first task should be removed"
        );
        assert!(
            store.get_by_id(&batch[1].id).await.unwrap().is_none(),
            "second task should be removed"
        );
        assert!(
            store.get_by_id(&batch[2].id).await.unwrap().is_none(),
            "third task should be removed"
        );
        assert!(
            store.get_by_id(&batch[3].id).await.unwrap().is_some(),
            "fourth task should be kept"
        );
    }

    #[tokio::test]
    async fn test_delay_elapsed() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(2);

        batch[0].status = InflightActivationStatus::Delay;
        batch[0].delay_until = Some(Utc::now() - Duration::from_secs(1));

        batch[1].status = InflightActivationStatus::Delay;
        batch[1].delay_until = Some(Utc::now() + Duration::from_secs(1));

        assert!(store.store(batch.clone()).await.is_ok());
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Delay)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Pending)
                .await
                .unwrap(),
            0
        );
        let result_context = do_upkeep(config.clone(), store.clone(), producer.clone()).await;
        assert_eq!(result_context.delay_elapsed, 1);
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Pending)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .get_pending_activation(None)
                .await
                .unwrap()
                .unwrap()
                .id,
            "id_0",
        );
        assert!(store.get_pending_activation(None).await.unwrap().is_none());
        sleep(Duration::from_secs(2)).await;
        let result_context = do_upkeep(config.clone(), store.clone(), producer.clone()).await;
        assert_eq!(result_context.delay_elapsed, 1);
        assert_eq!(
            store
                .count_by_status(InflightActivationStatus::Pending)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .get_pending_activation(None)
                .await
                .unwrap()
                .unwrap()
                .id,
            "id_1",
        );
    }
}
