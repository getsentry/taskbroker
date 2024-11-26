use metrics::counter;
use prost::Message;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use sentry_protos::sentry::v1::TaskActivation;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::{select, time};
use tracing::{error, info, instrument};
use uuid::Uuid;

use crate::{
    config::Config,
    inflight_activation_store::{InflightActivation, InflightActivationStore},
};

/// Start the upkeep task that periodically performs upkeep
/// on the inflight store
pub async fn start_upkeep(config: Arc<Config>, store: Arc<InflightActivationStore>) {
    let kafka_config = config.kafka_producer_config();
    let producer: FutureProducer = kafka_config
        .create()
        .expect("Could not create kafka producer in upkeep");
    let producer_arc = Arc::new(producer);

    let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    let mut timer = time::interval(Duration::from_millis(config.upkeep_task_interval_ms));
    let mutex = Arc::new(Mutex::new(0));
    loop {
        select! {
            _ = timer.tick() => {
                let lock = mutex.try_lock();
                if lock.is_ok() {
                    do_upkeep(config.clone(), store.clone(), producer_arc.clone()).await;
                } else {
                    info!("Could not acquire upkeep mutex lock");
                    counter!("upkeep.start_upkeep.mutex.failed").increment(1);
                }
            }
            _ = guard.wait() => {
                info!("Cancellation token received, shutting down upkeep");
                break;
            }
        }
    }
}

// Debugging context
struct UpkeepResults {
    retried: u64,
    processing_deadline_reset: u64,
    deadletter_at_expired: u64,
    deadlettered: u64,
    completed: u64,
}

impl UpkeepResults {
    fn empty(&self) -> bool {
        self.retried == 0
            && self.processing_deadline_reset == 0
            && self.deadletter_at_expired == 0
            && self.deadlettered == 0
            && self.completed == 0
    }
}

#[instrument(name = "consumer::do_upkeep", skip(store, config, producer))]
pub async fn do_upkeep(
    config: Arc<Config>,
    store: Arc<InflightActivationStore>,
    producer: Arc<FutureProducer>,
) {
    let mut result_context = UpkeepResults {
        retried: 0,
        processing_deadline_reset: 0,
        deadletter_at_expired: 0,
        deadlettered: 0,
        completed: 0,
    };

    // 1. Handle retry tasks
    if let Ok(retries) = store.get_retry_activations().await {
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
    if let Ok(processing_count) = store.handle_processing_deadline().await {
        result_context.processing_deadline_reset = processing_count;
    }

    // 5. Advance state on tasks past deadletter_at
    if let Ok(deadletter_count) = store.handle_deadletter_at().await {
        result_context.deadletter_at_expired = deadletter_count;
    }

    // 6. Handle failure state tasks
    if let Ok(deadletter_activations) = store.handle_failed_tasks().await {
        let mut ids: Vec<String> = vec![];
        // Submit deadlettered tasks to dlq.
        for activation in deadletter_activations {
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
    if let Ok(remove_count) = store.remove_completed().await {
        result_context.completed = remove_count;
    }

    if !result_context.empty() {
        info!(
            result_context.completed,
            result_context.deadlettered,
            result_context.deadletter_at_expired,
            result_context.retried,
            "upkeep.complete",
        );
    }
}

/// Create a new activation that is a 'retry' of the passed inflight_activation
/// The retry_state.attempts is advanced as part of the retry state machine.
fn create_retry_activation(inflight_activation: &InflightActivation) -> TaskActivation {
    let mut new_activation = inflight_activation.activation.clone();

    new_activation.id = Uuid::new_v4().into();
    if new_activation.retry_state.is_some() {
        new_activation.retry_state.as_mut().unwrap().attempts += 1;
    }

    new_activation
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use rdkafka::producer::FutureProducer;
    use sentry_protos::sentry::v1::RetryState;

    use crate::{
        config::Config,
        inflight_activation_store::{InflightActivationStore, TaskActivationStatus},
        test_utils::{generate_temp_filename, make_activations},
        upkeep::do_upkeep,
    };

    fn create_config() -> Arc<Config> {
        Arc::new(Config::default())
    }

    fn create_producer(config: Arc<Config>) -> Arc<FutureProducer> {
        let producer: FutureProducer = config
            .kafka_producer_config()
            .create()
            .expect("Could not create kafka producer");

        Arc::new(producer)
    }

    async fn create_inflight_store() -> Arc<InflightActivationStore> {
        let url = generate_temp_filename();

        Arc::new(InflightActivationStore::new(&url).await.unwrap())
    }

    #[tokio::test]
    async fn test_retry_activation_is_appended_to_kafka() {
        // TODO
    }

    #[tokio::test]
    async fn test_retry_is_discarded_when_exhausted() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut records = make_activations(2);
        records[0].status = TaskActivationStatus::Retry;
        records[0].activation.retry_state = Some(RetryState {
            attempts: 1,
            kind: "".into(),
            discard_after_attempt: Some(1),
            deadletter_after_attempt: None,
        });

        assert!(store.store(records).await.is_ok());

        do_upkeep(config, store.clone(), producer).await;

        // Only 1 record left as the retry task should be discarded.
        assert_eq!(store.count().await.unwrap(), 1);
        assert_eq!(
            store
                .count_by_status(TaskActivationStatus::Pending)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_processing_deadline_updates() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(2);
        // Make a task past it is processing deadline
        batch[1].status = TaskActivationStatus::Processing;
        batch[1].processing_deadline =
            Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
        assert!(store.store(batch.clone()).await.is_ok());

        // Should start off with one in processing
        assert_eq!(
            store
                .count_by_status(TaskActivationStatus::Processing)
                .await
                .unwrap(),
            1
        );

        do_upkeep(config, store.clone(), producer).await;

        // 0 processing, 2 pending now
        assert_eq!(
            store
                .count_by_status(TaskActivationStatus::Processing)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .count_by_status(TaskActivationStatus::Pending)
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn test_past_deadletter_at_discard() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(3);
        // Because 1 is complete and has a higher offset than 0, 2 will be discarded
        batch[0].deadletter_at = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
        batch[1].status = TaskActivationStatus::Complete;
        batch[2].deadletter_at = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());

        assert!(store.store(batch.clone()).await.is_ok());
        do_upkeep(config, store.clone(), producer).await;

        assert_eq!(
            store
                .count_by_status(TaskActivationStatus::Pending)
                .await
                .unwrap(),
            1,
            "one pending task should remain"
        );
        assert_eq!(
            store
                .count_by_status(TaskActivationStatus::Complete)
                .await
                .unwrap(),
            0,
            "complete tasks were removed"
        );

        assert!(
            store.get_by_id(&batch[0].activation.id).await.is_ok(),
            "first task should remain"
        );
    }

    #[tokio::test]
    async fn test_remove_failed_publish_to_kafka() {
        // TODO
    }

    #[tokio::test]
    async fn test_remove_failed_discard() {
        let config = create_config();
        let store = create_inflight_store().await;
        let producer = create_producer(config.clone());

        let mut batch = make_activations(2);
        batch[0].status = TaskActivationStatus::Failure;
        assert!(store.store(batch).await.is_ok());

        do_upkeep(config, store.clone(), producer).await;

        assert_eq!(
            store.count().await.unwrap(),
            1,
            "failed task should be removed"
        );
        assert_eq!(
            store
                .count_by_status(TaskActivationStatus::Pending)
                .await
                .unwrap(),
            1,
            "pending task should remain"
        );
    }
}
