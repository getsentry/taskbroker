use std::{sync::Arc, time::Duration};
use prost::Message;
use rdkafka::{producer::{FutureProducer, FutureRecord}, util::Timeout};
use sentry_protos::sentry::v1::TaskActivation;
use tokio::{select, time};
use tracing::info;
use uuid::Uuid;

use crate::{config::Config, inflight_activation_store::{InflightActivation, InflightActivationStore}};

/// Start the upkeep task that periodically performs upkeep
/// on the inflight store
pub async fn start_upkeep(
    config: Arc<Config>,
    store: Arc<InflightActivationStore>,
    run_interval: Duration,
) {
    let kafka_config = config.kafka_client_config();
    let task_topic = config.kafka_topic.clone();
    let dlq_topic = config.kafka_deadletter_topic.clone();
    let producer: FutureProducer = kafka_config.create().expect("Could not create kafka producer in upkeep");

    let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    let mut timer = time::interval(run_interval);
    loop {
        select! {
            _ = timer.tick() => {
                // 1. Handle retry tasks
                if let Ok(retries) = store.get_retry_activations().await {
                    // 2. Append retries to kafka
                    let mut ids: Vec<String> = vec![];
                    for inflight in retries {
                        let retry_activation = create_retry_activation(&inflight);
                        let payload = retry_activation.encode_to_vec();
                        let message = FutureRecord::<(), _>::to(&task_topic).payload(&payload);
                        let _ = producer.send(message, Timeout::Never).await;
                        // TODO handle Result<Err>

                        ids.push(inflight.activation.id);
                    }
                    // 3. Update retry tasks to complete
                    let _ = store.mark_completed(ids).await;
                    // TODO handle Result<Err>
                }

                // 4. Handle processing deadlines
                let _ = store.handle_processing_deadline().await;

                // 5. Advance state on tasks past deadletter_at
                let _ = store.handle_deadletter_at().await;

                // 6. Handle failure state tasks
                if let Ok(deadletter_activations) = store.handle_failed_tasks().await {
                    let mut ids: Vec<String> = vec![];
                    // Submit deadlettered tasks to dlq.
                    for activation in deadletter_activations {
                        let payload = activation.encode_to_vec();
                        let message = FutureRecord::<(), _>::to(&dlq_topic).payload(&payload);
                        let _ = producer.send(message, Timeout::Never).await;
                        // TODO handle Result<Err>

                        ids.push(activation.id);
                    }
                    // 6. Update deadlettered tasks to complete
                    let _ = store.mark_completed(ids).await;
                }

                // 8. Cleanup completed tasks
                let _ = store.remove_completed().await;
            }
            _ = guard.wait() => {
                info!("Cancellation token received, shutting down upkeep");
                break;
            }
        }
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
