use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Error;
use prost::Message;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::grpc::taskworker_client::TaskworkerClient;
use crate::store::inflight_activation::InflightActivationStore;

/// Continuously fetches pending activations and pushes them to the configured taskworker.
/// This function runs until the shutdown guard signals termination.
pub async fn push_task_loop(
    store: Arc<InflightActivationStore>,
    config: Arc<Config>,
    client: TaskworkerClient,
) -> Result<(), Error> {
    info!(
        "Push task loop starting, pushing to worker at {}",
        config.taskworker_push_address
    );

    let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();

    loop {
        tokio::select! {
            _ = guard.wait() => {
                info!("Push task loop received shutdown signal");
                break;
            }
            _ = async {
                // Try to fetch a pending activation
                match store.peek_pending_activation().await {
                    Ok(Some(inflight)) => {
                        let start_time = Instant::now();
                        let task_id = inflight.id.clone();

                        // Decode the protobuf activation from stored bytes
                        let activation = match sentry_protos::taskbroker::v1::TaskActivation::decode(
                            &inflight.activation as &[u8]
                        ) {
                            Ok(activation) => activation,
                            Err(e) => {
                                error!("Failed to decode activation {}: {:?}", task_id, e);
                                metrics::counter!("push_task.decode_error").increment(1);
                                // This is a corrupted activation, skip it
                                // In a real scenario, we might want to mark it as failed
                                return;
                            }
                        };

                        metrics::counter!("push_task.attempt").increment(1);

                        // Try to push to the worker
                        let rpc_start = Instant::now();
                        match client.add_task(activation, config.taskworker_push_callback_url.clone()).await {
                            Ok(true) => {
                                // Worker accepted the task!
                                let rpc_duration = rpc_start.elapsed();
                                info!("Worker accepted task {} (RPC took {:?})", task_id, rpc_duration);

                                // Now atomically mark it as Processing if still Pending
                                match store.mark_as_processing_if_pending(&task_id).await {
                                    Ok(true) => {
                                        // Successfully marked as Processing
                                        info!("Task {} pushed and marked as Processing", task_id);
                                        metrics::counter!("push_task.success").increment(1);
                                        metrics::histogram!("push_task.duration")
                                            .record(start_time.elapsed());
                                    }
                                    Ok(false) => {
                                        // Task was already grabbed by another process (race condition)
                                        debug!(
                                            "Task {} was already taken by another process (pull model?)",
                                            task_id
                                        );
                                        metrics::counter!("push_task.race_condition").increment(1);
                                    }
                                    Err(e) => {
                                        error!("Failed to mark task {} as Processing: {:?}", task_id, e);
                                        metrics::counter!("push_task.db_error").increment(1);
                                        // Task will be retried on next iteration
                                        sleep(Duration::from_millis(100)).await;
                                    }
                                }
                            }
                            Ok(false) => {
                                // Worker rejected the task (queue full, etc.)
                                info!("Worker rejected task {} (queue full or other reason)", task_id);
                                metrics::counter!("push_task.rejected").increment(1);
                                // Task stays Pending, will be retried
                                // Brief sleep to avoid hammering a full worker
                                sleep(Duration::from_millis(10)).await;
                            }
                            Err(e) => {
                                // Connection or RPC error
                                warn!("Failed to push task {} to worker: {:?}", task_id, e);
                                metrics::counter!("push_task.connection_error").increment(1);
                                // Task stays Pending, will be retried
                                // Brief sleep before retrying
                                sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                    Ok(None) => {
                        // No pending tasks available
                        debug!("No pending tasks, sleeping briefly");
                        sleep(Duration::from_millis(10)).await;
                    }
                    Err(e) => {
                        // Database error while fetching
                        error!("Failed to fetch pending activation: {:?}", e);
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            } => {}
        }
    }

    info!("Push task loop shutting down");
    Ok(())
}
