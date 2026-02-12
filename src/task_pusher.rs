use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use prost::Message;
use sentry_protos::taskbroker::v1::TaskActivationStatus;
use sentry_protos::{taskbroker::v1::TaskActivation, taskworker::v1::PushTaskRequest};
use tokio::time::sleep;
use tonic::Status;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::store::inflight_activation::{
    InflightActivation, InflightActivationStatus, InflightActivationStore,
};
use crate::worker_client::WorkerClient;

pub struct TaskPusher {
    /// Load balancer client for pushing tasks.
    worker: WorkerClient,

    /// Configuration.
    config: Arc<Config>,

    /// Inflight activation store.
    store: Arc<dyn InflightActivationStore>,
}

impl TaskPusher {
    /// Create a new `TaskPusher` instance.
    pub async fn new(store: Arc<dyn InflightActivationStore>, config: Arc<Config>) -> Self {
        let worker = WorkerClient::new(config.worker_endpoint.clone())
            .await
            .unwrap();

        Self {
            store,
            config,
            worker,
        }
    }

    /// Start the worker update and push task loops.
    pub async fn start(mut self) -> Result<()> {
        info!("Push task loop starting...");

        let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();

        loop {
            tokio::select! {
                _ = guard.wait() => {
                    info!("Push task loop received shutdown signal");
                    break;
                }

                _ = async {
                    debug!("About to process next task...");
                    self.process_next_task().await;
                } => {}
            }
        }

        info!("Push task loop shutting down...");
        Ok(())
    }

    /// Grab the next pending task from the store and atomically mark it as processing.
    async fn process_next_task(&mut self) {
        let start = Instant::now();
        metrics::counter!("task_pusher.process_next_task.runs").increment(1);

        debug!("Getting the next task...");

        // Use atomic get-and-mark instead of peek + mark
        match self.store.get_pending_activation(None, None).await {
            Ok(Some(inflight)) => {
                let id = inflight.id.clone();
                debug!("Atomically fetched and marked task {id} as processing");

                if let Err(e) = self.handle_task_push(inflight).await {
                    // Task is already marked as processing, so `processing_deadline` will handle retry
                    error!("Pushing task {id} resulted in error - {:?}", e);
                } else {
                    debug!("Task {id} was sent to load balancer!");
                }
                metrics::histogram!("task_pusher.process_next_task.duration")
                    .record(start.elapsed());
            }

            Ok(None) => {
                debug!("No pending tasks, sleeping briefly");
                sleep(milliseconds(100)).await;
                metrics::histogram!("task_pusher.process_next_task.duration")
                    .record(start.elapsed());
            }

            Err(e) => {
                error!("Failed to fetch pending activation - {:?}", e);
                sleep(milliseconds(100)).await;
                metrics::histogram!("task_pusher.process_next_task.duration")
                    .record(start.elapsed());
            }
        }
    }

    /// Decode task activation and push it to a worker.
    async fn handle_task_push(&mut self, inflight: InflightActivation) -> Result<()> {
        let start = Instant::now();
        let task_id = inflight.id.clone();

        let activation = TaskActivation::decode(&inflight.activation as &[u8]).map_err(|e| {
            error!("Failed to decode activation {task_id} - {:?}", e);
            e
        })?;

        let result = self.push_to_worker(activation, &task_id).await;
        metrics::histogram!("task_pusher.handle_task_push.duration").record(start.elapsed());
        result
    }

    /// Build an RPC request and send it to the load balancer (fire-and-forget).
    /// The task has already been marked as processing before this call.
    async fn push_to_worker(&mut self, activation: TaskActivation, task_id: &str) -> Result<()> {
        let request = PushTaskRequest {
            task: Some(activation),
            broker: self
                .config
                .default_metrics_tags
                .get("host")
                .cloned()
                .unwrap_or("ANY".into()),
        };

        let result = self.worker.push_task(request).await;

        match result {
            Ok(response) => {
                debug!("Successfully sent task {task_id} to load balancer");

                match (response.id, response.status) {
                    (Some(id), Some(status)) => {
                        let status: InflightActivationStatus =
                            TaskActivationStatus::try_from(status)
                                .map_err(|e| anyhow!("Unable to deserialize status - {e:?}"))?
                                .into();

                        let update_result = self.store.set_status(&id, status).await;
                        if let Err(e) = update_result {
                            error!(
                                ?id,
                                ?status,
                                "Unable to update status of activation - {:?}",
                                e,
                            );

                            return Err(anyhow!("Unable to update status of {id:?} to {status:?}"));
                        }
                    }

                    (Some(_), None) | (None, Some(_)) => {
                        warn!("Either received task ID without status or status without task ID!")
                    }

                    _ => {}
                }

                Ok(())
            }

            Err(e) => {
                error!("Could not push task {task_id} to load balancer - {:?}", e);
                Err(e)
            }
        }
    }
}

#[inline]
fn milliseconds(i: u64) -> Duration {
    Duration::from_millis(i)
}
