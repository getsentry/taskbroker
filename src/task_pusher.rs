use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use prost::Message;
use sentry_protos::{taskbroker::v1::TaskActivation, taskworker::v1::PushTaskRequest};
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::store::inflight_activation::{InflightActivation, InflightActivationStore};
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
            }

            Ok(None) => {
                debug!("No pending tasks, sleeping briefly");
                sleep(milliseconds(100)).await;
            }

            Err(e) => {
                error!("Failed to fetch pending activation - {:?}", e);
                sleep(milliseconds(100)).await;
            }
        }
    }

    /// Decode task activation and push it to a worker.
    async fn handle_task_push(&mut self, inflight: InflightActivation) -> Result<()> {
        let task_id = inflight.id.clone();

        let activation = TaskActivation::decode(&inflight.activation as &[u8]).map_err(|e| {
            error!("Failed to decode activation {task_id} - {:?}", e);
            e
        })?;

        self.push_to_worker(activation, &task_id).await
    }

    /// Build an RPC request and send it to the load balancer (fire-and-forget).
    /// The task has already been marked as processing before this call.
    async fn push_to_worker(&mut self, activation: TaskActivation, task_id: &str) -> Result<()> {
        let request = PushTaskRequest {
            task: Some(activation),
            callback_url: format!(
                "{}:{}",
                self.config.default_metrics_tags["host"], self.config.grpc_port
            ),
        };

        let result = self.worker.submit(request).await;

        match result {
            Ok(()) => {
                debug!("Successfully sent task {task_id} to load balancer");
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
