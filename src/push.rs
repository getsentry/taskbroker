use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use prost::Message;
use sentry_protos::{taskbroker::v1::TaskActivation, taskworker::v1::PushTaskRequest};
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::pool::WorkerPool;
use crate::store::inflight_activation::{InflightActivation, InflightActivationStore};

pub struct TaskPusher {
    /// Pool of workers through which we will push tasks.
    pool: Arc<WorkerPool>,

    /// Broker configuration.
    config: Arc<Config>,

    /// Inflight activation store.
    store: Arc<InflightActivationStore>,
}

impl TaskPusher {
    /// Create a new `TaskPusher` instance.
    pub fn new(
        store: Arc<InflightActivationStore>,
        config: Arc<Config>,
        pool: Arc<WorkerPool>,
    ) -> Self {
        Self {
            store,
            config,
            pool,
        }
    }

    /// Start the worker update and push task loops.
    pub async fn start(self) -> Result<()> {
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
}

impl TaskPusher {
    /// Grab the next pending task from the store.
    async fn process_next_task(&self) {
        debug!("Getting the next task...");

        match self.store.peek_pending_activation().await {
            Ok(Some(inflight)) => {
                let id = inflight.id.clone();
                debug!("Found task {id} with status {:?}", inflight.status);

                if let Err(e) = self.handle_task_push(inflight).await {
                    debug!("Pushing task {id} resulted in error - {:?}", e);
                } else {
                    debug!("Task {id} was pushed!");
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
    async fn handle_task_push(&self, inflight: InflightActivation) -> Result<()> {
        let task_id = inflight.id.clone();

        let activation = TaskActivation::decode(&inflight.activation as &[u8]).map_err(|e| {
            error!("Failed to decode activation {task_id}: {:?}", e);
            e
        })?;

        self.push_to_worker(activation, &task_id).await
    }

    /// Build an RPC request and send it to the worker pool to be pushed.
    async fn push_to_worker(&self, activation: TaskActivation, task_id: &str) -> Result<()> {
        let request = PushTaskRequest {
            task: Some(activation),
            callback_url: format!(
                "{}:{}",
                self.config.default_metrics_tags["host"], self.config.grpc_port
            ),
        };

        let result = self.pool.push(request).await;

        match result {
            Ok(()) => {
                debug!("Pushed task {task_id}");
                self.mark_task_as_processing(task_id).await;
                Ok(())
            }

            Err(e) => {
                debug!("Could not push task {task_id} - {:?}", e);
                Err(e)
            }
        }
    }

    /// Mark task with id `task_id` as processing if it's still pending.
    async fn mark_task_as_processing(&self, task_id: &str) {
        match self.store.mark_as_processing_if_pending(task_id).await {
            Ok(true) => {
                debug!("Task {} pushed and marked as processing", task_id);
            }

            Ok(false) => {
                error!("Task {task_id} was already taken by another process (race condition)");
            }

            Err(e) => {
                error!("Failed to mark task {task_id} as processing - {:?}", e);
                sleep(milliseconds(100)).await;
            }
        }
    }
}

#[inline]
fn milliseconds(i: u64) -> Duration {
    Duration::from_millis(i)
}
