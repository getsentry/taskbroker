use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use prost::Message;
use sentry_protos::{taskbroker::v1::TaskActivation, taskworker::v1::PushTaskRequest};
use tokio::time::sleep;
use tracing::{debug, error, info};

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
                    self.get_next_task().await;
                } => {}
            }
        }

        info!("Push task loop shutting down...");
        Ok(())
    }

    /// Grab the next pending task from the store and atomically mark it as processing.
    async fn get_next_task(&mut self) {
        let start = Instant::now();
        metrics::counter!("task_pusher.process_next_task.runs").increment(1);

        debug!("Getting the next task...");

        // Use atomic get-and-mark instead of peek + mark
        match self.store.get_pending_activation(None, None).await {
            Ok(Some(inflight)) => {
                let id = inflight.id.clone();
                debug!("Atomically fetched and marked task {id} as processing");

                let worker = self.worker.clone();
                let callback_url = format!(
                    "{}:{}",
                    self.config.default_metrics_tags["host"], self.config.grpc_port
                );

                if let Err(e) = push_task(worker, callback_url, inflight).await {
                    // Task is already marked as processing, so `processing_deadline` will handle retry
                    error!("Pushing task {id} resulted in error - {:?}", e);

                    let _ = self
                        .store
                        .set_status(&id, InflightActivationStatus::Pending)
                        .await;
                } else {
                    debug!("Task {id} was sent to load balancer!");
                }

                metrics::histogram!("task_pusher.get_next_task.duration").record(start.elapsed());
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
}

/// Decode task activation and push it to a worker.
async fn push_task(
    mut worker: WorkerClient,
    callback_url: String,
    inflight: InflightActivation,
) -> Result<()> {
    let start = Instant::now();
    let task_id = inflight.id.clone();

    let activation = TaskActivation::decode(&inflight.activation as &[u8]).map_err(|e| {
        error!("Failed to decode activation {task_id} - {:?}", e);
        e
    })?;

    let request = PushTaskRequest {
        task: Some(activation),
        callback_url,
    };

    let result = match worker.submit(request).await {
        Ok(()) => {
            debug!("Successfully sent task {task_id} to load balancer");
            Ok(())
        }

        Err(e) => {
            error!("Could not push task {task_id} to load balancer - {:?}", e);
            Err(e)
        }
    };

    metrics::histogram!("task_pusher.push_task.duration").record(start.elapsed());
    result
}

#[inline]
fn milliseconds(i: u64) -> Duration {
    Duration::from_millis(i)
}
