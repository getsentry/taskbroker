use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use prost::Message;
use sentry_protos::{taskbroker::v1::TaskActivation, taskworker::v1::PushTaskRequest};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::store::inflight_activation::{InflightActivation, InflightActivationStore};
use crate::worker_client::WorkerClient;

/// Message sent to a push worker: (callback_url, inflight).
type PushMessage = (String, InflightActivation);

pub struct TaskPusher {
    /// Senders to push workers (round-robin). Each worker has its own gRPC connection.
    senders: Vec<mpsc::Sender<PushMessage>>,

    /// Index for round-robin send.
    next_sender_index: usize,

    /// Configuration.
    config: Arc<Config>,

    /// Inflight activation store.
    store: Arc<dyn InflightActivationStore>,

    /// Receivers (moved into worker tasks in start()).
    receivers: Vec<mpsc::Receiver<PushMessage>>,
}

impl TaskPusher {
    /// Create a new `TaskPusher` instance with a pool of worker channels (one connection per worker, created in start()).
    pub fn new(store: Arc<dyn InflightActivationStore>, config: Arc<Config>) -> Self {
        let n = config.push_worker_connections;
        let mut senders = Vec::with_capacity(n);
        let mut receivers = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel(config.push_channel_capacity);
            senders.push(tx);
            receivers.push(rx);
        }

        Self {
            senders,
            next_sender_index: 0,
            config,
            store,
            receivers,
        }
    }

    /// Start the worker pool and push task loop.
    pub async fn start(mut self) -> Result<()> {
        let n = self.senders.len();
        info!("Push task loop starting with {} worker connections...", n);

        let endpoint = self.config.worker_endpoint.clone();
        let receivers = std::mem::take(&mut self.receivers);

        for (i, mut rx) in receivers.into_iter().enumerate() {
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                let mut worker = match WorkerClient::new(endpoint).await {
                    Ok(w) => w,
                    Err(e) => {
                        error!("Push worker {} failed to connect - {:?}", i, e);
                        return;
                    }
                };
                while let Some((callback_url, inflight)) = rx.recv().await {
                    let id = inflight.id.clone();
                    if let Err(e) = push_task(&mut worker, callback_url, inflight).await {
                        error!("Pushing task {id} resulted in error - {:?}", e);
                    } else {
                        debug!("Task {id} was sent to load balancer!");
                    }
                }
            });
        }

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

                let callback_url = format!(
                    "{}:{}",
                    self.config.default_metrics_tags["host"], self.config.grpc_port
                );
                let idx = self.next_sender_index % self.senders.len();
                self.next_sender_index = self.next_sender_index.wrapping_add(1);

                if let Err(e) = self.senders[idx].send((callback_url, inflight)).await {
                    // Channel closed (workers shut down)
                    error!("Failed to send task {id} to push worker - {:?}", e);
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
    worker: &mut WorkerClient,
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
