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

/// Number of buckets (0..BUCKET_COUNT). Used to partition work across push threads.
const BUCKET_COUNT: i16 = 256;

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

    /// This pusher only fetches activations with bucket in [min, max] (inclusive).
    bucket_range: (i16, i16),
}

impl TaskPusher {
    /// Create a new `TaskPusher` instance with a pool of worker channels (one connection per worker, created in start()).
    /// This pusher only fetches pending activations with bucket in [bucket_range.0, bucket_range.1] (inclusive).
    pub fn new(
        store: Arc<dyn InflightActivationStore>,
        config: Arc<Config>,
        bucket_range: (i16, i16),
    ) -> Self {
        let n = config.push_threads;
        let mut senders = Vec::with_capacity(n);
        let mut receivers = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel(config.push_queue_size);
            senders.push(tx);
            receivers.push(rx);
        }

        Self {
            senders,
            next_sender_index: 0,
            config,
            store,
            receivers,
            bucket_range,
        }
    }

    /// Bucket range for push thread index `thread_index` out of `push_threads` (0..push_threads).
    /// Each thread gets a contiguous range of size 256 / push_threads (e.g. 4 threads -> 0-63, 64-127, 128-191, 192-255).
    pub fn bucket_range_for_thread(thread_index: usize, push_threads: usize) -> (i16, i16) {
        if push_threads == 0 {
            return (0, BUCKET_COUNT - 1);
        }
        let buckets_per_thread = (BUCKET_COUNT as usize) / push_threads;
        let min = (thread_index * buckets_per_thread) as i16;
        let max = ((thread_index + 1) * buckets_per_thread - 1) as i16;
        (min, max)
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

    /// Grab the next batch of pending tasks from the store and atomically mark them as processing.
    async fn get_next_task(&mut self) {
        let start = Instant::now();
        metrics::counter!("task_pusher.process_next_task.runs").increment(1);

        let batch_size = self.config.push_batch_size.max(1) as i32;
        debug!("Getting the next batch of up to {} tasks...", batch_size);

        match self
            .store
            .get_pending_activations_from_namespaces(
                None,
                None,
                Some(batch_size),
                self.bucket_range,
            )
            .await
        {
            Ok(activations) if !activations.is_empty() => {
                let records_fetched = activations.len();
                debug!(
                    "Atomically fetched and marked {} tasks as processing",
                    records_fetched
                );

                metrics::histogram!("task_pusher.batch.records_fetched")
                    .record(records_fetched as f64);

                metrics::histogram!("task_pusher.batch.records_requested")
                    .record(batch_size as f64);

                let callback_url = format!(
                    "{}:{}",
                    self.config.default_metrics_tags["host"], self.config.grpc_port
                );

                for inflight in activations {
                    let id = inflight.id.clone();
                    let idx = self.next_sender_index % self.senders.len();
                    self.next_sender_index = self.next_sender_index.wrapping_add(1);

                    if let Err(e) = self.senders[idx]
                        .send((callback_url.clone(), inflight))
                        .await
                    {
                        // Channel closed (workers shut down)
                        error!("Failed to send task {id} to push worker - {:?}", e);
                    }
                }

                metrics::histogram!("task_pusher.get_next_task.duration").record(start.elapsed());
            }

            Ok(_) => {
                debug!("No pending tasks, sleeping briefly");
                sleep(milliseconds(100)).await;
                metrics::histogram!("task_pusher.process_next_task.duration")
                    .record(start.elapsed());
            }

            Err(e) => {
                error!("Failed to fetch pending activations - {:?}", e);
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
