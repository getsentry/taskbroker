use std::cmp;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_backtrace::framed;
use chrono::Utc;
use elegant_departure::get_shutdown_guard;
use tokio::time::sleep;
use tonic::async_trait;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::push::{PushError, PushPool};
use crate::store::activation::InflightActivation;
use crate::store::traits::InflightActivationStore;
use crate::store::types::BucketRange;

/// This value should be a power of two. If it decreases, some ranges will no longer be queried.
/// That means the pending activation query will skip tasks within these ranges.
pub const MAX_FETCH_THREADS: i16 = 256;

/// Returns the largest positive divisor of [`MAX_FETCH_THREADS`] that is also a power of two.
pub fn normalize_fetch_threads(n: usize) -> usize {
    let n = n.max(1);
    let mut v = MAX_FETCH_THREADS;

    while v > 1 {
        if (v as usize) <= n {
            return v as usize;
        }

        v /= 2;
    }

    1
}

/// Inclusive bucket range for fetch thread `thread_index` when using `fetch_threads` concurrent fetch loops.
/// Requires `fetch_threads` to divide [`MAX_FETCH_THREADS`] (enforced via [`normalize_fetch_threads`]).
pub fn bucket_range_for_fetch_thread(thread_index: usize, fetch_threads: usize) -> BucketRange {
    let maximum = MAX_FETCH_THREADS as usize;
    let buckets_per_range = maximum / fetch_threads;

    let low = (thread_index * buckets_per_range) as i16;
    let high = ((thread_index + 1) * buckets_per_range - 1) as i16;

    (low, high)
}

/// Thin interface for the push pool. It mostly serves to enable proper unit testing, but it also decouples fetch logic from push logic even further.
#[async_trait]
pub trait TaskPusher {
    /// Submit a single task to the push pool.
    async fn submit_task(
        &self,
        activation: InflightActivation,
        time: Instant,
    ) -> Result<(), PushError>;
}

#[async_trait]
impl TaskPusher for PushPool {
    #[framed]
    async fn submit_task(
        &self,
        activation: InflightActivation,
        time: Instant,
    ) -> Result<(), PushError> {
        self.submit(activation, time).await
    }
}

/// Wrapper around `config.fetch_threads` asynchronous tasks, each of which fetches batches of pending activations from the store, passes them to the push pool, and repeats.
pub struct FetchPool<T: TaskPusher> {
    /// Inflight activation store.
    store: Arc<dyn InflightActivationStore>,

    /// Pool of push threads that push activations to the worker service.
    pusher: Arc<T>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl<T: TaskPusher + Send + Sync + 'static> FetchPool<T> {
    /// Initialize a new fetch pool.
    pub fn new(
        store: Arc<dyn InflightActivationStore>,
        config: Arc<Config>,
        pusher: Arc<T>,
    ) -> Self {
        Self {
            store,
            config,
            pusher,
        }
    }

    /// Spawns one task per effective fetch thread ([`normalize_fetch_threads`]), each claiming pending work only in its bucket subrange.
    #[framed]
    pub async fn start(&self) -> Result<()> {
        let fetch_wait_ms = self.config.fetch_wait_ms;
        let fetch_threads = normalize_fetch_threads(self.config.fetch_threads);

        let mut fetch_pool = crate::tokio::spawn_pool(fetch_threads, |thread_index| {
            let store = self.store.clone();
            let pusher = self.pusher.clone();
            let config = self.config.clone();

            let limit = Some(config.fetch_batch_size.max(1));
            let bucket = Some(bucket_range_for_fetch_thread(thread_index, fetch_threads));

            let guard = get_shutdown_guard().shutdown_on_drop();

            async_backtrace::frame!(async move {
                loop {
                    tokio::select! {
                        _ = guard.wait() => {
                            info!("Fetch loop received shutdown signal");
                            return;
                        }

                        _ = async {
                            let start = Instant::now();

                            debug!("Fetching next batch of pending activations...");
                            metrics::counter!("fetch.loop.count").increment(1);

                            let mut backoff = false;

                            let result = store.claim_activations_for_push(limit, bucket).await;
                            metrics::histogram!("fetch.claim_activations_for_push.duration").record(start.elapsed());

                            match result {
                                Ok(activations) if activations.is_empty() => {
                                    metrics::counter!("fetch.empty").increment(1);
                                    debug!("No pending activations");

                                    // Wait for pending activations to appear
                                    backoff = true;
                                }

                                Ok(activations) => {
                                    metrics::counter!("fetch.claimed")
                                        .increment(activations.len() as u64);
                                    metrics::histogram!("fetch.claim_batch_size")
                                        .record(activations.len() as f64);

                                    debug!("Fetched {} activations", activations.len());

                                    // Use this instant to track claimed → pushed latency
                                    let start = Instant::now();

                                    for activation in activations {
                                        let id = activation.id.clone();

                                        if activation.processing_attempts < 1 {
                                            let latency = cmp::max(0, activation.received_latency(Utc::now()));

                                            metrics::histogram!(
                                                "push.received_to_claimed.latency",
                                                "namespace" => activation.namespace.clone(),
                                                "taskname" => activation.taskname.clone(),
                                            )
                                            .record(latency as f64);
                                        } else {
                                            debug!(task_id = %id, namespace = activation.namespace, taskname = activation.taskname, "Activation already processed, skipping received → claimed latency recording");
                                        }

                                        match pusher.submit_task(activation, start).await {
                                            Ok(()) => metrics::counter!("fetch.submit", "result" => "ok").increment(1),

                                            Err(PushError::Timeout) => {
                                                metrics::counter!("fetch.submit", "result" => "timeout")
                                                    .increment(1);

                                                warn!(
                                                    task_id = %id,
                                                    "Submit to push pool timed out after {} milliseconds",
                                                    config.push_queue_timeout_ms
                                                );

                                                // Wait for push queue to empty
                                                backoff = true;
                                            }

                                            Err(PushError::Channel(e)) => {
                                                metrics::counter!("fetch.submit", "result" => "channel_error")
                                                    .increment(1);

                                                warn!(
                                                    task_id = %id,
                                                    error = ?e,
                                                    "Submit to push pool failed due to channel error",
                                                );

                                                // Wait before trying again
                                                backoff = true;
                                            }
                                        }
                                    }
                                }

                                Err(e) => {
                                    metrics::counter!("fetch.store_error").increment(1);
                                    warn!(
                                        error = ?e,
                                        "Store failed while fetching tasks"
                                    );

                                    // Store may be down, wait before trying again
                                    backoff = true;
                                }
                            };

                            metrics::histogram!("fetch.loop.duration")
                                .record(start.elapsed());

                            if backoff {
                                sleep(Duration::from_millis(fetch_wait_ms)).await;
                            }
                        } => {}
                    }
                }
            })
        });

        while let Some(res) = fetch_pool.join_next().await {
            if let Err(e) = res {
                return Err(e.into());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
