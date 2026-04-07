use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use tokio::time::sleep;
use tonic::async_trait;
use tracing::{debug, info, warn};

use crate::backoff::Backoff;
use crate::config::Config;
use crate::push::{PushError, PushPool};
use crate::store::inflight_activation::{BucketRange, InflightActivation, InflightActivationStore};

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
    /// Push a single task to the worker service.
    async fn push_task(&self, activation: InflightActivation) -> Result<(), PushError>;
}

#[async_trait]
impl TaskPusher for PushPool {
    async fn push_task(&self, activation: InflightActivation) -> Result<(), PushError> {
        self.submit(activation).await
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

    /// Shared backoff; fetch sleeps [`Backoff::get`] ms at the start of each iteration when non-zero.
    backoff: Arc<Backoff>,
}

impl<T: TaskPusher + Send + Sync + 'static> FetchPool<T> {
    /// Initialize a new fetch pool.
    pub fn new(
        store: Arc<dyn InflightActivationStore>,
        config: Arc<Config>,
        pusher: Arc<T>,
        backoff: Arc<Backoff>,
    ) -> Self {
        Self {
            store,
            config,
            pusher,
            backoff,
        }
    }

    /// Spawns one task per effective fetch thread ([`normalize_fetch_threads`]), each claiming pending work only in its bucket subrange.
    pub async fn start(&self) -> Result<()> {
        let fetch_threads = normalize_fetch_threads(self.config.fetch_threads);

        let mut fetch_pool = crate::tokio::spawn_pool(fetch_threads, |thread_index| {
            let store = self.store.clone();
            let pusher = self.pusher.clone();
            let config = self.config.clone();
            let backoff = self.backoff.clone();

            let limit = Some(config.fetch_batch_size.max(1));
            let bucket = Some(bucket_range_for_fetch_thread(thread_index, fetch_threads));

            let guard = get_shutdown_guard().shutdown_on_drop();

            async move {
                loop {
                    tokio::select! {
                        _ = guard.wait() => {
                            info!("Fetch loop received shutdown signal");
                            return;
                        }

                        _ = async {
                            debug!("Fetching next batch of pending activations...");
                            metrics::counter!("fetch.loop.count").increment(1);

                            let start = Instant::now();

                            let application = config.application.as_deref();
                            let namespaces = config.namespaces.as_deref();

                            match store
                                .get_pending_activations(application, namespaces, limit, bucket)
                                .await
                            {
                                Ok(activations) if activations.is_empty() => {
                                    debug!("No pending activations");

                                    // Wait for pending activations to appear
                                    sleep(Duration::from_millis(config.fetch_wait_ms)).await;
                                }

                                Ok(activations) => {
                                    debug!("Fetched {} activations", activations.len());

                                    for activation in activations {
                                        let w = backoff.get();

                                        if w > 0 {
                                            sleep(Duration::from_millis(w as u64)).await;
                                        }

                                        let id = activation.id.clone();

                                        if let Err(e) = pusher.push_task(activation).await {
                                            match e {
                                                PushError::Timeout => warn!(
                                                    task_id = %id,
                                                    "Submit to push pool timed out after {} milliseconds",
                                                    config.push_queue_timeout_ms
                                                ),

                                                PushError::Channel(e) => warn!(
                                                    task_id = %id,
                                                    error = ?e,
                                                    "Submit to push pool failed due to channel error",
                                                )
                                            }

                                            backoff.increase();
                                        }
                                    }
                                }

                                Err(e) => {
                                    warn!(
                                        error = ?e,
                                        "Store failed while fetching tasks"
                                    );

                                    // Store may be down, wait before trying again
                                    sleep(Duration::from_millis(config.fetch_wait_ms)).await;
                                }
                            };

                            metrics::histogram!("fetch.loop.duration")
                                .record(start.elapsed());
                        } => {}
                    }
                }
            }
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
