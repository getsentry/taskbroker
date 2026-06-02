use std::cmp;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_backtrace::framed;
use chrono::Utc;
use elegant_departure::get_shutdown_guard;
use flume::Sender;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::push::QueueError;
use crate::store::activation::Activation;
use crate::store::traits::ActivationStore;
use crate::store::types::BucketRange;
use crate::timed;

/// This value should be a power of two. If it decreases, some ranges will no longer be queried.
/// That means the pending activation query will skip tasks within these ranges.
pub const MAX_FETCH_THREADS: usize = 256;

/// Inclusive bucket range for fetch thread `thread_index` when using `fetch_threads` concurrent fetch loops.
/// Requires `fetch_threads` to divide [`MAX_FETCH_THREADS`] (enforced via [`normalize_fetch_threads`]).
pub fn bucket_range_for_fetch_thread(thread_index: usize, fetch_threads: usize) -> BucketRange {
    let maximum = MAX_FETCH_THREADS;
    let buckets_per_range = maximum / fetch_threads;

    let low = (thread_index * buckets_per_range) as i16;
    let high = ((thread_index + 1) * buckets_per_range - 1) as i16;

    (low, high)
}

/// Wrapper around `config.fetch_threads` asynchronous tasks, each of which fetches batches of pending activations from the store, passes them to the push pool, and repeats.
pub struct FetchPool {
    /// The sending end of a channel that accepts task activations.
    sender: Sender<(Activation, Instant)>,

    /// Activation store.
    store: Arc<dyn ActivationStore>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl FetchPool {
    /// Initialize a new fetch pool.
    pub fn new(
        sender: Sender<(Activation, Instant)>,
        store: Arc<dyn ActivationStore>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            sender,
            store,
            config,
        }
    }

    /// Spawns one task per effective fetch thread ([`normalize_fetch_threads`]), each claiming pending work only in its bucket subrange.
    #[framed]
    pub async fn start(&self) -> Result<()> {
        let fetch_wait_ms = self.config.fetch_wait_ms;
        let fetch_threads = self.config.fetch_threads;

        let mut fetch_pool = crate::tokio::spawn_pool(fetch_threads, |thread_index| {
            let store = self.store.clone();
            let sender = self.sender.clone();
            let config = self.config.clone();

            let limit = Some(config.fetch_batch_size);
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

                                        match push_task(activation, start, sender.clone(), config.clone()).await {
                                            Ok(()) => metrics::counter!("fetch.submit", "result" => "ok").increment(1),

                                            Err(QueueError::Timeout) => {
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

                                            Err(QueueError::Closed) => {
                                                metrics::counter!("fetch.submit", "result" => "closed")
                                                    .increment(1);

                                                warn!(
                                                    task_id = %id,
                                                    "Submit to push pool failed due to closed channel",
                                                );

                                                // We cannot recover from a closed channel
                                                return;
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

async fn push_task(
    activation: Activation,
    time: Instant,
    sender: Sender<(Activation, Instant)>,
    config: Arc<Config>,
) -> Result<(), QueueError> {
    metrics::gauge!("push.queue.depth").set(sender.len() as f64);

    let duration = Duration::from_millis(config.push_queue_timeout_ms);
    let timeout = tokio::time::timeout(duration, sender.send_async((activation, time)));

    match timed!(timeout, "push.queue.wait_duration") {
        // The channel was full so the send timed out
        Err(_) => Err(QueueError::Timeout),

        // The channel may close early if the push pool encounters an error
        Ok(Err(_)) => Err(QueueError::Closed),

        // Pushed to channel successfully
        Ok(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests;
