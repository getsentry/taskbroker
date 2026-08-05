use std::cmp;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use chrono::Utc;
use elegant_departure::get_shutdown_guard;
use flume::Sender;
use tokio::time::{Duration, sleep};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::push::QueueError;
use crate::store::activation::Activation;
use crate::store::traits::ActivationStore;
use crate::store::types::{BucketRange, TopicPartition};
use crate::timed;

/// Abstraction for a single fetch thread.
pub struct FetchThread {
    /// The sending end of a channel that accepts task activations.
    pub(super) sender: Sender<(Activation, Instant)>,

    /// Activation store.
    pub(super) store: Arc<dyn ActivationStore>,

    /// Taskbroker configuration.
    pub(super) config: Arc<Config>,

    /// Bucket subrange this fetch thread is responsible for.
    pub(super) bucket: BucketRange,
}

impl FetchThread {
    pub async fn start(&mut self) -> Result<()> {
        let fetch_backoff = self.config.fetch.backoff;
        let guard = get_shutdown_guard().shutdown_on_drop();

        loop {
            tokio::select! {
                _ = guard.wait() => {
                    info!("Fetch loop received shutdown signal");
                    return Ok(());
                }

                keep_running = self.fetch_once(fetch_backoff) => {
                    if !keep_running {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn fetch_once(&mut self, fetch_backoff: Duration) -> bool {
        let start = Instant::now();

        debug!("Fetching next batch of pending activations...");
        metrics::counter!("fetch.loop.count").increment(1);

        let mut backoff = false;

        let result = self
            .store
            .claim_activations_for_push(Some(self.config.fetch.batch_length), Some(self.bucket))
            .await;
        metrics::histogram!("fetch.claim_activations_for_push.duration").record(start.elapsed());

        match result {
            Ok(activations) if activations.is_empty() => {
                metrics::counter!("fetch.empty").increment(1);
                debug!("No pending activations");

                // Wait for pending activations to appear
                backoff = true;
            }

            Ok(activations) => {
                metrics::counter!("fetch.claimed").increment(activations.len() as u64);
                metrics::histogram!("fetch.claim_batch_size").record(activations.len() as f64);

                debug!("Fetched {} activations", activations.len());

                // Use this instant to track claimed → pushed latency
                let start = Instant::now();

                for activation in activations {
                    let id = activation.id.clone();

                    if activation.processing_attempts < 1 {
                        let latency = cmp::max(0, activation.received_latency(Utc::now()));

                        // Age-based drain can claim activations from partitions
                        // this broker doesn't own; tag so those orphans are visible.
                        let owned_partition = self.store.owns_partition(&TopicPartition::new(
                            activation.topic.clone(),
                            activation.partition,
                        ));

                        metrics::histogram!(
                            "push.received_to_claimed.latency",
                            "application" => activation.application.clone(),
                            "namespace" => activation.namespace.clone(),
                            "taskname" => activation.taskname.clone(),
                            "owned_partition" => owned_partition.to_string(),
                        )
                        .record(latency as f64);
                    } else {
                        debug!(
                            task_id = %id,
                            namespace = activation.namespace,
                            taskname = activation.taskname,
                            "Activation already processed, skipping received → claimed latency recording"
                        );
                    }

                    match self.push_task(activation, start).await {
                        Ok(()) => metrics::counter!("fetch.submit", "result" => "ok").increment(1),

                        Err(QueueError::Timeout) => {
                            metrics::counter!("fetch.submit", "result" => "timeout").increment(1);

                            warn!(
                                task_id = %id,
                                "Submit to push pool timed out after {} milliseconds",
                                self.config.push.queue.timeout.as_millis()
                            );

                            // Wait for push queue to empty
                            backoff = true;
                        }

                        Err(QueueError::Closed) => {
                            metrics::counter!("fetch.submit", "result" => "closed").increment(1);

                            warn!(
                                task_id = %id,
                                "Submit to push pool failed due to closed channel",
                            );

                            // We cannot recover from a closed channel
                            return false;
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

        metrics::histogram!("fetch.loop.duration").record(start.elapsed());

        if backoff {
            sleep(fetch_backoff).await;
        }

        true
    }

    async fn push_task(&self, activation: Activation, time: Instant) -> Result<(), QueueError> {
        metrics::gauge!("push.queue.depth").set(self.sender.len() as f64);

        let duration = self.config.push.queue.timeout;
        let future = self.sender.send_async((activation, time));
        let timeout = tokio::time::timeout(duration, future);

        match timed!(timeout, "push.queue.wait_duration") {
            // The channel was full so the send timed out
            Err(_) => Err(QueueError::Timeout),

            // The channel may close early if the push pool encounters an error
            Ok(Err(_)) => Err(QueueError::Closed),

            // Pushed to channel successfully
            Ok(_) => Ok(()),
        }
    }
}
