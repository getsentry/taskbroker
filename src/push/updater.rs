use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use elegant_departure::get_shutdown_guard;
use tokio::sync::{Mutex, MutexGuard, Notify, RwLock};
use tonic::async_trait;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::store::traits::ActivationStore;
use crate::timed;

/// Represents an entity that can update tasks in some way. Meant to abstract away
/// the update logic, which varies between delivery modes, batching configurations, and so on.
#[async_trait]
pub trait Updater: Send + Sync {
    /// Start the updater. Useful for updaters that run a background task.
    async fn start(&self) -> Result<()> {
        Ok(())
    }

    /// Update activation in some way given its ID.
    async fn update(&self, id: &str) -> Result<()>;

    /// Stop the updater. Useful for updaters that run a background task.
    fn stop(&self) {}
}

/// Used by push threads to update sent activations from "claimed" to "processing" in batches.
pub struct LazyUpdater {
    /// The taskbroker configuration.
    config: Arc<Config>,

    /// The activation store.
    store: Arc<dyn ActivationStore>,

    /// Last time the buffer was flushed.
    last_flush: Arc<RwLock<DateTime<Utc>>>,

    /// Sent activations that need to be updated.
    buffer: Arc<Mutex<Vec<String>>>,

    /// Signals the background task to stop.
    stop: Notify,
}

impl LazyUpdater {
    pub fn new(config: Arc<Config>, store: Arc<dyn ActivationStore>) -> Self {
        let buffer = Arc::new(Mutex::new(vec![]));
        let last_flush = Arc::new(RwLock::new(Utc::now()));
        let stop = Notify::new();

        Self {
            config,
            store,
            buffer,
            last_flush,
            stop,
        }
    }

    async fn lock_buffer(&self, operation: &'static str) -> MutexGuard<'_, Vec<String>> {
        match self.buffer.try_lock() {
            Ok(buffer) => {
                metrics::counter!(
                    "push.updater.buffer.lock",
                    "operation" => operation,
                    "result" => "uncontended"
                )
                .increment(1);

                buffer
            }

            Err(_) => {
                metrics::counter!(
                    "push.updater.buffer.lock",
                    "operation" => operation,
                    "result" => "contended"
                )
                .increment(1);

                timed!(
                    self.buffer.lock(),
                    "push.updater.buffer.lock.wait",
                    "operation" => operation
                )
            }
        }
    }

    /// Flush buffered activations to the store. Empties the buffer on success, refills on failure.
    async fn flush(&self, buffer: &mut MutexGuard<'_, Vec<String>>) -> Result<()> {
        let ids: Vec<_> = buffer.drain(..).collect();
        let expected = ids.len() as u64;

        match self.store.mark_processing_batch(&ids).await {
            Ok(actual) => {
                if actual < expected {
                    // This may happen if tasks are reverted back to pending OR completed too quickly
                    warn!(
                        "Push thread update batch contained {expected} records, but only {actual} were updated"
                    );
                }

                metrics::histogram!("push.updater.flush.expected").record(expected as f64);
                metrics::histogram!("push.updater.flush.actual").record(actual as f64);

                // Indicate that this is the last time we performed a periodic flush
                let mut last_flush = self.last_flush.write().await;
                *last_flush = Utc::now();

                Ok(())
            }

            Err(e) => {
                // Flush failed, return IDs to buffer
                buffer.extend(ids);
                Err(e)
            }
        }
    }
}

#[async_trait]
impl Updater for LazyUpdater {
    async fn start(&self) -> Result<()> {
        // Hold guard until the updater has stopped to delay shutdown
        let guard = get_shutdown_guard();

        // Flush every `interval` milliseconds
        let period = self.config.push.update.batch.interval;
        let mut interval = tokio::time::interval(period);

        loop {
            tokio::select! {
                _ = self.stop.notified() => {
                    info!("Stopping lazy updater...");
                    break;
                }

                _ = interval.tick(), if self.config.push.update.batched => {
                    // Lock the ID buffer
                    let mut buffer = self.lock_buffer("tick").await;

                    // Make sure we aren't flushing too soon
                    let now = Utc::now().timestamp_millis();
                    let elapsed = now - self.last_flush.read().await.timestamp_millis();

                    if elapsed < (self.config.push.update.batch.interval.as_millis() as i64) {
                        // Too soon!
                        continue;
                    }

                    match self.flush(&mut buffer).await {
                        Ok(_) => metrics::counter!("push.updater.flush", "reason" => "tick", "result" => "ok").increment(1),

                        Err(e) => {
                            metrics::counter!("push.updater.flush", "reason" => "tick", "result" => "error").increment(1);

                            error!(
                                error = ?e,
                                "Failed to flush claimed → processing updates (tick)"
                            );
                        }
                    }
                }
            }
        }

        // Perform one last flush before exiting
        let mut buffer = self.lock_buffer("exit").await;

        match self.flush(&mut buffer).await {
            Ok(_) => metrics::counter!("push.updater.flush", "reason" => "exit", "result" => "ok")
                .increment(1),

            Err(e) => {
                metrics::counter!("push.updater.flush", "reason" => "exit", "result" => "error")
                    .increment(1);

                error!(
                    error = ?e,
                    "Failed to flush claimed → processing updates (exit)"
                );
            }
        }

        drop(guard);
        Ok(())
    }

    async fn update(&self, id: &str) -> Result<()> {
        // Lock the ID buffer
        let mut buffer = self.lock_buffer("update").await;

        if buffer.len() >= self.config.push.update.batch.length {
            // Flush first
            match self.flush(&mut buffer).await {
                Ok(_) => {
                    metrics::counter!("push.updater.flush", "reason" => "full", "result" => "ok")
                        .increment(1)
                }

                Err(e) => {
                    metrics::counter!("push.updater.flush", "reason" => "full", "result" => "error").increment(1);

                    error!(
                        error = ?e,
                        "Failed to flush claimed → processing updates (full buffer)"
                    );

                    return Err(e);
                }
            }
        }

        buffer.push(id.to_string());
        Ok(())
    }

    fn stop(&self) {
        self.stop.notify_one();
    }
}

/// Used by push threads to update sent activations from "claimed" to "processing" individually.
pub struct EagerUpdater {
    /// The activation store.
    store: Arc<dyn ActivationStore>,
}

impl EagerUpdater {
    pub fn new(store: Arc<dyn ActivationStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Updater for EagerUpdater {
    async fn update(&self, id: &str) -> Result<()> {
        self.store.mark_activation_processing(id).await
    }
}

#[cfg(test)]
pub fn test_eager_updater(store: Arc<dyn ActivationStore>) -> Arc<dyn Updater> {
    let eager = EagerUpdater::new(store);
    Arc::new(eager) as Arc<dyn Updater>
}
