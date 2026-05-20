use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use elegant_departure::get_shutdown_guard;
use tokio::sync::{Mutex, MutexGuard, Notify};
use tonic::async_trait;
use tracing::{info, warn};

use crate::config::Config;
use crate::store::traits::InflightActivationStore;

/// Represents an entity that can update tasks in some way. Meant to abstract away
/// the update logic, which varies between delivery modes, batching configurations, and so on.
#[async_trait]
pub trait Updater: Send + Sync {
    /// Start the updater. Useful for updaters that run a background task.
    async fn start(&self) -> Result<()> {
        Ok(())
    }

    /// Update activation in some way given its ID.
    async fn update(&self, id: String) -> Result<()>;

    /// Stop the updater. Useful for updaters that run a background task.
    fn stop(&self) {}
}

/// Used by push threads to update sent activations from "claimed" to "processing" in batches.
pub struct LazyUpdater {
    /// The taskbroker configuration.
    config: Arc<Config>,

    /// The activation store.
    store: Arc<dyn InflightActivationStore>,

    /// Last time the buffer was flushed.
    last_flush: DateTime<Utc>,

    /// Sent activations that need to be updated.
    buffer: Arc<Mutex<Vec<String>>>,

    /// Signals the background task to stop.
    stop: Notify,
}

impl LazyUpdater {
    pub fn new(config: Arc<Config>, store: Arc<dyn InflightActivationStore>) -> Self {
        let buffer = Arc::new(Mutex::new(vec![]));
        let last_flush = Utc::now();
        let stop = Notify::new();

        Self {
            config,
            store,
            buffer,
            last_flush,
            stop,
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
        let period = Duration::from_millis(self.config.push_update_interval_ms as u64);
        let mut interval = tokio::time::interval(period);

        loop {
            tokio::select! {
                _ = self.stop.notified() => {
                    info!("Stopping lazy updater...");
                    break;
                }

                _ = interval.tick(), if self.config.batch_push_updates => {
                    // Lock the ID buffer
                    let mut buffer = self.buffer.lock().await;

                    // Make sure we aren't flushing too soon
                    let now = Utc::now().timestamp_millis();
                    let elapsed = self.last_flush.timestamp_millis()  - now;

                    if elapsed < (self.config.push_update_interval_ms as i64) {
                        // Too soon!
                        continue;
                    }

                    // We can propagate the error upwards here if desired
                    if let Err(_) = self.flush(&mut buffer).await {
                        todo!()
                    }
                }
            }
        }

        drop(guard);
        Ok(())
    }

    async fn update(&self, id: String) -> Result<()> {
        // Lock the ID buffer
        let mut buffer = self.buffer.lock().await;

        if buffer.len() >= self.config.push_update_batch_size {
            // Flush first
            if let Err(_) = self.flush(&mut buffer).await {
                todo!()
            }
        }

        buffer.push(id);
        Ok(())
    }

    fn stop(&self) {
        self.stop.notify_one();
    }
}

/// Used by push threads to update sent activations from "claimed" to "processing" individually.
pub struct EagerUpdater {
    /// The activation store.
    store: Arc<dyn InflightActivationStore>,
}

impl EagerUpdater {
    pub fn new(store: Arc<dyn InflightActivationStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Updater for EagerUpdater {
    async fn update(&self, id: String) -> Result<()> {
        self.store.mark_processing(&id).await
    }
}

#[cfg(test)]
pub fn test_eager_updater(store: Arc<dyn InflightActivationStore>) -> Arc<dyn Updater> {
    let eager = EagerUpdater::new(store);
    Arc::new(eager) as Arc<dyn Updater>
}
