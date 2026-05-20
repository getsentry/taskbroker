use std::{sync::Arc, time::Duration};

use anyhow::Result;
use chrono::{DateTime, Utc};
use elegant_departure::get_shutdown_guard;
use tokio::sync::{Mutex, MutexGuard};
use tonic::async_trait;
use tracing::{info, warn};

use crate::{config::Config, store::traits::InflightActivationStore};

#[async_trait]
pub trait Updater: Send + Sync {
    async fn start(&self) -> Result<()>;

    async fn update(&self, id: String) -> Result<()>;
}

pub struct LazyUpdater {
    /// The taskbroker configuration.
    config: Arc<Config>,

    /// The activation store.
    store: Arc<dyn InflightActivationStore>,

    /// Last time the buffer was flushed.
    last_flush: DateTime<Utc>,

    /// Sent activations that need to be updated.
    buffer: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Updater for LazyUpdater {
    async fn start(&self) -> Result<()> {
        let guard = get_shutdown_guard();

        // Flush every `interval` milliseconds
        let period = Duration::from_millis(self.config.push_update_interval_ms as u64);
        let mut interval = tokio::time::interval(period);

        loop {
            tokio::select! {
                _ = guard.wait() => {
                    info!("Shutting down batched updater...");
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
                    self.flush(&mut buffer).await;
                }
            }
        }

        Ok(())
    }

    async fn update(&self, id: String) -> Result<()> {
        // Lock the ID buffer
        let mut buffer = self.buffer.lock().await;

        if buffer.len() >= self.config.push_update_batch_size {
            // Flush first
            self.flush(&mut buffer).await?;
        }

        buffer.push(id);
        Ok(())
    }
}

impl LazyUpdater {
    pub fn new(config: Arc<Config>, store: Arc<dyn InflightActivationStore>) -> Self {
        let buffer = Arc::new(Mutex::new(vec![]));
        let last_flush = Utc::now();

        Self {
            config,
            store,
            buffer,
            last_flush,
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

pub struct EagerUpdater {
    /// The activation store.
    store: Arc<dyn InflightActivationStore>,
}

#[async_trait]
impl Updater for EagerUpdater {
    async fn start(&self) -> Result<()> {
        // There is nothing to do in the background, so we can return immediately
        todo!()
    }

    async fn update(&self, id: String) -> Result<()> {
        self.store.mark_processing(&id).await
    }
}
