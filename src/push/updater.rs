use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use elegant_departure::get_shutdown_guard;
use tokio::sync::{Mutex, Notify, mpsc};
use tonic::async_trait;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::store::traits::ActivationStore;

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

    /// Sends pushed activation IDs to the background updater task.
    tx: mpsc::Sender<String>,

    /// Receiver side for the updater task. `start` takes ownership while preserving the trait interface.
    rx: Mutex<Option<mpsc::Receiver<String>>>,

    /// Signals the background task to stop.
    stop: Notify,
}

impl LazyUpdater {
    pub fn new(config: Arc<Config>, store: Arc<dyn ActivationStore>) -> Self {
        let capacity = config.push.update.batch.length.max(1);
        let (tx, rx) = mpsc::channel(capacity);
        let stop = Notify::new();

        Self {
            config,
            store,
            tx,
            rx: Mutex::new(Some(rx)),
            stop,
        }
    }

    /// Flush buffered activations to the store. Empties the buffer on success, refills on failure.
    async fn flush(&self, buffer: &mut Vec<String>) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        let ids = std::mem::take(buffer);
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

                Ok(())
            }

            Err(e) => {
                // Flush failed, return IDs to buffer
                buffer.extend(ids);
                Err(e)
            }
        }
    }

    async fn flush_with_metrics(&self, buffer: &mut Vec<String>, reason: &'static str) {
        if buffer.is_empty() {
            return;
        }

        match self.flush(buffer).await {
            Ok(_) => metrics::counter!("push.updater.flush", "reason" => reason, "result" => "ok")
                .increment(1),

            Err(e) => {
                metrics::counter!("push.updater.flush", "reason" => reason, "result" => "error")
                    .increment(1);

                error!(
                    error = ?e,
                    "Failed to flush claimed → processing updates ({reason})"
                );
            }
        }
    }
}

#[async_trait]
impl Updater for LazyUpdater {
    async fn start(&self) -> Result<()> {
        // Hold guard until the updater has stopped to delay shutdown
        let guard = get_shutdown_guard();

        let mut rx = self
            .rx
            .lock()
            .await
            .take()
            .ok_or_else(|| anyhow!("lazy updater already started"))?;

        let batch_length = self.config.push.update.batch.length.max(1);
        let mut buffer = Vec::with_capacity(batch_length);

        // Flush every `interval` milliseconds
        let period = self
            .config
            .push
            .update
            .batch
            .interval
            .max(Duration::from_millis(1));

        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.stop.notified() => {
                    info!("Stopping lazy updater...");
                    break;
                }

                item = rx.recv() => {
                    let Some(id) = item else {
                        break;
                    };

                    buffer.push(id);

                    while buffer.len() < batch_length {
                        match rx.try_recv() {
                            Ok(id) => buffer.push(id),
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => break,
                        }
                    }

                    if buffer.len() >= batch_length {
                        self.flush_with_metrics(&mut buffer, "full").await;
                    }
                }

                _ = interval.tick() => {
                    self.flush_with_metrics(&mut buffer, "tick").await;
                }
            }
        }

        while let Ok(id) = rx.try_recv() {
            buffer.push(id);
        }

        // Perform one last flush before exiting
        self.flush_with_metrics(&mut buffer, "exit").await;

        drop(guard);
        Ok(())
    }

    async fn update(&self, id: &str) -> Result<()> {
        let start = Instant::now();

        self.tx
            .send(id.to_string())
            .await
            .with_context(|| "lazy updater channel closed")?;

        metrics::histogram!("push.updater.enqueue.duration").record(start.elapsed());
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
