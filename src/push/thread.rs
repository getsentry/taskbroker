use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{DateTime, Utc};
use elegant_departure::get_shutdown_guard;
use flume::Receiver;
use tokio::sync::{Mutex, MutexGuard};
use tracing::{info, warn};

use crate::config::Config;
use crate::push::WorkerMap;
use crate::store::activation::InflightActivation;
use crate::store::traits::InflightActivationStore;

/// Alias for documentation.
type Application = String;

/// Alias for ergonomics.
type Submission = (InflightActivation, Instant);

pub struct PushThread {
    /// The taskbroker configuration.
    config: Arc<Config>,

    /// The activation store.
    store: Arc<dyn InflightActivationStore>,

    /// Maps every application to its worker service.
    workers: WorkerMap,

    /// Last time the buffer was flushed.
    last_flush: DateTime<Utc>,

    /// Sent activations that need to be updated.
    buffer: Arc<Mutex<Vec<String>>>,

    /// Queue of claimed activations to be pushed.
    queue: Receiver<Submission>,
}

impl PushThread {
    pub fn new(
        config: Arc<Config>,
        store: Arc<dyn InflightActivationStore>,
        workers: WorkerMap,
        queue: Receiver<Submission>,
    ) -> Self {
        let buffer = Arc::new(Mutex::new(vec![]));
        let last_flush = Utc::now();

        Self {
            config,
            store,
            workers,
            last_flush,
            buffer,
            queue,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        // Exit when shutdown initiated
        let guard = get_shutdown_guard().shutdown_on_drop();

        // Flush every `interval` milliseconds
        let period = Duration::from_millis(self.config.push_update_interval_ms as u64);
        let mut interval = tokio::time::interval(period);

        loop {
            tokio::select! {
                _ = guard.wait() => {
                    info!("Push thread received shutdown signal!");
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

                message = self.queue.recv_async() => {
                    let (activation, time) = match message {
                        // Received activation from fetch thread
                        Ok(a) => a,

                        // Channel closed
                        Err(_) => break,
                    };

                    metrics::histogram!("push.queue.latency").record(time.elapsed());

                    // Push the task and mark it as processing
                    self.push_task(activation).await;
                }
            }
        }

        // Drain channel before exiting
        let activations: Vec<_> = self.queue.drain().collect();

        for (activation, time) in activations {
            metrics::histogram!("push.queue.latency").record(time.elapsed());

            // Push the task and mark it as processing
            self.push_task(activation).await;
        }

        Ok(())
    }

    async fn push_task(&mut self, activation: InflightActivation) -> Result<()> {
        // Store the ID for later since `push_task` claims ownership over `activation`
        let id = activation.id.clone();

        // First, determine the correct worker service
        let Some(worker) = self.workers.get_mut(&activation.application) else {
            // Missing application to worker mapping
            return Ok(());
        };

        // Then, push the task to that service
        worker.push_task(activation).await?;

        // Finally, mark the activation as processing
        self.update(id).await
    }

    /// Update one activation from claimed to processing.
    async fn update(&self, id: String) -> Result<()> {
        if self.config.batch_push_updates {
            // Lock the ID buffer
            let mut buffer = self.buffer.lock().await;

            if buffer.len() >= self.config.push_update_batch_size {
                // Flush first
                self.flush(&mut buffer).await?;
            }

            buffer.push(id);
            Ok(())
        } else {
            // We aren't batching claimed → processing updates
            self.store.mark_processing(&id).await
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
