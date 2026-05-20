use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use flume::Receiver;

use crate::config::Config;
use crate::push::updater::Updater;
use crate::store::activation::InflightActivation;
use crate::store::traits::InflightActivationStore;
use crate::worker::WorkerMap;

/// Alias for ergonomics.
pub type Submission = (InflightActivation, Instant);

/// Abstraction for a single push thread.
pub struct PushThread {
    /// The taskbroker configuration.
    pub(super) config: Arc<Config>,

    /// The activation store.
    pub(super) store: Arc<dyn InflightActivationStore>,

    /// Maps every application to its worker service.
    pub(super) workers: WorkerMap,

    /// Channel containing claimed activations to be pushed.
    pub(super) receiver: Receiver<Submission>,

    /// Entity that marks tasks as processing.
    pub(super) updater: Arc<dyn Updater>,
}

impl PushThread {
    pub async fn start(&mut self) -> Result<()> {
        let guard = get_shutdown_guard();

        loop {
            // We cannot exit before every fetch thread has exited, so don't exit on `guard.wait()` here
            tokio::select! {
                message = self.receiver.recv_async() => {
                    let (activation, time) = match message {
                        // Received activation from fetch thread
                        Ok(a) => a,

                        // We only exit when the push queue is closed, which happens when the fetch pool has shut down
                        Err(_) => break,
                    };

                    metrics::histogram!("push.queue.latency").record(time.elapsed());

                    // Push the task and mark it as processing
                    self.push_task(activation).await;
                }
            }
        }

        // Drain channel before exiting
        let activations: Vec<_> = self.receiver.drain().collect();

        for (activation, time) in activations {
            metrics::histogram!("push.queue.latency").record(time.elapsed());

            // Push the task and mark it as processing
            self.push_task(activation).await;
        }

        drop(guard);
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
        self.updater.update(id).await
    }
}
