use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_backtrace::framed;
use flume::{Receiver, Sender};
use tokio::task::JoinSet;
use tonic::async_trait;

use crate::config::Config;
use crate::push::thread::PushThread;
use crate::push::updater::Updater;
use crate::store::activation::InflightActivation;
use crate::worker::WorkerMap;

pub mod thread;
pub mod updater;

// Helper to compute the longest number of milliseconds an activation may be claimed.
pub fn compute_claim_lease_ms(config: &Config) -> u64 {
    // In the worst case, every activation in the batch will time out when appending to the push queue
    let queue_ms = config.fetch_batch_size as u64 * config.push_queue_timeout_ms;

    // In the worst case, every activation in the push queue will time out when sending
    let send_ms = config.push_queue_size as u64 * config.push_timeout_ms;

    let update_ms = if config.batch_push_updates {
        // In the worst case, we will need to wait an entire interval before flushing a batch of push updates
        config.push_update_interval_ms as u64
    } else {
        // Grace seconds will cover the update query duration until we decide to implement query timeouts
        0
    };

    // Account for grace seconds specified in configuration
    let grace_ms = config.claim_expiration_grace_sec * 1000;

    queue_ms + send_ms + update_ms + grace_ms
}

/// Thin interface for the push pool. It mostly serves to enable proper unit testing,
/// but it also decouples fetch logic from push logic even further.
#[async_trait]
pub trait TaskPusher {
    /// Submit a single task to the push pool.
    async fn push_task(&self, activation: InflightActivation, time: Instant) -> Result<()>;
}

/// Wrapper around `config.push_threads` asynchronous tasks, each of which receives an activation from the channel, sends it to the worker service, and repeats.
pub struct PushPool {
    /// The sending end of a channel that accepts task activations.
    sender: Sender<(InflightActivation, Instant)>,

    /// The receiving end of a channel that accepts task activations.
    receiver: Receiver<(InflightActivation, Instant)>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl PushPool {
    /// Initialize a new push pool.
    pub fn new(config: Arc<Config>) -> Self {
        let (sender, receiver) = flume::bounded(config.push_queue_size);

        Self {
            sender,
            receiver,
            config,
        }
    }

    /// Spawn `config.push_threads` asynchronous tasks, each of which repeatedly moves pending activations from the channel to the worker service until the shutdown signal is received.
    #[framed]
    pub async fn start(&self, workers: Vec<WorkerMap>, updater: Arc<dyn Updater>) -> Result<()> {
        let mut workers = workers.into_iter();

        // Start the updater
        let updaterd = tokio::spawn({
            let updater = updater.clone();
            async move { updater.start().await }
        });

        let mut threads: JoinSet<Result<()>> =
            crate::tokio::spawn_pool(self.config.push_threads, |_| {
                let mut thread = PushThread {
                    workers: workers.next().unwrap(),
                    receiver: self.receiver.clone(),
                    updater: updater.clone(),
                };

                async move { thread.start().await }
            });

        while let Some(result) = threads.join_next().await {
            if let Err(_) = result {
                todo!()
            }

            if let Ok(Err(_)) = result {
                todo!()
            }
        }

        // Now that the push threads have shut down, we can stop the updater
        updater.stop();

        let result = updaterd.await;

        if let Err(_) = result {
            todo!()
        }

        if let Ok(Err(_)) = result {
            todo!()
        }

        Ok(())
    }
}

#[async_trait]
impl TaskPusher for PushPool {
    #[framed]
    async fn push_task(&self, activation: InflightActivation, time: Instant) -> Result<()> {
        let duration = Duration::from_millis(self.config.push_queue_timeout_ms);
        let start = Instant::now();

        metrics::gauge!("push.queue.depth").set(self.sender.len() as f64);

        match tokio::time::timeout(duration, self.sender.send_async((activation, time))).await {
            // The channel has closed because all receivers were dropped
            Ok(Err(e)) => {
                // The only way this can happen is if the push threads have already exited, which is an invalid state
                unreachable!("{}", e);
            }

            // The channel was full so the pushing timed out
            Err(e) => {
                metrics::histogram!("push.queue.wait_duration").record(start.elapsed());
                Err(e.into())
            }

            Ok(_) => {
                metrics::histogram!("push.queue.wait_duration").record(start.elapsed());
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests;
