use std::sync::Arc;

use anyhow::Result;
use async_backtrace::framed;
use flume::Receiver;
use tokio::task::JoinSet;

use crate::config::Config;
use crate::push::thread::{PushItem, PushThread};
use crate::push::updater::Updater;
use crate::tokio::spawn_pool;
use crate::worker::WorkerMap;

mod thread;
pub mod updater;

/// Error returned when enqueueing an activation for the push workers fails.
#[derive(Debug)]
pub enum QueueError {
    /// The bounded queue stayed full until `push_queue_timeout_ms` elapsed.
    Timeout,

    /// Channel closed.
    Closed,
}

/// Wrapper around `config.push_threads` asynchronous tasks, each of which receives an activation from the channel, sends it to the worker service, and repeats.
pub struct PushPool {
    /// The receiving end of a channel that accepts task activations.
    receiver: Receiver<PushItem>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl PushPool {
    /// Initialize a new push pool.
    pub fn new(receiver: Receiver<PushItem>, config: Arc<Config>) -> Self {
        Self { receiver, config }
    }

    /// Spawn `config.push_threads` asynchronous tasks, each of which repeatedly moves pending activations from the channel to the worker service until the shutdown signal is received.
    #[framed]
    pub async fn start(&self, workers: Vec<WorkerMap>, updater: Arc<dyn Updater>) -> Result<()> {
        let mut workers = workers.into_iter();

        // Start updater daemon
        let upd = tokio::spawn({
            let updater = updater.clone();
            async move { updater.start().await }
        });

        let mut threads: JoinSet<Result<()>> = spawn_pool(self.config.push_threads, |_| {
            let mut thread = PushThread {
                workers: workers.next().unwrap(),
                receiver: self.receiver.clone(),
                updater: updater.clone(),
            };

            async move { thread.start().await }
        });

        while let Some(result) = threads.join_next().await {
            match result {
                // Propagate fatal errors
                Ok(r) => r?,

                // Join error
                Err(e) => return Err(e.into()),
            }
        }

        // Now that the push threads have shut down, we can stop the updater
        updater.stop();
        upd.await?
    }
}

#[cfg(test)]
mod tests;
