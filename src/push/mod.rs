use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_backtrace::framed;
use flume::{Receiver, SendError, Sender};
use tokio::task::JoinSet;
use tonic::async_trait;

use crate::config::Config;
use crate::push::thread::PushThread;
use crate::store::activation::Activation;
use crate::store::traits::ActivationStore;
use crate::timed;
use crate::tokio::spawn_pool;
use crate::worker::WorkerMap;

mod thread;

/// Thin interface for the push pool. It mostly serves to enable proper unit testing, but it also decouples fetch logic from push logic even further.
#[async_trait]
pub trait TaskPusher {
    /// Submit a single task to the push pool.
    async fn push_task(&self, activation: Activation, time: Instant) -> Result<(), PushError>;
}

/// Error returned when enqueueing an activation for the push workers fails.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum PushError {
    /// The bounded queue stayed full until `push_queue_timeout_ms` elapsed.
    Timeout,

    /// Channel disconnected (no receivers) or another failure.
    Channel(SendError<(Activation, Instant)>),
}

/// Wrapper around `config.push_threads` asynchronous tasks, each of which receives an activation from the channel, sends it to the worker service, and repeats.
pub struct PushPool {
    /// The sending end of a channel that accepts task activations.
    sender: Sender<(Activation, Instant)>,

    /// The receiving end of a channel that accepts task activations.
    receiver: Receiver<(Activation, Instant)>,

    /// Taskbroker configuration.
    config: Arc<Config>,

    /// Activation store, which we need for marking tasks as sent.
    store: Arc<dyn ActivationStore>,
}

impl PushPool {
    /// Initialize a new push pool.
    pub fn new(config: Arc<Config>, store: Arc<dyn ActivationStore>) -> Self {
        let (sender, receiver) = flume::bounded(config.push_queue_size);

        Self {
            sender,
            receiver,
            config,
            store,
        }
    }

    /// Spawn `config.push_threads` asynchronous tasks, each of which repeatedly moves pending activations from the channel to the worker service until the shutdown signal is received.
    #[framed]
    pub async fn start(&self, workers: Vec<WorkerMap>) -> Result<()> {
        let mut workers = workers.into_iter();

        let mut threads: JoinSet<Result<()>> = spawn_pool(self.config.push_threads, |_| {
            let mut thread = PushThread {
                workers: workers.next().unwrap(),
                receiver: self.receiver.clone(),
                store: self.store.clone(),
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

        Ok(())
    }
}

#[async_trait]
impl TaskPusher for PushPool {
    #[framed]
    async fn push_task(&self, activation: Activation, time: Instant) -> Result<(), PushError> {
        metrics::gauge!("push.queue.depth").set(self.sender.len() as f64);

        let duration = Duration::from_millis(self.config.push_queue_timeout_ms);
        let timeout = tokio::time::timeout(duration, self.sender.send_async((activation, time)));

        match timed!(timeout, "push.queue.wait_duration") {
            // The channel was full so the send timed out
            Err(_) => Err(PushError::Timeout),

            // The channel has a problem
            Ok(Err(e)) => Err(PushError::Channel(e)),

            // Pushed to channel successfully
            Ok(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;
