use chrono::Utc;
use std::cmp::max;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_backtrace::framed;
use elegant_departure::get_shutdown_guard;
use flume::{Receiver, SendError, Sender};
use tokio::task::JoinSet;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::store::activation::Activation;
use crate::store::traits::ActivationStore;
use crate::worker::WorkerMap;

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

        let mut push_pool: JoinSet<Result<()>> =
            crate::tokio::spawn_pool(self.config.push_threads, |_| {
                let receiver = self.receiver.clone();
                let store = self.store.clone();

                let mut workers = workers.next().unwrap();

                let guard = get_shutdown_guard().shutdown_on_drop();

                async_backtrace::frame!(async move {
                    loop {
                        tokio::select! {
                            _ = guard.wait() => {
                                info!("Push worker received shutdown signal");
                                break;
                            }

                            message = receiver.recv_async() => {
                                let (activation, time) = match message {
                                    // Received activation from fetch thread
                                    Ok(a) => a,

                                    // Channel closed
                                    Err(_) => break,
                                };

                                metrics::histogram!("push.queue.latency").record(time.elapsed());

                                push_task(store.clone(), &mut workers, activation).await;
                            }
                        }
                    }

                    // Drain channel before exiting without recording duration metrics since they don't matter at this time
                    for (activation, _) in receiver.drain() {
                        push_task(store.clone(), &mut workers, activation).await;
                    }

                    Ok(())
                })
            });

        while let Some(result) = push_pool.join_next().await {
            match result {
                Ok(r) => {
                    // Connection failed
                    r?
                }

                // Join failed
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    /// Send an activation to the internal asynchronous MPMC channel used by all running push threads. Times out after `config.push_queue_timeout_ms` milliseconds.
    #[framed]
    pub async fn submit(&self, activation: Activation, time: Instant) -> Result<(), PushError> {
        let duration = Duration::from_millis(self.config.push_queue_timeout_ms);
        let start = Instant::now();

        metrics::gauge!("push.queue.depth").set(self.sender.len() as f64);

        match tokio::time::timeout(duration, self.sender.send_async((activation, time))).await {
            Ok(Ok(())) => {
                metrics::histogram!("push.queue.wait_duration").record(start.elapsed());
                Ok(())
            }

            // The channel has a problem
            Ok(Err(e)) => {
                metrics::histogram!("push.queue.wait_duration").record(start.elapsed());
                Err(PushError::Channel(e))
            }

            // The channel was full so the send timed out
            Err(_elapsed) => {
                metrics::histogram!("push.queue.wait_duration").record(start.elapsed());
                Err(PushError::Timeout)
            }
        }
    }
}

/// Decode task activation and push it to a worker.
#[framed]
async fn push_task(
    store: Arc<dyn ActivationStore>,
    workers: &mut WorkerMap,
    activation: Activation,
) {
    let id = activation.id.clone();

    let Some(worker) = workers.get_mut(&activation.application) else {
        metrics::counter!("push.missing_worker_mapping", "application" => activation.application.clone()).increment(1);

        error!(
            task_id = %id,
            application = activation.application,
            "Task application has no worker pool mapping"
        );

        return;
    };

    match worker.push_task(activation.clone()).await {
        Ok(_) => {
            metrics::counter!("push.push_task", "result" => "ok").increment(1);
            debug!(task_id = %id, "Activation sent to worker");

            if activation.processing_attempts < 1 {
                let latency = max(0, activation.received_latency(Utc::now()));

                metrics::histogram!(
                    "push.received_to_push.latency",
                    "namespace" => activation.namespace,
                    "taskname" => activation.taskname,
                )
                .record(latency as f64);
            } else {
                debug!(task_id = %id, namespace = activation.namespace, taskname = activation.taskname, "Activation already processed, skipping received → push latency recording");
            }

            let start = Instant::now();
            let result = store.mark_activation_processing(&id).await;
            metrics::histogram!("push.mark_activation_processing.duration").record(start.elapsed());

            if let Err(e) = result {
                metrics::counter!("push.mark_activation_processing", "result" => "error")
                    .increment(1);

                error!(
                    task_id = %id,
                    error = ?e,
                    "Failed to mark activation as sent after push"
                );
            }
        }

        // Once claim expires, status will be set back to pending
        Err(e) => {
            metrics::counter!("push.push_task", "result" => "error").increment(1);

            error!(
                task_id = %id,
                error = ?e,
                "Failed to send activation to worker"
            )
        }
    }
}

#[cfg(test)]
mod tests;
