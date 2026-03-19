use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use flume::{Receiver, Sender};
use prost::Message;
use sentry_protos::taskbroker::v1::worker_service_client::WorkerServiceClient;
use sentry_protos::taskbroker::v1::{PushTaskRequest, TaskActivation};
use tonic::async_trait;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::store::inflight_activation::InflightActivation;

/// Thin interface for the worker client. It mostly serves to enable proper unit testing, but it also decouples the actual client implementation from our pushing logic.
#[async_trait]
trait WorkerClient {
    /// Send a single `PushTaskRequest` to the worker service.
    async fn send(&mut self, request: PushTaskRequest) -> Result<()>;
}

#[async_trait]
impl WorkerClient for WorkerServiceClient<Channel> {
    async fn send(&mut self, request: PushTaskRequest) -> Result<()> {
        self.push_task(request).await?;
        Ok(())
    }
}

/// Wrapper around `config.push_threads` asynchronous tasks, each of which receives an activation from the channel, sends it to the worker service, and repeats.
pub struct PushPool {
    /// The sending end of a channel that accepts task activations.
    sender: Sender<InflightActivation>,

    /// The receiving end of a channel that accepts task activations.
    receiver: Receiver<InflightActivation>,

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
    pub async fn start(&self) -> Result<()> {
        let mut push_pool = crate::tokio::spawn_pool(self.config.push_threads, |_| {
            let endpoint = self.config.worker_endpoint.clone();
            let receiver = self.receiver.clone();

            let guard = get_shutdown_guard().shutdown_on_drop();

            let callback_url = format!(
                "{}:{}",
                self.config.callback_addr, self.config.callback_port
            );

            async move {
                let mut worker = match WorkerServiceClient::connect(endpoint).await {
                    Ok(w) => w,
                    Err(e) => {
                        error!("Failed to connect to worker - {:?}", e);
                        return;
                    }
                };

                loop {
                    tokio::select! {
                        _ = guard.wait() => {
                            info!("Push worker received shutdown signal");
                            break;
                        }

                        message = receiver.recv_async() => {
                            let activation = match message {
                                // Received activation from fetch thread
                                Ok(a) => a,

                                // Channel closed
                                Err(_) => break
                            };

                            let id = activation.id.clone();

                            match push_task(&mut worker, activation, callback_url.clone()).await {
                                Ok(_) => debug!("Activation {id} was sent to worker!"),

                                // Once processing deadline expires, status will be set back to pending
                                Err(e) => error!("Pushing activation {id} resulted in error - {:?}", e)
                            };
                        }
                    }
                }

                // Drain channel before exiting
                for activation in receiver.drain() {
                    let id = activation.id.clone();

                    match push_task(&mut worker, activation, callback_url.clone()).await {
                        Ok(_) => debug!("Activation {id} was sent to worker!"),

                        // Once processing deadline expires, status will be set back to pending
                        Err(e) => error!("Pushing activation {id} resulted in error - {:?}", e),
                    };
                }
            }
        });

        while let Some(res) = push_pool.join_next().await {
            if let Err(e) = res {
                return Err(e.into());
            }
        }

        Ok(())
    }

    /// Send an activation to the internal asynchronous MPMC channel used by all running push threads. Times out after `config.push_timeout_ms` milliseconds.
    pub async fn submit(&self, activation: InflightActivation) -> Result<()> {
        let duration = Duration::from_millis(self.config.push_timeout_ms);

        tokio::time::timeout(duration, self.sender.send_async(activation))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "failed to submit to push pool within {} milliseconds",
                    self.config.push_timeout_ms
                )
            })??;

        Ok(())
    }
}

/// Decode task activation and push it to a worker.
async fn push_task<W: WorkerClient + Send>(
    worker: &mut W,
    activation: InflightActivation,
    callback_url: String,
) -> Result<()> {
    let start = Instant::now();
    let id = activation.id.clone();

    // Try to decode activation (if it fails, we will see the error where `push_task` is called)
    let task = TaskActivation::decode(&activation.activation as &[u8])?;

    let request = PushTaskRequest {
        task: Some(task),
        callback_url,
    };

    let result = match worker.send(request).await {
        Ok(_) => {
            debug!("Successfully sent activation {id} to worker service!");
            Ok(())
        }

        Err(e) => {
            error!("Could not push activation {id} to worker service - {:?}", e);
            Err(e)
        }
    };

    metrics::histogram!("push.push_task.duration").record(start.elapsed());
    result
}

#[cfg(test)]
mod tests;
