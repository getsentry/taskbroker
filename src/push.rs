use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use flume::{Receiver, Sender};
use prost::Message;
use sentry_protos::taskbroker::v1::worker_service_client::WorkerServiceClient;
use sentry_protos::taskbroker::v1::{PushTaskRequest, TaskActivation};
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::store::inflight_activation::InflightActivation;

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
        let mut handles = vec![];

        for _ in 0..self.config.push_threads {
            let endpoint = self.config.worker_endpoint.clone();

            let callback_url = format!("{}:{}", self.config.grpc_addr, self.config.grpc_port);
            let receiver = self.receiver.clone();
            let guard = get_shutdown_guard().shutdown_on_drop();

            let handle = tokio::spawn(async move {
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
                        Err(e) => error!("Pushing activation {id} resulted in error - {:?}", e),
                    };
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            if let Err(e) = handle.await {
                return Err(e.into());
            }
        }

        Ok(())
    }

    /// Send an activation to the internal asynchronous MPMC channel used by all running push threads.
    pub async fn submit(&self, activation: InflightActivation) -> Result<()> {
        Ok(self.sender.send_async(activation).await?)
    }
}

/// Decode task activation and push it to a worker.
async fn push_task(
    worker: &mut WorkerServiceClient<Channel>,
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

    let result = match worker.push_task(request).await {
        Ok(_) => {
            debug!("Successfully sent activation {id} to worker service!");
            Ok(())
        }

        Err(e) => {
            error!("Could not push activation {id} to worker service - {:?}", e);
            Err(e.into())
        }
    };

    metrics::histogram!("pusher.push_task.duration").record(start.elapsed());
    result
}
