use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use elegant_departure::get_shutdown_guard;
use flume::{Receiver, SendError, Sender};
use hmac::{Hmac, Mac};
use prost::Message;
use sentry_protos::taskbroker::v1::worker_service_client::WorkerServiceClient;
use sentry_protos::taskbroker::v1::{PushTaskRequest, TaskActivation};
use sha2::Sha256;
use tokio::task::JoinSet;
use tonic::async_trait;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::store::inflight_activation::{InflightActivation, InflightActivationStore};

type HmacSha256 = Hmac<Sha256>;

/// gRPC path for `WorkerService::PushTask` — keep in sync with `sentry_protos` generated client.
const WORKER_PUSH_TASK_PATH: &str = "/sentry_protos.taskbroker.v1.WorkerService/PushTask";

/// HMAC-SHA256(secret, grpc_path + ":" + message), hex-encoded. Matches Python `RequestSignatureInterceptor` and broker [`crate::grpc::auth_middleware`].
fn sentry_signature_hex(secret: &str, grpc_path: &str, message: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(grpc_path.as_bytes());
    mac.update(b":");
    mac.update(message);
    hex::encode(mac.finalize().into_bytes())
}

/// Error returned when enqueueing an activation for the push workers fails.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum PushError {
    /// The bounded queue stayed full until `push_queue_timeout_ms` elapsed.
    Timeout,

    /// Channel disconnected (no receivers) or another failure.
    Channel(SendError<InflightActivation>),
}

/// Thin interface for the worker client. It mostly serves to enable proper unit testing, but it also decouples the actual client implementation from our pushing logic.
#[async_trait]
trait WorkerClient {
    /// Send a single `PushTaskRequest` to the worker service.
    /// When `grpc_shared_secret` is not empty, it signs the request with `grpc_shared_secret[0]` and sets `sentry-signature` metadata (same scheme as Python pull client and broker `AuthLayer`).
    async fn send(&mut self, request: PushTaskRequest, grpc_shared_secret: &[String])
    -> Result<()>;
}

#[async_trait]
impl WorkerClient for WorkerServiceClient<Channel> {
    async fn send(
        &mut self,
        request: PushTaskRequest,
        grpc_shared_secret: &[String],
    ) -> Result<()> {
        let mut req = tonic::Request::new(request);

        if let Some(secret) = grpc_shared_secret.first() {
            let body = req.get_ref().encode_to_vec();
            let signature = sentry_signature_hex(secret, WORKER_PUSH_TASK_PATH, &body);
            let value = MetadataValue::try_from(signature.as_str())
                .context("sentry-signature metadata value must be valid ASCII")?;
            req.metadata_mut().insert("sentry-signature", value);
        }

        self.push_task(req)
            .await
            .map_err(|status| anyhow::anyhow!(status))?;

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

    /// Activation store, which we need for marking tasks as sent.
    store: Arc<dyn InflightActivationStore>,
}

impl PushPool {
    /// Initialize a new push pool.
    pub fn new(config: Arc<Config>, store: Arc<dyn InflightActivationStore>) -> Self {
        let (sender, receiver) = flume::bounded(config.push_queue_size);

        Self {
            sender,
            receiver,
            config,
            store,
        }
    }

    /// Spawn `config.push_threads` asynchronous tasks, each of which repeatedly moves pending activations from the channel to the worker service until the shutdown signal is received.
    pub async fn start(&self) -> Result<()> {
        let store = self.store.clone();
        let mut push_pool: JoinSet<Result<()>> = crate::tokio::spawn_pool(
            self.config.push_threads,
            |_| {
                let endpoint = self.config.worker_endpoint.clone();
                let receiver = self.receiver.clone();
                let store = store.clone();

                let guard = get_shutdown_guard().shutdown_on_drop();

                let callback_url = format!(
                    "{}:{}",
                    self.config.callback_addr, self.config.callback_port
                );

                let timeout = Duration::from_millis(self.config.push_timeout_ms);
                let grpc_shared_secret = self.config.grpc_shared_secret.clone();

                async move {
                    metrics::counter!("push.worker.connect.attempt").increment(1);

                    let mut worker = match WorkerServiceClient::connect(endpoint).await {
                        Ok(w) => {
                            metrics::counter!("push.worker.connect", "result" => "ok").increment(1);
                            w
                        }

                        Err(e) => {
                            metrics::counter!("push.worker.connect", "result" => "error")
                                .increment(1);
                            error!("Failed to connect to worker - {:?}", e);

                            // When we fail to connect, the taskbroker will shut down, but this may change in the future
                            return Err(e.into());
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
                                let callback_url = callback_url.clone();

                                match push_task(
                                    &mut worker,
                                    activation,
                                    callback_url,
                                    timeout,
                                    grpc_shared_secret.as_slice(),
                                )
                                .await
                                {
                                    Ok(_) => {
                                        metrics::counter!("push.delivery", "result" => "ok").increment(1);
                                        debug!(task_id = %id, "Activation sent to worker");

                                        if let Err(e) = store.mark_activation_processing(&id).await {
                                            metrics::counter!("push.mark_activation_processing", "result" => "error").increment(1);

                                            error!(
                                                task_id = %id,
                                                error = ?e,
                                                "Failed to mark activation as sent after push"
                                            );
                                        }
                                    }

                                    // Once claim expires, status will be set back to pending
                                    Err(e) => {
                                        metrics::counter!("push.delivery", "result" => "error").increment(1);

                                        error!(
                                            task_id = %id,
                                            error = ?e,
                                            "Failed to send activation to worker"
                                        )
                                    }
                                };
                            }
                        }
                    }

                    // Drain channel before exiting
                    for activation in receiver.drain() {
                        let id = activation.id.clone();
                        let callback_url = callback_url.clone();

                        match push_task(
                            &mut worker,
                            activation,
                            callback_url,
                            timeout,
                            grpc_shared_secret.as_slice(),
                        )
                        .await
                        {
                            Ok(_) => {
                                metrics::counter!("push.delivery", "result" => "ok").increment(1);
                                debug!(task_id = %id, "Activation sent to worker");

                                if let Err(e) = store.mark_activation_processing(&id).await {
                                    metrics::counter!("push.mark_activation_processing", "result" => "error").increment(1);

                                    error!(
                                        task_id = %id,
                                        error = ?e,
                                        "Failed to mark activation as processing after push"
                                    );
                                }
                            }

                            // Once processing deadline expires, status will be set back to pending
                            Err(e) => {
                                metrics::counter!("push.delivery", "result" => "error")
                                    .increment(1);

                                error!(
                                    task_id = %id,
                                    error = ?e,
                                    "Failed to send activation to worker"
                                )
                            }
                        };
                    }

                    Ok(())
                }
            },
        );

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
    pub async fn submit(&self, activation: InflightActivation) -> Result<(), PushError> {
        let duration = Duration::from_millis(self.config.push_queue_timeout_ms);
        let start = Instant::now();

        metrics::gauge!("push.queue.depth").set(self.sender.len() as f64);

        match tokio::time::timeout(duration, self.sender.send_async(activation)).await {
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
async fn push_task<W: WorkerClient + Send>(
    worker: &mut W,
    activation: InflightActivation,
    callback_url: String,
    timeout: Duration,
    grpc_shared_secret: &[String],
) -> Result<()> {
    let start = Instant::now();
    metrics::counter!("push.push_task.attempt").increment(1);

    // Try to decode activation (if it fails, we will see the error where `push_task` is called)
    let task = match TaskActivation::decode(&activation.activation as &[u8]) {
        Ok(task) => task,
        Err(err) => {
            metrics::histogram!("push.push_task.duration").record(start.elapsed());
            return Err(err.into());
        }
    };

    let request = PushTaskRequest {
        task: Some(task),
        callback_url,
    };

    let result = match tokio::time::timeout(timeout, worker.send(request, grpc_shared_secret)).await
    {
        Ok(r) => r,
        Err(e) => Err(e.into()),
    };

    metrics::histogram!("push.push_task.duration").record(start.elapsed());
    result
}

#[cfg(test)]
mod tests;
