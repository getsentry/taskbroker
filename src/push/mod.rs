use std::cmp::max;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use async_backtrace::framed;
use chrono::Utc;
use elegant_departure::get_shutdown_guard;
use flume::{Receiver, SendError, Sender};
use hmac::{Hmac, Mac};
use prost::Message;
use sentry_protos::taskbroker::v1::worker_service_client::WorkerServiceClient;
use sentry_protos::taskbroker::v1::{PushTaskRequest, TaskActivation};
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, async_trait};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::push::thread::PushThread;
use crate::store::activation::InflightActivation;
use crate::store::traits::InflightActivationStore;

pub mod thread;

type HmacSha256 = Hmac<Sha256>;

pub type WorkerMap = HashMap<String, Box<dyn WorkerClient>>;

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

/// Thin interface for the worker client. It mostly serves to enable proper unit testing,
/// but it also decouples the actual client implementation from our pushing logic.
#[async_trait]
trait WorkerClient: Send + Sync {
    /// Send a single `PushTaskRequest` to the worker service.
    async fn push_task(&mut self, activation: InflightActivation) -> Result<()>;
}

/// Wrapper around worker connection that provides authentication and timeouts.
struct Worker {
    /// Connection to the worker service.
    client: WorkerServiceClient<Channel>,

    /// List of shared secrets.
    secrets: Vec<String>,

    /// Wait this much time on push.
    timeout: Duration,
}

#[async_trait]
impl WorkerClient for Worker {
    #[framed]
    async fn push_task(&mut self, activation: InflightActivation) -> Result<()> {
        // Try to decode activation
        let task =
            TaskActivation::decode(&activation.activation as &[u8]).map_err(|e| anyhow!(e))?;

        // The callback URL isn't used by push taskworkers anymore, so we can use an empty string until it's removed from the schema
        let request = PushTaskRequest {
            task: Some(task),
            callback_url: "".into(),
        };

        // Wrap inside a Tonic request
        let mut request = Request::new(request);

        // Sign if secrets are present
        if let Some(secret) = self.secrets.first() {
            let body = request.get_ref().encode_to_vec();
            let signature = sentry_signature_hex(secret, WORKER_PUSH_TASK_PATH, &body);
            let value = MetadataValue::try_from(signature.as_str())
                .context("sentry-signature metadata value must be valid ASCII")?;

            request.metadata_mut().insert("sentry-signature", value);
        }

        // Push with timeout
        let future = self.client.push_task(request);
        tokio::time::timeout(self.timeout, future).await??;

        Ok(())
    }
}

/// Wrapper around `config.push_threads` asynchronous tasks, each of which receives an activation from the channel, sends it to the worker service, and repeats.
pub struct PushPool {
    /// The sending end of a channel that accepts task activations.
    sender: Sender<(InflightActivation, Instant)>,

    /// The receiving end of a channel that accepts task activations.
    receiver: Receiver<(InflightActivation, Instant)>,

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
    #[framed]
    pub async fn start(&self, workers: Vec<WorkerMap>) -> Result<()> {
        let mut workers = workers.into_iter();

        let mut push_pool: JoinSet<Result<()>> =
            crate::tokio::spawn_pool(self.config.push_threads, |_| {
                let mut thread = PushThread::new(
                    self.config.clone(),
                    self.store.clone(),
                    workers.next().unwrap(),
                    self.receiver.clone(),
                );

                async move { thread.start().await }
            });

        while let Some(result) = push_pool.join_next().await {
            match result {
                Ok(r) => r?,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    /// Send an activation to the internal asynchronous MPMC channel used by all running push threads. Times out after `config.push_queue_timeout_ms` milliseconds.
    #[framed]
    pub async fn submit(
        &self,
        activation: InflightActivation,
        time: Instant,
    ) -> Result<(), PushError> {
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

#[cfg(test)]
mod tests;
