use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use async_backtrace::framed;
use hmac::{Hmac, Mac};
use prost::Message;
use sentry_protos::taskbroker::v1::worker_service_client::WorkerServiceClient;
use sentry_protos::taskbroker::v1::{PushTaskRequest, TaskActivation};
use sha2::Sha256;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, async_trait};

use crate::config::Config;
use crate::store::activation::Activation;

#[cfg(test)]
use tokio::sync::Notify;

// Alias for ergonomics.
pub type WorkerMap = HashMap<String, Box<dyn WorkerClient>>;

/// gRPC path for `WorkerService::PushTask` that should be kept in sync with `sentry_protos` generated client.
pub const WORKER_PUSH_TASK_PATH: &str = "/sentry_protos.taskbroker.v1.WorkerService/PushTask";

/// Helper to compute HMAC-SHA256(secret, grpc_path + ":" + message) in hexadecimal. Matches Python `RequestSignatureInterceptor`.
fn sentry_signature_hex(secret: &str, grpc_path: &str, message: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(grpc_path.as_bytes());
    mac.update(b":");
    mac.update(message);
    hex::encode(mac.finalize().into_bytes())
}

/// Thin interface for the worker client. It mostly serves to enable proper unit testing,
/// but it also decouples the actual client implementation from our pushing logic.
#[async_trait]
pub trait WorkerClient: Send + Sync {
    /// Send a single activation to the worker service.
    async fn push_task(&mut self, activation: Activation) -> Result<()>;
}

/// Wrapper around worker connection that provides authentication and timeouts.
pub struct Worker {
    /// Connection to the worker service.
    client: WorkerServiceClient<Channel>,

    /// List of secrets shared with the worker.
    secrets: Vec<String>,

    /// Wait this much time before giving up on a push.
    timeout: Duration,
}

impl Worker {
    pub async fn connect(config: Arc<Config>, endpoint: String) -> Result<Self> {
        let client = WorkerServiceClient::connect(endpoint).await?;

        let secrets = config.grpc_shared_secret.clone();
        let timeout = Duration::from_millis(config.push_timeout_ms);

        Ok(Self {
            client,
            secrets,
            timeout,
        })
    }
}

#[async_trait]
impl WorkerClient for Worker {
    #[framed]
    async fn push_task(&mut self, activation: Activation) -> Result<()> {
        metrics::counter!("worker.push_task.attempts").increment(1);

        // Try to decode activation
        let task =
            TaskActivation::decode(&activation.activation as &[u8]).map_err(|e| anyhow!(e))?;

        let request = PushTaskRequest {
            task: Some(task),
            callback_url: "".into(), // Workers don't use this anymore, so use an empty string until field is removed from schema
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

/// Fake worker client that records pushed activation IDs and optionally fails.
#[cfg(test)]
struct MockWorkerClient {
    /// Pushed activation IDs.
    pushed: Vec<String>,

    /// Should `push_task` fail?
    fail: bool,
}

#[cfg(test)]
impl MockWorkerClient {
    fn new(fail: bool) -> Self {
        Self {
            pushed: vec![],
            fail,
        }
    }
}

#[cfg(test)]
#[async_trait]
impl WorkerClient for MockWorkerClient {
    async fn push_task(&mut self, activation: Activation) -> Result<()> {
        TaskActivation::decode(&activation.activation as &[u8]).map_err(|e| anyhow!(e))?;
        self.pushed.push(activation.id);

        if self.fail {
            return Err(anyhow!("mock send failure"));
        }

        Ok(())
    }
}

/// Fake worker client that fires a `Notify` when `push_task` is called.
#[cfg(test)]
struct NotifyingWorkerClient {
    /// Fire off notification when `push_task` is called
    notify: Arc<Notify>,

    /// Should `push_task` fail?
    fail: bool,
}

#[cfg(test)]
#[async_trait]
impl WorkerClient for NotifyingWorkerClient {
    async fn push_task(&mut self, _activation: Activation) -> Result<()> {
        self.notify.notify_one();

        if self.fail {
            return Err(anyhow!("mock send failure"));
        }

        Ok(())
    }
}

/// Create a map of notifying worker clients for tests.
#[cfg(test)]
pub fn test_worker_map(fail: bool, notify: Arc<Notify>) -> WorkerMap {
    let mut workers = HashMap::new();

    let client = NotifyingWorkerClient { fail, notify };

    workers.insert("sentry".into(), Box::new(client) as Box<dyn WorkerClient>);
    workers
}

#[cfg(test)]
mod tests {
    use crate::test_utils::make_activations;

    use super::MockWorkerClient;
    use super::WORKER_PUSH_TASK_PATH;
    use super::WorkerClient;
    use super::sentry_signature_hex;

    #[tokio::test]
    async fn worker_push_task_returns_ok_on_client_success() {
        let activation = make_activations(1).remove(0);
        let mut worker = MockWorkerClient::new(false);

        let result = worker.push_task(activation.clone()).await;
        assert!(result.is_ok(), "push_task should succeed");
        assert_eq!(worker.pushed, vec![activation.id]);
    }

    #[tokio::test]
    async fn worker_push_task_returns_err_on_invalid_payload() {
        let mut activation = make_activations(1).remove(0);
        activation.activation = vec![1, 2, 3, 4];

        let mut worker = MockWorkerClient::new(false);
        let result = worker.push_task(activation).await;

        assert!(result.is_err(), "invalid payload should fail decoding");
        assert!(
            worker.pushed.is_empty(),
            "worker should not record a push if decode fails"
        );
    }

    #[tokio::test]
    async fn worker_push_task_propagates_client_error() {
        let activation = make_activations(1).remove(0);
        let mut worker = MockWorkerClient::new(true);

        let result = worker.push_task(activation.clone()).await;
        assert!(result.is_err(), "worker push errors should propagate");
        assert_eq!(worker.pushed, vec![activation.id]);
    }

    #[test]
    fn worker_sentry_signature_hex_matches_hmac_contract() {
        let digest = sentry_signature_hex("super secret", WORKER_PUSH_TASK_PATH, b"hello");

        assert_eq!(
            digest,
            "cfdf2c4d2efd3a2d28bd10de6471ef352d03f49d2d3bd880ee7051ea39fbe239"
        );
    }
}
