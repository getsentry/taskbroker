use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, anyhow};
use async_backtrace::framed;
use hmac::{Hmac, Mac};
use prost::Message;
use sentry_protos::taskbroker::v1::{
    PushTaskRequest, TaskActivation, worker_service_client::WorkerServiceClient,
};
use sha2::Sha256;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, async_trait};

use crate::store::activation::InflightActivation;

// Alias for ergonomics.
pub type WorkerMap = HashMap<String, Box<dyn WorkerClient>>;

/// gRPC path for `WorkerService::PushTask` that should be kept in sync with `sentry_protos` generated client.
pub const WORKER_PUSH_TASK_PATH: &str = "/sentry_protos.taskbroker.v1.WorkerService/PushTask";

/// HMAC-SHA256(secret, grpc_path + ":" + message), hex-encoded. Matches Python `RequestSignatureInterceptor` and broker [`crate::grpc::auth_middleware`].
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
    async fn push_task(&mut self, activation: InflightActivation) -> Result<()>;
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

mod tests {
    use hmac::Hmac;
    use sha2::Sha256;

    use crate::{test_utils::make_activations, worker::WORKER_PUSH_TASK_PATH};

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
        let mut mac = Hmac::<Sha256>::new_from_slice(b"super secret")
            .expect("HMAC accepts keys of any length");
        mac.update(WORKER_PUSH_TASK_PATH.as_bytes());
        mac.update(b":");
        mac.update(b"hello");
        let digest = hex::encode(mac.finalize().into_bytes());

        assert_eq!(
            digest,
            "6408482d9e6d4975ada4c0302fda813c5718e571e6f9a2d6e2803cb48528044e"
        );
    }
}
