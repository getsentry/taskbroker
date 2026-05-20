use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use prost::Message;
use sentry_protos::taskbroker::v1::TaskActivation;

use crate::store::activation::InflightActivation;
use crate::test_utils::make_activations;
use crate::worker::{Worker, WorkerClient, sentry_signature_hex};

/// Fake worker client that records task IDs and optionally fails.
struct MockWorkerClient {
    captured_task_ids: Vec<String>,
    should_fail: bool,
}

impl MockWorkerClient {
    fn new(should_fail: bool) -> Self {
        Self {
            captured_task_ids: vec![],
            should_fail,
        }
    }
}

#[async_trait]
impl WorkerClient for MockWorkerClient {
    async fn push_task(&mut self, activation: InflightActivation) -> anyhow::Result<()> {
        let task =
            TaskActivation::decode(&activation.activation as &[u8]).map_err(|e| anyhow!(e))?;
        self.captured_task_ids.push(task.id);

        if self.should_fail {
            return Err(anyhow!("mock send failure"));
        }

        Ok(())
    }
}

#[tokio::test]
async fn push_task_returns_ok_on_client_success() {
    let activation = make_activations(1).remove(0);
    let mut worker = MockWorkerClient::new(false);

    let result = worker.push_task(activation.clone()).await;
    assert!(result.is_ok(), "push_task should succeed");
    assert_eq!(worker.captured_task_ids, vec![activation.id]);
}

#[tokio::test]
async fn push_task_returns_err_on_invalid_payload() {
    let mut activation = make_activations(1).remove(0);
    activation.activation = vec![1, 2, 3, 4];

    let mut worker = MockWorkerClient::new(false);
    let result = worker.push_task(activation).await;

    assert!(result.is_err(), "invalid payload should fail decoding");
    assert!(
        worker.captured_task_ids.is_empty(),
        "worker should not record a task id if decode fails"
    );
}

#[tokio::test]
async fn push_task_propagates_client_error() {
    let activation = make_activations(1).remove(0);
    let mut worker = MockWorkerClient::new(true);

    let result = worker.push_task(activation.clone()).await;
    assert!(result.is_err(), "worker send errors should propagate");
    assert_eq!(worker.captured_task_ids, vec![activation.id]);
}

#[test]
fn sentry_signature_hex_matches_hmac_contract() {
    let digest = sentry_signature_hex("super secret", "/test/path", b"hello");
    assert_eq!(
        digest,
        "6408482d9e6d4975ada4c0302fda813c5718e571e6f9a2d6e2803cb48528044e"
    );
}

#[tokio::test]
async fn worker_connect_fails_for_unreachable_endpoint() {
    let result = Worker::connect("http://127.0.0.1:1".into()).await;
    assert!(
        result.is_err(),
        "connect should return Err for an unreachable endpoint"
    );
}

#[tokio::test]
async fn worker_connect_with_options_fails_for_unreachable_endpoint() {
    let result =
        Worker::connect_with_options("http://127.0.0.1:1".into(), vec![], Duration::from_secs(1))
            .await;
    assert!(
        result.is_err(),
        "connect_with_options should return Err for an unreachable endpoint"
    );
}
