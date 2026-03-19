use std::sync::Arc;

use anyhow::anyhow;
use sentry_protos::taskbroker::v1::PushTaskRequest;
use tokio::time::{Duration, timeout};
use tonic::async_trait;

use super::*;
use crate::config::Config;
use crate::test_utils::make_activations;

/// Fake worker client for unit testing.
struct MockWorkerClient {
    /// Capture all received requests so we can assert things about them.
    captured_requests: Vec<PushTaskRequest>,

    /// Should requests to the worker client fail?
    should_fail: bool,
}

impl MockWorkerClient {
    fn new(should_fail: bool) -> Self {
        let captured_requests = vec![];

        Self {
            captured_requests,
            should_fail,
        }
    }
}

#[async_trait]
impl WorkerClient for MockWorkerClient {
    async fn send(&mut self, request: PushTaskRequest) -> Result<()> {
        self.captured_requests.push(request);

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
    let callback_url = "taskbroker:50051".to_string();

    let result = push_task(&mut worker, activation.clone(), callback_url.clone()).await;
    assert!(result.is_ok(), "push_task should succeed");
    assert_eq!(worker.captured_requests.len(), 1);

    let request = &worker.captured_requests[0];
    assert_eq!(request.callback_url, callback_url);
    assert_eq!(
        request.task.as_ref().map(|task| task.id.as_str()),
        Some(activation.id.as_str())
    );
}

#[tokio::test]
async fn push_task_returns_err_on_invalid_payload() {
    let mut activation = make_activations(1).remove(0);
    activation.activation = vec![1, 2, 3, 4];

    let mut worker = MockWorkerClient::new(false);
    let result = push_task(&mut worker, activation, "taskbroker:50051".to_string()).await;

    assert!(result.is_err(), "invalid payload should fail decoding");
    assert!(
        worker.captured_requests.is_empty(),
        "worker should not be called if decode fails"
    );
}

#[tokio::test]
async fn push_task_propagates_client_error() {
    let activation = make_activations(1).remove(0);
    let mut worker = MockWorkerClient::new(true);

    let result = push_task(&mut worker, activation, "taskbroker:50051".to_string()).await;
    assert!(result.is_err(), "worker send errors should propagate");
    assert_eq!(worker.captured_requests.len(), 1);
}

#[tokio::test]
async fn push_pool_submit_enqueues_item() {
    let config = Arc::new(Config {
        push_queue_size: 2,
        ..Config::default()
    });

    let pool = PushPool::new(config);
    let activation = make_activations(1).remove(0);

    let result = pool.submit(activation).await;
    assert!(result.is_ok(), "submit should enqueue activation");
}

#[tokio::test]
async fn push_pool_submit_backpressures_when_queue_full() {
    let config = Arc::new(Config {
        push_queue_size: 1,
        ..Config::default()
    });

    let pool = PushPool::new(config);

    let first = make_activations(1).remove(0);
    let second = make_activations(1).remove(0);

    pool.submit(first)
        .await
        .expect("first submit should fill queue");

    let second_submit = timeout(Duration::from_millis(50), pool.submit(second)).await;
    assert!(
        second_submit.is_err(),
        "second submit should block when queue is full"
    );
}
