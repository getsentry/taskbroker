use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sentry_protos::taskbroker::v1::PushTaskRequest;
use tokio::sync::{Notify, mpsc};
use tokio::time::{Duration, timeout};

use crate::config::Config;
use crate::store::activation::{InflightActivation, InflightActivationStatus};
use crate::store::traits::InflightActivationStore;
use crate::store::types::FailedTasksForwarder;
use crate::test_utils::{create_test_store, make_activations};

use super::*;

/// Fake worker client that records requests and optionally fails.
struct MockWorkerClient {
    captured_requests: Vec<PushTaskRequest>,
    should_fail: bool,
}

impl MockWorkerClient {
    fn new(should_fail: bool) -> Self {
        Self {
            captured_requests: vec![],
            should_fail,
        }
    }
}

#[async_trait]
impl WorkerClient for MockWorkerClient {
    async fn send(
        &mut self,
        request: PushTaskRequest,
        _grpc_shared_secret: &[String],
    ) -> Result<()> {
        self.captured_requests.push(request);
        if self.should_fail {
            return Err(anyhow!("mock send failure"));
        }
        Ok(())
    }
}

/// Fake worker client that fires a Notify when send() is called.
struct NotifyingWorkerClient {
    should_fail: bool,
    notify: Arc<Notify>,
}

#[async_trait]
impl WorkerClient for NotifyingWorkerClient {
    async fn send(&mut self, _request: PushTaskRequest, _: &[String]) -> Result<()> {
        self.notify.notify_one();
        if self.should_fail {
            return Err(anyhow!("mock send failure"));
        }
        Ok(())
    }
}

/// Minimal fake store that records which activation IDs have been marked as processing.
/// All IDs marked via either `mark_processing` or successful `mark_processing_batch`.
#[derive(Clone)]
struct MockStore {
    marked_processing: Arc<Mutex<Vec<String>>>,
    mark_processing_calls: Arc<Mutex<Vec<String>>>,
    mark_processing_batches: Arc<Mutex<Vec<Vec<String>>>>,
    mark_processing_batch_should_fail: Arc<Mutex<bool>>,
}

impl Default for MockStore {
    fn default() -> Self {
        Self {
            marked_processing: Arc::new(Mutex::new(vec![])),
            mark_processing_calls: Arc::new(Mutex::new(vec![])),
            mark_processing_batches: Arc::new(Mutex::new(vec![])),
            mark_processing_batch_should_fail: Arc::new(Mutex::new(false)),
        }
    }
}

impl MockStore {
    fn marked_ids(&self) -> Vec<String> {
        self.marked_processing.lock().unwrap().clone()
    }

    fn mark_processing_direct_calls(&self) -> Vec<String> {
        self.mark_processing_calls.lock().unwrap().clone()
    }

    fn mark_processing_batch_calls(&self) -> Vec<Vec<String>> {
        self.mark_processing_batches.lock().unwrap().clone()
    }

    fn set_mark_processing_batch_fail(&self, fail: bool) {
        *self.mark_processing_batch_should_fail.lock().unwrap() = fail;
    }
}

#[async_trait]
impl InflightActivationStore for MockStore {
    async fn store(&self, _batch: Vec<InflightActivation>) -> anyhow::Result<u64> {
        Ok(0)
    }

    fn assign_partitions(&self, _partitions: Vec<i32>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn claim_activations(
        &self,
        _application: Option<&str>,
        _namespaces: Option<&[String]>,
        _limit: Option<i32>,
        _bucket: Option<crate::store::types::BucketRange>,
        _mark_processing: bool,
    ) -> anyhow::Result<Vec<InflightActivation>> {
        Ok(vec![])
    }

    async fn mark_processing(&self, id: &str) -> anyhow::Result<()> {
        self.mark_processing_calls
            .lock()
            .unwrap()
            .push(id.to_string());
        self.marked_processing.lock().unwrap().push(id.to_string());
        Ok(())
    }

    async fn mark_processing_batch(&self, ids: &[String]) -> anyhow::Result<u64> {
        if *self.mark_processing_batch_should_fail.lock().unwrap() {
            return Err(anyhow!("mock mark_processing_batch failure"));
        }

        self.mark_processing_batches
            .lock()
            .unwrap()
            .push(ids.to_vec());

        let mut guard = self.marked_processing.lock().unwrap();

        for id in ids {
            guard.push(id.clone());
        }

        Ok(ids.len() as u64)
    }

    async fn set_status(
        &self,
        _id: &str,
        _status: InflightActivationStatus,
    ) -> anyhow::Result<Option<InflightActivation>> {
        Ok(None)
    }

    async fn set_status_batch(
        &self,
        _ids: &[String],
        _status: InflightActivationStatus,
    ) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn pending_activation_max_lag(&self, _now: &DateTime<Utc>) -> f64 {
        0.0
    }

    async fn count_by_status(&self, _status: InflightActivationStatus) -> anyhow::Result<usize> {
        Ok(0)
    }

    async fn count(&self) -> anyhow::Result<usize> {
        Ok(0)
    }

    async fn get_by_id(&self, _id: &str) -> anyhow::Result<Option<InflightActivation>> {
        Ok(None)
    }

    async fn set_processing_deadline(
        &self,
        _id: &str,
        _deadline: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_activation(&self, _id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn vacuum_db(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn full_vacuum_db(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn db_size(&self) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn get_retry_activations(&self) -> anyhow::Result<Vec<InflightActivation>> {
        Ok(vec![])
    }

    async fn handle_claim_expiration(&self) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn handle_processing_deadline(&self) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn handle_processing_attempts(&self) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn handle_expires_at(&self) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn handle_delay_until(&self) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn handle_failed_tasks(&self) -> anyhow::Result<FailedTasksForwarder> {
        Ok(FailedTasksForwarder {
            to_discard: vec![],
            to_deadletter: vec![],
        })
    }

    async fn mark_completed(&self, _ids: Vec<String>) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn remove_completed(&self) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn remove_killswitched(&self, _killswitched_tasks: Vec<String>) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn clear(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Factory that fires `notify` when send() is called, then succeeds or fails per `should_fail`.
fn notifying_factory(should_fail: bool, notify: Arc<Notify>) -> WorkerFactory {
    Arc::new(move |_: String| {
        let notify = notify.clone();
        Box::pin(async move {
            Ok(Box::new(NotifyingWorkerClient {
                should_fail,
                notify,
            }) as Box<dyn WorkerClient + Send>)
        })
    })
}

/// Factory that always fails to connect (simulates a broken endpoint).
fn failing_connect_factory() -> WorkerFactory {
    Arc::new(|_: String| Box::pin(async { Err(anyhow::anyhow!("simulated connect failure")) }))
}

#[tokio::test]
async fn push_task_returns_ok_on_client_success() {
    let activation = make_activations(1).remove(0);
    let mut worker = MockWorkerClient::new(false);
    let callback_url = "taskbroker:50051".to_string();

    let result = push_task(
        &mut worker,
        activation.clone(),
        callback_url.clone(),
        Duration::from_secs(5),
        &[],
    )
    .await;
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
    let result = push_task(
        &mut worker,
        activation,
        "taskbroker:50051".to_string(),
        Duration::from_secs(5),
        &[],
    )
    .await;

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

    let result = push_task(
        &mut worker,
        activation,
        "taskbroker:50051".to_string(),
        Duration::from_secs(5),
        &[],
    )
    .await;
    assert!(result.is_err(), "worker send errors should propagate");
    assert_eq!(worker.captured_requests.len(), 1);
}

#[tokio::test]
async fn push_pool_submit_enqueues_item() {
    let config = Arc::new(Config {
        push_queue_size: 2,
        ..Config::default()
    });

    let store = create_test_store("sqlite").await;
    let pool = PushPool::new(config, store, None);
    let activation = make_activations(1).remove(0);

    let time = Instant::now();
    let result = pool.submit(activation, time).await;
    assert!(result.is_ok(), "submit should enqueue activation");
}

#[tokio::test]
async fn push_pool_submit_backpressures_when_queue_full() {
    let config = Arc::new(Config {
        push_queue_size: 1,
        ..Config::default()
    });

    let store = create_test_store("sqlite").await;
    let pool = PushPool::new(config, store, None);

    let time = Instant::now();
    let first = make_activations(1).remove(0);
    let second = make_activations(1).remove(0);

    pool.submit(first, time)
        .await
        .expect("first submit should fill queue");

    let second_submit = timeout(Duration::from_millis(50), pool.submit(second, time)).await;
    assert!(
        second_submit.is_err(),
        "second submit should block when queue is full"
    );
}

#[test]
fn sentry_signature_hex_matches_hmac_contract() {
    let digest = sentry_signature_hex("super secret", "/test/path", b"hello");
    assert_eq!(
        digest,
        "6408482d9e6d4975ada4c0302fda813c5718e571e6f9a2d6e2803cb48528044e"
    );
}

/// When the worker factory fails to connect, start() returns an error immediately.
#[tokio::test]
async fn push_pool_start_worker_connect_failure_returns_error() {
    let config = Arc::new(Config {
        worker_map: [("sentry".into(), "unused".into())].into(),
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = PushPool::new_with_factory(config, store, failing_connect_factory(), None);

    let result = pool.start().await;
    assert!(
        result.is_err(),
        "start() should return Err when the worker factory fails to connect"
    );
}

/// After a successful push for a first-attempt activation (processing_attempts == 0),
/// mark_processing must be called on the store.
#[tokio::test]
async fn push_pool_start_marks_activation_processing_on_first_attempt() {
    let notify = Arc::new(Notify::new());
    let config = Arc::new(Config {
        worker_map: [("sentry".into(), "unused".into())].into(),
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = Arc::new(PushPool::new_with_factory(
        config,
        store.clone(),
        notifying_factory(false, notify.clone()),
        None,
    ));

    let pool_start = pool.clone();
    tokio::spawn(async move { pool_start.start().await });

    let activation = make_activations(1).remove(0);
    assert_eq!(activation.processing_attempts, 0);

    let id = activation.id.clone();
    let time = Instant::now();

    pool.submit(activation, time)
        .await
        .expect("submit should succeed");

    // Wait for the worker to call send(), then give it time to call mark_processing
    timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect("timed out waiting for push to be delivered");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        store.marked_ids(),
        vec![id],
        "mark_processing should be called after a successful first-attempt push"
    );
}

/// After a successful push for a retried activation (processing_attempts > 0),
/// mark_processing must be called and latency recording is skipped.
#[tokio::test]
async fn push_pool_start_marks_activation_processing_on_retry() {
    let notify = Arc::new(Notify::new());
    let config = Arc::new(Config {
        worker_map: [("sentry".into(), "unused".into())].into(),
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = Arc::new(PushPool::new_with_factory(
        config,
        store.clone(),
        notifying_factory(false, notify.clone()),
        None,
    ));

    let pool_start = pool.clone();
    tokio::spawn(async move { pool_start.start().await });

    let mut activation = make_activations(1).remove(0);
    activation.processing_attempts = 1;

    let id = activation.id.clone();
    let time = Instant::now();

    pool.submit(activation, time)
        .await
        .expect("submit should succeed");

    timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect("timed out waiting for push to be delivered");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        store.marked_ids(),
        vec![id],
        "mark_processing should be called after a successful retry push"
    );
}

/// When the worker fails to deliver an activation, mark_processing must NOT be called.
#[tokio::test]
async fn push_pool_start_does_not_mark_processing_on_push_failure() {
    let notify = Arc::new(Notify::new());
    let config = Arc::new(Config {
        worker_map: [("sentry".into(), "unused".into())].into(),
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = Arc::new(PushPool::new_with_factory(
        config,
        store.clone(),
        notifying_factory(true, notify.clone()),
        None,
    ));

    let pool_start = pool.clone();
    tokio::spawn(async move { pool_start.start().await });

    let activation = make_activations(1).remove(0);
    let time = Instant::now();

    pool.submit(activation, time)
        .await
        .expect("submit should succeed");

    timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect("timed out waiting for push attempt");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        store.marked_ids().is_empty(),
        "mark_processing should not be called when push fails"
    );
}

/// With `update_tx` set, a successful push on the main loop enqueues the task ID on the channel.
/// Shutdown drain does not use batching - it applies `mark_processing` per activation, so this test
/// does not assert on direct `mark_processing` calls (those can appear only from drain under shutdown).
#[tokio::test]
async fn push_pool_forwards_successful_push_to_update_channel() {
    let notify = Arc::new(Notify::new());
    let (update_tx, mut update_rx) = mpsc::channel::<String>(8);

    let config = Arc::new(Config {
        worker_map: [("sentry".into(), "unused".into())].into(),
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = Arc::new(PushPool::new_with_factory(
        config,
        store.clone(),
        notifying_factory(false, notify.clone()),
        Some(update_tx),
    ));

    let pool_start = pool.clone();
    tokio::spawn(async move { pool_start.start().await });

    let activation = make_activations(1).remove(0);
    let id = activation.id.clone();
    let time = Instant::now();

    pool.submit(activation, time)
        .await
        .expect("Submit should succeed");

    timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect("Timed out waiting for push to be delivered");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        store.mark_processing_batch_calls().is_empty(),
        "Method `mark_processing_batch` runs only via `flush_updates`, not the push worker"
    );

    let ch_id = update_rx
        .recv()
        .await
        .expect("Task ID should be sent on update channel");
    assert_eq!(ch_id, id);
}

/// Function `flush_updates` drains the buffer into `mark_processing_batch` and clears the buffer.
#[tokio::test]
async fn flush_updates_applies_batch_and_clears_buffer() {
    let store = Arc::new(MockStore::default());
    let mut buf = vec!["id_0".to_string()];

    flush_updates(store.clone(), &mut buf).await;

    assert!(
        buf.is_empty(),
        "buffer should be cleared after successful flush"
    );
    assert!(store.mark_processing_direct_calls().is_empty());
    assert_eq!(
        store.mark_processing_batch_calls(),
        vec![vec!["id_0".to_string()]]
    );
    assert_eq!(store.marked_ids(), vec!["id_0".to_string()]);
}

/// On `mark_processing_batch` error, `flush_updates` restores IDs into the buffer for retry.
#[tokio::test]
async fn flush_updates_restores_buffer_on_batch_error() {
    let store = Arc::new(MockStore::default());
    store.set_mark_processing_batch_fail(true);

    let mut buf = vec!["a".to_string(), "b".to_string()];
    flush_updates(store.clone(), &mut buf).await;

    assert_eq!(buf, vec!["a".to_string(), "b".to_string()]);
    assert!(store.mark_processing_batch_calls().is_empty());
    assert!(store.marked_ids().is_empty());
}

/// After a successful worker push, a closed `update_tx` receiver means the main loop cannot enqueue
/// the id (no `mark_processing` there). Shutdown drain still uses individual `mark_processing` for
/// any remaining queue items, so this test does not assert the mock store stayed untouched.
#[tokio::test]
async fn push_pool_does_not_fallback_to_mark_processing_when_update_channel_closed() {
    let notify = Arc::new(Notify::new());
    let (update_tx, update_rx) = mpsc::channel::<String>(8);
    drop(update_rx);

    let config = Arc::new(Config {
        worker_map: [("sentry".into(), "unused".into())].into(),
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = Arc::new(PushPool::new_with_factory(
        config,
        store.clone(),
        notifying_factory(false, notify.clone()),
        Some(update_tx),
    ));

    let pool_start = pool.clone();
    tokio::spawn(async move { pool_start.start().await });

    let activation = make_activations(1).remove(0);
    let time = Instant::now();

    pool.submit(activation, time)
        .await
        .expect("Submit should succeed");

    timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect("Timed out waiting for push to be delivered");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(store.mark_processing_batch_calls().is_empty());
}
