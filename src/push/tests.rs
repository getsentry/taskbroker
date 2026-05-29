use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

use crate::config::Config;
use crate::store::activation::{Activation, ActivationStatus};
use crate::store::traits::ActivationStore;
use crate::store::types::FailedTasksForwarder;
use crate::test_utils::{create_test_store, make_activations};
use crate::worker::test_worker_map;

use super::*;

/// Minimal fake store that records which activation IDs have been marked as processing.
#[derive(Default, Clone)]
struct MockStore {
    marked_processing: Arc<Mutex<Vec<String>>>,
}

impl MockStore {
    fn marked_ids(&self) -> Vec<String> {
        self.marked_processing.lock().unwrap().clone()
    }
}

#[async_trait]
impl ActivationStore for MockStore {
    async fn store(&self, _batch: Vec<Activation>) -> anyhow::Result<u64> {
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
    ) -> anyhow::Result<Vec<Activation>> {
        Ok(vec![])
    }
    async fn mark_activation_processing(&self, id: &str) -> anyhow::Result<()> {
        self.marked_processing.lock().unwrap().push(id.to_string());
        Ok(())
    }
    async fn set_status(
        &self,
        _id: &str,
        _status: ActivationStatus,
        _max_attempts: Option<u32>,
        _delay_on_retry: Option<u64>,
    ) -> anyhow::Result<Option<Activation>> {
        Ok(None)
    }
    async fn set_status_batch(
        &self,
        _ids: &[String],
        _status: ActivationStatus,
    ) -> anyhow::Result<u64> {
        Ok(0)
    }
    async fn pending_activation_max_lag(&self, _now: &DateTime<Utc>) -> f64 {
        0.0
    }
    async fn count_by_status(&self, _status: ActivationStatus) -> anyhow::Result<usize> {
        Ok(0)
    }
    async fn count(&self) -> anyhow::Result<usize> {
        Ok(0)
    }
    async fn get_by_id(&self, _id: &str) -> anyhow::Result<Option<Activation>> {
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
    async fn get_retry_activations(&self) -> anyhow::Result<Vec<Activation>> {
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

#[tokio::test]
async fn push_pool_submit_enqueues_item() {
    let config = Arc::new(Config {
        push_queue_size: 2,
        ..Config::default()
    });

    let store = create_test_store("sqlite").await;
    let pool = PushPool::new(config, store);
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
    let pool = PushPool::new(config, store);

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

/// After a successful push for a first-attempt activation (processing_attempts == 0),
/// mark_activation_processing must be called on the store.
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
    let pool = Arc::new(PushPool::new(config, store.clone()));

    let pool_start = pool.clone();
    let worker_notify = notify.clone();
    tokio::spawn(async move {
        pool_start
            .start(vec![test_worker_map(false, worker_notify)])
            .await
    });

    let activation = make_activations(1).remove(0);
    assert_eq!(activation.processing_attempts, 0);

    let id = activation.id.clone();
    let time = Instant::now();

    pool.submit(activation, time)
        .await
        .expect("submit should succeed");

    // Wait for the worker to call push_task(), then give it time to call mark_activation_processing.
    timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect("timed out waiting for push to be delivered");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        store.marked_ids(),
        vec![id],
        "mark_activation_processing should be called after a successful first-attempt push"
    );
}

/// After a successful push for a retried activation (processing_attempts > 0),
/// mark_activation_processing must be called and latency recording is skipped.
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
    let pool = Arc::new(PushPool::new(config, store.clone()));

    let pool_start = pool.clone();
    let worker_notify = notify.clone();
    tokio::spawn(async move {
        pool_start
            .start(vec![test_worker_map(false, worker_notify)])
            .await
    });

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
        "mark_activation_processing should be called after a successful retry push"
    );
}

/// When the worker fails to deliver an activation, mark_activation_processing must NOT be called.
#[tokio::test]
async fn push_pool_start_does_not_mark_activation_processing_on_push_failure() {
    let notify = Arc::new(Notify::new());
    let config = Arc::new(Config {
        worker_map: [("sentry".into(), "unused".into())].into(),
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = Arc::new(PushPool::new(config, store.clone()));

    let pool_start = pool.clone();
    let worker_notify = notify.clone();
    tokio::spawn(async move {
        pool_start
            .start(vec![test_worker_map(true, worker_notify)])
            .await
    });

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
        "mark_activation_processing should not be called when push fails"
    );
}
