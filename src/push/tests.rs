use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use prost::Message;
use sentry_protos::taskbroker::v1::TaskActivation;
use sha2::Sha256;
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

use crate::config::Config;
use crate::store::activation::{InflightActivation, InflightActivationStatus};
use crate::store::traits::InflightActivationStore;
use crate::store::types::FailedTasksForwarder;
use crate::test_utils::{create_test_store, make_activations};
use crate::worker::{WorkerClient, WorkerMap};

use super::PushPool;

/// Fake worker client that records pushed activation IDs and optionally fails.
struct MockWorkerClient {
    /// Pushed activation IDs.
    pushed: Vec<String>,

    /// Should `push_task` fail?
    fail: bool,
}

impl MockWorkerClient {
    fn new(fail: bool) -> Self {
        Self {
            pushed: vec![],
            fail,
        }
    }
}

#[async_trait]
impl WorkerClient for MockWorkerClient {
    async fn push_task(&mut self, activation: InflightActivation) -> Result<()> {
        TaskActivation::decode(&activation.activation as &[u8]).map_err(|e| anyhow!(e))?;
        self.pushed.push(activation.id);

        if self.fail {
            return Err(anyhow!("mock send failure"));
        }

        Ok(())
    }
}

/// Fake worker client that fires a `Notify` when `push_task` is called.
struct NotifyingWorkerClient {
    /// Fire off notification when `push_task` is called
    notify: Arc<Notify>,

    /// Should `push_task` fail?
    fail: bool,
}

#[async_trait]
impl WorkerClient for NotifyingWorkerClient {
    async fn push_task(&mut self, _activation: InflightActivation) -> Result<()> {
        self.notify.notify_one();

        if self.fail {
            return Err(anyhow!("mock send failure"));
        }

        Ok(())
    }
}

/// Create a map of notifying worker clients for tests.
fn test_worker_map(fail: bool, notify: Arc<Notify>) -> WorkerMap {
    let mut workers = HashMap::new();

    let client = NotifyingWorkerClient { fail, notify };

    workers.insert("sentry".into(), Box::new(client) as Box<dyn WorkerClient>);
    workers
}

/// Minimal fake store that records which activation IDs have been marked as processing.
#[derive(Clone)]
struct MockStore {
    marked_processing: Arc<Mutex<Vec<String>>>,
}

impl Default for MockStore {
    fn default() -> Self {
        Self {
            marked_processing: Arc::new(Mutex::new(vec![])),
        }
    }
}

impl MockStore {
    fn marked_ids(&self) -> Vec<String> {
        self.marked_processing.lock().unwrap().clone()
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
        self.marked_processing.lock().unwrap().push(id.to_string());
        Ok(())
    }

    async fn mark_processing_batch(&self, ids: &[String]) -> anyhow::Result<u64> {
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

// --- PushPool tests ---

#[tokio::test]
async fn push_pool_push_task_enqueues_item() {
    let config = Arc::new(Config {
        push_queue_size: 2,
        ..Config::default()
    });

    let store = create_test_store("sqlite").await;
    let pool = PushPool::new(config, store);
    let activation = make_activations(1).remove(0);

    let time = Instant::now();
    let result = pool.push_task(activation, time).await;
    assert!(result.is_ok(), "push_task should enqueue activation");
}

#[tokio::test]
async fn push_pool_push_task_backpressures_when_queue_full() {
    let config = Arc::new(Config {
        push_queue_size: 1,
        ..Config::default()
    });

    let store = create_test_store("sqlite").await;
    let pool = PushPool::new(config, store);

    let time = Instant::now();
    let first = make_activations(1).remove(0);
    let second = make_activations(1).remove(0);

    pool.push_task(first, time)
        .await
        .expect("first push_task should fill queue");

    let second_push = timeout(Duration::from_millis(50), pool.push_task(second, time)).await;
    assert!(
        second_push.is_err(),
        "second push_task should time out when queue is full"
    );
}

#[tokio::test]
async fn push_pool_start_marks_activation_processing_on_first_attempt() {
    let notify = Arc::new(Notify::new());
    let config = Arc::new(Config {
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = Arc::new(PushPool::new(config, store.clone()));

    let workers = vec![test_worker_map(false, notify.clone())];
    let pool_start = pool.clone();
    tokio::spawn(async move {
        pool_start.start(workers).await.expect("push pool start");
    });

    let activation = make_activations(1).remove(0);
    assert_eq!(activation.processing_attempts, 0);

    let id = activation.id.clone();
    let time = Instant::now();

    pool.push_task(activation, time)
        .await
        .expect("push_task should succeed");

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

#[tokio::test]
async fn push_pool_start_marks_activation_processing_on_retry() {
    let notify = Arc::new(Notify::new());
    let config = Arc::new(Config {
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = Arc::new(PushPool::new(config, store.clone()));

    let workers = vec![test_worker_map(false, notify.clone())];
    let pool_start = pool.clone();
    tokio::spawn(async move {
        pool_start.start(workers).await.expect("push pool start");
    });

    let mut activation = make_activations(1).remove(0);
    activation.processing_attempts = 1;

    let id = activation.id.clone();
    let time = Instant::now();

    pool.push_task(activation, time)
        .await
        .expect("push_task should succeed");

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

#[tokio::test]
async fn push_pool_start_does_not_mark_processing_on_push_failure() {
    let notify = Arc::new(Notify::new());
    let config = Arc::new(Config {
        push_threads: 1,
        push_queue_size: 10,
        ..Config::default()
    });
    let store = Arc::new(MockStore::default());
    let pool = Arc::new(PushPool::new(config, store.clone()));

    let workers = vec![test_worker_map(true, notify.clone())];
    let pool_start = pool.clone();
    tokio::spawn(async move {
        pool_start.start(workers).await.expect("push pool start");
    });

    let activation = make_activations(1).remove(0);
    let time = Instant::now();

    pool.push_task(activation, time)
        .await
        .expect("push_task should succeed");

    timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect("timed out waiting for push attempt");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        store.marked_ids().is_empty(),
        "mark_processing should not be called when push fails"
    );
}
