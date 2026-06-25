use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

use crate::config::Config;
use crate::config::push::{PushConfig, PushQueueConfig};
use crate::push::updater::test_eager_updater;
use crate::store::activation::{Activation, ActivationStatus};
use crate::store::traits::ActivationStore;
use crate::store::types::{FailedTasksForwarder, TopicPartition};
use crate::test_utils::make_activations;
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
    async fn store(&self, _batch: &[Activation]) -> Result<u64> {
        Ok(0)
    }

    fn assign_partitions(&self, _partitions: &mut dyn Iterator<Item = TopicPartition>) {}

    fn revoke_partitions(&self, _partitions: &mut dyn Iterator<Item = TopicPartition>) {}

    fn owns_partition(&self, _partition: &TopicPartition) -> bool {
        true
    }

    async fn claim_activations(
        &self,
        _application: Option<&str>,
        _namespaces: Option<&[String]>,
        _limit: Option<i32>,
        _bucket: Option<crate::store::types::BucketRange>,
        _mark_processing: bool,
    ) -> Result<Vec<Activation>> {
        Ok(vec![])
    }

    async fn mark_activation_processing(&self, id: &str) -> Result<()> {
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
        _status: ActivationStatus,
        _max_attempts: Option<u32>,
        _delay_on_retry: Option<u64>,
    ) -> Result<Option<Activation>> {
        Ok(None)
    }

    async fn set_status_batch(&self, _ids: &[String], _status: ActivationStatus) -> Result<u64> {
        Ok(0)
    }

    async fn pending_activation_max_lag(&self, _now: &DateTime<Utc>) -> f64 {
        0.0
    }

    async fn count_by_status(&self, _status: ActivationStatus) -> Result<usize> {
        Ok(0)
    }

    async fn count(&self) -> Result<usize> {
        Ok(0)
    }

    async fn get_by_id(&self, _id: &str) -> Result<Option<Activation>> {
        Ok(None)
    }

    async fn set_processing_deadline(
        &self,
        _id: &str,
        _deadline: Option<DateTime<Utc>>,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_activation(&self, _id: &str) -> Result<()> {
        Ok(())
    }

    async fn delete_activation_batch(&self, _ids: &[String]) -> Result<u64> {
        Ok(0)
    }

    async fn vacuum_db(&self) -> Result<()> {
        Ok(())
    }

    async fn full_vacuum_db(&self) -> Result<()> {
        Ok(())
    }

    async fn db_size(&self) -> Result<u64> {
        Ok(0)
    }

    async fn get_retry_activations(&self) -> Result<Vec<Activation>> {
        Ok(vec![])
    }

    async fn handle_claim_expiration(&self) -> Result<u64> {
        Ok(0)
    }

    async fn handle_processing_deadline(&self) -> Result<u64> {
        Ok(0)
    }

    async fn handle_processing_attempts(&self) -> Result<u64> {
        Ok(0)
    }

    async fn handle_expires_at(&self) -> Result<u64> {
        Ok(0)
    }

    async fn handle_delay_until(&self) -> Result<u64> {
        Ok(0)
    }

    async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder> {
        Ok(FailedTasksForwarder {
            to_discard: vec![],
            to_deadletter: vec![],
        })
    }

    async fn mark_completed(&self, _ids: Vec<String>) -> Result<u64> {
        Ok(0)
    }

    async fn remove_completed(&self) -> Result<u64> {
        Ok(0)
    }

    async fn remove_killswitched(&self, _killswitched_tasks: Vec<String>) -> Result<u64> {
        Ok(0)
    }

    async fn clear(&self) -> Result<()> {
        Ok(())
    }
}

/// After a successful push for a first-attempt activation (processing_attempts == 0),
/// mark_activation_processing must be called on the store.
#[tokio::test]
async fn push_pool_start_marks_activation_processing_on_first_attempt() {
    let notify = Arc::new(Notify::new());

    let config = Arc::new(Config {
        worker_map: [("sentry".into(), "unused".into())].into(),
        push: PushConfig {
            threads: 1,
            queue: PushQueueConfig {
                size: 10,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    });

    let store = Arc::new(MockStore::default());
    let (sender, receiver) = flume::bounded(config.push.queue.size);
    let pool = Arc::new(PushPool::new(receiver, config));

    let workers = vec![test_worker_map(false, notify.clone())];
    let updater = test_eager_updater(store.clone());

    tokio::spawn({
        let store = store.clone();
        async move { pool.start(workers, updater, store.clone()).await }
    });

    let activation = make_activations(1).remove(0);
    assert_eq!(activation.processing_attempts, 0);

    let id = activation.id.clone();
    let time = Instant::now();

    // Simulate a fetch thread pushing an activation to the queue
    sender.send_async((activation, time)).await.unwrap();

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
        push: PushConfig {
            threads: 1,
            queue: PushQueueConfig {
                size: 10,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    });

    let store = Arc::new(MockStore::default());
    let (sender, receiver) = flume::bounded(config.push.queue.size);
    let pool = Arc::new(PushPool::new(receiver, config));

    let workers = vec![test_worker_map(false, notify.clone())];
    let updater = test_eager_updater(store.clone());

    tokio::spawn({
        let store = store.clone();
        async move { pool.start(workers, updater, store.clone()).await }
    });

    let mut activation = make_activations(1).remove(0);
    activation.processing_attempts = 1;

    let id = activation.id.clone();
    let time = Instant::now();

    // Simulate a fetch thread pushing an activation to the queue
    sender.send_async((activation, time)).await.unwrap();

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
        push: PushConfig {
            threads: 1,
            queue: PushQueueConfig {
                size: 10,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    });

    let store = Arc::new(MockStore::default());
    let (sender, receiver) = flume::bounded(config.push.queue.size);
    let pool = Arc::new(PushPool::new(receiver, config));

    let workers = vec![test_worker_map(true, notify.clone())];
    let updater = test_eager_updater(store.clone());

    tokio::spawn({
        let store = store.clone();
        async move { pool.start(workers, updater, store.clone()).await }
    });

    let activation = make_activations(1).remove(0);
    let time = Instant::now();

    // Simulate a fetch thread pushing an activation to the queue
    sender.send_async((activation, time)).await.unwrap();

    timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect("timed out waiting for push attempt");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        store.marked_ids().is_empty(),
        "mark_activation_processing should not be called when push fails"
    );
}
