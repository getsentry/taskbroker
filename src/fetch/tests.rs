use std::sync::Arc;

use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tonic::async_trait;

use super::*;
use crate::config::Config;
use crate::push::PushError;
use crate::store::inflight_activation::InflightActivation;
use crate::store::inflight_activation::InflightActivationStore;
use crate::store::inflight_activation::{
    FailedTasksForwarder, InflightActivationStatus, QueryResult,
};
use crate::test_utils::make_activations;

/// Store stub that returns one activation once OR is always empty OR always fails.
struct MockStore {
    /// A single (optional) pending activation.
    pending: Mutex<Option<InflightActivation>>,

    /// Should operations fail?
    fail: bool,
}

impl MockStore {
    fn empty() -> Self {
        Self {
            pending: Mutex::new(None),
            fail: false,
        }
    }

    fn one(activation: InflightActivation) -> Self {
        Self {
            pending: Mutex::new(Some(activation)),
            fail: false,
        }
    }

    fn error() -> Self {
        Self {
            pending: Mutex::new(None),
            fail: true,
        }
    }
}

#[async_trait]
impl InflightActivationStore for MockStore {
    async fn vacuum_db(&self) -> Result<(), Error> {
        unimplemented!()
    }

    async fn full_vacuum_db(&self) -> Result<(), Error> {
        unimplemented!()
    }

    async fn db_size(&self) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn get_by_id(&self, _id: &str) -> Result<Option<InflightActivation>, Error> {
        unimplemented!()
    }

    async fn store(&self, _batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        unimplemented!()
    }

    async fn get_pending_activation(
        &self,
        _application: Option<&str>,
        _namespaces: Option<&[String]>,
    ) -> Result<Option<InflightActivation>, Error> {
        if self.fail {
            return Err(anyhow!("mock store error"));
        }

        Ok(self.pending.lock().await.take())
    }

    async fn get_pending_activations(
        &self,
        _application: Option<&str>,
        _namespaces: Option<&[String]>,
        _limit: Option<i32>,
    ) -> Result<Vec<InflightActivation>, Error> {
        unimplemented!()
    }

    async fn pending_activation_max_lag(&self, _now: &DateTime<Utc>) -> f64 {
        unimplemented!()
    }

    async fn count_by_status(&self, _status: InflightActivationStatus) -> Result<usize, Error> {
        unimplemented!()
    }

    async fn count(&self) -> Result<usize, Error> {
        unimplemented!()
    }

    async fn set_status(
        &self,
        _id: &str,
        _status: InflightActivationStatus,
    ) -> Result<Option<InflightActivation>, Error> {
        unimplemented!()
    }

    async fn set_processing_deadline(
        &self,
        _id: &str,
        _deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    async fn delete_activation(&self, _id: &str) -> Result<(), Error> {
        unimplemented!()
    }

    async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        unimplemented!()
    }

    async fn clear(&self) -> Result<(), Error> {
        unimplemented!()
    }

    async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn handle_processing_attempts(&self) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn handle_expires_at(&self) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn handle_delay_until(&self) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
        unimplemented!()
    }

    async fn mark_completed(&self, _ids: Vec<String>) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn remove_completed(&self) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn remove_killswitched(&self, _killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        unimplemented!()
    }
}

/// Records task IDs passed to `push_task`. If `fail` is true, returns an error.
struct RecordingPusher {
    /// What IDs have been pushed?
    pushed_ids: Mutex<Vec<String>>,

    /// Should pushing fail?
    fail: bool,
}

impl RecordingPusher {
    fn new(fail: bool) -> Self {
        let pushed_ids = Mutex::new(vec![]);
        Self { pushed_ids, fail }
    }
}

#[async_trait]
impl TaskPusher for RecordingPusher {
    async fn push_task(&self, activation: InflightActivation) -> Result<(), PushError> {
        self.pushed_ids.lock().await.push(activation.id.clone());

        if self.fail {
            return Err(PushError::Timeout);
        }

        Ok(())
    }
}

fn test_config() -> Arc<Config> {
    Arc::new(Config {
        fetch_threads: 1,
        fetch_wait_ms: 5,
        ..Config::default()
    })
}

#[tokio::test]
async fn fetch_pool_delivers_activation_to_pusher() {
    let activation = make_activations(1).remove(0);
    let store: Arc<dyn InflightActivationStore> = Arc::new(MockStore::one(activation.clone()));
    let pusher = Arc::new(RecordingPusher::new(false));

    let pool = FetchPool::new(store, test_config(), pusher.clone());
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(200)).await;
    assert_eq!(pusher.pushed_ids.lock().await.as_slice(), &[activation.id]);

    handle.abort();
}

#[tokio::test]
async fn fetch_pool_calls_pusher_once_when_push_errors() {
    let activation = make_activations(1).remove(0);
    let store: Arc<dyn InflightActivationStore> = Arc::new(MockStore::one(activation));
    let pusher = Arc::new(RecordingPusher::new(true));

    let pool = FetchPool::new(store, test_config(), pusher.clone());
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(100)).await;
    assert_eq!(pusher.pushed_ids.lock().await.len(), 1);

    handle.abort();
}

#[tokio::test]
async fn fetch_pool_skips_pusher_when_store_errors() {
    let store: Arc<dyn InflightActivationStore> = Arc::new(MockStore::error());
    let pusher = Arc::new(RecordingPusher::new(false));

    let pool = FetchPool::new(store, test_config(), pusher.clone());
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(80)).await;
    assert!(pusher.pushed_ids.lock().await.is_empty());

    handle.abort();
}

#[tokio::test]
async fn fetch_pool_skips_pusher_when_no_pending() {
    let store: Arc<dyn InflightActivationStore> = Arc::new(MockStore::empty());
    let pusher = Arc::new(RecordingPusher::new(false));

    let pool = FetchPool::new(store, test_config(), pusher.clone());
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(80)).await;
    assert!(pusher.pushed_ids.lock().await.is_empty());

    handle.abort();
}
