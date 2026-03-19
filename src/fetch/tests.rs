use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};
use tonic::async_trait;

use super::*;
use crate::config::Config;
use crate::store::inflight_activation::InflightActivation;
use crate::store::inflight_activation::InflightActivationStore;
use crate::store::inflight_activation::{
    FailedTasksForwarder, InflightActivationStatus, QueryResult,
};
use crate::test_utils::make_activations;

#[allow(clippy::large_enum_variant)]
enum MockPendingResult {
    Some(InflightActivation),
    None,
    Err,
}

/// Fake store for testing.
struct MockStore {
    /// How should all calls to `get_pending_activation` respond?
    pending_result: MockPendingResult,

    /// How many calls to `get_pending_activation` have been performed?
    pending_calls: AtomicUsize,
}

impl MockStore {
    fn new(pending_result: MockPendingResult) -> Self {
        let pending_calls = AtomicUsize::new(0);

        Self {
            pending_result,
            pending_calls,
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
        _namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
        self.pending_calls.fetch_add(1, Ordering::SeqCst);
        match &self.pending_result {
            MockPendingResult::Some(activation) => Ok(Some(activation.clone())),
            MockPendingResult::None => Ok(None),
            MockPendingResult::Err => Err(anyhow!("mock store error")),
        }
    }

    async fn get_pending_activations_from_namespaces(
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

/// Fake push pool for testing.
struct MockTaskPusher {
    /// List of the IDs of all the activations that have been pushed.
    pushed_ids: Mutex<Vec<String>>,

    /// Should `push_task` fail?
    should_fail: bool,
}

impl MockTaskPusher {
    fn new(should_fail: bool) -> Self {
        let pushed_ids = Mutex::new(vec![]);

        Self {
            pushed_ids,
            should_fail,
        }
    }
}

#[async_trait]
impl TaskPusher for MockTaskPusher {
    async fn push_task(&self, activation: InflightActivation) -> Result<()> {
        self.pushed_ids.lock().await.push(activation.id);

        if self.should_fail {
            return Err(anyhow!("mock push error"));
        }

        Ok(())
    }
}

#[tokio::test]
async fn fetch_activation_submits_when_pending_exists() {
    let activation = make_activations(1).remove(0);
    let store: Arc<dyn InflightActivationStore> =
        Arc::new(MockStore::new(MockPendingResult::Some(activation.clone())));
    let pusher = Arc::new(MockTaskPusher::new(false));

    let found = fetch_activation(store, pusher.clone())
        .await
        .expect("fetch should succeed");
    assert!(
        found,
        "should return true when activation was found and submitted"
    );

    let pushed = pusher.pushed_ids.lock().await;
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0], activation.id);
}

#[tokio::test]
async fn fetch_activation_logs_submit_error_and_returns_err() {
    let activation = make_activations(1).remove(0);
    let store: Arc<dyn InflightActivationStore> =
        Arc::new(MockStore::new(MockPendingResult::Some(activation)));
    let pusher = Arc::new(MockTaskPusher::new(true));

    let result = fetch_activation(store, pusher.clone()).await;
    assert!(
        result.is_err(),
        "should return error when push fails (after reverting status to pending)"
    );

    let pushed = pusher.pushed_ids.lock().await;
    assert_eq!(
        pushed.len(),
        1,
        "should attempt one push before returning error"
    );
}

#[tokio::test]
async fn fetch_activation_no_pending_returns_false() {
    let store: Arc<dyn InflightActivationStore> = Arc::new(MockStore::new(MockPendingResult::None));
    let pusher = Arc::new(MockTaskPusher::new(false));

    let found = fetch_activation(store, pusher.clone())
        .await
        .expect("fetch should succeed");
    assert!(!found, "should return false when no activation is pending");

    let pushed = pusher.pushed_ids.lock().await;
    assert!(
        pushed.is_empty(),
        "should not push if no activation is pending"
    );
}

#[tokio::test]
async fn fetch_activation_store_error_returns_false() {
    let store: Arc<dyn InflightActivationStore> = Arc::new(MockStore::new(MockPendingResult::Err));
    let pusher = Arc::new(MockTaskPusher::new(false));

    let result = fetch_activation(store, pusher.clone()).await;
    assert!(
        result.is_err(),
        "should return error when pending activation lookup fails"
    );

    let pushed = pusher.pushed_ids.lock().await;
    assert!(
        pushed.is_empty(),
        "should not push when pending activation lookup fails"
    );
}

#[tokio::test]
async fn fetch_pool_start_spawns_at_least_one_worker_when_fetch_threads_zero() {
    let store: Arc<dyn InflightActivationStore> = Arc::new(MockStore::new(MockPendingResult::None));
    let pusher = Arc::new(MockTaskPusher::new(false));

    let config = Arc::new(Config {
        fetch_threads: 0,
        ..Config::default()
    });

    let pool = FetchPool::new(store, config, pusher);

    let result = timeout(Duration::from_millis(50), pool.start()).await;
    assert!(
        result.is_err(),
        "start() should not complete immediately when fetch_threads is 0 because .max(1) starts one worker loop"
    );
}
