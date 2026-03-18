use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use tokio::time::sleep;
use tonic::async_trait;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::push::PushPool;
use crate::store::inflight_activation::InflightActivation;
use crate::store::inflight_activation::InflightActivationStore;

/// Thin interface for the push pool. It mostly serves to enable proper unit testing, but it also decouples fetch logic from push logic even further.
#[async_trait]
pub trait TaskPusher {
    /// Push a single task to the worker service.
    async fn push_task(&self, activation: InflightActivation) -> Result<()>;
}

#[async_trait]
impl TaskPusher for PushPool {
    async fn push_task(&self, activation: InflightActivation) -> Result<()> {
        self.submit(activation).await
    }
}

/// Wrapper around `config.fetch_threads` asynchronous tasks, each of which fetches a pending activation from the store, passes is to the push pool, and repeats.
pub struct FetchPool<T: TaskPusher> {
    /// Inflight activation store.
    store: Arc<dyn InflightActivationStore>,

    /// Pool of push threads that push activations to the worker service.
    pusher: Arc<T>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl<T: TaskPusher + Send + Sync + 'static> FetchPool<T> {
    /// Initialize a new fetch pool.
    pub fn new(
        store: Arc<dyn InflightActivationStore>,
        config: Arc<Config>,
        pusher: Arc<T>,
    ) -> Self {
        Self {
            store,
            config,
            pusher,
        }
    }

    /// Spawn `config.fetch_threads` asynchronous tasks, each of which repeatedly moves pending activations from the store to the push pool until the shutdown signal is received.
    pub async fn start(&self) -> Result<()> {
        let mut handles = vec![];

        for _ in 0..self.config.fetch_threads.max(1) {
            let guard = get_shutdown_guard().shutdown_on_drop();

            let store = self.store.clone();
            let task_pusher = self.pusher.clone();

            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = guard.wait() => {
                            info!("Fetch loop received shutdown signal");
                            break;
                        }

                        _ = async {
                            debug!("About to fetch next activation...");
                            fetch_activations(store.clone(), task_pusher.clone()).await;
                        } => {}
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            if let Err(e) = handle.await {
                return Err(e.into());
            }
        }

        Ok(())
    }
}

/// Grab the next pending activation from the store, mark it as processing, and send to push channel.
pub async fn fetch_activations<T: TaskPusher>(
    store: Arc<dyn InflightActivationStore>,
    pusher: Arc<T>,
) {
    let start = Instant::now();
    metrics::counter!("fetch.fetch_activations.runs").increment(1);

    debug!("Fetching next pending activation...");

    match store.get_pending_activation(None, None).await {
        Ok(Some(activation)) => {
            let id = activation.id.clone();
            debug!("Atomically fetched and marked task {id} as processing");

            if let Err(e) = pusher.push_task(activation).await {
                error!("Failed to submit task {id} to push pool - {:?}", e);
            }

            metrics::histogram!("fetch.fetch_activations.duration").record(start.elapsed());
        }

        Ok(_) => {
            debug!("No pending activations, sleeping briefly...");
            sleep(Duration::from_millis(100)).await;

            metrics::histogram!("fetch.fetch_activations.duration").record(start.elapsed());
        }

        Err(e) => {
            error!("Failed to fetch pending activation - {:?}", e);
            sleep(Duration::from_millis(100)).await;

            metrics::histogram!("fetch.fetch_activations.duration").record(start.elapsed());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::{Error, anyhow};
    use chrono::{DateTime, Utc};
    use tokio::sync::Mutex;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::store::inflight_activation::{
        FailedTasksForwarder, InflightActivationStatus, QueryResult,
    };
    use crate::test_utils::make_activations;

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

        async fn remove_killswitched(
            &self,
            _killswitched_tasks: Vec<String>,
        ) -> Result<u64, Error> {
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
    async fn fetch_activations_submits_when_pending_exists() {
        let activation = make_activations(1).remove(0);
        let store: Arc<dyn InflightActivationStore> =
            Arc::new(MockStore::new(MockPendingResult::Some(activation.clone())));
        let pusher = Arc::new(MockTaskPusher::new(false));

        fetch_activations(store, pusher.clone()).await;

        let pushed = pusher.pushed_ids.lock().await;
        assert_eq!(pushed.len(), 1);
        assert_eq!(pushed[0], activation.id);
    }

    #[tokio::test]
    async fn fetch_activations_logs_submit_error_but_does_not_fail() {
        let activation = make_activations(1).remove(0);
        let store: Arc<dyn InflightActivationStore> =
            Arc::new(MockStore::new(MockPendingResult::Some(activation)));
        let pusher = Arc::new(MockTaskPusher::new(true));

        fetch_activations(store, pusher.clone()).await;

        let pushed = pusher.pushed_ids.lock().await;
        assert_eq!(pushed.len(), 1, "should attempt one push even if it fails");
    }

    #[tokio::test]
    async fn fetch_activations_no_pending_does_not_submit() {
        let store: Arc<dyn InflightActivationStore> =
            Arc::new(MockStore::new(MockPendingResult::None));
        let pusher = Arc::new(MockTaskPusher::new(false));

        fetch_activations(store, pusher.clone()).await;

        let pushed = pusher.pushed_ids.lock().await;
        assert!(
            pushed.is_empty(),
            "should not push if no activation is pending"
        );
    }

    #[tokio::test]
    async fn fetch_activations_store_error_does_not_submit() {
        let store: Arc<dyn InflightActivationStore> =
            Arc::new(MockStore::new(MockPendingResult::Err));
        let pusher = Arc::new(MockTaskPusher::new(false));

        fetch_activations(store, pusher.clone()).await;

        let pushed = pusher.pushed_ids.lock().await;
        assert!(
            pushed.is_empty(),
            "should not push when pending activation lookup fails"
        );
    }

    #[tokio::test]
    async fn fetch_pool_start_spawns_at_least_one_worker_when_fetch_threads_zero() {
        let store: Arc<dyn InflightActivationStore> =
            Arc::new(MockStore::new(MockPendingResult::None));
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
}
