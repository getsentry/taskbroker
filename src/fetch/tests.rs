use std::sync::Arc;

use anyhow::{Error, anyhow};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tonic::async_trait;

use super::*;
use crate::config::Config;
use crate::push::PushError;
use crate::store::activation::{Activation, ActivationStatus};
use crate::store::traits::ClaimStore;
use crate::store::types::BucketRange;
use crate::test_utils::make_activations;

/// Store stub that returns one activation once OR is always empty OR always fails.
struct MockStore {
    /// A single (optional) pending activation.
    pending: Mutex<Option<Activation>>,

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

    fn one(activation: Activation) -> Self {
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
impl ClaimStore for MockStore {
    async fn claim_activations(
        &self,
        _application: Option<&str>,
        _namespaces: Option<&[String]>,
        _limit: Option<i32>,
        _bucket: Option<BucketRange>,
        mark_processing: bool,
    ) -> Result<Vec<Activation>, Error> {
        if self.fail {
            return Err(anyhow!("mock store error"));
        }

        Ok(match self.pending.lock().await.take() {
            Some(mut a) => {
                a.status = if mark_processing {
                    ActivationStatus::Processing
                } else {
                    ActivationStatus::Claimed
                };
                vec![a]
            }
            None => vec![],
        })
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
    async fn submit_task(&self, activation: Activation) -> Result<(), PushError> {
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
    let store = Arc::new(MockStore::one(activation.clone()));
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
    let store = Arc::new(MockStore::one(activation));
    let pusher = Arc::new(RecordingPusher::new(true));

    let pool = FetchPool::new(store, test_config(), pusher.clone());
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(100)).await;
    assert_eq!(pusher.pushed_ids.lock().await.len(), 1);

    handle.abort();
}

#[tokio::test]
async fn fetch_pool_skips_pusher_when_store_errors() {
    let store = Arc::new(MockStore::error());
    let pusher = Arc::new(RecordingPusher::new(false));

    let pool = FetchPool::new(store, test_config(), pusher.clone());
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(80)).await;
    assert!(pusher.pushed_ids.lock().await.is_empty());

    handle.abort();
}

#[tokio::test]
async fn fetch_pool_skips_pusher_when_no_pending() {
    let store = Arc::new(MockStore::empty());
    let pusher = Arc::new(RecordingPusher::new(false));

    let pool = FetchPool::new(store, test_config(), pusher.clone());
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(80)).await;
    assert!(pusher.pushed_ids.lock().await.is_empty());

    handle.abort();
}
