use std::sync::Arc;
use std::time::Instant;

use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tonic::async_trait;

use crate::config::Config;
use crate::config::fetch::FetchConfig;
use crate::config::push::{PushConfig, PushQueueConfig};
use crate::store::activation::{Activation, ActivationStatus};
use crate::store::traits::ActivationStore;
use crate::store::types::{BucketRange, Claim, FailedTasksForwarder, TopicPartition};
use crate::test_utils::make_activations;

use super::*;

/// Store stub that returns one batch once OR is always empty OR always fails.
struct MockStore {
    /// Pending activations, all claimed by the first fetch.
    pending: Mutex<Vec<Activation>>,

    /// Every `set_status` call and released claim, in order.
    status_updates: Mutex<Vec<(String, ActivationStatus)>>,

    /// Should operations fail?
    fail: bool,
}

impl MockStore {
    fn empty() -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
            status_updates: Mutex::new(Vec::new()),
            fail: false,
        }
    }

    fn one(activation: Activation) -> Self {
        Self::many(vec![activation])
    }

    fn many(activations: Vec<Activation>) -> Self {
        Self {
            pending: Mutex::new(activations),
            status_updates: Mutex::new(Vec::new()),
            fail: false,
        }
    }

    fn error() -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
            status_updates: Mutex::new(Vec::new()),
            fail: true,
        }
    }
}

#[async_trait]
impl ActivationStore for MockStore {
    fn assign_partitions(&self, _partitions: &mut dyn Iterator<Item = TopicPartition>) {}

    fn revoke_partitions(&self, _partitions: &mut dyn Iterator<Item = TopicPartition>) {}

    fn owns_partition(&self, _partition: &TopicPartition) -> bool {
        true
    }

    async fn vacuum_db(&self) -> Result<(), Error> {
        unimplemented!()
    }

    async fn full_vacuum_db(&self) -> Result<(), Error> {
        unimplemented!()
    }

    async fn db_size(&self) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn get_by_id(&self, _id: &str) -> Result<Option<Activation>, Error> {
        unimplemented!()
    }

    async fn store(&self, _batch: &[Activation]) -> Result<u64, Error> {
        unimplemented!()
    }

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

        let claimed = self
            .pending
            .lock()
            .await
            .drain(..)
            .map(|mut a| {
                if mark_processing {
                    a.status = ActivationStatus::Processing;
                } else {
                    a.status = ActivationStatus::Claimed;
                    a.claim_expires_at = Some(Utc::now() + chrono::Duration::minutes(1));
                }
                a
            })
            .collect();

        Ok(claimed)
    }

    async fn mark_activation_processing(&self, _id: &str) -> Result<(), Error> {
        Ok(())
    }

    async fn mark_processing_batch(&self, _ids: &[String]) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn pending_activation_max_lag(&self, _now: &DateTime<Utc>) -> f64 {
        unimplemented!()
    }

    async fn count_by_status(&self, _status: ActivationStatus) -> Result<usize, Error> {
        unimplemented!()
    }

    async fn count(&self) -> Result<usize, Error> {
        unimplemented!()
    }

    async fn set_status(
        &self,
        id: &str,
        status: ActivationStatus,
        _max_attempts: Option<u32>,
        _delay_on_retry: Option<u64>,
    ) -> Result<Option<Activation>, Error> {
        if self.fail {
            return Err(anyhow!("mock store error"));
        }

        self.status_updates
            .lock()
            .await
            .push((id.to_owned(), status));

        Ok(None)
    }

    async fn release_claims(&self, claims: &[Claim]) -> Result<u64, Error> {
        if self.fail {
            return Err(anyhow!("mock store error"));
        }

        self.status_updates.lock().await.extend(
            claims
                .iter()
                .map(|claim| (claim.id.clone(), ActivationStatus::Pending)),
        );

        Ok(claims.len() as u64)
    }

    async fn set_status_batch(
        &self,
        _ids: &[String],
        _status: ActivationStatus,
    ) -> Result<u64, Error> {
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

    async fn delete_activation_batch(&self, _ids: &[String]) -> Result<u64, Error> {
        unimplemented!()
    }

    async fn get_retry_activations(&self) -> Result<Vec<Activation>, Error> {
        unimplemented!()
    }

    async fn clear(&self) -> Result<(), Error> {
        unimplemented!()
    }

    async fn handle_claim_expiration(&self) -> Result<u64, Error> {
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

    async fn remove_killswitched_selectors(
        &self,
        _selectors: Vec<crate::killswitch::KillswitchSelector>,
    ) -> Result<u64, Error> {
        unimplemented!()
    }
}

fn test_config() -> Arc<Config> {
    Arc::new(Config {
        fetch: FetchConfig {
            threads: 1,
            backoff: Duration::from_millis(5),
            ..Default::default()
        },
        ..Default::default()
    })
}

#[tokio::test]
async fn fetch_pool_fetch_and_enqueue() {
    let activation = make_activations(1).remove(0);
    let store: Arc<dyn ActivationStore> = Arc::new(MockStore::one(activation.clone()));

    let config = test_config();
    let (sender, receiver) = flume::bounded(config.push.queue.size);

    let pool = FetchPool::new(sender, store, config);
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(200)).await;
    assert_eq!(receiver.recv_async().await.unwrap().0.id, activation.id);

    handle.abort();
}

#[tokio::test]
async fn fetch_pool_store_error() {
    let store: Arc<dyn ActivationStore> = Arc::new(MockStore::error());

    let config = test_config();
    let (sender, receiver) = flume::bounded(config.push.queue.size);

    let pool = FetchPool::new(sender.clone(), store, config);
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(80)).await;
    assert!(receiver.is_empty());

    handle.abort();
}

#[tokio::test]
async fn fetch_pool_no_pending() {
    let store: Arc<dyn ActivationStore> = Arc::new(MockStore::empty());

    let config = test_config();
    let (sender, receiver) = flume::bounded(config.push.queue.size);

    let pool = FetchPool::new(sender, store, config);
    let handle = tokio::spawn(async move { pool.start().await });

    sleep(Duration::from_millis(80)).await;
    assert!(receiver.is_empty());

    handle.abort();
}

/// Build a single fetch thread covering every bucket.
///
/// The claim tests drive `fetch_once` directly rather than `FetchPool::start`,
/// because `start` registers a process-wide `elegant_departure` shutdown guard.
/// Any test that lets a fetch loop exit or aborts it trips that guard for the
/// whole test binary, after which later fetch loops return without doing work.
fn fetch_thread(
    store: Arc<dyn ActivationStore>,
    sender: Sender<(Activation, Instant)>,
    config: Arc<Config>,
) -> FetchThread {
    FetchThread {
        sender,
        store,
        config,
        bucket: bucket_range_for_fetch_thread(0, 1),
    }
}

/// A submit timeout leaves the activation claimed in the store with nothing
/// holding it, so the fetch thread has to release it itself. Otherwise the row
/// waits for `handle_claim_expiration`, whose lease is sized for the worst case
/// push and can strand a single activation for minutes.
#[tokio::test]
async fn fetch_once_undoes_claim_on_submit_timeout() {
    let mut activations = make_activations(2);
    let filler = activations.remove(0);
    let claimed = activations.remove(0);

    let store = Arc::new(MockStore::one(claimed.clone()));

    let config = Arc::new(Config {
        push: PushConfig {
            queue: PushQueueConfig {
                size: 1,
                timeout: Duration::from_millis(20),
            },
            ..Default::default()
        },
        ..Default::default()
    });

    let (sender, receiver) = flume::bounded(config.push.queue.size);

    // Occupy the only queue slot so the submit cannot complete
    sender
        .send_async((filler, Instant::now()))
        .await
        .expect("an empty queue should accept one activation");

    let mut thread = fetch_thread(store.clone(), sender, config);

    // A submit timeout is recoverable, so the loop keeps running
    assert!(thread.fetch_once(Duration::from_millis(0)).await);

    assert_eq!(
        vec![(claimed.id.clone(), ActivationStatus::Pending)],
        *store.status_updates.lock().await
    );

    // Only the filler ever reached the push pool
    assert_eq!(1, receiver.len());
}

/// A slot opening mid-wait lets the submit through, so no claim is released.
#[tokio::test]
async fn fetch_once_submits_when_queue_drains() {
    let mut activations = make_activations(2);
    let filler = activations.remove(0);
    let claimed = activations.remove(0);

    let store = Arc::new(MockStore::one(claimed.clone()));

    let config = Arc::new(Config {
        push: PushConfig {
            queue: PushQueueConfig {
                size: 1,
                timeout: Duration::from_millis(500),
            },
            ..Default::default()
        },
        ..Default::default()
    });

    let (sender, receiver) = flume::bounded(config.push.queue.size);

    // Occupy the only queue slot so the first submit attempt is refused
    sender
        .send_async((filler, Instant::now()))
        .await
        .expect("an empty queue should accept one activation");

    // Free the slot partway through the submit window
    let drain = tokio::spawn({
        let receiver = receiver.clone();

        async move {
            sleep(Duration::from_millis(50)).await;
            receiver.recv_async().await.expect("the filler is queued");
        }
    });

    let mut thread = fetch_thread(store.clone(), sender, config);

    assert!(thread.fetch_once(Duration::from_millis(0)).await);
    drain.await.expect("drain task should finish");

    // The activation reached the push pool, so its claim still stands
    assert!(store.status_updates.lock().await.is_empty());

    assert_eq!(claimed.id, receiver.recv_async().await.unwrap().0.id);
}

/// Same contract on the path where the push pool has gone away.
#[tokio::test]
async fn fetch_once_undoes_claim_on_closed_queue() {
    let claimed = make_activations(1).remove(0);
    let store = Arc::new(MockStore::one(claimed.clone()));

    let config = test_config();
    let (sender, receiver) = flume::bounded(config.push.queue.size);

    // Dropping the receiving end makes the submit fail immediately
    drop(receiver);

    let mut thread = fetch_thread(store.clone(), sender, config);

    // A closed queue is unrecoverable, so the loop stops
    assert!(!thread.fetch_once(Duration::from_millis(0)).await);

    assert_eq!(
        vec![(claimed.id.clone(), ActivationStatus::Pending)],
        *store.status_updates.lock().await
    );
}

/// A timeout means the queue stayed full for the whole window, so the rest of
/// the batch would only wait out the same timeout one by one. Release the whole
/// batch in one go and back off instead.
#[tokio::test]
async fn fetch_once_releases_rest_of_batch_on_submit_timeout() {
    let mut activations = make_activations(4);
    let filler = activations.remove(0);
    let claimed = activations;

    let store = Arc::new(MockStore::many(claimed.clone()));

    let config = Arc::new(Config {
        push: PushConfig {
            queue: PushQueueConfig {
                size: 1,
                timeout: Duration::from_millis(20),
            },
            ..Default::default()
        },
        ..Default::default()
    });

    let (sender, receiver) = flume::bounded(config.push.queue.size);

    // Occupy the only queue slot so the first submit cannot complete
    sender
        .send_async((filler, Instant::now()))
        .await
        .expect("an empty queue should accept one activation");

    let mut thread = fetch_thread(store.clone(), sender, config);

    let start = Instant::now();
    assert!(thread.fetch_once(Duration::from_millis(0)).await);

    // Only the first activation waited out the timeout
    assert!(start.elapsed() < Duration::from_millis(60));

    let expected: Vec<_> = claimed
        .iter()
        .map(|a| (a.id.clone(), ActivationStatus::Pending))
        .collect();
    assert_eq!(expected, *store.status_updates.lock().await);

    assert_eq!(1, receiver.len());
}

/// A closed queue stops the fetch loop, so every activation left in the batch
/// has to be released on the way out rather than stranded until lease expiry.
#[tokio::test]
async fn fetch_once_releases_rest_of_batch_on_closed_queue() {
    let claimed = make_activations(3);
    let store = Arc::new(MockStore::many(claimed.clone()));

    let config = test_config();
    let (sender, receiver) = flume::bounded(config.push.queue.size);

    // Dropping the receiving end makes the submit fail immediately
    drop(receiver);

    let mut thread = fetch_thread(store.clone(), sender, config);

    assert!(!thread.fetch_once(Duration::from_millis(0)).await);

    let expected: Vec<_> = claimed
        .iter()
        .map(|a| (a.id.clone(), ActivationStatus::Pending))
        .collect();
    assert_eq!(expected, *store.status_updates.lock().await);
}
