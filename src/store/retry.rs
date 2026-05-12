use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::store::activation::{InflightActivation, InflightActivationStatus};
use crate::store::traits::InflightActivationStore;
use crate::store::types::{BucketRange, DepthCounts, FailedTasksForwarder};

const BASE_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 5000;

/// Returns true if the error message indicates a transient database error
/// that is likely to succeed on retry.
fn is_transient_error(err: &Error) -> bool {
    let msg = err.to_string();
    msg.contains("server conn crashed")
        || msg.contains("server shutting down")
        || msg.contains("connection reset")
        || msg.contains("broken pipe")
        || msg.contains("connection refused")
        || msg.contains("pool timed out")
}

/// Calculates the backoff duration for a given attempt using exponential backoff.
fn backoff_duration(attempt: u32) -> Duration {
    let ms = BASE_BACKOFF_MS
        .saturating_mul(1 << attempt)
        .min(MAX_BACKOFF_MS);
    Duration::from_millis(ms)
}

/// A wrapper around an `InflightActivationStore` that retries transient
/// database errors with exponential backoff.
pub struct RetryStore {
    inner: Arc<dyn InflightActivationStore>,
    max_retries: u32,
}

impl RetryStore {
    pub fn new(inner: Arc<dyn InflightActivationStore>, max_retries: u32) -> Self {
        Self { inner, max_retries }
    }
}

/// Macro to reduce boilerplate for delegating trait methods with retry logic.
/// For each method call, if the inner store returns a transient error,
/// we retry up to `self.max_retries` times with exponential backoff.
macro_rules! retry_method {
    ($self:ident, $method:ident ( $($arg:expr),* $(,)? )) => {{
        let mut attempt = 0u32;
        loop {
            match $self.inner.$method( $($arg),* ).await {
                Ok(val) => {
                    if attempt > 0 {
                        info!(
                            method = stringify!($method),
                            attempt,
                            "Query succeeded after retry"
                        );
                        metrics::counter!(
                            "store.retry.succeeded",
                            "method" => stringify!($method),
                        )
                        .increment(1);
                    }
                    return Ok(val);
                }
                Err(err) if attempt < $self.max_retries && is_transient_error(&err) => {
                    let backoff = backoff_duration(attempt);
                    warn!(
                        method = stringify!($method),
                        attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %err,
                        "Transient database error, retrying"
                    );
                    metrics::counter!(
                        "store.retry.attempt",
                        "method" => stringify!($method),
                    )
                    .increment(1);
                    sleep(backoff).await;
                    attempt += 1;
                }
                Err(err) => {
                    if attempt > 0 {
                        metrics::counter!(
                            "store.retry.exhausted",
                            "method" => stringify!($method),
                        )
                        .increment(1);
                    }
                    return Err(err);
                }
            }
        }
    }};
}

/// Same as `retry_method!` but clones an owned argument before each attempt,
/// since the inner method consumes it.
macro_rules! retry_method_clone {
    ($self:ident, $method:ident, $owned_arg:expr $(,)?) => {{
        let mut attempt = 0u32;
        loop {
            let cloned = $owned_arg.clone();
            match $self.inner.$method(cloned).await {
                Ok(val) => {
                    if attempt > 0 {
                        info!(
                            method = stringify!($method),
                            attempt,
                            "Query succeeded after retry"
                        );
                        metrics::counter!(
                            "store.retry.succeeded",
                            "method" => stringify!($method),
                        )
                        .increment(1);
                    }
                    return Ok(val);
                }
                Err(err) if attempt < $self.max_retries && is_transient_error(&err) => {
                    let backoff = backoff_duration(attempt);
                    warn!(
                        method = stringify!($method),
                        attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %err,
                        "Transient database error, retrying"
                    );
                    metrics::counter!(
                        "store.retry.attempt",
                        "method" => stringify!($method),
                    )
                    .increment(1);
                    sleep(backoff).await;
                    attempt += 1;
                }
                Err(err) => {
                    if attempt > 0 {
                        metrics::counter!(
                            "store.retry.exhausted",
                            "method" => stringify!($method),
                        )
                        .increment(1);
                    }
                    return Err(err);
                }
            }
        }
    }};
}

#[async_trait]
impl InflightActivationStore for RetryStore {
    async fn store(&self, batch: Vec<InflightActivation>) -> Result<u64, Error> {
        retry_method_clone!(self, store, batch)
    }

    fn assign_partitions(&self, partitions: Vec<i32>) -> Result<(), Error> {
        self.inner.assign_partitions(partitions)
    }

    async fn claim_activations(
        &self,
        application: Option<&str>,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
        bucket: Option<BucketRange>,
        mark_processing: bool,
    ) -> Result<Vec<InflightActivation>, Error> {
        retry_method!(
            self,
            claim_activations(application, namespaces, limit, bucket, mark_processing)
        )
    }

    async fn mark_activation_processing(&self, id: &str) -> Result<(), Error> {
        retry_method!(self, mark_activation_processing(id))
    }

    async fn set_status(
        &self,
        id: &str,
        status: InflightActivationStatus,
    ) -> Result<Option<InflightActivation>, Error> {
        retry_method!(self, set_status(id, status))
    }

    async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> f64 {
        self.inner.pending_activation_max_lag(now).await
    }

    async fn count_by_status(&self, status: InflightActivationStatus) -> Result<usize, Error> {
        retry_method!(self, count_by_status(status))
    }

    async fn count(&self) -> Result<usize, Error> {
        retry_method!(self, count())
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
        retry_method!(self, get_by_id(id))
    }

    async fn count_depths(&self) -> Result<DepthCounts, Error> {
        retry_method!(self, count_depths())
    }

    async fn set_processing_deadline(
        &self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        retry_method!(self, set_processing_deadline(id, deadline))
    }

    async fn delete_activation(&self, id: &str) -> Result<(), Error> {
        retry_method!(self, delete_activation(id))
    }

    async fn vacuum_db(&self) -> Result<(), Error> {
        retry_method!(self, vacuum_db())
    }

    async fn full_vacuum_db(&self) -> Result<(), Error> {
        retry_method!(self, full_vacuum_db())
    }

    async fn db_size(&self) -> Result<u64, Error> {
        retry_method!(self, db_size())
    }

    async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        retry_method!(self, get_retry_activations())
    }

    async fn handle_claim_expiration(&self) -> Result<u64, Error> {
        retry_method!(self, handle_claim_expiration())
    }

    async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        retry_method!(self, handle_processing_deadline())
    }

    async fn handle_processing_attempts(&self) -> Result<u64, Error> {
        retry_method!(self, handle_processing_attempts())
    }

    async fn handle_expires_at(&self) -> Result<u64, Error> {
        retry_method!(self, handle_expires_at())
    }

    async fn handle_delay_until(&self) -> Result<u64, Error> {
        retry_method!(self, handle_delay_until())
    }

    async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
        retry_method!(self, handle_failed_tasks())
    }

    async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        retry_method_clone!(self, mark_completed, ids)
    }

    async fn remove_completed(&self) -> Result<u64, Error> {
        retry_method!(self, remove_completed())
    }

    async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        retry_method_clone!(self, remove_killswitched, killswitched_tasks)
    }

    async fn clear(&self) -> Result<(), Error> {
        retry_method!(self, clear())
    }

    async fn remove_db(&self) -> Result<(), Error> {
        self.inner.remove_db().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A mock store that fails with a configurable error a set number of
    /// times before succeeding.
    struct MockFailingStore {
        fail_count: AtomicU32,
        transient: bool,
    }

    impl MockFailingStore {
        fn new(fail_count: u32, transient: bool) -> Self {
            Self {
                fail_count: AtomicU32::new(fail_count),
                transient,
            }
        }

        fn make_error(&self) -> Error {
            if self.transient {
                anyhow!("error returned from database: server conn crashed?")
            } else {
                anyhow!("some non-transient error")
            }
        }

        fn should_fail(&self) -> bool {
            let remaining = self.fail_count.load(Ordering::SeqCst);
            if remaining > 0 {
                self.fail_count.fetch_sub(1, Ordering::SeqCst);
                true
            } else {
                false
            }
        }
    }

    #[async_trait]
    impl InflightActivationStore for MockFailingStore {
        async fn store(&self, _batch: Vec<InflightActivation>) -> Result<u64, Error> {
            if self.should_fail() {
                Err(self.make_error())
            } else {
                Ok(1)
            }
        }

        fn assign_partitions(&self, _partitions: Vec<i32>) -> Result<(), Error> {
            Ok(())
        }

        async fn claim_activations(
            &self,
            _application: Option<&str>,
            _namespaces: Option<&[String]>,
            _limit: Option<i32>,
            _bucket: Option<BucketRange>,
            _mark_processing: bool,
        ) -> Result<Vec<InflightActivation>, Error> {
            if self.should_fail() {
                Err(self.make_error())
            } else {
                Ok(vec![])
            }
        }

        async fn mark_activation_processing(&self, _id: &str) -> Result<(), Error> {
            if self.should_fail() {
                Err(self.make_error())
            } else {
                Ok(())
            }
        }

        async fn set_status(
            &self,
            _id: &str,
            _status: InflightActivationStatus,
        ) -> Result<Option<InflightActivation>, Error> {
            if self.should_fail() {
                Err(self.make_error())
            } else {
                Ok(None)
            }
        }

        async fn pending_activation_max_lag(&self, _now: &DateTime<Utc>) -> f64 {
            0.0
        }

        async fn count_by_status(
            &self,
            _status: InflightActivationStatus,
        ) -> Result<usize, Error> {
            Ok(0)
        }

        async fn count(&self) -> Result<usize, Error> {
            Ok(0)
        }

        async fn get_by_id(&self, _id: &str) -> Result<Option<InflightActivation>, Error> {
            Ok(None)
        }

        async fn set_processing_deadline(
            &self,
            _id: &str,
            _deadline: Option<DateTime<Utc>>,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn delete_activation(&self, _id: &str) -> Result<(), Error> {
            Ok(())
        }

        async fn vacuum_db(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn full_vacuum_db(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn db_size(&self) -> Result<u64, Error> {
            Ok(0)
        }

        async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
            Ok(vec![])
        }

        async fn handle_claim_expiration(&self) -> Result<u64, Error> {
            Ok(0)
        }

        async fn handle_processing_deadline(&self) -> Result<u64, Error> {
            Ok(0)
        }

        async fn handle_processing_attempts(&self) -> Result<u64, Error> {
            Ok(0)
        }

        async fn handle_expires_at(&self) -> Result<u64, Error> {
            Ok(0)
        }

        async fn handle_delay_until(&self) -> Result<u64, Error> {
            Ok(0)
        }

        async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
            Ok(FailedTasksForwarder {
                to_discard: vec![],
                to_deadletter: vec![],
            })
        }

        async fn mark_completed(&self, _ids: Vec<String>) -> Result<u64, Error> {
            Ok(0)
        }

        async fn remove_completed(&self) -> Result<u64, Error> {
            Ok(0)
        }

        async fn remove_killswitched(
            &self,
            _killswitched_tasks: Vec<String>,
        ) -> Result<u64, Error> {
            Ok(0)
        }

        async fn clear(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_transient_errors() {
        let mock = Arc::new(MockFailingStore::new(2, true));
        let store = RetryStore::new(mock, 3);

        let result = store.store(vec![]).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_retry_exhausted_surfaces_error() {
        let mock = Arc::new(MockFailingStore::new(5, true));
        let store = RetryStore::new(mock, 3);

        let result = store.store(vec![]).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("server conn crashed")
        );
    }

    #[tokio::test]
    async fn test_non_transient_error_not_retried() {
        let mock = Arc::new(MockFailingStore::new(1, false));
        let store = RetryStore::new(mock.clone(), 3);

        let result = store.store(vec![]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-transient"));
        // The fail count was decremented once (the initial attempt), no retries
        assert_eq!(mock.fail_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_set_status_retries_on_transient_error() {
        let mock = Arc::new(MockFailingStore::new(1, true));
        let store = RetryStore::new(mock, 3);

        let result = store
            .set_status("test-id", InflightActivationStatus::Complete)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_claim_activations_retries_on_transient_error() {
        let mock = Arc::new(MockFailingStore::new(2, true));
        let store = RetryStore::new(mock, 3);

        let result = store
            .claim_activations(None, None, Some(1), None, true)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_mark_activation_processing_retries_on_transient_error() {
        let mock = Arc::new(MockFailingStore::new(2, true));
        let store = RetryStore::new(mock, 3);

        let result = store.mark_activation_processing("test-id").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_zero_retries_surfaces_immediately() {
        let mock = Arc::new(MockFailingStore::new(1, true));
        let store = RetryStore::new(mock, 0);

        let result = store.store(vec![]).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_is_transient_error() {
        assert!(is_transient_error(&anyhow!(
            "Unable to write to sqlite: error returned from database: server conn crashed?"
        )));
        assert!(is_transient_error(&anyhow!(
            "error returned from database: server shutting down"
        )));
        assert!(is_transient_error(&anyhow!("connection reset")));
        assert!(is_transient_error(&anyhow!("broken pipe")));
        assert!(is_transient_error(&anyhow!("connection refused")));
        assert!(is_transient_error(&anyhow!("pool timed out")));
        assert!(!is_transient_error(&anyhow!("unique constraint violation")));
        assert!(!is_transient_error(&anyhow!("syntax error in SQL")));
    }

    #[test]
    fn test_backoff_duration() {
        assert_eq!(backoff_duration(0), Duration::from_millis(100));
        assert_eq!(backoff_duration(1), Duration::from_millis(200));
        assert_eq!(backoff_duration(2), Duration::from_millis(400));
        assert_eq!(backoff_duration(3), Duration::from_millis(800));
        // Should cap at MAX_BACKOFF_MS
        assert_eq!(backoff_duration(10), Duration::from_millis(MAX_BACKOFF_MS));
    }
}
