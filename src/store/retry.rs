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

/// Returns true if the error is a transient database/connection error
/// that is likely to succeed on retry. Downcasts the anyhow::Error to
/// sqlx::Error to match on structured variants rather than parsing strings.
fn is_retryable_error(err: &Error) -> bool {
    matches!(
        err.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::Io(_))
            | Some(sqlx::Error::PoolTimedOut)
            | Some(sqlx::Error::PoolClosed)
            | Some(sqlx::Error::WorkerCrashed)
    )
}

/// A wrapper around an `InflightActivationStore` that retries failed
/// database queries with a fixed delay between attempts.
pub struct RetryStore {
    inner: Arc<dyn InflightActivationStore>,
    max_retries: u32,
    retry_delay: Duration,
}

impl RetryStore {
    pub fn new(
        inner: Arc<dyn InflightActivationStore>,
        max_retries: u32,
        retry_delay_ms: u64,
    ) -> Self {
        Self {
            inner,
            max_retries,
            retry_delay: Duration::from_millis(retry_delay_ms),
        }
    }
}

/// Macro to reduce boilerplate for delegating trait methods with retry logic.
/// For each method call, if the inner store returns a retryable error,
/// we retry up to `self.max_retries` times with a fixed delay.
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
                Err(err) if attempt < $self.max_retries && is_retryable_error(&err) => {
                    warn!(
                        method = stringify!($method),
                        attempt,
                        error = %err,
                        "Retryable database error, retrying"
                    );
                    metrics::counter!(
                        "store.retry.attempt",
                        "method" => stringify!($method),
                    )
                    .increment(1);
                    sleep($self.retry_delay).await;
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
        retry_method!(self, store(batch.clone()))
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
        retry_method!(self, mark_completed(ids.clone()))
    }

    async fn remove_completed(&self) -> Result<u64, Error> {
        retry_method!(self, remove_completed())
    }

    async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        retry_method!(self, remove_killswitched(killswitched_tasks.clone()))
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
    use rstest::rstest;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Helper to create a retryable error (sqlx transient error wrapped in anyhow).
    fn retryable_error() -> Error {
        Error::from(sqlx::Error::PoolTimedOut)
    }

    /// Helper to create a non-retryable error (sqlx database/logic error wrapped in anyhow).
    fn non_retryable_error() -> Error {
        Error::from(sqlx::Error::RowNotFound)
    }

    /// A mock store that fails a set number of times before succeeding.
    /// The `retryable` flag controls whether errors are transient (retryable)
    /// or permanent (non-retryable).
    struct MockFailingStore {
        fail_count: AtomicU32,
        retryable: bool,
    }

    impl MockFailingStore {
        fn new(fail_count: u32, retryable: bool) -> Self {
            Self {
                fail_count: AtomicU32::new(fail_count),
                retryable,
            }
        }

        fn make_error(&self) -> Error {
            if self.retryable {
                retryable_error()
            } else {
                non_retryable_error()
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

        async fn count_by_status(&self, _status: InflightActivationStatus) -> Result<usize, Error> {
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
    async fn test_retry_succeeds_after_retryable_errors() {
        let mock = Arc::new(MockFailingStore::new(2, true));
        let store = RetryStore::new(mock, 3, 0);

        let result = store.store(vec![]).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_retry_exhausted_surfaces_error() {
        let mock = Arc::new(MockFailingStore::new(5, true));
        let store = RetryStore::new(mock, 3, 0);

        let result = store.store(vec![]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_non_retryable_error_not_retried() {
        let mock = Arc::new(MockFailingStore::new(1, false));
        let store = RetryStore::new(mock.clone(), 3, 0);

        let result = store.store(vec![]).await;
        assert!(result.is_err());
        // The fail count was decremented once (the initial attempt), no retries
        assert_eq!(mock.fail_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_set_status_retries_on_retryable_error() {
        let mock = Arc::new(MockFailingStore::new(1, true));
        let store = RetryStore::new(mock, 3, 0);

        let result = store
            .set_status("test-id", InflightActivationStatus::Complete)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_claim_activations_retries_on_retryable_error() {
        let mock = Arc::new(MockFailingStore::new(2, true));
        let store = RetryStore::new(mock, 3, 0);

        let result = store
            .claim_activations(None, None, Some(1), None, true)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_mark_activation_processing_retries_on_retryable_error() {
        let mock = Arc::new(MockFailingStore::new(2, true));
        let store = RetryStore::new(mock, 3, 0);

        let result = store.mark_activation_processing("test-id").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_zero_retries_surfaces_immediately() {
        let mock = Arc::new(MockFailingStore::new(1, true));
        let store = RetryStore::new(mock, 0, 0);

        let result = store.store(vec![]).await;
        assert!(result.is_err());
    }

    /// Mirrors the main.rs wiring: when max_retries is None, RetryStore is not
    /// used and the inner store is called directly — retryable errors surface
    /// immediately. When Some, RetryStore wraps the inner store and retries.
    #[rstest]
    #[case::none_bypasses_retry(None, true)]
    #[case::some_enables_retry(Some(3), false)]
    #[tokio::test]
    async fn test_config_retry_wiring(#[case] max_retries: Option<u32>, #[case] expect_err: bool) {
        // Mock fails once with a retryable error then succeeds
        let inner: Arc<dyn InflightActivationStore> = Arc::new(MockFailingStore::new(1, true));

        // Simulate main.rs wiring
        let store: Arc<dyn InflightActivationStore> = match max_retries {
            Some(n) => Arc::new(RetryStore::new(inner, n, 0)),
            None => inner,
        };

        let result = store.store(vec![]).await;
        assert_eq!(result.is_err(), expect_err);
    }

    #[test]
    fn test_is_retryable_error() {
        // Retryable: transient connection/pool errors
        assert!(is_retryable_error(&Error::from(sqlx::Error::PoolTimedOut)));
        assert!(is_retryable_error(&Error::from(sqlx::Error::PoolClosed)));
        assert!(is_retryable_error(&Error::from(sqlx::Error::WorkerCrashed)));
        assert!(is_retryable_error(&Error::from(sqlx::Error::Io(
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset")
        ))));

        // Not retryable: logic/schema errors
        assert!(!is_retryable_error(&Error::from(sqlx::Error::RowNotFound)));
        assert!(!is_retryable_error(&Error::from(
            sqlx::Error::ColumnNotFound("id".into())
        )));
        assert!(!is_retryable_error(&Error::from(sqlx::Error::Protocol(
            "unexpected".into()
        ))));
    }
}
