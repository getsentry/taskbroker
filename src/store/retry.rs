use std::future::Future;
use std::time::Duration;

use anyhow::Error;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::config::Config;

/// Configuration for query-level retry behavior.
pub struct RetryConfig {
    pub max_retries: u32,
    pub retry_delay: Duration,
}

impl RetryConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_retries: config.store.db_query_max_retries,
            retry_delay: config.store.db_query_retry_delay,
        }
    }
}

/// Returns true if the error is a transient database/connection error
/// that is likely to succeed on retry. Downcasts the anyhow::Error to
/// sqlx::Error to match on structured variants rather than parsing strings.
fn is_retryable_error(err: &Error) -> bool {
    matches!(
        err.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::Io(_)) | Some(sqlx::Error::PoolTimedOut)
    )
}

/// Retries a query-producing closure on transient database errors.
///
/// The closure `f` is called on each attempt, producing a fresh future.
/// This ensures connection re-acquisition and query re-execution happen
/// naturally on each retry.
pub async fn retry_query<F, Fut, T>(
    config: &RetryConfig,
    label: &'static str,
    f: F,
) -> Result<T, Error>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(val) => {
                if attempt > 0 {
                    debug!(label, attempt, "Query succeeded after retry");
                    metrics::counter!("store.retry.succeeded", "method" => label).increment(1);
                }
                return Ok(val);
            }
            Err(err) if attempt < config.max_retries && is_retryable_error(&err) => {
                warn!(
                    label,
                    attempt,
                    error = %err,
                    "Retryable database error, retrying"
                );
                metrics::counter!("store.retry.attempt", "method" => label).increment(1);
                sleep(config.retry_delay).await;
                attempt += 1;
            }
            Err(err) => {
                if attempt > 0 && is_retryable_error(&err) {
                    metrics::counter!("store.retry.exhausted", "method" => label).increment(1);
                }
                return Err(err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::store::StoreConfig;

    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn retryable_error() -> Error {
        Error::from(sqlx::Error::PoolTimedOut)
    }

    fn non_retryable_error() -> Error {
        Error::from(sqlx::Error::RowNotFound)
    }

    fn test_config(max_retries: u32) -> RetryConfig {
        RetryConfig {
            max_retries,
            retry_delay: Duration::from_millis(0),
        }
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_retryable_errors() {
        let fail_count = AtomicU32::new(2);
        let config = test_config(3);

        let result = retry_query(&config, "test", || async {
            let remaining = fail_count.load(Ordering::SeqCst);
            if remaining > 0 {
                fail_count.fetch_sub(1, Ordering::SeqCst);
                Err(retryable_error())
            } else {
                Ok(42u64)
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_exhausted_surfaces_error() {
        let fail_count = AtomicU32::new(5);
        let config = test_config(3);

        let result = retry_query(&config, "test", || async {
            let remaining = fail_count.load(Ordering::SeqCst);
            if remaining > 0 {
                fail_count.fetch_sub(1, Ordering::SeqCst);
                Err(retryable_error())
            } else {
                Ok(42u64)
            }
        })
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_non_retryable_error_not_retried() {
        let call_count = AtomicU32::new(0);
        let config = test_config(3);

        let result = retry_query(&config, "test", || async {
            call_count.fetch_add(1, Ordering::SeqCst);
            Err::<u64, _>(non_retryable_error())
        })
        .await;

        assert!(result.is_err());
        // Called only once — no retries for non-retryable errors
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retryable_then_non_retryable_stops_without_exhausting() {
        let call_count = AtomicU32::new(0);
        let config = test_config(3);

        let result = retry_query(&config, "test", || async {
            let count = call_count.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                // First attempt: retryable error — triggers retry
                Err::<u64, _>(retryable_error())
            } else {
                // Second attempt: non-retryable error — should stop immediately
                Err::<u64, _>(non_retryable_error())
            }
        })
        .await;

        assert!(result.is_err());
        // Called exactly twice: first attempt + one retry, then stopped
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_zero_retries_surfaces_immediately() {
        let config = test_config(0);

        let result = retry_query(&config, "test", || async {
            Err::<u64, _>(retryable_error())
        })
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_success_on_first_attempt_no_retry() {
        let config = test_config(3);

        let result = retry_query(&config, "test", || async { Ok(1u64) }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_is_retryable_error() {
        // Retryable: transient connection/pool errors
        assert!(is_retryable_error(&Error::from(sqlx::Error::PoolTimedOut)));
        assert!(is_retryable_error(&Error::from(sqlx::Error::Io(
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset")
        ))));

        // Not retryable: unrecoverable or logic/schema errors
        assert!(!is_retryable_error(&Error::from(sqlx::Error::PoolClosed)));
        assert!(!is_retryable_error(&Error::from(
            sqlx::Error::WorkerCrashed
        )));
        assert!(!is_retryable_error(&Error::from(sqlx::Error::RowNotFound)));
        assert!(!is_retryable_error(&Error::from(
            sqlx::Error::ColumnNotFound("id".into())
        )));
        assert!(!is_retryable_error(&Error::from(sqlx::Error::Protocol(
            "unexpected".into()
        ))));
    }

    #[test]
    fn test_config_none_means_zero_retries() {
        let config = RetryConfig::from_config(&Arc::new(Config {
            store: StoreConfig {
                db_query_max_retries: 0,
                ..StoreConfig::default()
            },
            ..Config::default()
        }));
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn test_config_uses_configured_retries() {
        let config = RetryConfig::from_config(&Arc::new(Config {
            store: StoreConfig {
                db_query_max_retries: 5,
                ..StoreConfig::default()
            },
            ..Config::default()
        }));
        assert_eq!(config.max_retries, 5);
    }
}
