use anyhow::{Error, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::join;
use tracing::warn;

use crate::store::activation::{Activation, ActivationStatus};
use crate::store::types::{BucketRange, DepthCounts, FailedTasksForwarder};

/// This trait only contains methods used by the consumer component.
#[async_trait]
pub trait IngestStore: Send + Sync {
    /// Write a batch of activations to the store.
    async fn write(&self, batch: Vec<Activation>) -> Result<u64, Error>;

    /// Change the Kafka partitions for which this instance is responsible.
    async fn assign_partitions(&self, partitions: Vec<i32>) -> Result<(), Error>;
}

/// This trait only contains methods for counting operations, used for backpressure and metrics.
#[async_trait]
pub trait CountStore: Send + Sync {
    /// Get the size of the database in bytes.
    async fn size(&self) -> Result<u64, Error>;

    /// Count all activations
    async fn count(&self) -> Result<usize, Error>;

    /// Count activations by status.
    async fn count_by_status(&self, status: ActivationStatus) -> Result<usize, Error>;

    /// Queue depths for pending, delay, and processing (writer backpressure and upkeep gauges).
    /// Default implementation uses separate calls, but stores may override with a single query.
    async fn count_depths(&self) -> Result<DepthCounts, Error> {
        let (pending, delay, claimed, processing) = join!(
            self.count_by_status(ActivationStatus::Pending),
            self.count_by_status(ActivationStatus::Delay),
            self.count_by_status(ActivationStatus::Claimed),
            self.count_by_status(ActivationStatus::Processing),
        );

        Ok(DepthCounts {
            pending: pending?,
            delay: delay?,
            claimed: claimed?,
            processing: processing?,
        })
    }

    /// Get the age of the oldest pending activation in seconds
    async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> f64;
}

/// This trait only contains methods for managing claims.
#[async_trait]
pub trait ClaimStore: Send + Sync {
    /// Get `limit` pending activations with several optional filters.
    /// If `mark_processing` is true, sets `Processing` and `processing_deadline`, otherwise `Claimed` and `claim_expires_at`.
    /// If no limit is provided, all matching activations will be returned.
    async fn claim_activations(
        &self,
        application: Option<&str>,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
        bucket: Option<BucketRange>,
        mark_processing: bool,
    ) -> Result<Vec<Activation>, Error>;

    /// Claims `limit` activations and updates status to `Claimed` until `mark_processing` moves them to `Processing`.
    async fn claim_activations_for_push(
        &self,
        application: Option<&str>,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
        bucket: Option<BucketRange>,
    ) -> Result<Vec<Activation>, Error> {
        // If a namespace filter is used, an application must also be used
        if namespaces.is_some() && application.is_none() {
            warn!(
                ?namespaces,
                "Received request for namespaced task without application"
            );

            return Ok(vec![]);
        }

        self.claim_activations(application, namespaces, limit, bucket, false)
            .await
    }

    /// Claims one activation with application `application` and namespace `namespace`.
    async fn claim_activation_for_pull(
        &self,
        application: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<Option<Activation>, Error> {
        // Convert single namespace to vector for internal use
        let namespaces = namespace.map(|ns| vec![ns.to_string()]);

        // If a namespace filter is used, an application must also be used
        if namespaces.is_some() && application.is_none() {
            warn!(
                ?namespaces,
                "Received request for namespaced task without application"
            );

            return Ok(None);
        }

        let mut rows = self
            .claim_activations(application, namespaces.as_deref(), Some(1), None, true)
            .await?;

        // If we are getting more than one task here, something is broken
        if rows.len() > 1 {
            Err(anyhow!("Found more than one row despite limit of one"))
        } else {
            Ok(rows.pop())
        }
    }
}

/// This trait only contains methods used in push mode.
#[async_trait]
pub trait PushStore: Send + Sync {
    /// Record successful push.
    async fn mark_processing(&self, id: &str) -> Result<(), Error>;
}

/// This trait only contains methods used in pull mode.
#[async_trait]
pub trait PullStore: ClaimStore {
    /// Update the status of a specific activation.
    async fn set_status(
        &self,
        id: &str,
        status: ActivationStatus,
    ) -> Result<Option<Activation>, Error>;
}

/// This trait only contains methods used by upkeep.
#[async_trait]
pub trait UpkeepStore {
    /// Get all activations with status Retry
    async fn get_retry_activations(&self) -> Result<Vec<Activation>, Error>;

    /// Revert expired push claims back to pending status.
    async fn handle_claim_expiration(&self) -> Result<u64, Error>;

    /// Update tasks that exceeded their processing deadline
    async fn handle_processing_deadline(&self) -> Result<u64, Error>;

    /// Update tasks that exceeded max processing attempts
    async fn handle_processing_attempts(&self) -> Result<u64, Error>;

    /// Delete tasks past their expires_at deadline
    async fn handle_expires_at(&self) -> Result<u64, Error>;

    /// Update delayed tasks past their delay_until deadline to Pending
    async fn handle_delay_until(&self) -> Result<u64, Error>;

    /// Process failed tasks for discard or deadletter.
    async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error>;

    /// Mark tasks as completd by ID.
    async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error>;

    /// Remove completed tasks.
    async fn remove_completed(&self) -> Result<u64, Error>;

    /// Remove killswitched tasks.
    async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error>;

    /// Trigger incremental vacuum to reclaim free pages in the database.
    async fn vacuum_db(&self) -> Result<(), Error>;

    /// Perform a full vacuum on the database.
    async fn full_vacuum_db(&self) -> Result<(), Error>;
}

/// Umbrella interface that represents ALL possible operations on any runtime store.
#[async_trait]
pub trait Store:
    IngestStore + ClaimStore + UpkeepStore + CountStore + PushStore + PullStore
{
}

#[async_trait]
pub trait TestStore: Store {
    /// Get an activation by id
    async fn get_by_id(&self, id: &str) -> Result<Option<Activation>, Error>;

    /// Set the processing deadline for a specific activation
    async fn set_processing_deadline(
        &self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;

    /// Delete an activation by id
    async fn delete_activation(&self, id: &str) -> Result<(), Error>;

    /// Clear all activations from the store
    async fn clear(&self) -> Result<(), Error>;

    /// Remove the database, used only in tests
    async fn remove_db(&self) -> Result<(), Error> {
        Ok(())
    }
}
