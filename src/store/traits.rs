use anyhow::{Error, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::join;
use tracing::warn;

use crate::store::activation::{InflightActivation, InflightActivationStatus};
use crate::store::types::{BucketRange, DepthCounts, FailedTasksForwarder};

#[async_trait]
pub trait InflightActivationStore: Send + Sync {
    /// CONSUMER OPERATIONS
    /// Store a batch of activations
    async fn store(&self, batch: Vec<InflightActivation>) -> Result<u64, Error>;

    fn assign_partitions(&self, partitions: Vec<i32>) -> Result<(), Error>;

    /// Get `limit` pending activations, optionally filtered by namespaces and bucket subrange.
    /// If `mark_processing` is true, sets status to `Processing` and `processing_deadline`; otherwise `Claimed` and `claim_expires_at`.
    /// If no limit is provided, all matching activations will be returned.
    async fn claim_activations(
        &self,
        application: Option<&str>,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
        bucket: Option<BucketRange>,
        mark_processing: bool,
    ) -> Result<Vec<InflightActivation>, Error>;

    /// Claims `limit` activations within the `bucket` range. Push mode uses status `Claimed` until `mark_processing` moves to `Processing`.
    async fn claim_activations_for_push(
        &self,
        limit: Option<i32>,
        bucket: Option<BucketRange>,
    ) -> Result<Vec<InflightActivation>, Error> {
        self.claim_activations(None, None, limit, bucket, false)
            .await
    }

    /// Claims `limit` activations with application `application` and namespace `namespace`.
    async fn claim_activation_for_pull(
        &self,
        application: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
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

    /// Record successful push.
    async fn mark_processing(&self, id: &str) -> Result<(), Error>;

    /// Record a batch of successful pushes.
    async fn mark_processing_batch(&self, ids: &[String]) -> Result<u64, Error>;

    /// Update the status of a specific activation
    async fn set_status(
        &self,
        id: &str,
        status: InflightActivationStatus,
    ) -> Result<Option<InflightActivation>, Error>;

    /// Update the status of multiple activations in one batch.
    async fn set_status_batch(
        &self,
        ids: &[String],
        status: InflightActivationStatus,
    ) -> Result<u64, Error>;

    /// COUNT OPERATIONS
    /// Get the age of the oldest pending activation in seconds
    async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> f64;

    /// Count activations with Pending status
    async fn count_pending_activations(&self) -> Result<usize, Error> {
        self.count_by_status(InflightActivationStatus::Pending)
            .await
    }

    /// Count activations by status
    async fn count_by_status(&self, status: InflightActivationStatus) -> Result<usize, Error>;

    /// Count all activations
    async fn count(&self) -> Result<usize, Error>;

    /// ACTIVATION OPERATIONS
    /// Get an activation by id
    async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error>;

    /// Queue depths for pending, delay, and processing (writer backpressure and upkeep gauges).
    /// Default implementation uses separate calls, but stores may override with a single query.
    async fn count_depths(&self) -> Result<DepthCounts, Error> {
        let (pending, delay, claimed, processing) = join!(
            self.count_by_status(InflightActivationStatus::Pending),
            self.count_by_status(InflightActivationStatus::Delay),
            self.count_by_status(InflightActivationStatus::Claimed),
            self.count_by_status(InflightActivationStatus::Processing),
        );

        Ok(DepthCounts {
            pending: pending?,
            delay: delay?,
            claimed: claimed?,
            processing: processing?,
        })
    }

    /// Set the processing deadline for a specific activation
    async fn set_processing_deadline(
        &self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;

    /// Delete an activation by id
    async fn delete_activation(&self, id: &str) -> Result<(), Error>;

    /// DATABASE OPERATIONS
    /// Trigger incremental vacuum to reclaim free pages in the database
    async fn vacuum_db(&self) -> Result<(), Error>;

    /// Perform a full vacuum on the database
    async fn full_vacuum_db(&self) -> Result<(), Error>;

    /// Get the size of the database in bytes
    async fn db_size(&self) -> Result<u64, Error>;

    /// UPKEEP OPERATIONS
    /// Get all activations with status Retry
    async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error>;

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

    /// Process failed tasks for discard or deadletter
    async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error>;

    /// Mark tasks as complete by id
    async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error>;

    /// Remove completed tasks
    async fn remove_completed(&self) -> Result<u64, Error>;

    /// Remove killswitched tasks
    async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error>;

    /// TEST OPERATIONS
    /// Clear all activations from the store
    async fn clear(&self) -> Result<(), Error>;

    /// Remove the database, used only in tests
    async fn remove_db(&self) -> Result<(), Error> {
        Ok(())
    }
}
