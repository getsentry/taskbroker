//! Fake activation store for performance testing. Does not persist data; responds to
//! queries with standard dummy tasks so the store is not the bottleneck.

use crate::store::inflight_activation::{
    FailedTasksForwarder, InflightActivation, InflightActivationStatus, InflightActivationStore,
    QueryResult,
};
use anyhow::Error;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use prost::Message as _;
use sentry_protos::taskbroker::v1::TaskActivation;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Fake activation store. No persistence; query methods return standard dummy tasks
/// (application "sentry", namespace "examples", taskname "examples.simple_task", parameters "{}",
/// no expiration, no delay) so the rest of the pipeline can be benchmarked without store I/O.
pub struct FakeActivationStore {
    id_counter: AtomicU64,
}

impl FakeActivationStore {
    pub fn new() -> Self {
        Self {
            id_counter: AtomicU64::new(0),
        }
    }

    /// Build one standard dummy task: sentry / examples / examples.simple_task, parameters "{}", no expiry, no delay.
    fn dummy_task(&self) -> InflightActivation {
        let id = format!("fake-{}", self.id_counter.fetch_add(1, Ordering::Relaxed));
        let now = Utc::now();
        let activation_bytes = Self::encode_minimal_activation(&id, now);
        InflightActivation {
            id: id.clone(),
            application: "sentry".into(),
            namespace: "examples".into(),
            taskname: "examples.simple_task".into(),
            activation: activation_bytes,
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at: now,
            received_at: now,
            processing_attempts: 0,
            processing_deadline_duration: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            on_attempts_exceeded: sentry_protos::taskbroker::v1::OnAttemptsExceeded::Discard,
            at_most_once: false,
        }
    }

    fn encode_minimal_activation(id: &str, now: DateTime<Utc>) -> Vec<u8> {
        #[allow(deprecated)]
        let activation = TaskActivation {
            id: id.to_string(),
            application: Some("sentry".to_string()),
            namespace: "examples".into(),
            taskname: "examples.simple_task".into(),
            parameters: "{}".into(),
            headers: HashMap::new(),
            received_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
            retry_state: None,
            processing_deadline_duration: 0,
            expires: None,
            delay: None,
        };
        activation.encode_to_vec()
    }
}

impl Default for FakeActivationStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl InflightActivationStore for FakeActivationStore {
    async fn vacuum_db(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn full_vacuum_db(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn db_size(&self) -> Result<u64, Error> {
        Ok(0)
    }

    async fn get_by_id(&self, _id: &str) -> Result<Option<InflightActivation>, Error> {
        Ok(None)
    }

    async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        Ok(QueryResult {
            rows_affected: batch.len() as u64,
        })
    }

    async fn get_pending_activations_from_namespaces(
        &self,
        _application: Option<&str>,
        _namespaces: Option<&[String]>,
        limit: Option<i32>,
    ) -> Result<Vec<InflightActivation>, Error> {
        let n = limit.unwrap_or(1).max(0).min(1) as usize;
        Ok((0..n).map(|_| self.dummy_task()).collect())
    }

    async fn pending_activation_max_lag(&self, _now: &DateTime<Utc>) -> f64 {
        0.0
    }

    async fn peek_pending_activation(&self) -> Result<Option<InflightActivation>, Error> {
        Ok(Some(self.dummy_task()))
    }

    async fn mark_as_processing_if_pending(&self, _id: &str) -> Result<bool, Error> {
        Ok(true)
    }

    async fn count_by_status(&self, _status: InflightActivationStatus) -> Result<usize, Error> {
        Ok(0)
    }

    async fn count(&self) -> Result<usize, Error> {
        Ok(0)
    }

    async fn set_status(
        &self,
        _id: &str,
        _status: InflightActivationStatus,
    ) -> Result<Option<InflightActivation>, Error> {
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

    async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        Ok(vec![])
    }

    async fn clear(&self) -> Result<(), Error> {
        Ok(())
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

    async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        Ok(ids.len() as u64)
    }

    async fn remove_completed(&self) -> Result<u64, Error> {
        Ok(0)
    }

    async fn remove_killswitched(&self, _killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        Ok(0)
    }
}
