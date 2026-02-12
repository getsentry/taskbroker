//! In-memory activation store using a `HashMap`. No durability; optimized for speed.

use crate::store::inflight_activation::{
    FailedTasksForwarder, InflightActivation, InflightActivationStatus, InflightActivationStore,
    QueryResult,
};
use anyhow::Error;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sentry_protos::taskbroker::v1::OnAttemptsExceeded;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use tracing::instrument;

/// Config for the in-memory map store. Only fields used by upkeep/behavior are needed.
#[derive(Clone, Debug)]
pub struct MapActivationStoreConfig {
    pub max_processing_attempts: usize,
    pub processing_deadline_grace_sec: u64,
}

impl MapActivationStoreConfig {
    pub fn from_config(config: &crate::config::Config) -> Self {
        Self {
            max_processing_attempts: config.max_processing_attempts,
            processing_deadline_grace_sec: config.processing_deadline_grace_sec,
        }
    }
}

/// In-memory activation store. No durability; all operations are fast map lookups/updates.
pub struct MapActivationStore {
    map: RwLock<HashMap<String, InflightActivation>>,
    config: MapActivationStoreConfig,
}

impl MapActivationStore {
    pub fn new(config: MapActivationStoreConfig) -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            config,
        }
    }
}

#[async_trait]
impl InflightActivationStore for MapActivationStore {
    async fn vacuum_db(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn full_vacuum_db(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn db_size(&self) -> Result<u64, Error> {
        Ok(0)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
        let guard = self
            .map
            .read()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        Ok(guard.get(id).cloned())
    }

    #[instrument(skip_all)]
    async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        if batch.is_empty() {
            return Ok(QueryResult { rows_affected: 0 });
        }
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let mut rows_affected = 0u64;
        for activation in batch {
            if guard.insert(activation.id.clone(), activation).is_none() {
                rows_affected += 1;
            }
        }
        Ok(QueryResult { rows_affected })
    }

    #[instrument(skip_all)]
    async fn get_pending_activations_from_namespaces(
        &self,
        application: Option<&str>,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
    ) -> Result<Vec<InflightActivation>, Error> {
        let now = Utc::now();
        let grace_sec = self.config.processing_deadline_grace_sec;

        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;

        let mut pending: Vec<(String, DateTime<Utc>, i32)> = guard
            .iter()
            .filter(|(_, a)| {
                a.status == InflightActivationStatus::Pending
                    && (a.expires_at.is_none() || a.expires_at.unwrap() > now)
                    && application.map_or(true, |app| app == a.application)
                    && namespaces.map_or(true, |ns| ns.is_empty() || ns.contains(&a.namespace))
            })
            .map(|(id, a)| (id.clone(), a.added_at, a.processing_deadline_duration))
            .collect();

        pending.sort_by(|a, b| a.1.cmp(&b.1));
        let take = limit
            .map(|n| n as usize)
            .unwrap_or(pending.len())
            .min(pending.len());
        let ids: Vec<String> = pending
            .into_iter()
            .take(take)
            .map(|(id, _, _)| id)
            .collect();

        for id in &ids {
            if let Some(a) = guard.get_mut(id) {
                a.status = InflightActivationStatus::Processing;
                a.processing_deadline = Some(
                    now + Duration::seconds(a.processing_deadline_duration as i64)
                        + Duration::seconds(grace_sec as i64),
                );
            }
        }

        let out: Vec<InflightActivation> = ids
            .into_iter()
            .filter_map(|id| guard.get(&id).cloned())
            .collect();
        Ok(out)
    }

    async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> f64 {
        let guard = match self.map.read() {
            Ok(g) => g,
            Err(_) => return 0.0,
        };
        let oldest = guard
            .values()
            .filter(|a| a.status == InflightActivationStatus::Pending && a.processing_attempts == 0)
            .min_by_key(|a| a.received_at);
        if let Some(a) = oldest {
            let millis = now.signed_duration_since(a.received_at).num_milliseconds()
                - a.delay_until.map_or(0, |d| {
                    d.signed_duration_since(a.received_at).num_milliseconds()
                });
            return millis as f64 / 1000.0;
        }
        0.0
    }

    #[instrument(skip_all)]
    async fn count_by_status(&self, status: InflightActivationStatus) -> Result<usize, Error> {
        let guard = self
            .map
            .read()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        Ok(guard.values().filter(|a| a.status == status).count())
    }

    async fn count(&self) -> Result<usize, Error> {
        let guard = self
            .map
            .read()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        Ok(guard.len())
    }

    #[instrument(skip_all)]
    async fn set_status(
        &self,
        id: &str,
        status: InflightActivationStatus,
    ) -> Result<Option<InflightActivation>, Error> {
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let a = guard.get_mut(id);
        let out = a.map(|a| {
            a.status = status;
            a.clone()
        });
        Ok(out)
    }

    #[instrument(skip_all)]
    async fn set_processing_deadline(
        &self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        if let Some(a) = guard.get_mut(id) {
            a.processing_deadline = deadline;
        }
        Ok(())
    }

    #[instrument(skip_all)]
    async fn delete_activation(&self, id: &str) -> Result<(), Error> {
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        guard.remove(id);
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        let guard = self
            .map
            .read()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        Ok(guard
            .values()
            .filter(|a| a.status == InflightActivationStatus::Retry)
            .cloned()
            .collect())
    }

    async fn clear(&self) -> Result<(), Error> {
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        guard.clear();
        Ok(())
    }

    #[instrument(skip_all)]
    async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let mut count = 0u64;
        for a in guard.values_mut() {
            if a.status != InflightActivationStatus::Processing {
                continue;
            }
            let Some(deadline) = a.processing_deadline else {
                continue;
            };
            if deadline >= now {
                continue;
            }
            a.processing_deadline = None;
            if a.at_most_once {
                a.status = InflightActivationStatus::Failure;
            } else {
                a.status = InflightActivationStatus::Pending;
                a.processing_attempts += 1;
            }
            count += 1;
        }
        Ok(count)
    }

    #[instrument(skip_all)]
    async fn handle_processing_attempts(&self) -> Result<u64, Error> {
        let max = self.config.max_processing_attempts as i32;
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let mut count = 0u64;
        for a in guard.values_mut() {
            if a.status == InflightActivationStatus::Pending && a.processing_attempts >= max {
                a.status = InflightActivationStatus::Failure;
                count += 1;
            }
        }
        Ok(count)
    }

    #[instrument(skip_all)]
    async fn handle_expires_at(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let to_remove: Vec<String> = guard
            .iter()
            .filter(|(_, a)| {
                a.status == InflightActivationStatus::Pending
                    && a.expires_at.map_or(false, |t| t < now)
            })
            .map(|(id, _)| id.clone())
            .collect();
        let n = to_remove.len() as u64;
        for id in to_remove {
            guard.remove(&id);
        }
        Ok(n)
    }

    #[instrument(skip_all)]
    async fn handle_delay_until(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let mut count = 0u64;
        for a in guard.values_mut() {
            if a.status == InflightActivationStatus::Delay
                && a.delay_until.map_or(false, |t| t < now)
            {
                a.status = InflightActivationStatus::Pending;
                count += 1;
            }
        }
        Ok(count)
    }

    #[instrument(skip_all)]
    async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let failed: Vec<(String, Vec<u8>, OnAttemptsExceeded)> = guard
            .iter()
            .filter(|(_, a)| a.status == InflightActivationStatus::Failure)
            .map(|(id, a)| (id.clone(), a.activation.clone(), a.on_attempts_exceeded))
            .collect();

        let mut forwarder = FailedTasksForwarder {
            to_discard: vec![],
            to_deadletter: vec![],
        };
        for (id, activation, on_attempts_exceeded) in failed {
            if on_attempts_exceeded == OnAttemptsExceeded::Discard
                || on_attempts_exceeded == OnAttemptsExceeded::Unspecified
            {
                forwarder.to_discard.push((id, activation));
            } else if on_attempts_exceeded == OnAttemptsExceeded::Deadletter {
                forwarder.to_deadletter.push((id, activation));
            }
        }

        let discard_ids: Vec<String> = forwarder
            .to_discard
            .iter()
            .map(|(id, _)| id.clone())
            .collect();
        for a in guard.values_mut() {
            if discard_ids.contains(&a.id) {
                a.status = InflightActivationStatus::Complete;
            }
        }
        Ok(forwarder)
    }

    #[instrument(skip_all)]
    async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let mut count = 0u64;
        for id in ids {
            if let Some(a) = guard.get_mut(&id) {
                a.status = InflightActivationStatus::Complete;
                count += 1;
            }
        }
        Ok(count)
    }

    #[instrument(skip_all)]
    async fn remove_completed(&self) -> Result<u64, Error> {
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let to_remove: Vec<String> = guard
            .iter()
            .filter(|(_, a)| a.status == InflightActivationStatus::Complete)
            .map(|(id, _)| id.clone())
            .collect();
        let n = to_remove.len() as u64;
        for id in to_remove {
            guard.remove(&id);
        }
        Ok(n)
    }

    #[instrument(skip_all)]
    async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        let set: HashSet<&str> = killswitched_tasks.iter().map(String::as_str).collect();
        let mut guard = self
            .map
            .write()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let to_remove: Vec<String> = guard
            .iter()
            .filter(|(_, a)| set.contains(a.taskname.as_str()))
            .map(|(id, _)| id.clone())
            .collect();
        let n = to_remove.len() as u64;
        for id in to_remove {
            guard.remove(&id);
        }
        Ok(n)
    }
}
