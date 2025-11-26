use crate::store::inner_redis_activation_store::InnerRedisActivationStore;
use crate::store::redis_utils::HashKey;
use base64::{Engine as _, engine::general_purpose};
use thiserror::Error;

use tracing::{error, info, instrument};
// use deadpool_redis::Pool;
use crate::config::Config;
use crate::store::inflight_activation::{
    InflightActivation, InflightActivationStatus, QueryResult,
};
use anyhow::Error;
use chrono::{DateTime, Duration, Utc};
use cityhasher;
use deadpool_redis::cluster::{
    Config as RedisClusterConfig, Pool as RedisClusterPool, Runtime as RedisClusterRuntime,
};
use deadpool_redis::{Config as RedisConfig, Pool, Runtime};
use redis::AsyncTypedCommands;
use sentry_protos::taskbroker::v1::OnAttemptsExceeded;
use std::collections::HashMap;
// use std::sync::RwLock;
use tokio::sync::RwLock;

#[derive(Error, Debug)]
pub enum RedisActivationError {
    #[error("Redis connection error: {error}")]
    Connection { error: String },

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Serialization error: {error}")]
    Serialization { error: String },

    #[error("Activation not found: {id}")]
    NotFound { id: String },

    #[error("Invalid activation status: {status}")]
    InvalidStatus { status: String },

    #[error("Database operation failed: {operation}: {error}")]
    DatabaseOperation { operation: String, error: String },

    #[error("Timeout while waiting for lock")]
    Timeout,
}

pub struct RedisActivationStoreConfig {
    pub topics: HashMap<String, Vec<i32>>,
    pub namespaces: Vec<String>,
    pub num_buckets: usize,
    pub payload_ttl_seconds: u64,
    pub processing_deadline_grace_sec: u64,
    pub max_processing_attempts: usize,
}

impl RedisActivationStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            topics: HashMap::from([(config.kafka_topic.clone(), vec![0])]),
            namespaces: config.namespaces.clone(),
            num_buckets: config.num_redis_buckets,
            payload_ttl_seconds: config.payload_ttl_seconds,
            processing_deadline_grace_sec: config.processing_deadline_grace_sec,
            max_processing_attempts: config.max_processing_attempts,
        }
    }
}

pub async fn create_redis_pool(urls: Vec<String>) -> Result<Pool, RedisActivationError> {
    // if urls.len() == 1 {
    //     let cfg = RedisConfig::from_url(urls[0].clone());
    //     let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    //     return Ok(pool);
    // }
    // let cfg = RedisClusterConfig::from_urls(urls);
    // let pool = cfg.create_pool(Some(RedisClusterRuntime::Tokio1)).unwrap();
    let cfg = RedisConfig::from_url(urls[0].clone());
    let pool =
        cfg.create_pool(Some(Runtime::Tokio1))
            .map_err(|e| RedisActivationError::Connection {
                error: e.to_string(),
            })?;
    Ok(pool)
}

// This exists to allow the RedisActivationStore to mutate its partitions without needing
// to have every caller of the store have to explicitly acquire a lock.
#[derive(Debug)]
pub struct RedisActivationStore {
    inner: RwLock<InnerRedisActivationStore>,
}

// Wraps the InnerRedisActivationStore to manage the locking to avoid the outer code having to handle it.
// Is also responsible for handling errors from the InnerRedisActivationStore.
impl RedisActivationStore {
    pub async fn new(
        urls: Vec<String>,
        config: RedisActivationStoreConfig,
    ) -> Result<Self, RedisActivationError> {
        let replicas = urls.len();
        let pool = create_redis_pool(urls).await?;

        let inner = InnerRedisActivationStore::new(
            pool,
            replicas,
            config.topics.clone(),
            config.namespaces.clone(),
            config.num_buckets,
            config.payload_ttl_seconds,
            config.processing_deadline_grace_sec,
            config.max_processing_attempts,
        )
        .await;
        if inner.is_err() {
            return Err(RedisActivationError::Connection {
                error: (inner.err().unwrap()).to_string(),
            });
        }
        Ok(Self {
            inner: RwLock::new(inner.unwrap()),
        })
    }

    pub async fn store(
        &self,
        batch: Vec<InflightActivation>,
    ) -> Result<QueryResult, RedisActivationError> {
        let result = self.inner.read().await.store(batch).await;
        if result.is_err() {
            let error_string = result.err().unwrap().to_string();
            println!("error: {:?}", error_string);
            return Err(RedisActivationError::DatabaseOperation {
                operation: "store".to_string(),
                error: error_string,
            });
        }
        Ok(result.unwrap())
    }

    // Called when rebalancing partitions
    pub async fn rebalance_partitions(&self, topic: String, partitions: Vec<i32>) {
        self.inner
            .write()
            .await
            .rebalance_partitions(topic, partitions);
    }

    pub async fn count_processing_activations(&self) -> Result<usize, RedisActivationError> {
        let result = self.inner.read().await.count_processing_activations().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "count_processing_activations".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn count_delayed_activations(&self) -> Result<usize, RedisActivationError> {
        let result = self.inner.read().await.count_delayed_activations().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "count_delayed_activations".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn count_pending_activations(&self) -> Result<usize, RedisActivationError> {
        let result = self.inner.read().await.count_pending_activations().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "count_pending_activations".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn count_retry_activations(&self) -> Result<usize, RedisActivationError> {
        let result = self.inner.read().await.count_retry_activations().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "count_retry_activations".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn count_deadletter_activations(&self) -> Result<usize, RedisActivationError> {
        let result = self.inner.read().await.count_deadletter_activations().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "count_deadletter_activations".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn db_size(&self) -> Result<u64, RedisActivationError> {
        let result = self.inner.read().await.db_size().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "db_size".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn delete_all_keys(&self) -> Result<(), RedisActivationError> {
        let result = self.inner.read().await.delete_all_keys().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "delete_all_keys".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(())
    }

    pub async fn get_pending_activation(
        &self,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, RedisActivationError> {
        let result = self
            .inner
            .read()
            .await
            .get_pending_activation(namespace)
            .await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "get_pending_activation".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        let activation = result.unwrap();
        if activation.is_none() {
            return Ok(None);
        }
        self.inner.write().await.incr_next_key_idx_for_pending();
        Ok(Some(activation.unwrap()))
    }

    pub async fn get_pending_activations_from_namespaces(
        &self,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
    ) -> Result<Vec<InflightActivation>, RedisActivationError> {
        let result = self
            .inner
            .read()
            .await
            .get_pending_activations_from_namespaces(namespaces, limit)
            .await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "get_pending_activations_from_namespaces".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn get_by_id(
        &self,
        hash_key: HashKey,
        activation_id: &str,
    ) -> Result<Option<InflightActivation>, RedisActivationError> {
        let result = self
            .inner
            .read()
            .await
            .get_by_id(hash_key, activation_id)
            .await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "get_by_id".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn set_status(
        &self,
        activation_id: &str,
        status: InflightActivationStatus,
    ) -> Result<(), RedisActivationError> {
        let result = self
            .inner
            .read()
            .await
            .set_status(activation_id, status)
            .await;
        if result.is_err() {
            let error_string = result.err().unwrap().to_string();
            println!("error: {:?}", error_string);
            return Err(RedisActivationError::DatabaseOperation {
                operation: "set_status".to_string(),
                error: error_string,
            });
        }
        Ok(())
    }

    pub async fn get_retry_activations(
        &self,
    ) -> Result<Vec<InflightActivation>, RedisActivationError> {
        let result = self.inner.read().await.get_retry_activations().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "get_retry_activations".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn mark_retry_completed(
        &self,
        activations: Vec<InflightActivation>,
    ) -> Result<u64, RedisActivationError> {
        let result = self
            .inner
            .read()
            .await
            .mark_retry_completed(activations)
            .await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "mark_retry_completed".to_string(),
                error: result.err().unwrap().to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn handle_processing_deadline(
        &self,
    ) -> Result<(u64, u64, u64), RedisActivationError> {
        let result = self.inner.read().await.handle_processing_deadline().await;
        if result.is_err() {
            let error_string = result.err().unwrap().to_string();
            println!("error: {:?}", error_string);
            return Err(RedisActivationError::DatabaseOperation {
                operation: "handle_processing_deadline".to_string(),
                error: error_string,
            });
        }
        Ok(result.unwrap())
    }

    pub async fn handle_processing_attempts(&self) -> Result<u64, RedisActivationError> {
        let result = self.inner.read().await.handle_processing_attempts().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "handle_processing_attempts".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn handle_expires_at(&self) -> Result<u64, RedisActivationError> {
        let result = self.inner.read().await.handle_expires_at().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "handle_expires_at".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn handle_delay_until(&self) -> Result<u64, RedisActivationError> {
        let result = self.inner.read().await.handle_delay_until().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "handle_delay_until".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn handle_deadletter_tasks(
        &self,
    ) -> Result<Vec<(String, Vec<u8>)>, RedisActivationError> {
        let result = self.inner.read().await.handle_deadletter_tasks().await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "handle_deadletter_tasks".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn mark_deadletter_completed(
        &self,
        ids: Vec<String>,
    ) -> Result<u64, RedisActivationError> {
        let result = self.inner.read().await.mark_deadletter_completed(ids).await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "mark_deadletter_completed".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn remove_killswitched(
        &self,
        killswitched_tasks: Vec<String>,
    ) -> Result<u64, RedisActivationError> {
        let result = self
            .inner
            .read()
            .await
            .remove_killswitched(killswitched_tasks)
            .await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "remove_killswitched".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn mark_demoted_completed(
        &self,
        ids: Vec<String>,
    ) -> Result<u64, RedisActivationError> {
        let result = self.inner.read().await.mark_demoted_completed(ids).await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "mark_demoted_completed".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }

    pub async fn pending_activation_max_lag(
        &self,
        now: &DateTime<Utc>,
    ) -> Result<u64, RedisActivationError> {
        let result = self
            .inner
            .read()
            .await
            .pending_activation_max_lag(now)
            .await;
        if result.is_err() {
            return Err(RedisActivationError::DatabaseOperation {
                operation: "pending_activation_max_lag".to_string(),
                error: (result.err().unwrap()).to_string(),
            });
        }
        Ok(result.unwrap())
    }
}
