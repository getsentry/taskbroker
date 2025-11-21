use tracing::{error, info, instrument};
// use deadpool_redis::Pool;
use crate::config::Config;
use crate::store::inflight_activation::{
    InflightActivation, InflightActivationStatus, QueryResult,
};
use anyhow::Error;
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

pub enum KeyPrefix {
    Payload,
    IDLookup,
    Pending,
    Processing,
    Delay,
    Retry,
    Deadletter,
    Expired,
}

pub struct RedisActivationStoreConfig {
    pub topics: HashMap<String, Vec<i32>>,
    pub namespaces: Vec<String>,
    pub num_buckets: usize,
    pub payload_ttl_seconds: u64,
}

impl RedisActivationStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            topics: HashMap::from([(config.kafka_topic.clone(), vec![0])]),
            namespaces: config.namespaces.clone(),
            num_buckets: config.num_redis_buckets,
            payload_ttl_seconds: config.payload_ttl_seconds,
        }
    }
}

pub async fn create_redis_pool(urls: Vec<String>) -> Result<Pool, Error> {
    // if urls.len() == 1 {
    //     let cfg = RedisConfig::from_url(urls[0].clone());
    //     let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    //     return Ok(pool);
    // }
    // let cfg = RedisClusterConfig::from_urls(urls);
    // let pool = cfg.create_pool(Some(RedisClusterRuntime::Tokio1)).unwrap();
    let cfg = RedisConfig::from_url(urls[0].clone());
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    Ok(pool)
}

// This exists to allow the RedisActivationStore to mutate its partitions without needing
// to have every caller of the store have to explicitly acquire a lock.
#[derive(Debug)]
pub struct RedisActivationStore {
    inner: RwLock<InnerRedisActivationStore>,
}

// Wraps the InnerRedisActivationStore to manage the locking to avoid the outer code having to handle it.
impl RedisActivationStore {
    pub async fn new(urls: Vec<String>, config: RedisActivationStoreConfig) -> Result<Self, Error> {
        let inner = InnerRedisActivationStore::new(urls, config).await.unwrap();
        Ok(Self {
            inner: RwLock::new(inner),
        })
    }

    pub async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        self.inner.read().await.store(batch).await
    }

    // Called when rebalancing partitions
    pub async fn rebalance_partitions(&self, topic: String, partitions: Vec<i32>) {
        self.inner
            .write()
            .await
            .rebalance_partitions(topic, partitions);
    }

    pub async fn count_processing_activations(&self) -> Result<usize, Error> {
        self.inner.read().await.count_processing_activations().await
    }

    pub async fn count_delayed_activations(&self) -> Result<usize, Error> {
        self.inner.read().await.count_delayed_activations().await
    }

    pub async fn count_pending_activations(&self) -> Result<usize, Error> {
        self.inner.read().await.count_pending_activations().await
    }

    pub async fn db_size(&self) -> Result<u64, Error> {
        self.inner.read().await.db_size().await
    }

    pub async fn delete_all_keys(&self) -> Result<(), Error> {
        self.inner.read().await.delete_all_keys().await
    }

    pub async fn get_pending_activation(
        &self,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
        let activation = self
            .inner
            .read()
            .await
            .get_pending_activation(namespace)
            .await?;
        if activation.is_none() {
            return Ok(None);
        }
        self.inner.write().await.incr_next_key_idx_for_pending();
        Ok(Some(activation.unwrap()))
    }

    pub async fn set_status(
        &self,
        activation_id: &str,
        status: InflightActivationStatus,
    ) -> Result<(), Error> {
        self.inner
            .read()
            .await
            .set_status(activation_id, status)
            .await
    }
}

#[derive(Debug)]
struct InnerRedisActivationStore {
    pool: Pool,
    replicas: usize,
    topics: HashMap<String, Vec<i32>>,
    namespaces: Vec<String>,
    num_buckets: usize,
    payload_ttl_seconds: u64,
    bucket_hashes: Vec<String>,
    next_key_idx_for_pending: usize,
    total_possible_keys: usize,
}

impl InnerRedisActivationStore {
    pub async fn new(urls: Vec<String>, config: RedisActivationStoreConfig) -> Result<Self, Error> {
        let replicas = urls.len();
        let pool = create_redis_pool(urls).await?;
        let bucket_hashes = (0..config.num_buckets)
            .map(|i| format!("{:04x}", i))
            .collect();

        Ok(Self {
            pool,
            replicas,
            topics: config.topics.clone(),
            namespaces: config.namespaces.clone(),
            num_buckets: config.num_buckets,
            bucket_hashes,
            payload_ttl_seconds: config.payload_ttl_seconds,
            next_key_idx_for_pending: 0,
            total_possible_keys: 0,
        })
    }

    fn compute_bucket(&self, activation_id: &str) -> String {
        let hashint: u64 = cityhasher::hash(activation_id);
        format!("{:04x}", hashint % self.num_buckets as u64)
    }

    fn build_key_with_activation(
        &self,
        prefix: KeyPrefix,
        namespace: &str,
        topic: &str,
        partition: i32,
        activation_id: &str,
    ) -> String {
        self.build_key(
            prefix,
            namespace,
            topic,
            partition,
            self.compute_bucket(activation_id).as_str(),
        )
    }

    fn build_key_with_bucket(
        &self,
        prefix: KeyPrefix,
        namespace: &str,
        topic: &str,
        partition: i32,
        bucket_hash: &str,
    ) -> String {
        self.build_key(prefix, namespace, topic, partition, bucket_hash)
    }

    fn get_id_lookup_key(&self, activation_id: &str) -> String {
        format!("idlookup:{}", activation_id)
    }

    fn build_key(
        &self,
        prefix: KeyPrefix,
        namespace: &str,
        topic: &str,
        partition: i32,
        suffix: &str,
    ) -> String {
        match prefix {
            KeyPrefix::Payload => {
                format!("payload:{}:{}:{}:{}", namespace, topic, partition, suffix)
            }
            KeyPrefix::IDLookup => "idlookup:".to_string(),
            KeyPrefix::Pending => {
                format!("pending:{}:{}:{}:{}", namespace, topic, partition, suffix)
            }
            KeyPrefix::Processing => format!(
                "processing:{}:{}:{}:{}",
                namespace, topic, partition, suffix
            ),
            KeyPrefix::Delay => format!("delay:{}:{}:{}:{}", namespace, topic, partition, suffix),
            KeyPrefix::Retry => format!("retry:{}:{}:{}:{}", namespace, topic, partition, suffix),
            KeyPrefix::Deadletter => format!(
                "deadletter:{}:{}:{}:{}",
                namespace, topic, partition, suffix
            ),
            KeyPrefix::Expired => {
                format!("expired:{}:{}:{}:{}", namespace, topic, partition, suffix)
            }
        }
    }

    // Called when rebalancing partitions
    fn rebalance_partitions(&mut self, topic: String, partitions: Vec<i32>) {
        self.topics.insert(topic.clone(), partitions.clone());
        self.total_possible_keys = 0;
        for (_, partitions) in self.topics.iter() {
            for _ in partitions.iter() {
                for _ in self.namespaces.iter() {
                    for _ in self.bucket_hashes.iter() {
                        self.total_possible_keys += 1;
                    }
                }
            }
        }
        info!(
            "Rebalanced partitions for topic {}: {:?}: {:?}: total possible keys: {}",
            topic, partitions, self.topics, self.total_possible_keys
        );
    }

    async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        let mut conn = self.pool.get().await?;
        let mut rows_affected: u64 = 0;
        for activation in batch {
            let payload_key = format!(
                "{}:{}",
                self.build_key_with_activation(
                    KeyPrefix::Payload,
                    activation.namespace.as_str(),
                    activation.topic.as_str(),
                    activation.partition,
                    activation.id.as_str()
                ),
                activation.id.as_str()
            );

            let mut pipe = redis::pipe();
            pipe.atomic()
                .hset(payload_key.clone(), "id", activation.id.clone())
                .arg("activation")
                .arg(activation.activation)
                .arg("partition")
                .arg(activation.partition)
                .arg("offset")
                .arg(activation.offset)
                .arg("added_at")
                .arg(activation.added_at.timestamp())
                .arg("received_at")
                .arg(activation.received_at.timestamp())
                .arg("processing_attempts")
                .arg(activation.processing_attempts)
                .arg("processing_deadline_duration")
                .arg(activation.processing_deadline_duration)
                .arg("status")
                .arg(format!("{:?}", activation.status))
                .arg("at_most_once")
                .arg(activation.at_most_once)
                .arg("namespace")
                .arg(activation.namespace.clone())
                .arg("taskname")
                .arg(activation.taskname)
                .arg("on_attempts_exceeded")
                .arg(activation.on_attempts_exceeded.as_str_name());
            let mut expected_args = 13;
            if activation.expires_at.is_some() {
                pipe.arg("expires_at")
                    .arg(activation.expires_at.unwrap().timestamp());
                expected_args += 1;
            }
            if activation.delay_until.is_some() {
                pipe.arg("delay_until")
                    .arg(activation.delay_until.unwrap().timestamp());
                expected_args += 1;
            }
            if activation.processing_deadline.is_some() {
                pipe.arg("processing_deadline")
                    .arg(activation.processing_deadline.unwrap().timestamp());
                expected_args += 1;
            }
            pipe.expire(payload_key.clone(), self.payload_ttl_seconds as i64);

            pipe.hset(
                self.get_id_lookup_key(activation.id.clone().as_str()),
                "id",
                activation.id.clone(),
            )
            .arg("topic")
            .arg(activation.topic.clone())
            .arg("partition")
            .arg(activation.partition)
            .arg("namespace")
            .arg(activation.namespace.clone());

            let mut queue_key_used = String::new();
            if activation.delay_until.is_some() {
                let delay_key = self.build_key_with_activation(
                    KeyPrefix::Delay,
                    activation.namespace.as_str(),
                    activation.topic.as_str(),
                    activation.partition,
                    activation.id.as_str(),
                );
                pipe.zadd(
                    delay_key.clone(),
                    activation.id.clone(),
                    activation.delay_until.unwrap().timestamp(),
                );
                queue_key_used = delay_key;
            } else {
                let pending_key = self.build_key_with_activation(
                    KeyPrefix::Pending,
                    activation.namespace.as_str(),
                    activation.topic.as_str(),
                    activation.partition,
                    activation.id.as_str(),
                );
                pipe.rpush(pending_key.clone(), activation.id.clone());
                queue_key_used = pending_key;
            }

            let mut expired_key = String::new();
            if activation.expires_at.is_some() {
                expired_key = self.build_key_with_activation(
                    KeyPrefix::Expired,
                    activation.namespace.as_str(),
                    activation.topic.as_str(),
                    activation.partition,
                    activation.id.as_str(),
                );
                pipe.zadd(
                    expired_key.clone(),
                    activation.id.clone(),
                    activation.expires_at.unwrap().timestamp(),
                );
            }
            pipe.cmd("WAIT").arg(1).arg(1000);

            let result: Vec<i32> = match pipe.query_async(&mut conn).await {
                Ok(result) => result,
                Err(err) => {
                    error!(
                        "Failed to store activation {} in Redis: {}",
                        payload_key.clone(),
                        err
                    );
                    return Err(anyhow::anyhow!(
                        "Failed to store activation: {}",
                        payload_key.clone()
                    ));
                }
            };

            if result.len() != 4 && result.len() != 5 {
                return Err(anyhow::anyhow!(
                    "Failed to store activation: incorrect number of commands run: expected 4 or 5, got {} for key {}",
                    result.len(),
                    payload_key.clone()
                ));
            }
            // WAIT returns the number of replicas that had the write propagated
            // If there is only one node then it will return 0.
            if result[result.len() - 1] < self.replicas as i32 - 1 {
                return Err(anyhow::anyhow!(
                    "Activation {} was not stored on any replica",
                    payload_key
                ));
            }

            // HSET returns the number of fields set
            if result[0] != expected_args {
                return Err(anyhow::anyhow!(
                    "Failed to store activation: expected {} arguments, got {} for key {}",
                    expected_args,
                    result[0],
                    payload_key.clone()
                ));
            }
            // EXPIRE returns 1 on success and 0 on failure
            if result[1] != 1 {
                return Err(anyhow::anyhow!(
                    "Failed to expire activation for key {}",
                    payload_key
                ));
            }
            // Both ZADD and RPUSH return a count of elements in the structure
            if result[2] <= 0 {
                return Err(anyhow::anyhow!(
                    "Failed to add activation to queue for key {}",
                    queue_key_used
                ));
            }
            // Check if the ZADD happened on the expired key
            if result.len() == 5 && result[3] <= 0 {
                return Err(anyhow::anyhow!(
                    "Failed to add activation to expired queue for key {}",
                    expired_key
                ));
            }
            // Check to ensure that the WAIT command returned at least one replica
            if result.len() == 5 && result[4] <= 0 {
                return Err(anyhow::anyhow!(
                    "Failed to wait for activation to be stored on at least one replica for key {}",
                    payload_key
                ));
            }
            rows_affected += 1;
        }
        Ok(QueryResult { rows_affected })
    }

    async fn add_to_pending(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let pending_key = self.build_key_with_activation(
            KeyPrefix::Pending,
            activation.namespace.as_str(),
            activation.topic.as_str(),
            activation.partition,
            activation.id.as_str(),
        );
        let newlen: usize = conn
            .rpush(pending_key.clone(), activation.id.clone())
            .await?;
        if newlen == 0 {
            return Err(anyhow::anyhow!(
                "Failed to add activation to pending: {}",
                pending_key.clone()
            ));
        }
        Ok(())
    }

    async fn add_to_processing(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let processing_key = self.build_key_with_activation(
            KeyPrefix::Processing,
            activation.namespace.as_str(),
            activation.topic.as_str(),
            activation.partition,
            activation.id.as_str(),
        );
        let newlen: usize = conn
            .zadd(
                processing_key.clone(),
                activation.processing_deadline.unwrap().timestamp(),
                activation.id.clone(),
            )
            .await?;
        if newlen == 0 {
            return Err(anyhow::anyhow!(
                "Failed to add activation to processing: {}",
                processing_key.clone()
            ));
        }
        Ok(())
    }

    async fn add_to_delay(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let delay_key = self.build_key_with_activation(
            KeyPrefix::Delay,
            activation.namespace.as_str(),
            activation.topic.as_str(),
            activation.partition,
            activation.id.as_str(),
        );
        let newlen: usize = conn
            .zadd(
                delay_key.clone(),
                activation.delay_until.unwrap().timestamp(),
                activation.id.clone(),
            )
            .await?;
        if newlen == 0 {
            return Err(anyhow::anyhow!(
                "Failed to add activation to delay: {}",
                delay_key.clone()
            ));
        }
        Ok(())
    }

    async fn add_to_retry(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let retry_key = self.build_key_with_activation(
            KeyPrefix::Retry,
            activation.namespace.as_str(),
            activation.topic.as_str(),
            activation.partition,
            activation.id.as_str(),
        );
        let newlen: usize = conn.rpush(retry_key.clone(), activation.id.clone()).await?;
        if newlen == 0 {
            return Err(anyhow::anyhow!(
                "Failed to add activation to retry: {}",
                retry_key.clone()
            ));
        }
        Ok(())
    }

    async fn add_to_deadletter(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let deadletter_key = self.build_key_with_activation(
            KeyPrefix::Deadletter,
            activation.namespace.as_str(),
            activation.topic.as_str(),
            activation.partition,
            activation.id.as_str(),
        );
        let newlen: usize = conn
            .rpush(deadletter_key.clone(), activation.id.clone())
            .await?;
        if newlen == 0 {
            return Err(anyhow::anyhow!(
                "Failed to add activation to deadletter: {}",
                deadletter_key.clone()
            ));
        }
        Ok(())
    }

    async fn delete_activation(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        let payload_key = format!(
            "{}:{}",
            self.build_key_with_activation(
                KeyPrefix::Payload,
                activation.namespace.as_str(),
                activation.topic.as_str(),
                activation.partition,
                activation.id.as_str()
            ),
            activation.id.as_str()
        );
        pipe.del(payload_key.clone());
        pipe.del(self.get_id_lookup_key(activation.id.as_str()));
        let results: Vec<i32> = pipe.query_async(&mut conn).await?;
        if results.len() != 2 {
            return Err(anyhow::anyhow!(
                "Failed to delete activation: incorrect number of commands run: expected 2, got {} for key {}",
                results.len(),
                payload_key.clone()
            ));
        }
        if results[0] != 1 {
            return Err(anyhow::anyhow!(
                "Failed to delete payload for key {}",
                payload_key.clone()
            ));
        }
        if results[1] != 1 {
            return Err(anyhow::anyhow!(
                "Failed to delete id lookup for key {}",
                activation.id.clone()
            ));
        }
        Ok(())
    }

    // Only used in testing
    async fn delete_all_keys(&self) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let keys: Vec<String> = conn.keys("*").await?;
        for key in keys {
            conn.del(key).await?;
        }
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_pending_activation(
        &self,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
        let namespaces = namespace.map(|ns| vec![ns.to_string()]);
        let result = self
            .get_pending_activations_from_namespaces(namespaces.as_deref(), Some(1))
            .await?;
        if result.is_empty() {
            return Ok(None);
        }
        Ok(Some(result[0].clone()))
    }

    /// Get a pending activation from specified namespaces
    /// If namespaces is None, gets from any namespace
    /// If namespaces is Some(&[...]), gets from those namespaces
    #[instrument(skip_all)]
    async fn get_pending_activations_from_namespaces(
        &self,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
    ) -> Result<Vec<InflightActivation>, Error> {
        let mut local_idx = 0;
        let mut conn = self.pool.get().await?;
        let mut activations: Vec<InflightActivation> = Vec::new();
        for (topic, partitions) in self.topics.iter() {
            for partition in partitions.iter() {
                for namespace in self.namespaces.iter() {
                    if namespaces.is_some() && !namespaces.unwrap().contains(namespace) {
                        continue;
                    }
                    for bucket_hash in self.bucket_hashes.iter() {
                        if local_idx < self.next_key_idx_for_pending {
                            local_idx += 1;
                            continue;
                        }
                        local_idx += 1; // In case of failure below

                        // Get the next pending activation
                        let pending_key = self.build_key_with_activation(
                            KeyPrefix::Pending,
                            namespace.as_str(),
                            topic.as_str(),
                            *partition,
                            bucket_hash.as_str(),
                        );
                        let result = conn.lindex(pending_key.clone(), 0).await?;
                        if result.is_none() {
                            continue;
                        }
                        let activation_id: String = result.unwrap().to_string();

                        let act_result = self
                            .get_by_id(
                                namespace.as_str(),
                                topic.as_str(),
                                *partition,
                                &activation_id,
                            )
                            .await?;
                        if act_result.is_none() {
                            continue;
                        }
                        let activation = act_result.unwrap();

                        // Push the activation to processing. This will not create two entries for the same activation in the case of duplicates.
                        let processing_key = self.build_key_with_activation(
                            KeyPrefix::Processing,
                            namespace.as_str(),
                            topic.as_str(),
                            *partition,
                            bucket_hash.as_str(),
                        );
                        let result: usize = conn
                            .zadd(
                                processing_key.clone(),
                                activation.id.clone(),
                                activation.processing_deadline.unwrap().timestamp(),
                            )
                            .await?;
                        if result == 0 {
                            return Err(anyhow::anyhow!(
                                "Failed to move activation to processing: {} {}",
                                processing_key,
                                activation_id
                            ));
                        }

                        let result: usize = conn
                            .lrem(pending_key.clone(), 1, activation_id.clone())
                            .await?;
                        if result == 0 {
                            info!(
                                "Attempted to lrem an activation from pending queue, but it was not found: {} {}",
                                pending_key, activation_id
                            );
                            metrics::counter!("inflight_redis_activation_store_lrem_not_found")
                                .increment(1);
                        }

                        activations.push(activation);
                        if limit.is_none() {
                            return Ok(activations);
                        } else if activations.len() >= limit.unwrap() as usize {
                            return Ok(activations);
                        }
                    }
                }
            }
        }
        Ok(activations)
    }

    fn incr_next_key_idx_for_pending(&mut self) {
        self.next_key_idx_for_pending += 1;
        if self.next_key_idx_for_pending >= self.total_possible_keys {
            self.next_key_idx_for_pending = 0;
        }
    }

    /// Get an activation by id. Primarily used for testing
    async fn get_by_id(
        &self,
        namespace: &str,
        topic: &str,
        partition: i32,
        id: &str,
    ) -> Result<Option<InflightActivation>, Error> {
        let mut conn = self.pool.get().await?;
        let payload_key =
            self.build_key_with_activation(KeyPrefix::Payload, namespace, topic, partition, id);
        let result: HashMap<String, String> = conn.hgetall(payload_key.clone()).await?;
        if result.is_empty() {
            return Ok(None);
        }
        let activation: InflightActivation = result.into();
        Ok(Some(activation))
    }

    async fn get_by_id_lookup(
        &self,
        activation_id: &str,
    ) -> Result<Option<InflightActivation>, Error> {
        let mut conn = self.pool.get().await?;
        let result: HashMap<String, String> =
            conn.hgetall(self.get_id_lookup_key(activation_id)).await?;
        if result.is_empty() {
            return Ok(None);
        }

        let namespace: String = result.get("namespace").unwrap().to_string();
        let topic: String = result.get("topic").unwrap().to_string();
        let partition: i32 = result.get("partition").unwrap().parse().unwrap();
        let activation = self
            .get_by_id(namespace.as_str(), topic.as_str(), partition, activation_id)
            .await?;
        Ok(activation)
    }

    async fn set_status(
        &self,
        activation_id: &str,
        status: InflightActivationStatus,
    ) -> Result<(), Error> {
        let activation = self.get_by_id_lookup(activation_id).await?;
        if activation.is_none() {
            return Err(anyhow::anyhow!(
                "Activation not found for id: {}",
                activation_id
            ));
        }
        let activation = activation.unwrap();
        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        pipe.atomic();
        if status == InflightActivationStatus::Retry {
            pipe.rpush(
                self.build_key_with_activation(
                    KeyPrefix::Retry,
                    activation.namespace.as_str(),
                    activation.topic.as_str(),
                    activation.partition,
                    activation.id.as_str(),
                ),
                activation_id,
            );
        } else if status == InflightActivationStatus::Failure
            && activation.on_attempts_exceeded == OnAttemptsExceeded::Deadletter
        {
            pipe.rpush(
                self.build_key_with_activation(
                    KeyPrefix::Deadletter,
                    activation.namespace.as_str(),
                    activation.topic.as_str(),
                    activation.partition,
                    activation.id.as_str(),
                ),
                activation_id,
            );
        }
        let processing_key = self.build_key_with_activation(
            KeyPrefix::Processing,
            activation.namespace.as_str(),
            activation.topic.as_str(),
            activation.partition,
            activation.id.as_str(),
        );
        pipe.zrem(processing_key, activation_id);
        pipe.del(self.build_key_with_activation(
            KeyPrefix::Payload,
            activation.namespace.as_str(),
            activation.topic.as_str(),
            activation.partition,
            activation.id.as_str(),
        ));
        pipe.del(self.get_id_lookup_key(activation_id));
        let results: Vec<i32> = pipe.query_async(&mut *conn).await?;
        if results.len() != 3 {
            return Err(anyhow::anyhow!(
                "Failed to set status: incorrect number of commands run: expected 4, got {} for key {}",
                results.len(),
                activation_id
            ));
        }
        if results[0] >= 0 {
            // The RPUSH to retry/deadletter
            return Err(anyhow::anyhow!(
                "Activation discarded instead of being handled: {}",
                activation_id
            ));
        }
        if results[1] != 1 {
            // Removing from processing set
            return Err(anyhow::anyhow!(
                "Failed to remove activation from processing set: {}",
                activation_id
            ));
        }
        if results[2] != 1 {
            // Deleting payload
            return Err(anyhow::anyhow!(
                "Failed to delete payload: {}",
                activation_id
            ));
        }
        if results[3] != 1 {
            // Deleting id lookup
            return Err(anyhow::anyhow!(
                "Failed to delete id lookup: {}",
                activation_id
            ));
        }
        Ok(())
    }

    pub async fn move_delay_to_pending(&self) -> Result<(), Error> {
        Ok(())
    }

    pub async fn get_processing_deadline_exceeded_activations(
        &self,
    ) -> Result<Vec<InflightActivation>, Error> {
        return Ok(vec![]);
    }

    pub async fn get_processing_attempts_for_activation(&self, id: &str) -> Result<i32, Error> {
        return Ok(0);
    }

    pub async fn retry_activation_locally(&self, id: &str) -> Result<(), Error> {
        // Increment processing attempts by 1 and push back to pending in transaction
        return Ok(());
    }

    pub async fn remove_from_processing(&self, id: &str) -> Result<(), Error> {
        // Remove from processing in transaction
        return Ok(());
    }

    pub async fn remove_from_pending(&self, id: &str) -> Result<(), Error> {
        // Remove from pending in transaction
        return Ok(());
    }

    pub async fn remove_from_delay(&self, id: &str) -> Result<(), Error> {
        // Remove from delay in transaction
        return Ok(());
    }

    pub async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        return Ok(vec![]);
    }

    pub async fn get_deadletter_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        return Ok(vec![]);
    }

    pub async fn get_expired_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        return Ok(vec![]);
    }

    #[instrument(skip_all)]
    pub async fn count_pending_activations(&self) -> Result<usize, Error> {
        let mut conn = self.pool.get().await?;
        let mut total_count = 0;
        for (topic, partitions) in self.topics.iter() {
            for partition in partitions.iter() {
                for namespace in self.namespaces.iter() {
                    for bucket_hash in self.bucket_hashes.iter() {
                        let pending_key = self.build_key(
                            KeyPrefix::Pending,
                            namespace.as_str(),
                            topic.as_str(),
                            *partition,
                            bucket_hash.as_str(),
                        );
                        let count: usize = conn.llen(pending_key).await?;
                        total_count += count;
                    }
                }
            }
        }
        return Ok(total_count);
    }

    #[instrument(skip_all)]
    pub async fn count_delayed_activations(&self) -> Result<usize, Error> {
        let mut conn = self.pool.get().await?;
        let mut total_count = 0;
        for (topic, partitions) in self.topics.iter() {
            for partition in partitions.iter() {
                for namespace in self.namespaces.iter() {
                    for bucket_hash in self.bucket_hashes.iter() {
                        let delay_key = self.build_key(
                            KeyPrefix::Delay,
                            namespace.as_str(),
                            topic.as_str(),
                            *partition,
                            bucket_hash.as_str(),
                        );
                        let count: usize = conn.zcard(delay_key.clone()).await?;
                        total_count += count;
                    }
                }
            }
        }
        return Ok(total_count);
    }

    #[instrument(skip_all)]
    pub async fn count_processing_activations(&self) -> Result<usize, Error> {
        let mut conn = self.pool.get().await?;
        let mut total_count = 0;
        for (topic, partitions) in self.topics.iter() {
            for partition in partitions.iter() {
                for namespace in self.namespaces.iter() {
                    for bucket_hash in self.bucket_hashes.iter() {
                        let processing_key = self.build_key(
                            KeyPrefix::Processing,
                            namespace.as_str(),
                            topic.as_str(),
                            *partition,
                            bucket_hash.as_str(),
                        );
                        let count: usize = conn.zcard(processing_key.clone()).await?;
                        total_count += count;
                    }
                }
            }
        }
        return Ok(total_count);
    }

    async fn db_size(&self) -> Result<u64, Error> {
        return Ok(0);
    }
}
