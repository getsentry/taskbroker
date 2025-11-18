use tracing::instrument;
// use deadpool_redis::Pool;
use crate::config::Config;
use crate::store::inflight_activation::{InflightActivation, QueryResult};
use anyhow::Error;
// use deadpool_redis::cluster::{Config as RedisConfig, Pool, Runtime};
use deadpool_redis::{Config as RedisConfig, Pool, Runtime};
use redis::AsyncTypedCommands;
use uuid::Uuid;

pub enum KeyPrefix {
    Payload,
    Pending,
    Processing,
    Delay,
    Retry,
    Deadletter,
    Expired,
}

pub struct RedisActivationStoreConfig {
    pub topics: Vec<String>,
    pub partitions: Vec<i32>,
    pub namespaces: Vec<String>,
    pub num_buckets: usize,
    pub payload_ttl_seconds: u64,
}

impl RedisActivationStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            topics: vec![config.kafka_topic.clone()],
            partitions: vec![0],
            namespaces: config.namespaces.clone(),
            num_buckets: config.num_redis_buckets,
            payload_ttl_seconds: config.payload_ttl_seconds,
        }
    }
}

pub async fn create_redis_pool(urls: Vec<String>) -> Result<Pool, Error> {
    let cfg = RedisConfig::from_url(urls[0].clone());
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    // let cfg = RedisConfig::from_urls(urls);
    // let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    Ok(pool)
}

pub struct RedisActivationStore {
    pool: Pool,
    topics: Vec<String>,
    partitions: Vec<i32>,
    namespaces: Vec<String>,
    num_buckets: usize,
    payload_ttl_seconds: u64,
}

impl RedisActivationStore {
    pub async fn new(urls: Vec<String>, config: RedisActivationStoreConfig) -> Result<Self, Error> {
        let pool = create_redis_pool(urls).await?;
        Ok(Self {
            pool,
            topics: config.topics.clone(),
            partitions: config.partitions,
            namespaces: config.namespaces.clone(),
            num_buckets: config.num_buckets,
            payload_ttl_seconds: config.payload_ttl_seconds,
        })
    }

    pub fn build_key_with_activation(
        &self,
        prefix: KeyPrefix,
        namespace: String,
        topic: String,
        partition: i32,
        activation_id: String,
    ) -> String {
        let uuid = Uuid::parse_str(&activation_id).unwrap();
        let as_u128: u128 = uuid.as_u128();
        self.build_key(
            prefix,
            namespace,
            topic,
            partition,
            format!("{:04x}", as_u128 % self.num_buckets as u128),
        )
    }

    pub fn build_key_with_bucket(
        &self,
        prefix: KeyPrefix,
        namespace: String,
        topic: String,
        partition: i32,
        bucket: usize,
    ) -> String {
        self.build_key(
            prefix,
            namespace,
            topic,
            partition,
            format!("{:04x}", bucket),
        )
    }

    pub fn build_key(
        &self,
        prefix: KeyPrefix,
        namespace: String,
        topic: String,
        partition: i32,
        suffix: String,
    ) -> String {
        match prefix {
            KeyPrefix::Payload => {
                format!("payload:{}:{}:{}:{}", namespace, topic, partition, suffix)
            }
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

    pub async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        let mut conn = self.pool.get().await?;
        for activation in batch {
            let payload_key = format!(
                "{}:{}",
                self.build_key_with_activation(
                    KeyPrefix::Payload,
                    activation.namespace.clone(),
                    self.topics[0].clone(),
                    activation.partition,
                    activation.id.clone()
                ),
                activation.id.clone()
            );

            let mut expected_commands = 3;
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
                .arg("expires_at")
                .arg(activation.expires_at.map(|dt| dt.timestamp()))
                .arg("delay_until")
                .arg(activation.delay_until.map(|dt| dt.timestamp()))
                .arg("processing_deadline_duration")
                .arg(activation.processing_deadline_duration)
                .arg("processing_deadline")
                .arg(activation.processing_deadline.map(|dt| dt.timestamp()))
                .arg("status")
                .arg(format!("{:?}", activation.status))
                .arg("at_most_once")
                .arg(activation.at_most_once)
                .arg("namespace")
                .arg(activation.namespace.clone())
                .arg("taskname")
                .arg(activation.taskname)
                .arg("on_attempts_exceeded")
                .arg(activation.on_attempts_exceeded as i32);
            pipe.expire(payload_key.clone(), self.payload_ttl_seconds as i64);

            if activation.delay_until.is_some() {
                let delay_key = self.build_key_with_activation(
                    KeyPrefix::Delay,
                    activation.namespace.clone(),
                    self.topics[0].clone(),
                    activation.partition,
                    activation.id.clone(),
                );
                pipe.zadd(
                    delay_key,
                    activation.delay_until.unwrap().timestamp(),
                    activation.id.clone(),
                );
            } else {
                let pending_key = self.build_key_with_activation(
                    KeyPrefix::Pending,
                    activation.namespace.clone(),
                    self.topics[0].clone(),
                    activation.partition,
                    activation.id.clone(),
                );
                pipe.rpush(pending_key, activation.id.clone());
            }

            if activation.expires_at.is_some() {
                let expired_key = self.build_key_with_activation(
                    KeyPrefix::Expired,
                    activation.namespace.clone(),
                    self.topics[0].clone(),
                    activation.partition,
                    activation.id.clone(),
                );
                pipe.zadd(
                    expired_key,
                    activation.expires_at.unwrap().timestamp(),
                    activation.id.clone(),
                );
                expected_commands += 1;
            }

            let result: Vec<i32> = pipe.query_async(&mut conn).await?;
            if result.len() != expected_commands {
                return Err(anyhow::anyhow!(
                    "Failed to store activation: {}",
                    payload_key.clone()
                ));
            }
        }
        Ok(QueryResult { rows_affected: 0 })
    }

    // Called when rebalancing partitions
    pub async fn rebalance_partitions(&mut self, partitions: Vec<i32>) -> Result<(), Error> {
        self.partitions = partitions;
        Ok(())
    }

    pub async fn add_to_pending(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let pending_key = self.build_key_with_activation(
            KeyPrefix::Pending,
            activation.namespace.clone(),
            self.topics[0].clone(),
            activation.partition,
            activation.id.clone(),
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

    pub async fn add_to_processing(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let processing_key = self.build_key_with_activation(
            KeyPrefix::Processing,
            activation.namespace.clone(),
            self.topics[0].clone(),
            activation.partition,
            activation.id.clone(),
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

    pub async fn add_to_delay(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let delay_key = self.build_key_with_activation(
            KeyPrefix::Delay,
            activation.namespace.clone(),
            self.topics[0].clone(),
            activation.partition,
            activation.id.clone(),
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

    pub async fn add_to_retry(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let retry_key = self.build_key_with_activation(
            KeyPrefix::Retry,
            activation.namespace.clone(),
            self.topics[0].clone(),
            activation.partition,
            activation.id.clone(),
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

    pub async fn add_to_deadletter(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let deadletter_key = self.build_key_with_activation(
            KeyPrefix::Deadletter,
            activation.namespace.clone(),
            self.topics[0].clone(),
            activation.partition,
            activation.id.clone(),
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

    pub async fn delete_activation(&self, activation: InflightActivation) -> Result<(), Error> {
        let mut conn = self.pool.get().await?;
        let payload_key = format!(
            "{}:{}",
            self.build_key_with_activation(
                KeyPrefix::Payload,
                activation.namespace.clone(),
                self.topics[0].clone(),
                activation.partition,
                activation.id.clone()
            ),
            activation.id.clone()
        );
        let deleted: usize = conn.del(payload_key.clone()).await?;
        if deleted == 0 {
            return Err(anyhow::anyhow!(
                "Failed to delete activation: {}",
                payload_key.clone()
            ));
        }
        Ok(())
    }

    /// Get an activation by id. Primarily used for testing
    pub async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
        return Ok(None);
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
    pub async fn get_pending_activation(
        &self,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
        Ok(None)
    }

    /// Get a pending activation from specified namespaces
    /// If namespaces is None, gets from any namespace
    /// If namespaces is Some(&[...]), gets from those namespaces
    #[instrument(skip_all)]
    pub async fn get_pending_activations_from_namespaces(
        &self,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
    ) -> Result<Vec<InflightActivation>, Error> {
        return Ok(vec![]);
    }

    pub async fn count_pending_activations(&self) -> Result<usize, Error> {
        return Ok(0);
    }

    pub async fn count_delayed_activations(&self) -> Result<usize, Error> {
        return Ok(0);
    }

    pub async fn count_processing_activations(&self) -> Result<usize, Error> {
        return Ok(0);
    }

    pub async fn db_size(&self) -> Result<u64, Error> {
        return Ok(0);
    }
}
