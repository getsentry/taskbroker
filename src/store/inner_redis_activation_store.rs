use crate::store::inflight_activation::{
    InflightActivation, InflightActivationStatus, QueryResult,
};
use crate::store::redis_utils::{HashKey, KeyBuilder, RandomStartIterator};
use anyhow::Error;
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Duration, Utc};
use deadpool_redis::Pool;
use futures::future::try_join_all;
use redis::AsyncTypedCommands;
use sentry_protos::taskbroker::v1::OnAttemptsExceeded;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{error, info, instrument};

#[derive(Debug)]
pub struct InnerRedisActivationStore {
    pool: Pool,
    replicas: usize,
    topics: HashMap<String, Vec<i32>>,
    namespaces: Vec<String>,
    payload_ttl_seconds: u64,
    bucket_hashes: Vec<String>,
    hash_keys: Vec<HashKey>,
    key_builder: KeyBuilder,
    processing_deadline_grace_sec: i64,
    max_processing_attempts: i32,
}

impl InnerRedisActivationStore {
    pub async fn new(
        pool: Pool,
        replicas: usize,
        topics: HashMap<String, Vec<i32>>,
        namespaces: Vec<String>,
        num_buckets: usize,
        payload_ttl_seconds: u64,
        processing_deadline_grace_sec: u64,
        max_processing_attempts: usize,
    ) -> Result<Self, Error> {
        let bucket_hashes = (0..num_buckets).map(|i| format!("{:04x}", i)).collect();
        let mut hash_keys = Vec::new();
        for (topic, partitions) in topics.iter() {
            for partition in partitions.iter() {
                for namespace in namespaces.iter() {
                    hash_keys.push(HashKey::new(namespace.clone(), topic.clone(), *partition));
                }
            }
        }

        Ok(Self {
            pool,
            replicas,
            topics,
            namespaces,
            bucket_hashes,
            hash_keys,
            payload_ttl_seconds,
            key_builder: KeyBuilder::new(num_buckets),
            processing_deadline_grace_sec: processing_deadline_grace_sec as i64, // Duration expects i64
            max_processing_attempts: max_processing_attempts as i32,
        })
    }

    // Called when rebalancing partitions
    pub fn rebalance_partitions(&mut self, topic: String, partitions: Vec<i32>) {
        // This assumes that the broker is always consuming from the same topics and only the partitions are changing
        self.topics.insert(topic.clone(), partitions.clone());
        self.hash_keys.clear();
        let mut hashkeys = 0;
        for (topic, partitions) in self.topics.iter() {
            for partition in partitions.iter() {
                for namespace in self.namespaces.iter() {
                    self.hash_keys
                        .push(HashKey::new(namespace.clone(), topic.clone(), *partition));
                    hashkeys += self.bucket_hashes.len();
                }
            }
        }
        info!(
            "Rebalanced partitions for topic {}: {:?}: {:?}: total hashkeys: {}",
            topic, partitions, self.topics, hashkeys
        );
    }

    pub async fn get_conn(&self) -> Result<deadpool_redis::Connection, Error> {
        let start_time = Instant::now();
        let conn = self.pool.get().await?;
        let conn_duration = start_time.duration_since(start_time);
        metrics::histogram!("redis_store.conn_duration").record(conn_duration.as_millis() as f64);
        Ok(conn)
    }

    #[instrument(skip_all)]
    pub async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        let mut conn = self.get_conn().await?;
        let mut rows_affected: u64 = 0;
        let start_time = Instant::now();
        for activation in batch {
            let payload_key = self
                .key_builder
                .get_payload_key(
                    HashKey::new(
                        activation.namespace.clone(),
                        activation.topic.clone(),
                        activation.partition,
                    ),
                    activation.id.as_str(),
                )
                .build_redis_key();

            // Base64 encode the activation since Redis HGETALL doesn't handle the bytes correctly (it tries to UTF-8 decode it)
            let encoded_activation = general_purpose::STANDARD.encode(&activation.activation);
            let mut pipe = redis::pipe();
            pipe.atomic()
                .hset(payload_key.clone(), "id", activation.id.clone())
                .arg("activation")
                .arg(encoded_activation)
                .arg("status")
                .arg(format!("{:?}", activation.status))
                .arg("topic")
                .arg(activation.topic.clone())
                .arg("partition")
                .arg(activation.partition)
                .arg("offset")
                .arg(activation.offset)
                .arg("added_at")
                .arg(activation.added_at.timestamp_millis())
                .arg("received_at")
                .arg(activation.received_at.timestamp_millis())
                .arg("processing_attempts")
                .arg(activation.processing_attempts)
                .arg("processing_deadline_duration")
                .arg(activation.processing_deadline_duration)
                .arg("at_most_once")
                .arg(activation.at_most_once.to_string())
                .arg("namespace")
                .arg(activation.namespace.clone())
                .arg("taskname")
                .arg(activation.taskname)
                .arg("on_attempts_exceeded")
                .arg(activation.on_attempts_exceeded.as_str_name());

            let mut expected_args = 14;
            if activation.expires_at.is_some() {
                pipe.arg("expires_at")
                    .arg(activation.expires_at.unwrap().timestamp_millis());
                expected_args += 1;
            }
            if activation.delay_until.is_some() {
                pipe.arg("delay_until")
                    .arg(activation.delay_until.unwrap().timestamp_millis());
                expected_args += 1;
            }
            if activation.processing_deadline.is_some() {
                pipe.arg("processing_deadline")
                    .arg(activation.processing_deadline.unwrap().timestamp_millis());
                expected_args += 1;
            }
            pipe.expire(payload_key.clone(), self.payload_ttl_seconds as i64);

            let mut queue_key_used = String::new();
            if activation.delay_until.is_some() {
                let delay_key = self
                    .key_builder
                    .get_delay_key(
                        HashKey::new(
                            activation.namespace.clone(),
                            activation.topic.clone(),
                            activation.partition,
                        ),
                        activation.id.as_str(),
                    )
                    .build_redis_key();
                pipe.zadd(
                    delay_key.clone(),
                    activation.id.clone(),
                    activation.delay_until.unwrap().timestamp_millis() as isize,
                );
                queue_key_used = delay_key;
            } else {
                let pending_key = self
                    .key_builder
                    .get_pending_key(
                        HashKey::new(
                            activation.namespace.clone(),
                            activation.topic.clone(),
                            activation.partition,
                        ),
                        activation.id.as_str(),
                    )
                    .build_redis_key();
                pipe.rpush(pending_key.clone(), activation.id.clone());
                queue_key_used = pending_key;
            }

            let mut expired_key = String::new();
            if activation.expires_at.is_some() {
                expired_key = self
                    .key_builder
                    .get_expired_key(
                        HashKey::new(
                            activation.namespace.clone(),
                            activation.topic.clone(),
                            activation.partition,
                        ),
                        activation.id.as_str(),
                    )
                    .build_redis_key();
                pipe.zadd(
                    expired_key.clone(),
                    activation.id.clone(),
                    activation.expires_at.unwrap().timestamp_millis() as isize,
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
            // This should always return 0
            if *result.last().unwrap() != 0 {
                return Err(anyhow::anyhow!(
                    "Failed to wait for activation to be stored on at least one replica for key {}",
                    payload_key
                ));
            }

            // This key has to be set separately since the transaction expects all keys to be in the same hash slot
            // and this can't be guaranteed since it doesn't contain the hash key.
            let mut pipe = redis::pipe();
            let lookup_key = self
                .key_builder
                .get_id_lookup_key(activation.id.as_str())
                .build_redis_key();
            pipe.hset(lookup_key.clone(), "id", activation.id.clone())
                .arg("topic")
                .arg(activation.topic.clone())
                .arg("partition")
                .arg(activation.partition)
                .arg("namespace")
                .arg(activation.namespace.clone());

            pipe.expire(lookup_key.clone(), self.payload_ttl_seconds as i64);
            let result: Vec<i32> = pipe.query_async(&mut conn).await?;
            if result.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Failed to set id lookup for key {}",
                    lookup_key.clone()
                ));
            }
            if result[0] != 4 {
                return Err(anyhow::anyhow!(
                    "Failed to set id lookup for key {}",
                    lookup_key.clone()
                ));
            }
            if result[1] != 1 {
                return Err(anyhow::anyhow!(
                    "Failed to expire id lookup for key {}",
                    lookup_key.clone()
                ));
            }
            rows_affected += 1;
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.store_duration").record(duration.as_millis() as f64);
        metrics::counter!("redis_store.store_count").increment(rows_affected);
        Ok(QueryResult { rows_affected })
    }

    pub async fn cleanup_activation(
        &self,
        hashkey: HashKey,
        activation_id: &str,
    ) -> Result<(), Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let payload_key = self
            .key_builder
            .get_payload_key(hashkey, activation_id)
            .build_redis_key();
        let id_lookup_key = self
            .key_builder
            .get_id_lookup_key(activation_id)
            .build_redis_key();
        let result: usize = conn.del(payload_key.clone()).await?;
        if result != 1 {
            return Err(anyhow::anyhow!(
                "Failed to cleanup payload for key {}",
                payload_key.clone()
            ));
        }
        let result: usize = conn.del(id_lookup_key.clone()).await?;
        if result != 1 {
            return Err(anyhow::anyhow!(
                "Failed to cleanup id lookup for key {}",
                id_lookup_key.clone()
            ));
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.cleanup_duration").record(duration.as_millis() as f64);
        Ok(())
    }
    /// Discard an activation. If the activation is at_most_once, remove the payloads.
    #[instrument(skip_all)]
    pub async fn discard_activation(
        &self,
        hashkey: HashKey,
        activation_id: &str,
    ) -> Result<(), Error> {
        // If the activation is not found, return a no-op.
        // If the activation is at_most_once, discard the activation and remove the payloads.
        // If it has deadletter configured, move it to the deadletter queue and keep the payloads.
        let start_time = Instant::now();
        let fields = self
            .get_fields_by_id(
                hashkey.clone(),
                activation_id,
                &["at_most_once", "on_attempts_exceeded"],
            )
            .await?;
        if fields.is_empty() {
            return Ok(());
        }
        let at_most_once = fields.get("at_most_once").unwrap().parse::<bool>().unwrap();
        let on_attempts_exceeded =
            OnAttemptsExceeded::from_str_name(fields.get("on_attempts_exceeded").unwrap().as_str())
                .unwrap();
        let mut conn = self.get_conn().await?;
        if !at_most_once && on_attempts_exceeded == OnAttemptsExceeded::Deadletter {
            let deadletter_key = self
                .key_builder
                .get_deadletter_key(hashkey.clone(), activation_id)
                .build_redis_key();
            let result: usize = conn.rpush(deadletter_key.clone(), activation_id).await?;
            if result == 0 {
                return Err(anyhow::anyhow!(
                    "Failed to add activation to deadletter: {}",
                    deadletter_key.clone()
                ));
            }
            let end_time = Instant::now();
            let duration = end_time.duration_since(start_time);
            metrics::histogram!("redis_store.discard_activation_duration", "deadletter" => "true")
                .record(duration.as_millis() as f64);
            return Ok(());
        }
        self.cleanup_activation(hashkey, activation_id).await?;
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.cleanup_activation_duration", "deadletter" => "false")
            .record(duration.as_millis() as f64);
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn get_pending_activation(
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
    pub async fn get_pending_activations_from_namespaces(
        &self,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
    ) -> Result<Vec<InflightActivation>, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let mut activations: Vec<InflightActivation> = Vec::new();
        let random_iterator = RandomStartIterator::new(self.hash_keys.len());
        let mut buckets_checked = 0;
        let mut hashes_checked = 0;
        for idx in random_iterator {
            hashes_checked += 1;
            let hash_key = self.hash_keys[idx].clone();
            if namespaces.is_some() && !namespaces.unwrap().contains(&hash_key.namespace) {
                metrics::counter!(
                    "redis_store.get_pending_activations_from_namespaces.namespace_not_found"
                )
                .increment(1);
                continue;
            }
            let hash_iterator = RandomStartIterator::new(self.bucket_hashes.len());
            for bucket_idx in hash_iterator {
                let bucket_hash = self.bucket_hashes[bucket_idx].clone();
                buckets_checked += 1;
                // Get the next pending activation
                let get_by_id_start_time = Instant::now();
                let pending_key = self
                    .key_builder
                    .get_pending_key_for_iter(hash_key.clone(), bucket_hash.as_str())
                    .build_redis_key();
                let result = conn.lindex(pending_key.clone(), 0).await?;
                if result.is_none() {
                    let get_by_id_duration =
                        get_by_id_start_time.duration_since(get_by_id_start_time);
                    metrics::histogram!("redis_store.get_pending_activations_from_namespaces.get_by_id_duration.duration", "result" => "false").record(get_by_id_duration.as_millis() as f64);
                    continue;
                }
                let activation_id: String = result.unwrap().to_string();

                let act_result = self.get_by_id(hash_key.clone(), &activation_id).await?;
                if act_result.is_none() {
                    let get_by_id_duration =
                        get_by_id_start_time.duration_since(get_by_id_start_time);
                    metrics::histogram!("redis_store.get_pending_activations_from_namespaces.get_by_id_duration.duration", "result" => "false").record(get_by_id_duration.as_millis() as f64);
                    metrics::counter!("redis_store.get_pending_activations_from_namespaces.get_by_id_duration.not_found").increment(1);
                    continue;
                }
                let activation = act_result.unwrap();

                // Push the activation to processing. This will not create two entries for the same activation in the case of duplicates.
                let processing_key = self
                    .key_builder
                    .get_processing_key(hash_key.clone(), &activation_id)
                    .build_redis_key();
                let processing_deadline = match activation.processing_deadline {
                    None => Utc::now() + Duration::seconds(self.processing_deadline_grace_sec),
                    Some(apd) => apd,
                }
                .timestamp_millis();
                let result: usize = conn
                    .zadd(
                        processing_key.clone(),
                        activation.id.clone(),
                        processing_deadline,
                    )
                    .await?;
                if result == 0 {
                    // If the activation is already in the processing set, this is not an error.
                    error!(
                        "Failed to move activation to processing: {} {}",
                        processing_key, activation_id
                    );
                    metrics::counter!("redis_store.get_pending_activations_from_namespaces.already_moved_to_processing").increment(1);
                }

                let result: usize = conn
                    .lrem(pending_key.clone(), 1, activation_id.clone())
                    .await?;
                if result == 0 {
                    error!(
                        "Attempted to lrem an activation from pending queue, but it was not found: {} {}",
                        pending_key, activation_id
                    );
                    metrics::counter!("redis_store.get_pending_activations_from_namespaces.already_removed_from_pending")
                        .increment(1);
                }
                let get_by_id_duration = get_by_id_start_time.duration_since(get_by_id_start_time);
                metrics::histogram!("redis_store.get_pending_activations_from_namespaces.get_by_id_duration", "result" => "true").record(get_by_id_duration.as_millis() as f64);
                activations.push(activation);
                if activations.len() >= limit.unwrap() as usize {
                    let end_time = Instant::now();
                    let duration = end_time.duration_since(start_time);
                    metrics::histogram!(
                        "redis_store.get_pending_activations_from_namespaces.duration"
                    )
                    .record(duration.as_millis() as f64);
                    metrics::histogram!(
                        "redis_store.get_pending_activations_from_namespaces.buckets_checked"
                    )
                    .record(buckets_checked as f64);
                    metrics::histogram!(
                        "redis_store.get_pending_activations_from_namespaces.hashes_checked"
                    )
                    .record(hashes_checked as f64);
                    return Ok(activations);
                }
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.get_pending_activations_from_namespaces.duration")
            .record(duration.as_millis() as f64);
        metrics::counter!("redis_store.get_pending_activations_from_namespaces.buckets_checked")
            .increment(buckets_checked);
        metrics::counter!("redis_store.get_pending_activations_from_namespaces.hashes_checked")
            .increment(hashes_checked);
        Ok(activations)
    }

    /// Get an activation by id. Primarily used for testing
    pub async fn get_by_id(
        &self,
        hash_key: HashKey,
        activation_id: &str,
    ) -> Result<Option<InflightActivation>, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let payload_key = self
            .key_builder
            .get_payload_key(hash_key, activation_id)
            .build_redis_key();
        let result: HashMap<String, String> = conn.hgetall(payload_key.clone()).await?;
        if result.is_empty() {
            metrics::counter!("redis_store.get_by_id", "result" => "false").increment(1);
            return Ok(None);
        }
        let activation: InflightActivation = result.into();
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.get_by_id_duration").record(duration.as_millis() as f64);
        metrics::counter!("redis_store.get_by_id", "result" => "true").increment(1);
        Ok(Some(activation))
    }

    pub async fn get_by_id_lookup(
        &self,
        activation_id: &str,
    ) -> Result<Option<InflightActivation>, Error> {
        let result = self.get_hashkey_by_id(activation_id).await?;
        if result.is_none() {
            return Ok(None);
        }

        let hash_key = result.unwrap();
        let activation = self.get_by_id(hash_key, activation_id).await?;
        Ok(activation)
    }

    pub async fn get_hashkey_by_id(&self, activation_id: &str) -> Result<Option<HashKey>, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let result: HashMap<String, String> = conn
            .hgetall(
                self.key_builder
                    .get_id_lookup_key(activation_id)
                    .build_redis_key(),
            )
            .await?;
        if result.is_empty() {
            metrics::counter!("redis_store.get_hashkey_by_id", "result" => "false").increment(1);
            let end_time = Instant::now();
            let duration = end_time.duration_since(start_time);
            metrics::histogram!("redis_store.get_hashkey_by_id_duration")
                .record(duration.as_millis() as f64);
            return Ok(None);
        }
        metrics::counter!("redis_store.get_hashkey_by_id", "result" => "true").increment(1);
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.get_hashkey_by_id_duration")
            .record(duration.as_millis() as f64);
        Ok(Some(HashKey::new(
            result.get("namespace").unwrap().to_string(),
            result.get("topic").unwrap().to_string(),
            result.get("partition").unwrap().parse().unwrap(),
        )))
    }

    pub async fn get_fields_by_id(
        &self,
        hash_key: HashKey,
        activation_id: &str,
        fields: &[&str],
    ) -> Result<HashMap<String, String>, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let payload_key = self
            .key_builder
            .get_payload_key(hash_key, activation_id)
            .build_redis_key();
        let mut pipe = redis::pipe();
        pipe.hmget(payload_key.clone(), fields[0]);
        for field in fields.iter().skip(1) {
            pipe.arg(field);
        }
        let result: Vec<Vec<String>> = pipe.query_async(&mut *conn).await?;
        // Returns an array of tuples with the values in the same order as the fields array.
        // These needs to be combined into a map.
        let mut fields_map = HashMap::new();
        for values in result.iter() {
            for (idx, arg_name) in fields.iter().enumerate() {
                fields_map.insert(arg_name.to_string(), values[idx].clone());
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.get_fields_by_id_duration")
            .record(duration.as_millis() as f64);
        Ok(fields_map)
    }

    pub async fn set_status(
        &self,
        activation_id: &str,
        status: InflightActivationStatus,
    ) -> Result<(), Error> {
        // If the activation is not found, return a no-op
        let start_time = Instant::now();
        let activation = self.get_by_id_lookup(activation_id).await?;
        if activation.is_none() {
            error!(
                "Activation not found for id: {}, skipping status update",
                activation_id
            );
            metrics::counter!("redis_store.set_status.activation_not_found").increment(1);
            return Ok(());
        }
        let activation = activation.unwrap();
        let hash_key = HashKey::new(
            activation.namespace.clone(),
            activation.topic.clone(),
            activation.partition,
        );

        let mut conn = self.get_conn().await?;
        let mut pipe = redis::pipe();
        pipe.atomic();
        let mut has_failure = false;
        if status == InflightActivationStatus::Retry {
            has_failure = true;
            let retry_key = self
                .key_builder
                .get_retry_key(hash_key.clone(), activation.id.as_str())
                .build_redis_key();
            pipe.rpush(retry_key, activation_id);
        } else if status == InflightActivationStatus::Failure
            && activation.on_attempts_exceeded == OnAttemptsExceeded::Deadletter
        {
            has_failure = true;
            pipe.rpush(
                self.key_builder
                    .get_deadletter_key(hash_key.clone(), activation.id.as_str())
                    .build_redis_key(),
                activation_id,
            );
        } else if status == InflightActivationStatus::Complete {
            self.cleanup_activation(hash_key.clone(), activation.id.as_str())
                .await?;
        }
        let processing_key = self
            .key_builder
            .get_processing_key(hash_key.clone(), activation.id.as_str())
            .build_redis_key();
        pipe.zrem(processing_key, activation_id);

        let results: Vec<i32> = pipe.query_async(&mut *conn).await?;
        let expected_commands = if has_failure { 2 } else { 1 };
        if results.len() != expected_commands {
            return Err(anyhow::anyhow!(
                "Failed to set status: incorrect number of commands run: expected {}, got {} for key {}",
                expected_commands,
                results.len(),
                activation_id
            ));
        }

        let processing_removed = if has_failure { results[1] } else { results[0] };
        if has_failure && results[0] != 1 {
            return Err(anyhow::anyhow!(
                "Failed to add activation to retry/deadletter queue: {}",
                activation_id
            ));
        }

        if processing_removed != 1 {
            // If another worker already removed the activation from the processing set, this is not an error.
            error!(
                "Failed to remove activation from processing set: {}",
                activation_id
            );
            metrics::counter!("redis_store.set_status.already_removed_from_processing")
                .increment(1);
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.set_status.duration", "status" => format!("{:?}", status))
            .record(duration.as_millis() as f64);
        Ok(())
    }

    pub async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let mut activation_ids: Vec<(HashKey, String)> = Vec::new();
        for hash_key in self.hash_keys.iter() {
            for bucket_hash in self.bucket_hashes.iter() {
                let retry_key = self
                    .key_builder
                    .get_retry_key_for_iter(hash_key.clone(), bucket_hash.as_str());
                let result: Vec<String> = conn.lrange(retry_key.build_redis_key(), 0, -1).await?;
                activation_ids.extend(
                    result
                        .iter()
                        .map(|id| (retry_key.hashkey.clone(), id.clone())),
                );
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.get_retry_activations.retry_loop.duration")
            .record(duration.as_millis() as f64);
        metrics::counter!("redis_store.get_retry_activations.retry_loop.activations_found")
            .increment(activation_ids.len() as u64);
        if activation_ids.is_empty() {
            return Ok(Vec::new());
        }

        let get_by_id_start_time = Instant::now();
        let activations = try_join_all(
            activation_ids
                .iter()
                .map(|(hashkey, id)| self.get_by_id(hashkey.clone(), id)),
        )
        .await?;
        let end_time = Instant::now();
        let duration = end_time.duration_since(get_by_id_start_time);
        metrics::histogram!("redis_store.get_retry_activations.get_by_id_duration")
            .record(duration.as_millis() as f64);
        metrics::counter!("redis_store.get_retry_activations.get_by_id.activations_found")
            .increment(activations.len() as u64);
        metrics::counter!("redis_store.get_retry_activations.get_by_id.activations_not_found")
            .increment((activation_ids.len() - activations.len()) as u64);
        let total_duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.get_retry_activations.total_duration")
            .record(total_duration.as_millis() as f64);
        Ok(activations.into_iter().flatten().collect())
    }

    pub async fn mark_retry_completed(
        &self,
        activations: Vec<InflightActivation>,
    ) -> Result<u64, Error> {
        if activations.is_empty() {
            return Ok(0);
        }
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();

        // Since this is a global operation, there is no guarantee that the keys will have the same hash key.
        // Group the activations by hash key and then remove them in transactions.
        // TODO: This is wrong, it should include the bucket hash as well.
        let mut hash_key_to_activations = HashMap::new();
        for activation in activations.iter() {
            let hash_key = HashKey::new(
                activation.namespace.clone(),
                activation.topic.clone(),
                activation.partition,
            );
            hash_key_to_activations
                .entry(hash_key)
                .or_insert(Vec::new())
                .push(activation.clone());
        }

        let mut id_lookup_keys: Vec<String> = Vec::new();
        let mut rows_affected: u64 = 0;
        for (hash_key, activations) in hash_key_to_activations.iter() {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for activation in activations.iter() {
                let retry_key = self
                    .key_builder
                    .get_retry_key(hash_key.clone(), activation.id.as_str())
                    .build_redis_key();
                pipe.lrem(retry_key, 0, activation.id.as_str());
                pipe.del(
                    self.key_builder
                        .get_payload_key(hash_key.clone(), activation.id.as_str())
                        .build_redis_key(),
                );
                id_lookup_keys.push(
                    self.key_builder
                        .get_id_lookup_key(activation.id.as_str())
                        .build_redis_key(),
                );
            }
            let results: Vec<usize> = pipe.query_async(&mut *conn).await?;
            if results.is_empty() {
                continue;
            }
            // Only sum every other element. This will be the output of the LREM command, which returns how many
            // elements were removed from the retry queue.
            rows_affected += results
                .iter()
                .enumerate()
                .filter(|(i, _)| i % 2 == 0)
                .map(|(_, value)| *value)
                .sum::<usize>() as u64;
        }

        let mut pipe = redis::pipe();
        pipe.del(id_lookup_keys[0].clone());
        for id_lookup_key in id_lookup_keys.iter().skip(1) {
            pipe.arg(id_lookup_key);
        }

        // Since these keys expire, it's not a big deal if not all of them are deleted here.
        let deleted_count: Vec<usize> = pipe.query_async(&mut *conn).await?;
        if deleted_count[0] != id_lookup_keys.len() {
            error!(
                "Failed to delete all retry id lookup keys: expected {}, got {}",
                id_lookup_keys.len(),
                deleted_count[0]
            );
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.mark_retry_completed.duration")
            .record(duration.as_millis() as f64);
        metrics::counter!("redis_store.mark_retry_completed.rows_affected")
            .increment(rows_affected);
        Ok(rows_affected)
    }

    pub async fn handle_processing_deadline(&self) -> Result<(u64, u64, u64), Error> {
        // Get all the activations that have exceeded their processing deadline
        // Idempotent activations that fail their processing deadlines go directly to failure
        // there are no retries, as the worker will reject the activation due to idempotency keys.
        // If the task has processing attempts remaining, it is moved back to pending with attempts += 1
        // Otherwise it is either discarded or moved to retry/deadletter.
        let mut conn = self.get_conn().await?;
        let mut total_rows_affected: u64 = 0;
        let mut discarded_count: u64 = 0;
        let mut processing_attempts_exceeded_count: u64 = 0;
        let start_time = Instant::now();
        for hash_key in self.hash_keys.iter() {
            for bucket_hash in self.bucket_hashes.iter() {
                let processing_key = self
                    .key_builder
                    .get_processing_key_for_iter(hash_key.clone(), bucket_hash.as_str())
                    .build_redis_key();
                // ZRANGEBYSCORE is deprecated but ZRANGE ... BYSCORE is also not supported so?
                let activations: Vec<String> = conn
                    .zrangebyscore(
                        processing_key.clone(),
                        "-inf".to_string(),
                        Utc::now().timestamp_millis() as isize,
                    )
                    .await?;
                if activations.is_empty() {
                    continue;
                }
                total_rows_affected += activations.len() as u64;
                for activation_id in activations.iter() {
                    let single_activation_start_time = Instant::now();
                    let fields = self
                        .get_fields_by_id(
                            hash_key.clone(),
                            activation_id,
                            &["processing_attempts", "at_most_once"],
                        )
                        .await?;
                    if fields.is_empty() {
                        error!(
                            "Failed to get payload for activation past processing deadline: {}",
                            activation_id
                        );
                        let single_activation_duration = single_activation_start_time
                            .duration_since(single_activation_start_time);
                        metrics::histogram!("redis_store.handle_processing_deadline.single_activation.duration", "status" => "not_found").record(single_activation_duration.as_millis() as f64);
                        continue;
                    }
                    let at_most_once = fields
                        .get("at_most_once")
                        .unwrap_or(&"false".to_string())
                        .parse::<bool>()
                        .unwrap();
                    if at_most_once {
                        let result = conn.zrem(processing_key.clone(), activation_id).await?;
                        if result != 1 {
                            return Err(anyhow::anyhow!(
                                "Failed to remove activation from processing set: {}",
                                activation_id
                            ));
                        }
                        self.discard_activation(hash_key.clone(), activation_id)
                            .await?;
                        discarded_count += 1;
                        let single_activation_duration = single_activation_start_time
                            .duration_since(single_activation_start_time);
                        metrics::histogram!("redis_store.handle_processing_deadline.single_activation.duration", "status" => "at_most_once").record(single_activation_duration.as_millis() as f64);
                        continue;
                    }
                    let processing_attempts = fields
                        .get("processing_attempts")
                        .unwrap_or(&"0".to_string())
                        .parse::<i32>()
                        .unwrap();
                    if processing_attempts >= self.max_processing_attempts {
                        // Check for deadletter/dlq
                        processing_attempts_exceeded_count += 1;
                        let result = conn.zrem(processing_key.clone(), activation_id).await?;
                        if result != 1 {
                            return Err(anyhow::anyhow!(
                                "Failed to remove activation from processing set: {}",
                                activation_id
                            ));
                        }
                        self.discard_activation(hash_key.clone(), activation_id)
                            .await?;
                        discarded_count += 1;
                        let single_activation_duration = single_activation_start_time
                            .duration_since(single_activation_start_time);
                        metrics::histogram!("redis_store.handle_processing_deadline.single_activation.duration", "status" => "processing_attempts_exceeded").record(single_activation_duration.as_millis() as f64);
                        continue;
                    }
                    // Move back to pending
                    let pending_key = self
                        .key_builder
                        .get_pending_key(hash_key.clone(), activation_id)
                        .build_redis_key();
                    let payload_key = self
                        .key_builder
                        .get_payload_key(hash_key.clone(), activation_id)
                        .build_redis_key();
                    let mut pipe = redis::pipe();
                    pipe.atomic();
                    pipe.hset(
                        payload_key,
                        "processing_attempts",
                        (processing_attempts + 1).to_string(),
                    );
                    pipe.rpush(pending_key, activation_id);
                    pipe.zrem(processing_key.clone(), activation_id);
                    let results: Vec<usize> = pipe.query_async(&mut *conn).await?;
                    let single_activation_duration =
                        single_activation_start_time.duration_since(single_activation_start_time);
                    metrics::histogram!("redis_store.handle_processing_deadline.single_activation.duration", "status" => "moved_to_pending").record(single_activation_duration.as_millis() as f64);
                    if results.len() != 3 {
                        return Err(anyhow::anyhow!(
                            "Failed to move activation back to pending: incorrect number of commands run: expected 3, got {} for key {}",
                            results.len(),
                            activation_id
                        ));
                    }
                    // processing_attempts should already be a key in the payload, so this should return 0
                    if results[0] != 0 {
                        return Err(anyhow::anyhow!(
                            "Failed to increment processing attempts: {}",
                            activation_id
                        ));
                    }
                    if results[1] != 1 {
                        return Err(anyhow::anyhow!(
                            "Failed to add activation to pending queue: {}",
                            activation_id
                        ));
                    }
                    if results[2] != 1 {
                        return Err(anyhow::anyhow!(
                            "Failed to remove activation from processing set: {}",
                            activation_id
                        ));
                    }
                }
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.handle_processing_deadline.total_duration")
            .record(duration.as_millis() as f64);
        Ok((
            total_rows_affected,
            discarded_count,
            processing_attempts_exceeded_count,
        ))
    }

    pub async fn handle_expires_at(&self) -> Result<u64, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let mut total_rows_affected = 0;
        for hash_key in self.hash_keys.iter() {
            for bucket_hash in self.bucket_hashes.iter() {
                let single_bucket_start_time = Instant::now();
                let expires_at_key = self
                    .key_builder
                    .get_expired_key_for_iter(hash_key.clone(), bucket_hash.as_str())
                    .build_redis_key();
                let activations: Vec<String> = conn
                    .zrangebyscore(
                        expires_at_key.clone(),
                        0,
                        Utc::now().timestamp_millis() as isize,
                    )
                    .await?;
                if activations.is_empty() {
                    continue;
                }
                total_rows_affected += activations.len() as u64;
                let mut pipe = redis::pipe();
                pipe.atomic();
                for activation_id in activations.iter() {
                    let pending_key = self
                        .key_builder
                        .get_pending_key(hash_key.clone(), activation_id)
                        .build_redis_key();
                    pipe.lrem(pending_key, 0, activation_id);
                    pipe.zrem(expires_at_key.clone(), activation_id);
                    self.discard_activation(hash_key.clone(), activation_id)
                        .await?;
                }
                let results: Vec<usize> = pipe.query_async(&mut *conn).await?;
                let single_bucket_duration =
                    single_bucket_start_time.duration_since(single_bucket_start_time);
                metrics::histogram!("redis_store.handle_expires_at.single_bucket.duration")
                    .record(single_bucket_duration.as_millis() as f64);
                if results.len() != 2 * activations.len() {
                    return Err(anyhow::anyhow!(
                        "Failed to remove expired activations: {}",
                        expires_at_key
                    ));
                }
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.handle_expires_at.total_duration")
            .record(duration.as_millis() as f64);
        Ok(total_rows_affected)
    }

    pub async fn handle_delay_until(&self) -> Result<u64, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let mut total_rows_affected = 0;
        for hash_key in self.hash_keys.iter() {
            for bucket_hash in self.bucket_hashes.iter() {
                let single_bucket_start_time = Instant::now();
                let delay_until_key = self
                    .key_builder
                    .get_delay_key_for_iter(hash_key.clone(), bucket_hash.as_str())
                    .build_redis_key();
                let activations: Vec<String> = conn
                    .zrangebyscore(
                        delay_until_key.clone(),
                        0,
                        Utc::now().timestamp_millis() as isize,
                    )
                    .await?;
                if activations.is_empty() {
                    let single_bucket_duration =
                        single_bucket_start_time.duration_since(single_bucket_start_time);
                    metrics::histogram!("redis_store.handle_delay_until.single_bucket.duration", "result" => "no_activations").record(single_bucket_duration.as_millis() as f64);
                    continue;
                }
                total_rows_affected += activations.len() as u64;
                let mut pipe = redis::pipe();
                pipe.atomic();
                for activation_id in activations.iter() {
                    let pending_key = self
                        .key_builder
                        .get_pending_key(hash_key.clone(), activation_id)
                        .build_redis_key();
                    pipe.rpush(pending_key, activation_id);
                    pipe.zrem(delay_until_key.clone(), activation_id);
                }
                let results: Vec<usize> = pipe.query_async(&mut *conn).await?;
                let single_bucket_duration =
                    single_bucket_start_time.duration_since(single_bucket_start_time);
                metrics::histogram!("redis_store.handle_delay_until.single_bucket.duration", "result" => "removed_activations").record(single_bucket_duration.as_millis() as f64);
                if results.len() != 2 * activations.len() {
                    return Err(anyhow::anyhow!(
                        "Failed to remove expired activations: {}",
                        delay_until_key
                    ));
                }
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.handle_delay_until.total_duration")
            .record(duration.as_millis() as f64);
        Ok(total_rows_affected)
    }

    pub async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        // TODO
        Ok(0)
    }

    pub async fn mark_demoted_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        // TODO
        Ok(0)
    }

    pub async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> Result<u64, Error> {
        // TODO
        Ok(0)
    }

    #[instrument(skip_all)]
    pub async fn count_pending_activations(&self) -> Result<usize, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let mut total_count = 0;
        for hash_key in self.hash_keys.iter() {
            for bucket_hash in self.bucket_hashes.iter() {
                let pending_key = self
                    .key_builder
                    .get_pending_key_for_iter(hash_key.clone(), bucket_hash.as_str())
                    .build_redis_key();
                let count: usize = conn.llen(pending_key).await?;
                total_count += count;
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.count_pending_activations.total_duration")
            .record(duration.as_millis() as f64);
        Ok(total_count)
    }

    #[instrument(skip_all)]
    pub async fn count_delayed_activations(&self) -> Result<usize, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let mut total_count = 0;
        for hash_key in self.hash_keys.iter() {
            for bucket_hash in self.bucket_hashes.iter() {
                let delay_key = self
                    .key_builder
                    .get_delay_key_for_iter(hash_key.clone(), bucket_hash.as_str())
                    .build_redis_key();
                let count: usize = conn.zcard(delay_key.clone()).await?;
                total_count += count;
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.count_delayed_activations.total_duration")
            .record(duration.as_millis() as f64);
        Ok(total_count)
    }

    #[instrument(skip_all)]
    pub async fn count_processing_activations(&self) -> Result<usize, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let mut total_count = 0;
        for hash_key in self.hash_keys.iter() {
            for bucket_hash in self.bucket_hashes.iter() {
                let processing_key = self
                    .key_builder
                    .get_processing_key_for_iter(hash_key.clone(), bucket_hash.as_str())
                    .build_redis_key();
                let count: usize = conn.zcard(processing_key.clone()).await?;
                total_count += count;
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.count_processing_activations.total_duration")
            .record(duration.as_millis() as f64);
        Ok(total_count)
    }

    pub async fn count_retry_activations(&self) -> Result<usize, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let mut total_count = 0;
        for hash_key in self.hash_keys.iter() {
            for bucket_hash in self.bucket_hashes.iter() {
                let retry_key = self
                    .key_builder
                    .get_retry_key_for_iter(hash_key.clone(), bucket_hash.as_str())
                    .build_redis_key();
                let count: usize = conn.llen(retry_key.clone()).await?;
                total_count += count;
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.count_retry_activations.total_duration")
            .record(duration.as_millis() as f64);
        Ok(total_count)
    }

    pub async fn count_deadletter_activations(&self) -> Result<usize, Error> {
        let mut conn = self.get_conn().await?;
        let start_time = Instant::now();
        let mut total_count = 0;
        for hash_key in self.hash_keys.iter() {
            for bucket_hash in self.bucket_hashes.iter() {
                let retry_key = self
                    .key_builder
                    .get_deadletter_key_for_iter(hash_key.clone(), bucket_hash.as_str())
                    .build_redis_key();
                let count: usize = conn.llen(retry_key.clone()).await?;
                total_count += count;
            }
        }
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        metrics::histogram!("redis_store.count_deadletter_activations.total_duration")
            .record(duration.as_millis() as f64);
        Ok(total_count)
    }

    // Only used in testing
    pub async fn delete_all_keys(&self) -> Result<(), Error> {
        error!("deleting all keys");
        let mut conn = self.get_conn().await?;
        let keys: Vec<String> = conn.keys("*").await?;
        let mut deleted_keys = 0;
        for key in keys {
            conn.del(key).await?;
            deleted_keys += 1;
        }
        error!("deleted {:?} keys", deleted_keys);
        Ok(())
    }

    pub async fn db_size(&self) -> Result<u64, Error> {
        // Not needed
        Ok(0)
    }

    pub async fn handle_deadletter_tasks(&self) -> Result<Vec<(String, Vec<u8>)>, Error> {
        // Not needed
        Ok(vec![])
    }

    pub async fn mark_deadletter_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        // Not needed
        Ok(0)
    }

    pub async fn handle_processing_attempts(&self) -> Result<u64, Error> {
        // Not needed
        Ok(0)
    }
}
