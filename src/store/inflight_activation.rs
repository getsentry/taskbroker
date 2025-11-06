use std::{str::FromStr, time::Instant};

use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use redis::{AsyncCommands, cluster::ClusterClient};
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivationStatus};
use tracing::instrument;

use crate::config::Config;

// Lua scripts for atomic operations
const STORE_BATCH_SCRIPT: &str = r#"
-- Store a batch of tasks
-- KEYS: none
-- ARGV: serialized tasks as JSON array
for i, task_json in ipairs(ARGV) do
    local task = cjson.decode(task_json)
    local task_key = "task:" .. task.id
    local sorted_set_key = task.status .. ":" .. task.namespace

    -- Store task hash
    redis.call('HSET', task_key,
        'id', task.id,
        'activation', task.activation,
        'status', task.status,
        'partition', task.partition,
        'offset', task.offset,
        'added_at', task.added_at,
        'received_at', task.received_at,
        'processing_attempts', task.processing_attempts,
        'processing_deadline_duration', task.processing_deadline_duration,
        'expires_at', task.expires_at or '',
        'delay_until', task.delay_until or '',
        'processing_deadline', task.processing_deadline or '',
        'at_most_once', task.at_most_once and '1' or '0',
        'namespace', task.namespace,
        'taskname', task.taskname,
        'on_attempts_exceeded', task.on_attempts_exceeded
    )

    -- Add to sorted set (score is added_at timestamp)
    redis.call('ZADD', sorted_set_key, task.added_at, task.id)

    -- Increment counter
    redis.call('INCR', 'count:' .. task.status)

    -- Add namespace to set
    redis.call('SADD', 'all_namespaces', task.namespace)
end
return #ARGV
"#;

const SET_STATUS_SCRIPT: &str = r#"
-- Change task status
-- KEYS[1]: task_id
-- ARGV[1]: new_status
local task_id = KEYS[1]
local new_status = ARGV[1]
local task_key = "task:" .. task_id

-- Get current status and namespace
local old_status = redis.call('HGET', task_key, 'status')
local namespace = redis.call('HGET', task_key, 'namespace')
local added_at = redis.call('HGET', task_key, 'added_at')

if not old_status or not namespace then
    return redis.error_reply("Task not found")
end

-- Remove from old sorted set
redis.call('ZREM', old_status .. ':' .. namespace, task_id)

-- Update status in hash
redis.call('HSET', task_key, 'status', new_status)

-- Add to new sorted set
redis.call('ZADD', new_status .. ':' .. namespace, tonumber(added_at), task_id)

-- Update counters
redis.call('DECR', 'count:' .. old_status)
redis.call('INCR', 'count:' .. new_status)

return 1
"#;

const DELETE_TASK_SCRIPT: &str = r#"
-- Delete a task completely
-- KEYS[1]: task_id
local task_id = KEYS[1]
local task_key = "task:" .. task_id

-- Get task data
local status = redis.call('HGET', task_key, 'status')
local namespace = redis.call('HGET', task_key, 'namespace')

if not status or not namespace then
    return 0
end

-- Remove from sorted set
redis.call('ZREM', status .. ':' .. namespace, task_id)

-- Delete task hash
redis.call('DEL', task_key)

-- Decrement counter
redis.call('DECR', 'count:' .. status)

return 1
"#;

/// The members of this enum should be synced with the members
/// of InflightActivationStatus in sentry_protos
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InflightActivationStatus {
    /// Unused but necessary to align with sentry-protos
    Unspecified,
    Pending,
    Processing,
    Failure,
    Retry,
    Complete,
    Delay,
}

impl std::fmt::Display for InflightActivationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl InflightActivationStatus {
    /// Is the current value a 'conclusion' status that can be supplied over GRPC.
    pub fn is_conclusion(&self) -> bool {
        matches!(
            self,
            InflightActivationStatus::Complete
                | InflightActivationStatus::Retry
                | InflightActivationStatus::Failure
        )
    }
}

impl From<TaskActivationStatus> for InflightActivationStatus {
    fn from(item: TaskActivationStatus) -> Self {
        match item {
            TaskActivationStatus::Unspecified => InflightActivationStatus::Unspecified,
            TaskActivationStatus::Pending => InflightActivationStatus::Pending,
            TaskActivationStatus::Processing => InflightActivationStatus::Processing,
            TaskActivationStatus::Failure => InflightActivationStatus::Failure,
            TaskActivationStatus::Retry => InflightActivationStatus::Retry,
            TaskActivationStatus::Complete => InflightActivationStatus::Complete,
        }
    }
}

impl FromStr for InflightActivationStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Unspecified" => Ok(InflightActivationStatus::Unspecified),
            "Pending" => Ok(InflightActivationStatus::Pending),
            "Processing" => Ok(InflightActivationStatus::Processing),
            "Failure" => Ok(InflightActivationStatus::Failure),
            "Retry" => Ok(InflightActivationStatus::Retry),
            "Complete" => Ok(InflightActivationStatus::Complete),
            "Delay" => Ok(InflightActivationStatus::Delay),
            _ => Err(format!("Unknown status: {}", s)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InflightActivation {
    pub id: String,
    /// The protobuf activation that was received from kafka
    pub activation: Vec<u8>,

    /// The current status of the activation
    pub status: InflightActivationStatus,

    /// The partition the activation was received from
    pub partition: i32,

    /// The offset the activation had
    pub offset: i64,

    /// The timestamp when the activation was stored in activation store.
    pub added_at: DateTime<Utc>,

    /// The timestamp a task was stored in Kafka
    pub received_at: DateTime<Utc>,

    /// The number of times the activation has been attempted to be processed. This counter is
    /// incremented everytime a task is reset from processing back to pending. When this
    /// exceeds max_processing_attempts, the task is discarded/deadlettered.
    pub processing_attempts: i32,

    /// The duration in seconds that a worker has to complete task execution.
    /// When an activation is moved from pending -> processing a result is expected
    /// in this many seconds.
    pub processing_deadline_duration: u32,

    /// If the task has specified an expiry, this is the timestamp after which the task should be removed from inflight store
    pub expires_at: Option<DateTime<Utc>>,

    /// If the task has specified a delay, this is the timestamp after which the task can be sent to workers
    pub delay_until: Option<DateTime<Utc>>,

    /// The timestamp for when processing should be complete
    pub processing_deadline: Option<DateTime<Utc>>,

    /// What to do when the maximum number of attempts to complete a task is exceeded
    pub on_attempts_exceeded: OnAttemptsExceeded,

    /// Whether or not the activation uses at_most_once.
    /// When enabled activations are not retried when processing_deadlines
    /// are exceeded.
    pub at_most_once: bool,

    /// Details about the task
    pub namespace: String,
    pub taskname: String,
}

impl InflightActivation {
    /// The number of milliseconds between an activation's received timestamp
    /// and now. Used for calculating max lag in pending activations
    pub fn lag_ms(&self, now: &DateTime<Utc>) -> i64 {
        let duration = (*now - self.received_at).num_milliseconds();
        duration
    }

    /// The latency in milliseconds between received_at and the provided time
    pub fn received_latency(&self, now: DateTime<Utc>) -> i64 {
        (now - self.received_at).num_milliseconds()
    }
}

#[derive(Clone, Debug)]
pub struct QueryResult {
    pub rows_affected: u64,
}

pub struct InflightActivationStoreConfig {
    pub redis_url: String,
    pub max_processing_attempts: usize,
    pub processing_deadline_grace_sec: u64,
}

impl InflightActivationStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            redis_url: config.redis_url.clone(),
            max_processing_attempts: config.max_processing_attempts,
            processing_deadline_grace_sec: config.processing_deadline_grace_sec,
        }
    }
}

pub struct InflightActivationStore {
    client: ClusterClient,
    config: InflightActivationStoreConfig,
}

impl InflightActivationStore {
    pub async fn new(config: InflightActivationStoreConfig) -> Result<Self, Error> {
        // Try to connect as cluster first, fall back to single node
        let client = match ClusterClient::new(vec![config.redis_url.clone()]) {
            Ok(c) => c,
            Err(_) => {
                // Fall back to single node client wrapped in cluster client
                return Err(anyhow!("Failed to create Redis cluster client"));
            }
        };

        // Test connection
        let mut conn = client.get_async_connection().await?;
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;

        Ok(Self { client, config })
    }

    async fn get_connection(&self) -> Result<redis::cluster_async::ClusterConnection, Error> {
        Ok(self.client.get_async_connection().await?)
    }

    #[instrument(skip_all)]
    pub async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        if batch.is_empty() {
            return Ok(QueryResult { rows_affected: 0 });
        }

        let mut conn = self.get_connection().await?;
        let start = Instant::now();

        // For simplicity in PoC, insert one by one (in production, use pipeline or lua script)
        let mut rows_affected = 0;
        for activation in batch {
            let task_key = format!("task:{}", activation.id);
            let sorted_set_key = format!("{}:{}", activation.status, activation.namespace);

            // Store task as hash
            let _: () = conn
                .hset_multiple(
                    &task_key,
                    &[
                        ("id", activation.id.as_bytes()),
                        ("activation", activation.activation.as_slice()),
                        ("status", activation.status.to_string().as_bytes()),
                        ("partition", &activation.partition.to_le_bytes()),
                        ("offset", &activation.offset.to_le_bytes()),
                        ("added_at", &activation.added_at.timestamp().to_le_bytes()),
                        (
                            "received_at",
                            &activation.received_at.timestamp().to_le_bytes(),
                        ),
                        (
                            "processing_attempts",
                            &activation.processing_attempts.to_le_bytes(),
                        ),
                        (
                            "processing_deadline_duration",
                            &activation.processing_deadline_duration.to_le_bytes(),
                        ),
                        ("namespace", activation.namespace.as_bytes()),
                        ("taskname", activation.taskname.as_bytes()),
                        (
                            "at_most_once",
                            &[if activation.at_most_once { 1u8 } else { 0u8 }],
                        ),
                        (
                            "on_attempts_exceeded",
                            &(activation.on_attempts_exceeded as i32).to_le_bytes(),
                        ),
                    ],
                )
                .await?;

            // Store optional fields
            if let Some(expires_at) = activation.expires_at {
                let _: () = conn
                    .hset(&task_key, "expires_at", expires_at.timestamp())
                    .await?;
            }
            if let Some(delay_until) = activation.delay_until {
                let _: () = conn
                    .hset(&task_key, "delay_until", delay_until.timestamp())
                    .await?;
            }
            if let Some(processing_deadline) = activation.processing_deadline {
                let _: () = conn
                    .hset(
                        &task_key,
                        "processing_deadline",
                        processing_deadline.timestamp(),
                    )
                    .await?;
            }

            // Add to sorted set (score is added_at timestamp)
            let _: () = conn
                .zadd(
                    &sorted_set_key,
                    &activation.id,
                    activation.added_at.timestamp(),
                )
                .await?;

            // Increment counter
            let _: () = conn.incr(format!("count:{}", activation.status), 1).await?;

            // Add namespace to set
            let _: () = conn.sadd("all_namespaces", &activation.namespace).await?;

            rows_affected += 1;
        }

        metrics::histogram!("redis.store").record(start.elapsed());
        Ok(QueryResult {
            rows_affected: rows_affected as u64,
        })
    }

    pub async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
        let mut conn = self.get_connection().await?;
        let task_key = format!("task:{}", id);

        let exists: bool = conn.exists(&task_key).await?;
        if !exists {
            return Ok(None);
        }

        self.get_task_from_hash(&mut conn, id).await.map(Some)
    }

    async fn get_task_from_hash(
        &self,
        conn: &mut redis::cluster_async::ClusterConnection,
        id: &str,
    ) -> Result<InflightActivation, Error> {
        let task_key = format!("task:{}", id);
        let data: Vec<(String, Vec<u8>)> = conn.hgetall(&task_key).await?;

        let mut map = std::collections::HashMap::new();
        for (k, v) in data {
            map.insert(k, v);
        }

        let parse_i32 = |key: &str| -> Result<i32, Error> {
            let bytes = map
                .get(key)
                .ok_or_else(|| anyhow!("Missing field: {}", key))?;
            Ok(i32::from_le_bytes(bytes.as_slice().try_into()?))
        };

        let parse_i64 = |key: &str| -> Result<i64, Error> {
            let bytes = map
                .get(key)
                .ok_or_else(|| anyhow!("Missing field: {}", key))?;
            Ok(i64::from_le_bytes(bytes.as_slice().try_into()?))
        };

        let parse_u32 = |key: &str| -> Result<u32, Error> {
            let bytes = map
                .get(key)
                .ok_or_else(|| anyhow!("Missing field: {}", key))?;
            Ok(u32::from_le_bytes(bytes.as_slice().try_into()?))
        };

        let parse_string = |key: &str| -> Result<String, Error> {
            let bytes = map
                .get(key)
                .ok_or_else(|| anyhow!("Missing field: {}", key))?;
            Ok(String::from_utf8(bytes.clone())?)
        };

        let parse_optional_i64 = |key: &str| -> Option<i64> {
            map.get(key)
                .and_then(|bytes| i64::from_le_bytes(bytes.as_slice().try_into().ok()?).into())
        };

        Ok(InflightActivation {
            id: parse_string("id")?,
            activation: map.get("activation").cloned().unwrap_or_default(),
            status: InflightActivationStatus::from_str(&parse_string("status")?)
                .map_err(|e| anyhow!(e))?,
            partition: parse_i32("partition")?,
            offset: parse_i64("offset")?,
            added_at: DateTime::from_timestamp(parse_i64("added_at")?, 0)
                .ok_or_else(|| anyhow!("Invalid timestamp"))?,
            received_at: DateTime::from_timestamp(parse_i64("received_at")?, 0)
                .ok_or_else(|| anyhow!("Invalid timestamp"))?,
            processing_attempts: parse_i32("processing_attempts")?,
            processing_deadline_duration: parse_u32("processing_deadline_duration")?,
            expires_at: parse_optional_i64("expires_at")
                .and_then(|ts| DateTime::from_timestamp(ts, 0)),
            delay_until: parse_optional_i64("delay_until")
                .and_then(|ts| DateTime::from_timestamp(ts, 0)),
            processing_deadline: parse_optional_i64("processing_deadline")
                .and_then(|ts| DateTime::from_timestamp(ts, 0)),
            at_most_once: map
                .get("at_most_once")
                .and_then(|b| b.first())
                .map(|&b| b == 1)
                .unwrap_or(false),
            namespace: parse_string("namespace")?,
            taskname: parse_string("taskname")?,
            on_attempts_exceeded: OnAttemptsExceeded::try_from(parse_i32("on_attempts_exceeded")?)
                .unwrap_or(OnAttemptsExceeded::Discard),
        })
    }

    #[instrument(skip_all)]
    pub async fn get_pending_activation(
        &self,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
        let mut conn = self.get_connection().await?;
        let now = Utc::now();

        // Get sorted set key
        let sorted_set_key = if let Some(ns) = namespace {
            format!("Pending:{}", ns)
        } else {
            // If no namespace specified, get all namespaces and check each
            let namespaces: Vec<String> = conn.smembers("all_namespaces").await?;
            for ns in namespaces {
                let key = format!("Pending:{}", ns);
                if let Some(activation) = self
                    .get_pending_from_sorted_set(&mut conn, &key, &now)
                    .await?
                {
                    return Ok(Some(activation));
                }
            }
            return Ok(None);
        };

        self.get_pending_from_sorted_set(&mut conn, &sorted_set_key, &now)
            .await
    }

    async fn get_pending_from_sorted_set(
        &self,
        conn: &mut redis::cluster_async::ClusterConnection,
        sorted_set_key: &str,
        now: &DateTime<Utc>,
    ) -> Result<Option<InflightActivation>, Error> {
        // Get oldest tasks (lowest score)
        let task_ids: Vec<String> = conn.zrange(sorted_set_key, 0, 9).await?;

        for task_id in task_ids {
            let activation = self.get_task_from_hash(conn, &task_id).await?;

            // Check if task is ready (not delayed and not expired)
            if let Some(delay_until) = activation.delay_until {
                if delay_until > *now {
                    continue;
                }
            }

            if let Some(expires_at) = activation.expires_at {
                if expires_at <= *now {
                    continue;
                }
            }

            return Ok(Some(activation));
        }

        Ok(None)
    }

    pub async fn get_pending_activations_from_namespaces(
        &self,
        namespaces: Option<&[String]>,
        limit: Option<usize>,
    ) -> Result<Vec<InflightActivation>, Error> {
        let mut conn = self.get_connection().await?;
        let now = Utc::now();
        let mut results = Vec::new();
        let limit = limit.unwrap_or(usize::MAX);

        let namespaces_to_check: Vec<String> = if let Some(ns_list) = namespaces {
            ns_list.to_vec()
        } else {
            conn.smembers("all_namespaces").await?
        };

        for namespace in namespaces_to_check {
            if results.len() >= limit {
                break;
            }

            let sorted_set_key = format!("Pending:{}", namespace);
            if let Some(activation) = self
                .get_pending_from_sorted_set(&mut conn, &sorted_set_key, &now)
                .await?
            {
                results.push(activation);
            }
        }

        Ok(results)
    }

    pub async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> f64 {
        let mut conn = match self.get_connection().await {
            Ok(c) => c,
            Err(_) => return 0.0,
        };

        let namespaces: Vec<String> = match conn.smembers("all_namespaces").await {
            Ok(ns) => ns,
            Err(_) => return 0.0,
        };

        let mut max_lag = 0.0;

        for namespace in namespaces {
            let sorted_set_key = format!("Pending:{}", namespace);
            let task_ids: Vec<String> = match conn.zrange(&sorted_set_key, 0, 0).await {
                Ok(ids) => ids,
                Err(_) => continue,
            };

            for task_id in task_ids {
                if let Ok(activation) = self.get_task_from_hash(&mut conn, &task_id).await {
                    // Skip delayed tasks
                    if let Some(delay_until) = activation.delay_until {
                        if delay_until > *now {
                            continue;
                        }
                    }
                    // Skip tasks that haven't been processed yet
                    if activation.processing_attempts == 0 {
                        let lag = activation.lag_ms(now) as f64 / 1000.0;
                        if lag > max_lag {
                            max_lag = lag;
                        }
                    }
                }
            }
        }

        max_lag
    }

    pub async fn count_pending_activations(&self) -> Result<usize, Error> {
        self.count_by_status(InflightActivationStatus::Pending)
            .await
    }

    pub async fn count_by_status(&self, status: InflightActivationStatus) -> Result<usize, Error> {
        let mut conn = self.get_connection().await?;
        let count: i64 = conn.get(format!("count:{}", status)).await.unwrap_or(0);
        Ok(count.max(0) as usize)
    }

    pub async fn count(&self) -> Result<usize, Error> {
        let mut total = 0;
        for status in &[
            InflightActivationStatus::Pending,
            InflightActivationStatus::Processing,
            InflightActivationStatus::Delay,
            InflightActivationStatus::Retry,
            InflightActivationStatus::Complete,
            InflightActivationStatus::Failure,
        ] {
            total += self.count_by_status(*status).await?;
        }
        Ok(total)
    }

    pub async fn set_status(
        &self,
        id: &str,
        new_status: InflightActivationStatus,
    ) -> Result<(), Error> {
        let mut conn = self.get_connection().await?;
        let start = Instant::now();

        // Get current task data
        let task = self
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow!("Task not found"))?;
        let old_status = task.status;
        let namespace = &task.namespace;

        // Remove from old sorted set
        let old_key = format!("{}:{}", old_status, namespace);
        let _: () = conn.zrem(&old_key, id).await?;

        // Update status in hash
        let task_key = format!("task:{}", id);
        let _: () = conn
            .hset(&task_key, "status", new_status.to_string())
            .await?;

        // Add to new sorted set
        let new_key = format!("{}:{}", new_status, namespace);
        let _: () = conn.zadd(&new_key, id, task.added_at.timestamp()).await?;

        // Update counters
        let _: () = conn.decr(format!("count:{}", old_status), 1).await?;
        let _: () = conn.incr(format!("count:{}", new_status), 1).await?;

        metrics::histogram!("redis.set_status").record(start.elapsed());
        Ok(())
    }

    pub async fn set_processing_deadline(
        &self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let mut conn = self.get_connection().await?;
        let task_key = format!("task:{}", id);

        if let Some(dl) = deadline {
            let _: () = conn
                .hset(&task_key, "processing_deadline", dl.timestamp())
                .await?;
        } else {
            let _: () = conn.hdel(&task_key, "processing_deadline").await?;
        }

        Ok(())
    }

    pub async fn delete_activation(&self, id: &str) -> Result<(), Error> {
        let mut conn = self.get_connection().await?;

        // Get task to find status and namespace
        let task = match self.get_by_id(id).await? {
            Some(t) => t,
            None => return Ok(()),
        };

        // Remove from sorted set
        let sorted_set_key = format!("{}:{}", task.status, task.namespace);
        let _: () = conn.zrem(&sorted_set_key, id).await?;

        // Delete task hash
        let task_key = format!("task:{}", id);
        let _: () = conn.del(&task_key).await?;

        // Decrement counter
        let _: () = conn.decr(format!("count:{}", task.status), 1).await?;

        Ok(())
    }

    pub async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        let mut conn = self.get_connection().await?;
        let namespaces: Vec<String> = conn.smembers("all_namespaces").await?;

        let mut results = Vec::new();
        for namespace in namespaces {
            let sorted_set_key = format!("Retry:{}", namespace);
            let task_ids: Vec<String> = conn.zrange(&sorted_set_key, 0, -1).await?;

            for task_id in task_ids {
                if let Ok(activation) = self.get_task_from_hash(&mut conn, &task_id).await {
                    results.push(activation);
                }
            }
        }

        Ok(results)
    }

    pub async fn clear(&self) -> Result<(), Error> {
        let mut conn = self.get_connection().await?;
        // For PoC, use FLUSHDB (in production, use key scanning)
        let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        let mut conn = self.get_connection().await?;
        let now = Utc::now();
        let namespaces: Vec<String> = conn.smembers("all_namespaces").await?;

        let mut total_updated = 0u64;

        for namespace in namespaces {
            let sorted_set_key = format!("Processing:{}", namespace);
            let task_ids: Vec<String> = conn.zrange(&sorted_set_key, 0, -1).await?;

            for task_id in task_ids {
                let task = match self.get_task_from_hash(&mut conn, &task_id).await {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                if let Some(deadline) = task.processing_deadline {
                    if deadline < now {
                        // Task has exceeded deadline
                        if task.at_most_once {
                            // Move to failure
                            self.set_status(&task_id, InflightActivationStatus::Failure)
                                .await?;
                        } else {
                            // Move back to pending and increment attempts
                            let task_key = format!("task:{}", task_id);
                            let attempts: i32 = conn.hget(&task_key, "processing_attempts").await?;
                            let _: () = conn
                                .hset(&task_key, "processing_attempts", attempts + 1)
                                .await?;
                            let _: () = conn.hdel(&task_key, "processing_deadline").await?;
                            self.set_status(&task_id, InflightActivationStatus::Pending)
                                .await?;
                        }
                        total_updated += 1;
                    }
                }
            }
        }

        Ok(total_updated)
    }

    #[instrument(skip_all)]
    pub async fn handle_processing_attempts(&self) -> Result<u64, Error> {
        let mut conn = self.get_connection().await?;
        let namespaces: Vec<String> = conn.smembers("all_namespaces").await?;

        let mut total_updated = 0u64;

        for namespace in namespaces {
            let sorted_set_key = format!("Pending:{}", namespace);
            let task_ids: Vec<String> = conn.zrange(&sorted_set_key, 0, -1).await?;

            for task_id in task_ids {
                let task = match self.get_task_from_hash(&mut conn, &task_id).await {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                if task.processing_attempts >= self.config.max_processing_attempts as i32 {
                    self.set_status(&task_id, InflightActivationStatus::Failure)
                        .await?;
                    total_updated += 1;
                }
            }
        }

        Ok(total_updated)
    }

    #[instrument(skip_all)]
    pub async fn handle_expires_at(&self) -> Result<u64, Error> {
        let mut conn = self.get_connection().await?;
        let now = Utc::now();
        let namespaces: Vec<String> = conn.smembers("all_namespaces").await?;

        let mut total_deleted = 0u64;

        for namespace in namespaces {
            let sorted_set_key = format!("Pending:{}", namespace);
            let task_ids: Vec<String> = conn.zrange(&sorted_set_key, 0, -1).await?;

            for task_id in task_ids {
                let task = match self.get_task_from_hash(&mut conn, &task_id).await {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                if let Some(expires_at) = task.expires_at {
                    if expires_at <= now {
                        self.delete_activation(&task_id).await?;
                        total_deleted += 1;
                    }
                }
            }
        }

        Ok(total_deleted)
    }

    #[instrument(skip_all)]
    pub async fn handle_delay_until(&self) -> Result<u64, Error> {
        let mut conn = self.get_connection().await?;
        let now = Utc::now();
        let namespaces: Vec<String> = conn.smembers("all_namespaces").await?;

        let mut total_updated = 0u64;

        for namespace in namespaces {
            let sorted_set_key = format!("Delay:{}", namespace);
            let task_ids: Vec<String> = conn.zrange(&sorted_set_key, 0, -1).await?;

            for task_id in task_ids {
                let task = match self.get_task_from_hash(&mut conn, &task_id).await {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                if let Some(delay_until) = task.delay_until {
                    if delay_until <= now {
                        self.set_status(&task_id, InflightActivationStatus::Pending)
                            .await?;
                        total_updated += 1;
                    }
                }
            }
        }

        Ok(total_updated)
    }

    pub async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
        let mut conn = self.get_connection().await?;
        let namespaces: Vec<String> = conn.smembers("all_namespaces").await?;

        let mut to_discard = Vec::new();
        let mut to_deadletter = Vec::new();

        for namespace in namespaces {
            let sorted_set_key = format!("Failure:{}", namespace);
            let task_ids: Vec<String> = conn.zrange(&sorted_set_key, 0, -1).await?;

            for task_id in task_ids {
                if let Ok(task) = self.get_task_from_hash(&mut conn, &task_id).await {
                    if task.on_attempts_exceeded == OnAttemptsExceeded::Discard {
                        to_discard.push(task_id.clone());
                    } else {
                        to_deadletter.push((task_id.clone(), task.activation));
                    }
                }
            }
        }

        Ok(FailedTasksForwarder {
            to_discard,
            to_deadletter,
        })
    }

    pub async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        let mut total = 0;
        for id in ids {
            self.set_status(&id, InflightActivationStatus::Complete)
                .await?;
            total += 1;
        }
        Ok(total)
    }

    pub async fn remove_completed(&self) -> Result<u64, Error> {
        let mut conn = self.get_connection().await?;
        let namespaces: Vec<String> = conn.smembers("all_namespaces").await?;

        let mut total_deleted = 0u64;

        for namespace in namespaces {
            let sorted_set_key = format!("Complete:{}", namespace);
            let task_ids: Vec<String> = conn.zrange(&sorted_set_key, 0, -1).await?;

            for task_id in task_ids {
                self.delete_activation(&task_id).await?;
                total_deleted += 1;
            }
        }

        Ok(total_deleted)
    }

    pub async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        let mut conn = self.get_connection().await?;
        let namespaces: Vec<String> = conn.smembers("all_namespaces").await?;

        let mut total_deleted = 0u64;

        for namespace in namespaces {
            for status in &[
                "Pending",
                "Processing",
                "Delay",
                "Retry",
                "Failure",
                "Complete",
            ] {
                let sorted_set_key = format!("{}:{}", status, namespace);
                let task_ids: Vec<String> = conn.zrange(&sorted_set_key, 0, -1).await?;

                for task_id in task_ids {
                    if let Ok(task) = self.get_task_from_hash(&mut conn, &task_id).await {
                        if killswitched_tasks.contains(&task.taskname) {
                            self.delete_activation(&task_id).await?;
                            total_deleted += 1;
                        }
                    }
                }
            }
        }

        Ok(total_deleted)
    }
}

pub struct FailedTasksForwarder {
    pub to_discard: Vec<String>,
    pub to_deadletter: Vec<(String, Vec<u8>)>,
}
