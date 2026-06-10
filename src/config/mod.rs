#![allow(clippy::result_large_err)]
use std::borrow::Cow;
use std::collections::BTreeMap;

use anyhow::{Result, anyhow};
use figment::providers::{Env, Format, Yaml};
use figment::{Figment, Metadata, Profile, Provider};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use tracing::warn;
use validator::{Validate, ValidationError};

use crate::Args;
use crate::config::store::StoreConfig;
use crate::fetch::MAX_FETCH_THREADS;
use crate::logging::LogFormat;

pub mod deprecated;
pub mod kafka;
pub mod raw;
pub mod store;

use deprecated::DeprecatedConfig;
use kafka::{ClusterConfig, TopicConfig};
use raw::RawModeConfig;
use store::DatabaseAdapter;

/// How the taskbroker delivers tasks to workers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryMode {
    /// Workers pull tasks from the broker.
    Pull,

    /// Broker pushes tasks to workers.
    Push,
}

#[derive(PartialEq, Debug, Deserialize, Serialize, Validate)]
pub struct Config {
    /// Deprecated configuration options. Not meant to be used.
    #[serde(flatten)]
    pub deprecated: DeprecatedConfig,

    /// The sentry DSN to use for error reporting.
    pub sentry_dsn: Option<String>,

    /// The environment to report to sentry errors to.
    pub sentry_env: Option<Cow<'static, str>>,

    /// The sampling rate for tracing data.
    pub traces_sample_rate: Option<f32>,

    /// The log filter to apply application logging to.
    /// See https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives
    pub log_filter: String,

    /// The log format to use
    pub log_format: LogFormat,

    /// The statsd address to report metrics to.
    pub statsd_addr: String,

    /// Default tags to add to all metrics.
    pub default_metrics_tags: BTreeMap<String, String>,

    /// The hostname and port of the gRPC server.
    pub grpc_addr: String,

    /// The port to bind the grpc service to
    pub grpc_port: u32,

    /// A list of shared secrets that clients use to authenticate.
    /// We support a list of secrets to allow for key rotation.
    pub grpc_shared_secret: Vec<String>,

    /// The topic to produce demoted "long" namespace tasks to.
    pub kafka_long_topic: String,

    /// Whether to create missing topics if they don't exist.
    pub create_missing_topics: bool,

    /// The kafka topic to publish dead letter messages on.
    /// Still valid in the new format: it names the produce-only topic in
    /// kafka_topics whose cluster the deadletter producer connects to.
    pub kafka_deadletter_topic: String,

    /// The topic to publish retry task activations to.
    /// When set, retries go to this topic instead of kafka_topic.
    /// Required for raw_mode where the main topic has other consumers.
    pub kafka_retry_topic: Option<String>,

    /// The default number of partitions for a topic
    pub default_topic_partitions: i32,

    /// The kafka session timeout in ms.
    /// Used as the default for topics that don't set their own
    /// `session_timeout_ms`.
    pub kafka_session_timeout_ms: usize,

    /// The amount of ms that the consumer will commit at.
    /// Used as the default for topics that don't set their own
    /// `auto_commit_interval_ms`.
    pub kafka_auto_commit_interval_ms: usize,

    /// The auto offset reset policy for the consumer.
    /// Used as the default for topics that don't set their own
    /// `auto_offset_reset`.
    pub kafka_auto_offset_reset: String,

    /// The number of ms for timeouts when publishing messages to kafka.
    pub kafka_send_timeout_ms: u64,

    /// The activation store configuration.
    pub store: StoreConfig,

    /// The path to the runtime config file
    pub runtime_config_path: Option<String>,

    /// The frequency at which upkeep tasks
    /// (discarding, retrying activations, etc.) are executed.
    pub upkeep_task_interval_ms: u64,

    /// The number of milliseconds between upkeep runs that indicates unhealthy
    /// performance that should trigger a restart.
    pub upkeep_unhealthy_interval_ms: u64,

    /// Whether to skip the health check if the pods are in a bad state.
    pub health_check_killswitched: bool,

    /// The number of seconds that deadline resets
    /// are skipped after startup. This delay allows workers
    /// time to publish results after a broker restart.
    pub upkeep_deadline_reset_skip_after_startup_sec: u64,

    /// The frequency at which db maintenance tasks
    /// (reclaiming free pages) are executed
    pub maintenance_task_interval_ms: u64,

    /// The maximum number of seconds a task can be delayed until.
    /// Tasks delayed greater than this duration are capped.
    pub max_delayed_task_allowed_sec: u64,

    /// The maximum number of bytes allowed for a message on the Kafka producer.
    /// If a message is bigger than this then the produce will fail.
    pub max_message_size: u64,

    /// The maximum size in bytes for gRPC messages sent to workers.
    /// Should be at least as large as max_message_size.
    pub grpc_max_message_size: usize,

    /// Enable to have the application perform `VACUUM` on the database
    /// when it starts up, but before the GRPC server, consumer and upkeep begin.
    pub full_vacuum_on_start: bool,

    /// Enable the upkeep thread to perforam a full `VACUUM` on the database
    /// periodically.
    pub full_vacuum_on_upkeep: bool,

    /// The interval in milliseconds between full `VACUUM`s on the database by the upkeep thread.
    pub vacuum_interval_ms: u64,

    /// When true, the upkeep loop emits the current `async_backtrace::taskdump_tree`
    /// snapshot at `debug!` every 30 seconds. Useful for diagnosing hangs in the
    /// store / fetch / push pipelines; off by default because the tree can be
    /// large and noisy.
    pub log_async_backtrace: bool,

    /// How to deliver tasks to workers: "push" or "pull".
    pub delivery_mode: DeliveryMode,

    /// The number of concurrent fetch loops in push mode, which should be ≤ `MAX_FETCH_THREADS` and a power of two.
    #[validate(range(min = 1, max = MAX_FETCH_THREADS), custom(function = "validate_power_of_two"))]
    pub fetch_threads: usize,

    /// Time in milliseconds to wait between fetch attempts when no pending activation is found.
    pub fetch_wait_ms: u64,

    /// The number of activations to claim with a single fetch query.
    #[validate(range(min = 1))]
    pub fetch_batch_size: i32,

    /// The number of concurrent push threads to run.
    #[validate(range(min = 1))]
    pub push_threads: usize,

    /// The size of the push queue.
    #[validate(range(min = 1))]
    pub push_queue_size: usize,

    /// Maximum time in milliseconds to wait when submitting an activation to the push pool.
    #[validate(range(min = 1))]
    pub push_queue_timeout_ms: u64,

    /// Maximum time in milliseconds for a single push RPC to the worker service. This should be greater than the worker's internal timeout.
    #[validate(range(min = 1))]
    pub push_timeout_ms: u64,

    /// Update statuses from the gRPC server in batches?
    pub batch_status_updates: bool,

    /// The size of a batch of status updates.
    #[validate(range(min = 1))]
    pub status_update_batch_size: usize,

    /// Maximum milliseconds to wait before flushing a batch of status updates.
    #[validate(range(min = 1))]
    pub status_update_interval_ms: u64,

    /// Update claimed → processing updates in batches? Only applies in PUSH mode.
    pub batch_push_updates: bool,

    /// The size of a batch of dispatch updates.
    #[validate(range(min = 1))]
    pub push_update_batch_size: usize,

    /// Maximum milliseconds to wait before flushing a batch of dispatch updates.
    #[validate(range(min = 1))]
    pub push_update_interval_ms: u32,

    /// Maps every application to its worker endpoint, both represented as strings.
    pub worker_map: BTreeMap<String, String>,

    /// The namespace to assign to raw mode activations.
    pub raw_namespace: Option<String>,

    /// The application to assign to raw mode activations.
    pub raw_application: Option<String>,

    /// The taskname to assign to raw mode activations.
    pub raw_taskname: Option<String>,

    /// Processing deadline duration in seconds for raw mode activations.
    ///
    /// This is an u16 because 1) we don't want to allow signed numbers 2) it can be cast into i32
    /// (which we use elsewhere) without error conditions. It doesn't actually have to be that small.
    pub raw_processing_deadline_duration: u16,

    /// Topic configurations. After normalization, this always contains
    /// at least one entry (from legacy config or explicit kafka_topics).
    #[serde(default)]
    pub kafka_topics: BTreeMap<String, TopicConfig>,

    /// Kafka cluster configurations.
    /// After normalization, this always contains at least the "default" cluster.
    #[serde(default)]
    pub kafka_clusters: BTreeMap<String, ClusterConfig>,
}

impl Default for Config {
    /// Field defaults. `kafka_topics`/`kafka_clusters` are left empty; call
    /// [`Config::normalize_and_validate`] (as `from_args` does) to populate
    /// them from the legacy fields before using the kafka helpers.
    fn default() -> Self {
        Self {
            deprecated: DeprecatedConfig::default(),
            sentry_dsn: None,
            sentry_env: None,
            traces_sample_rate: Some(0.0),
            log_filter: "info,librdkafka=warn,h2=off".to_owned(),
            log_format: LogFormat::Text,
            grpc_addr: "0.0.0.0".to_owned(),
            grpc_port: 50051,
            grpc_shared_secret: vec![],
            statsd_addr: "127.0.0.1:8126".parse().unwrap(),
            default_metrics_tags: Default::default(),
            kafka_long_topic: "taskworker-long".to_owned(),
            create_missing_topics: false,
            kafka_deadletter_topic: "taskworker-dlq".to_owned(),
            kafka_retry_topic: None,
            default_topic_partitions: 1,
            kafka_session_timeout_ms: 6000,
            kafka_auto_commit_interval_ms: 5000,
            kafka_auto_offset_reset: "latest".to_owned(),
            kafka_send_timeout_ms: 500,
            store: StoreConfig::default(),
            runtime_config_path: None,
            upkeep_task_interval_ms: 1000,
            upkeep_unhealthy_interval_ms: 5000,
            health_check_killswitched: false,
            upkeep_deadline_reset_skip_after_startup_sec: 60,
            maintenance_task_interval_ms: 6000,
            max_delayed_task_allowed_sec: 3600,
            max_message_size: 5000000,
            grpc_max_message_size: 52 * 1024 * 1024, // 52MB
            full_vacuum_on_start: true,
            full_vacuum_on_upkeep: true,
            vacuum_interval_ms: 30000,
            log_async_backtrace: false,
            delivery_mode: DeliveryMode::Pull,
            fetch_threads: 1,
            fetch_wait_ms: 100,
            fetch_batch_size: 1,
            push_threads: 1,
            push_queue_size: 1,
            push_queue_timeout_ms: 5000,
            push_timeout_ms: 30000,
            batch_status_updates: false,
            status_update_batch_size: 1,
            status_update_interval_ms: 100,
            batch_push_updates: false,
            push_update_batch_size: 1,
            push_update_interval_ms: 100,
            worker_map: [("sentry".into(), "http://127.0.0.1:50052".into())].into(),
            raw_namespace: None,
            raw_application: None,
            raw_taskname: None,
            raw_processing_deadline_duration: 30,
            kafka_topics: BTreeMap::new(),
            kafka_clusters: BTreeMap::new(),
        }
    }
}

impl Config {
    /// Build a config instance from defaults, env vars, file + CLI options
    pub fn from_args(args: &Args) -> Result<Self> {
        let mut builder = Figment::from(Config::default());

        if let Some(path) = &args.config {
            builder = builder.merge(Yaml::file(path));
        }

        // Use "__" for nested configurations via environment variables, like `TASKBROKER_KAFKA_TOPICS__PROFILES__CLUSTER`
        builder = builder.merge(Env::prefixed("TASKBROKER_").split("__"));
        let mut config: Config = builder.extract()?;

        // Map deprecated fields to current fields
        config.map_deprecated_options();

        // Normalize and validate Kafka values
        config.normalize_and_validate()?;

        // Validate all other values
        config.validate()?;

        Ok(config)
    }

    fn map_deprecated_options(&mut self) {
        // Map store configuration options
        deprecated::map! {
            self.deprecated.database_adapter               => self.store.database_adapter,
            self.deprecated.run_migrations                 => self.store.run_migrations,
            self.deprecated.pg_host                        => self.store.pg_host,
            self.deprecated.pg_port                        => self.store.pg_port,
            self.deprecated.pg_ddl_username                => self.store.pg_ddl_username,
            self.deprecated.pg_username                    => self.store.pg_username,
            self.deprecated.pg_password                    => self.store.pg_password,
            self.deprecated.pg_ddl_password                => self.store.pg_ddl_password,
            self.deprecated.pg_database_name               => self.store.pg_database_name,
            self.deprecated.pg_default_database_name       => self.store.pg_default_database_name,
            self.deprecated.pg_extra_query_params          => some(self.store.pg_extra_query_params),
            self.deprecated.db_path                        => self.store.db_path,
            self.deprecated.db_write_failure_backoff_ms    => self.store.db_write_failure_backoff_ms,
            self.deprecated.db_query_max_retries           => some(self.store.db_query_max_retries),
            self.deprecated.db_query_retry_delay_ms        => self.store.db_query_retry_delay_ms,
            self.deprecated.db_insert_batch_max_len        => self.store.db_insert_batch_max_len,
            self.deprecated.db_insert_batch_max_size       => self.store.db_insert_batch_max_size,
            self.deprecated.db_insert_batch_max_time_ms    => self.store.db_insert_batch_max_time_ms,
            self.deprecated.db_max_size                    => some(self.store.db_max_size),
            self.deprecated.max_pending_count              => self.store.max_pending_count,
            self.deprecated.max_delay_count                => self.store.max_delay_count,
            self.deprecated.max_processing_count           => self.store.max_processing_count,
            self.deprecated.max_processing_attempts        => self.store.max_processing_attempts,
            self.deprecated.processing_deadline_grace_sec  => self.store.processing_deadline_grace_sec,
            self.deprecated.vacuum_page_count              => some(self.store.vacuum_page_count),
            self.deprecated.enable_sqlite_status_metrics   => self.store.enable_sqlite_status_metrics,
        };
    }

    /// Normalize the legacy single-topic config into the new multi-topic
    /// format, then validate the result.
    ///
    /// The legacy fields (`kafka_topic`, `kafka_cluster`, etc.) and the new
    /// fields (`kafka_topics`, `kafka_clusters`) are mutually exclusive: mixing
    /// them is a hard error. When only legacy fields are used (including the
    /// zero-config case, where the historical `taskworker` defaults apply), they
    /// are normalized into `kafka_topics`/`kafka_clusters`. After this,
    /// `kafka_topics` and `kafka_clusters` are always populated.
    pub(crate) fn normalize_and_validate(&mut self) -> Result<()> {
        const DEFAULT_CLUSTER: &str = "default";
        const DEADLETTER_CLUSTER: &str = "deadletter";
        const DEFAULT_TOPIC: &str = "taskworker";
        const DEFAULT_CLUSTER_ADDRESS: &str = "127.0.0.1:9092";
        const DEFAULT_CONSUMER_GROUP: &str = "taskworker";

        let uses_new_format = !self.kafka_topics.is_empty() || !self.kafka_clusters.is_empty();
        // Any explicitly-set deprecated field describing a cluster (the main
        // consumed cluster or the deadletter cluster) or the deprecated global
        // raw mode. kafka_deadletter_topic is NOT deprecated and is intentionally
        // excluded.
        let uses_legacy = self.deprecated.kafka_topic.is_some()
            || self.deprecated.kafka_cluster.is_some()
            || self.deprecated.kafka_consumer_group.is_some()
            || self.deprecated.kafka_security_protocol.is_some()
            || self.deprecated.kafka_sasl_mechanism.is_some()
            || self.deprecated.kafka_sasl_username.is_some()
            || self.deprecated.kafka_sasl_password.is_some()
            || self.deprecated.kafka_ssl_ca_location.is_some()
            || self.deprecated.kafka_ssl_certificate_location.is_some()
            || self.deprecated.kafka_ssl_key_location.is_some()
            || self.deprecated.kafka_deadletter_cluster.is_some()
            || self.deprecated.kafka_deadletter_security_protocol.is_some()
            || self.deprecated.kafka_deadletter_sasl_mechanism.is_some()
            || self.deprecated.kafka_deadletter_sasl_username.is_some()
            || self.deprecated.kafka_deadletter_sasl_password.is_some()
            || self.deprecated.kafka_deadletter_ssl_ca_location.is_some()
            || self
                .deprecated
                .kafka_deadletter_ssl_certificate_location
                .is_some()
            || self.deprecated.kafka_deadletter_ssl_key_location.is_some()
            || self.deprecated.raw_mode.is_some();

        if uses_new_format && uses_legacy {
            return Err(anyhow!(
                "cannot mix the deprecated kafka_cluster/kafka_topic/kafka_consumer_group/\
                 kafka_deadletter_cluster (and related kafka_sasl_*/kafka_ssl_*/kafka_deadletter_* \
                 auth fields) with kafka_topics/kafka_clusters; use one config format or the other"
                    .to_owned(),
            ));
        }

        if uses_new_format {
            // New format: the maps are the source of truth. Require both halves
            // so a topic always has a cluster to resolve against.
            if self.kafka_topics.is_empty() {
                return Err(anyhow!(
                    "kafka_clusters is set but kafka_topics is empty".to_owned(),
                ));
            }
            if self.kafka_clusters.is_empty() {
                return Err(anyhow!(
                    "kafka_topics is set but kafka_clusters is empty".to_owned(),
                ));
            }
        } else {
            if self.deprecated.kafka_cluster.is_some() {
                warn!("kafka_cluster is deprecated, use kafka_clusters instead");
            }
            if self.deprecated.kafka_topic.is_some() {
                warn!("kafka_topic is deprecated, use kafka_topics instead");
            }
            if self.deprecated.kafka_consumer_group.is_some() {
                warn!("kafka_consumer_group is deprecated, use kafka_topics instead");
            }
            if self.deprecated.kafka_deadletter_cluster.is_some() {
                warn!(
                    "kafka_deadletter_cluster is deprecated, declare the deadletter topic in \
                     kafka_topics with a cluster reference instead"
                );
            }
            if self.deprecated.raw_mode.is_some() {
                warn!("raw_mode is deprecated, use kafka_topics.<topic>.raw instead");
            }

            let topic_name = self
                .deprecated
                .kafka_topic
                .clone()
                .unwrap_or_else(|| DEFAULT_TOPIC.to_owned());
            let address = self
                .deprecated
                .kafka_cluster
                .clone()
                .unwrap_or_else(|| DEFAULT_CLUSTER_ADDRESS.to_owned());
            let consumer_group = self
                .deprecated
                .kafka_consumer_group
                .clone()
                .unwrap_or_else(|| DEFAULT_CONSUMER_GROUP.to_owned());

            let prev = self.kafka_clusters.insert(
                DEFAULT_CLUSTER.to_owned(),
                ClusterConfig {
                    address: address.clone(),
                    security_protocol: self.deprecated.kafka_security_protocol.clone(),
                    sasl_mechanism: self.deprecated.kafka_sasl_mechanism.clone(),
                    sasl_username: self.deprecated.kafka_sasl_username.clone(),
                    sasl_password: self.deprecated.kafka_sasl_password.clone(),
                    ssl_ca_location: self.deprecated.kafka_ssl_ca_location.clone(),
                    ssl_certificate_location: self
                        .deprecated
                        .kafka_ssl_certificate_location
                        .clone(),
                    ssl_key_location: self.deprecated.kafka_ssl_key_location.clone(),
                },
            );
            assert!(
                prev.is_none(),
                "internal: duplicate '{DEFAULT_CLUSTER}' cluster"
            );

            // Migrate the deprecated deadletter cluster/auth fields into a
            // dedicated cluster. The deadletter producer historically falls back
            // to the main cluster's address when kafka_deadletter_cluster is
            // unset, while using its own (possibly empty) auth.
            let prev = self.kafka_clusters.insert(
                DEADLETTER_CLUSTER.to_owned(),
                ClusterConfig {
                    address: self
                        .deprecated
                        .kafka_deadletter_cluster
                        .clone()
                        .unwrap_or_else(|| address.clone()),
                    security_protocol: self.deprecated.kafka_deadletter_security_protocol.clone(),
                    sasl_mechanism: self.deprecated.kafka_deadletter_sasl_mechanism.clone(),
                    sasl_username: self.deprecated.kafka_deadletter_sasl_username.clone(),
                    sasl_password: self.deprecated.kafka_deadletter_sasl_password.clone(),
                    ssl_ca_location: self.deprecated.kafka_deadletter_ssl_ca_location.clone(),
                    ssl_certificate_location: self
                        .deprecated
                        .kafka_deadletter_ssl_certificate_location
                        .clone(),
                    ssl_key_location: self.deprecated.kafka_deadletter_ssl_key_location.clone(),
                },
            );
            assert!(
                prev.is_none(),
                "internal: duplicate '{DEADLETTER_CLUSTER}' cluster"
            );

            let raw_config = if let Some(true) = self.deprecated.raw_mode {
                Some(RawModeConfig {
                    namespace: self.raw_namespace.clone(),
                    application: self.raw_application.clone(),
                    taskname: self.raw_taskname.clone(),
                    processing_deadline_duration: Some(self.raw_processing_deadline_duration),
                    compression_level: None,
                })
            } else {
                None
            };

            let prev = self.kafka_topics.insert(
                topic_name.clone(),
                TopicConfig {
                    cluster: DEFAULT_CLUSTER.to_owned(),
                    consumer_group: consumer_group.clone(),
                    produce_only: false,
                    raw: raw_config,
                    session_timeout_ms: None,
                    auto_commit_interval_ms: None,
                    auto_offset_reset: None,
                },
            );
            assert!(prev.is_none(), "internal: duplicate topic '{topic_name}'");

            // Register the deadletter topic as a produce-only topic on its own
            // cluster. A non-empty return value means it collided with the main
            // topic, which would route deadletter messages to the wrong topic.
            let prev = self.kafka_topics.insert(
                self.kafka_deadletter_topic.clone(),
                TopicConfig {
                    cluster: DEADLETTER_CLUSTER.to_owned(),
                    consumer_group: consumer_group.clone(),
                    produce_only: true,
                    raw: None,
                    session_timeout_ms: None,
                    auto_commit_interval_ms: None,
                    auto_offset_reset: None,
                },
            );
            if prev.is_some() {
                return Err(anyhow!(
                    "kafka_deadletter_topic '{}' must differ from the consumed topic '{}'",
                    self.kafka_deadletter_topic,
                    topic_name
                ));
            }

            // Register the retry topic on the deadletter cluster: retries are
            // published by the upkeep producer, which is the same producer used
            // for the deadletter topic (see kafka_producer_cluster). Aliasing
            // the deadletter topic is rejected to avoid a name collision.
            if let Some(ref retry_topic) = self.kafka_retry_topic {
                if retry_topic == &self.kafka_deadletter_topic {
                    return Err(anyhow!(
                        "kafka_retry_topic '{}' must differ from kafka_deadletter_topic",
                        retry_topic
                    ));
                }
                self.kafka_topics
                    .entry(retry_topic.clone())
                    .or_insert_with(|| TopicConfig {
                        cluster: DEADLETTER_CLUSTER.to_owned(),
                        consumer_group,
                        produce_only: true,
                        raw: None,
                        session_timeout_ms: None,
                        auto_commit_interval_ms: None,
                        auto_offset_reset: None,
                    });
            }
        }

        // Validate cluster references
        for (topic_name, topic_config) in &self.kafka_topics {
            self.cluster(&topic_config.cluster).map_err(|_| {
                Box::new(figment::Error::from(format!(
                    "topic '{}' references unknown cluster '{}'",
                    topic_name, topic_config.cluster
                )))
            })?;
        }

        // Validate at least one consumable topic.
        let consumable = self.consumable_topics()?;

        // Multi-topic consumption is only supported on the sqlite adapter for
        // now. The postgres adapter filters claims by a single shared partition
        // list, but those partition numbers aren't unique across topics, so the
        // filter would mix partitions from different topics together. Note this
        // filtering exists only to avoid lock contention between brokers, not for
        // correctness; supporting multi-topic on postgres means reworking how we
        // avoid that contention (e.g. filtering by (topic, partition) or another
        // mechanism entirely). Reject the combination here, before any consumer
        // spawns.
        if consumable.len() > 1 && self.store.database_adapter == DatabaseAdapter::Postgres {
            return Err(anyhow!(
                "multi-topic consumption ({} consumable topics) is not supported with the \
                 postgres database adapter; use the sqlite adapter or a single consumable topic",
                consumable.len()
            ));
        }

        // The deadletter topic must be a declared topic so the producer can
        // resolve its cluster. In legacy mode it was added above; in the new
        // format the user must declare it (produce-only) in kafka_topics.
        if !self.kafka_topics.contains_key(&self.kafka_deadletter_topic) {
            return Err(anyhow!(
                "kafka_deadletter_topic '{}' is not defined in kafka_topics",
                self.kafka_deadletter_topic
            ));
        }

        // The upkeep producer connects to the deadletter topic's cluster but is
        // also reused to publish retries to the retry topic (or the consumed
        // topic when no retry topic is configured). A single producer can only
        // reach one cluster, so the retry target must be a declared topic on
        // the same cluster address as the deadletter topic; otherwise retries
        // would be published to the wrong brokers. In the legacy format the
        // retry topic is registered above; in the new format the user must
        // declare it in kafka_topics.
        // Normalize the retry topic so downstream code can always rely on it
        // being set:
        // - explicitly configured: used as-is.
        // - single non-raw consumed topic: retries loop back to that topic.
        // - raw mode: raw messages aren't activations, so retries must go to a
        //   separate activation-encoded topic; a retry topic is mandatory.
        // - multiple consumed topics: no unambiguous fallback, so a single
        //   shared retry topic is mandatory.
        if self.kafka_retry_topic.is_none() {
            let has_raw = consumable.iter().any(|(_, cfg)| cfg.raw.is_some());
            let count = consumable.len();
            let single_topic = (count == 1).then(|| consumable[0].0.to_owned());

            if has_raw {
                return Err(anyhow!(
                    "kafka_retry_topic must be set explicitly when a consumed topic uses raw mode"
                ));
            }
            match single_topic {
                Some(topic) => self.kafka_retry_topic = Some(topic),
                None => {
                    return Err(anyhow!(
                        "kafka_retry_topic is required when consuming from multiple topics ({} \
                         consumable topics configured)",
                        count
                    ));
                }
            }
        }
        let retry_target = self
            .kafka_retry_topic
            .as_deref()
            .expect("kafka_retry_topic is set above");
        let retry_topic_config = self.kafka_topics.get(retry_target).ok_or_else(|| {
            Box::new(figment::Error::from(format!(
                "kafka_retry_topic '{retry_target}' is not defined in kafka_topics"
            )))
        })?;
        let retry_address = &self.cluster(&retry_topic_config.cluster)?.address;
        let deadletter_address = &self
            .cluster(&self.kafka_topics[&self.kafka_deadletter_topic].cluster)?
            .address;
        if retry_address != deadletter_address {
            return Err(anyhow!(
                "retry target topic '{}' is on cluster '{}', but deadletter topic '{}' is on \
                 '{}'; they share a single producer and must be on the same cluster",
                retry_target,
                retry_address,
                self.kafka_deadletter_topic,
                deadletter_address
            ));
        }

        // Validate raw-mode topics up front. The raw fields are optional on
        // `RawModeConfig` (they don't apply to non-raw topics), but a raw topic
        // requires all of them. Catch missing fields here as a config error
        // rather than panicking when the consumer builds its deserializer.
        for (topic_name, topic_config) in &self.kafka_topics {
            let Some(raw) = &topic_config.raw else {
                continue;
            };
            let application = raw.application.as_deref().ok_or_else(|| {
                anyhow!("topic '{topic_name}' enables raw mode but is missing raw.application")
            })?;
            if !self.worker_map.contains_key(application) {
                return Err(anyhow!(
                    "topic '{topic_name}' raw.application '{application}' is not in worker_map"
                ));
            }
            if raw.namespace.is_none() {
                return Err(anyhow!(
                    "topic '{topic_name}' enables raw mode but is missing raw.namespace"
                ));
            }
            if raw.taskname.is_none() {
                return Err(anyhow!(
                    "topic '{topic_name}' enables raw mode but is missing raw.taskname"
                ));
            }
            if raw.processing_deadline_duration.is_none() {
                return Err(anyhow!(
                    "topic '{topic_name}' enables raw mode but is missing \
                     raw.processing_deadline_duration"
                ));
            }
            // Raw messages aren't activations, so retries must go to a separate
            // activation-encoded topic, never back to the raw topic itself.
            if self.kafka_retry_topic.as_deref() == Some(topic_name.as_str()) {
                return Err(anyhow!(
                    "kafka_retry_topic must differ from raw topic '{topic_name}'"
                ));
            }
        }

        Ok(())
    }

    /// Get all consumable (non-`produce_only`) topics and their configs, in
    /// `kafka_topics` (BTreeMap) order. Returns an error only when there are no
    /// consumable topics. This is the sole accessor for consumed topics; callers
    /// that only handle a single topic should iterate and select explicitly
    /// rather than assuming exactly one.
    pub fn consumable_topics(&self) -> Result<Vec<(&str, &TopicConfig)>, Box<figment::Error>> {
        let consumable: Vec<(&str, &TopicConfig)> = self
            .kafka_topics
            .iter()
            .filter(|(_, cfg)| !cfg.produce_only)
            .map(|(name, cfg)| (name.as_str(), cfg))
            .collect();

        if consumable.is_empty() {
            return Err(Box::new(figment::Error::from(
                "no consumable topic configured (all topics have produce_only: true)".to_owned(),
            )));
        }

        Ok(consumable)
    }

    /// The topic retries are produced to. `normalize_and_validate` always sets
    /// `kafka_retry_topic` (it defaults to the single consumed topic when only
    /// one non-raw topic is configured), so this is infallible after validation.
    /// Panics if config wasn't validated (call from_args, not extract directly).
    pub fn retry_topic(&self) -> &str {
        self.kafka_retry_topic
            .as_deref()
            .expect("kafka_retry_topic unset - was config validated?")
    }

    /// Get cluster config by name.
    /// Returns an error if the cluster doesn't exist.
    pub fn cluster(&self, name: &str) -> Result<&ClusterConfig, Box<figment::Error>> {
        self.kafka_clusters
            .get(name)
            .ok_or_else(|| Box::new(figment::Error::from(format!("unknown cluster: {}", name))))
    }

    /// Convert the application Config into rdkafka::ClientConfig for the consumer
    /// of a specific topic. Each consumed topic has its own consumer (own
    /// `group.id` and cluster), so multi-topic spawns one consumer per topic.
    /// Panics if `topic_name` isn't a declared topic (call from_args first).
    pub fn kafka_consumer_config_for(&self, topic_name: &str) -> ClientConfig {
        let topic_config = self
            .kafka_topics
            .get(topic_name)
            .unwrap_or_else(|| panic!("unknown topic '{topic_name}' - was config validated?"));
        let cluster = self
            .cluster(&topic_config.cluster)
            .expect("cluster lookup failed - was config validated?");

        // Per-topic consumer settings, falling back to the global defaults.
        let session_timeout_ms = topic_config
            .session_timeout_ms
            .unwrap_or(self.kafka_session_timeout_ms);
        let auto_commit_interval_ms = topic_config
            .auto_commit_interval_ms
            .unwrap_or(self.kafka_auto_commit_interval_ms);
        let auto_offset_reset = topic_config
            .auto_offset_reset
            .clone()
            .unwrap_or_else(|| self.kafka_auto_offset_reset.clone());

        let mut config = ClientConfig::new();
        cluster.apply_to(&mut config);
        config
            .set("group.id", topic_config.consumer_group.clone())
            .set("session.timeout.ms", session_timeout_ms.to_string())
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "true")
            .set(
                "auto.commit.interval.ms",
                auto_commit_interval_ms.to_string(),
            )
            .set("auto.offset.reset", auto_offset_reset)
            .set("enable.auto.offset.store", "false");

        config
    }

    /// Build an rdkafka::ClientConfig for an admin client on a named cluster.
    /// Carries only `bootstrap.servers` + that cluster's auth (no consumer
    /// settings), so topic creation targets the correct brokers.
    /// Panics if the cluster isn't declared (call from_args first).
    pub fn kafka_admin_config(&self, cluster_name: &str) -> ClientConfig {
        let cluster = self
            .cluster(cluster_name)
            .expect("cluster lookup failed - was config validated?");

        let mut config = ClientConfig::new();
        cluster.apply_to(&mut config);
        config
    }

    /// The cluster the deadletter / forwarding producer connects to: the
    /// cluster of the `kafka_deadletter_topic` entry in `kafka_topics` (in
    /// legacy mode this is the migrated "deadletter" cluster).
    /// Panics if config wasn't validated.
    pub fn kafka_producer_cluster(&self) -> &ClusterConfig {
        let dlq_topic = self
            .kafka_topics
            .get(&self.kafka_deadletter_topic)
            .expect("deadletter topic not in kafka_topics - was config validated?");
        self.cluster(&dlq_topic.cluster)
            .expect("cluster lookup failed - was config validated?")
    }

    /// Convert the application Config into rdkafka::ClientConfig for the
    /// deadletter / forwarding producer. The producer connects to the cluster
    /// of the `kafka_deadletter_topic` entry in `kafka_topics` (in legacy mode
    /// this is the migrated "deadletter" cluster).
    /// Panics if config wasn't validated.
    pub fn kafka_producer_config(&self) -> ClientConfig {
        let cluster = self.kafka_producer_cluster();

        let mut config = ClientConfig::new();
        cluster.apply_to(&mut config);
        config.set("message.max.bytes", format!("{}", self.max_message_size));
        config
    }
}

impl Provider for Config {
    fn metadata(&self) -> Metadata {
        Metadata::named("Taskbroker config")
    }

    fn data(&self) -> Result<figment::value::Map<Profile, figment::value::Dict>, figment::Error> {
        figment::providers::Serialized::defaults(Config::default()).data()
    }
}

/// Ensures that `n` is a power of two, used to validate `fetch_threads`.
fn validate_power_of_two(n: usize) -> Result<(), ValidationError> {
    if n.is_power_of_two() {
        Ok(())
    } else {
        Err(ValidationError::new("not_power_of_two"))
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use figment::Jail;
    use validator::Validate;

    use crate::logging::LogFormat;
    use crate::{Args, Run};

    use super::{Config, DatabaseAdapter, DeliveryMode};

    #[test]
    fn test_default() {
        let config = Config {
            ..Default::default()
        };
        assert_eq!(config.sentry_dsn, None);
        assert_eq!(config.sentry_env, None);
        assert_eq!(config.log_filter, "info,librdkafka=warn,h2=off");
        assert_eq!(config.log_format, LogFormat::Text);
        assert_eq!(config.grpc_port, 50051);
        assert_eq!(config.store.db_path, "./taskbroker-inflight.sqlite");
        assert_eq!(config.store.max_pending_count, 2048);
        assert_eq!(config.store.max_processing_count, 2048);
        assert_eq!(config.store.vacuum_page_count, None);
        assert_eq!(
            config.worker_map.get("sentry").map(String::as_str),
            Some("http://127.0.0.1:50052")
        );
    }

    #[test]
    fn test_validate_rejects_invalid_fields() {
        let mut config = Config {
            fetch_threads: 0,
            ..Config::default()
        };

        // Fetch threads cannot be zero
        assert!(config.validate().is_err());

        config.fetch_threads = 1;
        assert!(config.validate().is_ok());

        // Fetch threads must be a power of two
        config.fetch_threads = 3;
        assert!(config.validate().is_err());

        config.fetch_threads = 4;
        assert!(config.validate().is_ok());

        // Fetch threads must be ≤ 256
        config.fetch_threads = 512;
        assert!(config.validate().is_err());

        config.fetch_threads = 1;
        assert!(config.validate().is_ok());

        // Fetch batch size cannot be zero
        config.fetch_batch_size = 0;
        assert!(config.validate().is_err());

        config.fetch_batch_size = 1;
        assert!(config.validate().is_ok());

        // Push threads cannot be zero
        config.push_threads = 0;
        assert!(config.validate().is_err());

        config.push_threads = 1;
        assert!(config.validate().is_ok());

        // Push queue size cannot be zero
        config.push_queue_size = 0;
        assert!(config.validate().is_err());

        config.push_queue_size = 1;
        assert!(config.validate().is_ok());

        // Push queue timeout cannot be zero
        config.push_queue_timeout_ms = 0;
        assert!(config.validate().is_err());

        config.push_queue_timeout_ms = 1;
        assert!(config.validate().is_ok());

        // Push timeout cannot be zero
        config.push_timeout_ms = 0;
        assert!(config.validate().is_err());

        config.push_timeout_ms = 1;
        assert!(config.validate().is_ok());

        // Status update batch size cannot be zero
        config.status_update_batch_size = 0;
        assert!(config.validate().is_err());

        config.status_update_batch_size = 1;
        assert!(config.validate().is_ok());

        // Status update interval cannot be zero
        config.status_update_interval_ms = 0;
        assert!(config.validate().is_err());

        config.status_update_interval_ms = 1;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_from_args_config_file() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
                sentry_dsn: fake_dsn
                sentry_env: prod
                log_filter: debug,rdkafka=off
                log_format: json
                statsd_addr: 127.0.0.1:8126
                default_metrics_tags:
                    key_1: value_1
                kafka_cluster: 10.0.0.1:9092,10.0.0.2:9092
                kafka_topic: error-tasks
                kafka_deadletter_topic: error-tasks-dlq
                kafka_auto_offset_reset: earliest
                database_adapter: postgres
                db_path: ./taskbroker-error.sqlite
                db_max_size: 3000000000
                max_pending_count: 512
                max_processing_count: 512
                max_processing_attempts: 5
                vacuum_page_count: 1000
                full_vacuum_on_start: true
                worker_map:
                    sentry: http://worker-sentry:50052
                    launchpad: http://worker-launchpad:50053
            "#,
            )?;
            // Env vars always override config file
            jail.set_env("TASKBROKER_LOG_FILTER", "error");

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.sentry_dsn, Some("fake_dsn".to_owned()));
            assert_eq!(config.sentry_env, Some(Cow::Borrowed("prod")));
            assert_eq!(config.log_filter, "error");
            assert_eq!(config.log_format, LogFormat::Json);
            assert_eq!(
                config.default_metrics_tags,
                BTreeMap::from([("key_1".to_owned(), "value_1".to_owned())])
            );
            let (topic_name, topic_config) = config.consumable_topics().unwrap()[0];
            assert_eq!(topic_name, "error-tasks");
            assert_eq!(topic_config.consumer_group, "taskworker");
            assert_eq!(config.kafka_auto_offset_reset, "earliest".to_owned());
            assert_eq!(config.kafka_session_timeout_ms, 6000.to_owned());
            assert_eq!(config.kafka_deadletter_topic, "error-tasks-dlq".to_owned());
            assert_eq!(config.store.database_adapter, DatabaseAdapter::Postgres);
            assert_eq!(config.store.db_path, "./taskbroker-error.sqlite".to_owned());
            assert_eq!(config.store.max_pending_count, 512);
            assert_eq!(config.store.max_processing_count, 512);
            assert_eq!(config.store.max_processing_attempts, 5);
            assert_eq!(config.store.vacuum_page_count, Some(1000));
            assert_eq!(config.store.db_max_size, Some(3_000_000_000));
            assert!(config.full_vacuum_on_start);
            assert_eq!(
                config.worker_map,
                BTreeMap::from([
                    ("sentry".to_owned(), "http://worker-sentry:50052".to_owned(),),
                    (
                        "launchpad".to_owned(),
                        "http://worker-launchpad:50053".to_owned(),
                    ),
                ])
            );

            Ok(())
        });
    }

    #[test]
    fn test_from_args_env_and_args() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_LOG_FILTER", "error");
            jail.set_env("TASKBROKER_DATABASE_ADAPTER", "postgres");
            jail.set_env("TASKBROKER_MAX_PROCESSING_ATTEMPTS", "5");

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.log_filter, "error");
            assert_eq!(config.store.database_adapter, DatabaseAdapter::Postgres);
            assert_eq!(config.store.max_processing_attempts, 5);

            Ok(())
        });
    }

    #[test]
    fn test_from_args_env_test() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_LOG_FILTER", "error");
            jail.set_env("TASKBROKER_MAX_PROCESSING_ATTEMPTS", "5");
            jail.set_env("TASKBROKER_DEFAULT_METRICS_TAGS", "{key=value}");

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.sentry_dsn, None);
            assert_eq!(config.sentry_env, None);
            assert_eq!(config.log_filter, "error");
            let (topic_name, topic_config) = config.consumable_topics().unwrap()[0];
            assert_eq!(topic_name, "taskworker");
            assert_eq!(
                config.cluster(&topic_config.cluster).unwrap().address,
                "127.0.0.1:9092"
            );
            assert_eq!(config.kafka_deadletter_topic, "taskworker-dlq".to_owned());
            assert_eq!(
                config.store.db_path,
                "./taskbroker-inflight.sqlite".to_owned()
            );
            assert_eq!(config.store.max_pending_count, 2048);
            assert_eq!(config.store.max_processing_count, 2048);
            assert_eq!(config.store.max_processing_attempts, 5);
            assert_eq!(
                config.default_metrics_tags,
                BTreeMap::from([("key".to_owned(), "value".to_owned())])
            );
            assert_eq!(
                config.worker_map.get("sentry").map(String::as_str),
                Some("http://127.0.0.1:50052"),
                "partial env override must not drop worker_map defaults"
            );

            Ok(())
        });
    }

    /// `worker_map` uses the same env map encoding as `default_metrics_tags` (brace `key=value` pairs).
    #[test]
    fn test_worker_map_from_env() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_LOG_FILTER", "error");
            jail.set_env(
                "TASKBROKER_WORKER_MAP",
                "{sentry=http://127.0.0.1:60052,launchpad=http://127.0.0.1:60053}",
            );

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(
                config.worker_map,
                BTreeMap::from([
                    ("sentry".to_owned(), "http://127.0.0.1:60052".to_owned(),),
                    ("launchpad".to_owned(), "http://127.0.0.1:60053".to_owned(),),
                ])
            );

            Ok(())
        });
    }

    #[test]
    fn test_kafka_consumer_config() {
        let args = Args {
            run: Run::Broker,
            config: None,
        };
        let config = Config::from_args(&args).unwrap();
        let consumer_config = config.kafka_consumer_config_for("taskworker");

        assert_eq!(
            consumer_config.get("bootstrap.servers").unwrap(),
            "127.0.0.1:9092"
        );
        assert_eq!(consumer_config.get("group.id").unwrap(), "taskworker");
        assert!(consumer_config.get("session.timeout.ms").is_some());
    }

    #[test]
    fn test_kafka_consumer_config_auth() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_KAFKA_SECURITY_PROTOCOL", "sasl_plaintext");
            jail.set_env("TASKBROKER_KAFKA_SASL_MECHANISM", "SCRAM-SHA-256");
            jail.set_env("TASKBROKER_KAFKA_SASL_USERNAME", "taskbroker");
            jail.set_env("TASKBROKER_KAFKA_SASL_PASSWORD", "secret-tech");

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).unwrap();
            let consumer_config = config.kafka_consumer_config_for("taskworker");

            assert_eq!(
                consumer_config.get("security.protocol").unwrap(),
                "sasl_plaintext",
            );
            assert_eq!(
                consumer_config.get("sasl.mechanism").unwrap(),
                "SCRAM-SHA-256"
            );
            assert_eq!(consumer_config.get("sasl.username").unwrap(), "taskbroker");
            assert_eq!(consumer_config.get("sasl.password").unwrap(), "secret-tech");

            Ok(())
        });
    }

    #[test]
    fn test_kafka_consumer_config_ssl() {
        Jail::expect_with(|jail| {
            jail.set_env(
                "TASKBROKER_KAFKA_SSL_CA_LOCATION",
                "/etc/ssl/ca-certificate.pem",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_SSL_CERTIFICATE_LOCATION",
                "/etc/ssl/taskbroker/public.crt",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_SSL_KEY_LOCATION",
                "/etc/ssl/taskbroker/private.key",
            );

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).unwrap();
            let consumer_config = config.kafka_consumer_config_for("taskworker");

            assert_eq!(
                consumer_config.get("ssl.ca.location").unwrap(),
                "/etc/ssl/ca-certificate.pem",
            );
            assert_eq!(
                consumer_config.get("ssl.certificate.location").unwrap(),
                "/etc/ssl/taskbroker/public.crt"
            );
            assert_eq!(
                consumer_config.get("ssl.key.location").unwrap(),
                "/etc/ssl/taskbroker/private.key"
            );

            Ok(())
        });
    }

    #[test]
    fn test_kafka_producer_config() {
        let args = Args {
            run: Run::Broker,
            config: None,
        };
        let config = Config::from_args(&args).unwrap();
        let producer_config = config.kafka_producer_config();

        assert_eq!(
            producer_config.get("bootstrap.servers").unwrap(),
            "127.0.0.1:9092"
        );
        assert!(producer_config.get("group.id").is_none());
        assert!(producer_config.get("session.timeout.ms").is_none());
    }

    #[test]
    fn test_kafka_producer_config_auth() {
        Jail::expect_with(|jail| {
            jail.set_env(
                "TASKBROKER_KAFKA_DEADLETTER_SECURITY_PROTOCOL",
                "sasl_plaintext",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_DEADLETTER_SASL_MECHANISM",
                "SCRAM-SHA-256",
            );
            jail.set_env("TASKBROKER_KAFKA_DEADLETTER_SASL_USERNAME", "taskbroker");
            jail.set_env("TASKBROKER_KAFKA_DEADLETTER_SASL_PASSWORD", "secret-tech");

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).unwrap();
            let producer_config = config.kafka_producer_config();

            assert_eq!(
                producer_config.get("security.protocol").unwrap(),
                "sasl_plaintext"
            );
            assert_eq!(
                producer_config.get("sasl.mechanism").unwrap(),
                "SCRAM-SHA-256"
            );
            assert_eq!(producer_config.get("sasl.username").unwrap(), "taskbroker");
            assert_eq!(producer_config.get("sasl.password").unwrap(), "secret-tech");

            Ok(())
        });
    }

    #[test]
    fn test_kafka_producer_config_ssl() {
        Jail::expect_with(|jail| {
            jail.set_env(
                "TASKBROKER_KAFKA_DEADLETTER_SSL_CA_LOCATION",
                "/etc/ssl/ca-certificate.pem",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_DEADLETTER_SSL_CERTIFICATE_LOCATION",
                "/etc/ssl/taskbroker/public.crt",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_DEADLETTER_SSL_KEY_LOCATION",
                "/etc/ssl/taskbroker/private.key",
            );

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).unwrap();
            let producer_config = config.kafka_producer_config();

            assert_eq!(
                producer_config.get("ssl.ca.location").unwrap(),
                "/etc/ssl/ca-certificate.pem",
            );
            assert_eq!(
                producer_config.get("ssl.certificate.location").unwrap(),
                "/etc/ssl/taskbroker/public.crt"
            );
            assert_eq!(
                producer_config.get("ssl.key.location").unwrap(),
                "/etc/ssl/taskbroker/private.key"
            );

            Ok(())
        });
    }

    #[test]
    fn test_default_delivery_mode() {
        let config = Config::default();
        assert_eq!(config.delivery_mode, DeliveryMode::Pull);
    }

    #[test]
    fn test_from_args_delivery_mode_from_env() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_DELIVERY_MODE", "push");

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.delivery_mode, DeliveryMode::Push);

            Ok(())
        });
    }

    #[test]
    fn test_from_args_delivery_mode_from_config_file() {
        Jail::expect_with(|jail| {
            jail.create_file("config.yaml", "delivery_mode: push")?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.delivery_mode, DeliveryMode::Push);

            Ok(())
        });
    }

    #[test]
    fn test_multi_topic_config_from_yaml() {
        use super::{ClusterConfig, RawModeConfig};

        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
kafka_deadletter_topic: profiles-dlq
kafka_retry_topic: profiles-retry

worker_map:
  profiles: http://worker-profiles:50052

kafka_topics:
  profiles:
    cluster: profiles-cluster
    consumer_group: taskbroker-profiles
    raw:
      namespace: profiles
      application: profiles
      taskname: profiles.process
      processing_deadline_duration: 30
  profiles-retry:
    cluster: profiles-cluster
    consumer_group: taskbroker-profiles-retry
    produce_only: true
  profiles-dlq:
    cluster: profiles-cluster
    consumer_group: taskbroker-profiles-dlq
    produce_only: true

kafka_clusters:
  profiles-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let config = Config::from_args(&args).unwrap();

            let topics = &config.kafka_topics;
            assert_eq!(topics.len(), 3);

            let profiles = topics.get("profiles").unwrap();
            assert_eq!(profiles.cluster, "profiles-cluster");
            assert_eq!(profiles.consumer_group, "taskbroker-profiles");
            assert!(!profiles.produce_only);
            assert_eq!(
                profiles.raw,
                Some(RawModeConfig {
                    namespace: Some("profiles".to_owned()),
                    application: Some("profiles".to_owned()),
                    taskname: Some("profiles.process".to_owned()),
                    processing_deadline_duration: Some(30),
                    compression_level: None,
                })
            );

            let retry = topics.get("profiles-retry").unwrap();
            assert!(retry.produce_only);

            let clusters = &config.kafka_clusters;
            // Only the explicitly-declared cluster exists; no legacy "default"
            // cluster is injected when the new format is used.
            assert_eq!(clusters.len(), 1);
            assert_eq!(
                clusters.get("profiles-cluster"),
                Some(&ClusterConfig {
                    address: "10.0.0.1:9092".to_owned(),
                    security_protocol: None,
                    sasl_mechanism: None,
                    sasl_username: None,
                    sasl_password: None,
                    ssl_ca_location: None,
                    ssl_certificate_location: None,
                    ssl_key_location: None,
                })
            );
            assert!(!clusters.contains_key("default"));

            // Test consumable_topic() and cluster() helpers
            let (topic_name, topic_config) = config.consumable_topics().unwrap()[0];
            assert_eq!(topic_name, "profiles");
            assert_eq!(topic_config.cluster, "profiles-cluster");

            let cluster = config.cluster("profiles-cluster").unwrap();
            assert_eq!(cluster.address, "10.0.0.1:9092");

            Ok(())
        });
    }

    /// A raw topic missing a required field must be rejected at config time with
    /// a clear error, not panic later when the consumer builds its deserializer.
    #[test]
    fn test_raw_mode_missing_field_rejected_cleanly() {
        Jail::expect_with(|jail| {
            // raw is missing `namespace` (application/taskname/deadline present).
            jail.create_file(
                "config.yaml",
                r#"
kafka_deadletter_topic: profiles-dlq
kafka_retry_topic: profiles-retry

worker_map:
  profiles: http://worker-profiles:50052

kafka_topics:
  profiles:
    cluster: profiles-cluster
    consumer_group: taskbroker-profiles
    raw:
      application: profiles
      taskname: profiles.process
      processing_deadline_duration: 30
  profiles-retry:
    cluster: profiles-cluster
    consumer_group: taskbroker-profiles-retry
    produce_only: true
  profiles-dlq:
    cluster: profiles-cluster
    consumer_group: taskbroker-profiles-dlq
    produce_only: true

kafka_clusters:
  profiles-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            // A clean error, not a panic.
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string().contains("raw.namespace"),
                "expected a clean missing-field error, got: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_multi_topic_config_from_env() {
        Jail::expect_with(|jail| {
            // Note: figment lowercases env var keys after splitting on "__",
            // so MY_CLUSTER becomes my_cluster (with underscore, not hyphen).
            // The cluster reference value is preserved as-is.
            jail.set_env("TASKBROKER_KAFKA_TOPICS__PROFILES__CLUSTER", "my_cluster");
            jail.set_env(
                "TASKBROKER_KAFKA_TOPICS__PROFILES__CONSUMER_GROUP",
                "taskbroker-profiles",
            );
            // The deadletter topic must be a declared topic. The key segment is
            // lowercased to "profiles_dlq", so kafka_deadletter_topic must match.
            jail.set_env("TASKBROKER_KAFKA_DEADLETTER_TOPIC", "profiles_dlq");
            jail.set_env(
                "TASKBROKER_KAFKA_TOPICS__PROFILES_DLQ__CLUSTER",
                "my_cluster",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_TOPICS__PROFILES_DLQ__CONSUMER_GROUP",
                "taskbroker-profiles-dlq",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_TOPICS__PROFILES_DLQ__PRODUCE_ONLY",
                "true",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_CLUSTERS__MY_CLUSTER__ADDRESS",
                "10.0.0.2:9092",
            );

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).unwrap();

            let topics = &config.kafka_topics;
            assert_eq!(topics.len(), 2);

            let profiles = topics.get("profiles").unwrap();
            assert_eq!(profiles.cluster, "my_cluster");
            assert_eq!(profiles.consumer_group, "taskbroker-profiles");
            assert!(topics.get("profiles_dlq").unwrap().produce_only);

            let clusters = &config.kafka_clusters;
            assert_eq!(clusters.len(), 1);
            assert_eq!(clusters.get("my_cluster").unwrap().address, "10.0.0.2:9092");
            assert!(!clusters.contains_key("default"));

            // Test consumable_topic() helper
            let (topic_name, _) = config.consumable_topics().unwrap()[0];
            assert_eq!(topic_name, "profiles");

            Ok(())
        });
    }

    #[test]
    fn test_rejects_mixing_legacy_and_new_format() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
kafka_topic: legacy-topic
kafka_cluster: 127.0.0.1:9092

kafka_topics:
  profiles:
    cluster: my-cluster
    consumer_group: taskbroker-profiles

kafka_clusters:
  my-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string().contains("cannot mix"),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_multi_topic_requires_clusters() {
        // kafka_topics without any kafka_clusters is a misconfiguration.
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
kafka_topics:
  profiles:
    cluster: my-cluster
    consumer_group: taskbroker-profiles
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string().contains("kafka_clusters is empty"),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_multi_topic_validates_cluster_references() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
kafka_topics:
  profiles:
    cluster: nonexistent-cluster
    consumer_group: taskbroker-profiles

kafka_clusters:
  other-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string().contains("unknown cluster"),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_multi_topic_rejected_on_postgres() {
        Jail::expect_with(|jail| {
            // Multiple consumable topics are allowed on sqlite but rejected on
            // postgres, whose claim filtering can't distinguish partitions
            // across topics.
            jail.create_file(
                "config.yaml",
                r#"
database_adapter: postgres
kafka_deadletter_topic: tasks-dlq
kafka_retry_topic: tasks-retry

kafka_topics:
  profiles:
    cluster: my-cluster
    consumer_group: taskbroker-profiles
  subscriptions:
    cluster: my-cluster
    consumer_group: taskbroker-subscriptions
  tasks-retry:
    cluster: my-cluster
    consumer_group: taskbroker-retry
    produce_only: true
  tasks-dlq:
    cluster: my-cluster
    consumer_group: taskbroker-dlq
    produce_only: true

kafka_clusters:
  my-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string()
                    .contains("not supported with the postgres database adapter"),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_multi_topic_allows_one_consumable_with_produce_only() {
        Jail::expect_with(|jail| {
            // One consumable topic (profiles), two produce-only (retry + dlq).
            jail.create_file(
                "config.yaml",
                r#"
kafka_deadletter_topic: profiles-dlq

kafka_topics:
  profiles:
    cluster: my-cluster
    consumer_group: taskbroker-profiles
  profiles-retry:
    cluster: my-cluster
    consumer_group: taskbroker-profiles-retry
    produce_only: true
  profiles-dlq:
    cluster: my-cluster
    consumer_group: taskbroker-profiles-dlq
    produce_only: true

kafka_clusters:
  my-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let config = Config::from_args(&args).unwrap();

            let topics = &config.kafka_topics;
            assert_eq!(topics.len(), 3);

            // One consumable, two produce-only
            assert!(!topics.get("profiles").unwrap().produce_only);
            assert!(topics.get("profiles-retry").unwrap().produce_only);
            assert!(topics.get("profiles-dlq").unwrap().produce_only);

            // consumable_topic() returns the one consumable topic
            let (topic_name, _) = config.consumable_topics().unwrap()[0];
            assert_eq!(topic_name, "profiles");

            Ok(())
        });
    }

    #[test]
    fn test_multi_topic_rejects_zero_consumable_topics() {
        Jail::expect_with(|jail| {
            // All topics are produce-only - should fail
            jail.create_file(
                "config.yaml",
                r#"
kafka_topics:
  profiles:
    cluster: my-cluster
    consumer_group: taskbroker-profiles
    produce_only: true

kafka_clusters:
  my-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string().contains("no consumable topic"),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_multi_topic_requires_deadletter_topic() {
        // In the new format the deadletter topic must be declared in kafka_topics.
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
kafka_topics:
  profiles:
    cluster: my-cluster
    consumer_group: taskbroker-profiles

kafka_clusters:
  my-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string()
                    .contains("kafka_deadletter_topic 'taskworker-dlq' is not defined"),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_legacy_rejects_deadletter_topic_equal_to_main_topic() {
        // Topics are keyed by name, so a deadletter topic that shares the main
        // topic's name cannot carry its own (deadletter) cluster. Rather than
        // silently route deadletter messages to the consumed topic's cluster,
        // normalization must reject the collision.
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_KAFKA_TOPIC", "taskworker");
            jail.set_env("TASKBROKER_KAFKA_DEADLETTER_TOPIC", "taskworker");

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string().contains(
                    "kafka_deadletter_topic 'taskworker' must differ from the consumed topic \
                     'taskworker'"
                ),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_legacy_rejects_retry_topic_equal_to_deadletter_topic() {
        // The retry topic may alias the main topic, but not the deadletter
        // topic: that collision would silently give the retry topic the
        // deadletter cluster/role.
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_KAFKA_RETRY_TOPIC", "taskworker-dlq");
            jail.set_env("TASKBROKER_KAFKA_DEADLETTER_TOPIC", "taskworker-dlq");

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string().contains(
                    "kafka_retry_topic 'taskworker-dlq' must differ from kafka_deadletter_topic"
                ),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_per_topic_consumer_settings_override_globals() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
kafka_deadletter_topic: profiles-dlq
kafka_session_timeout_ms: 6000
kafka_auto_commit_interval_ms: 5000
kafka_auto_offset_reset: latest

kafka_topics:
  profiles:
    cluster: my-cluster
    consumer_group: taskbroker-profiles
    session_timeout_ms: 12000
    auto_commit_interval_ms: 1000
    auto_offset_reset: earliest
  profiles-dlq:
    cluster: my-cluster
    consumer_group: taskbroker-profiles-dlq
    produce_only: true

kafka_clusters:
  my-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let config = Config::from_args(&args).unwrap();
            let consumer_config = config.kafka_consumer_config_for("profiles");

            // Per-topic values win over the globals.
            assert_eq!(consumer_config.get("session.timeout.ms").unwrap(), "12000");
            assert_eq!(
                consumer_config.get("auto.commit.interval.ms").unwrap(),
                "1000"
            );
            assert_eq!(
                consumer_config.get("auto.offset.reset").unwrap(),
                "earliest"
            );

            Ok(())
        });
    }

    #[test]
    fn test_topic_consumer_settings_fall_back_to_globals() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
kafka_deadletter_topic: profiles-dlq
kafka_session_timeout_ms: 7000
kafka_auto_commit_interval_ms: 2000
kafka_auto_offset_reset: earliest

kafka_topics:
  profiles:
    cluster: my-cluster
    consumer_group: taskbroker-profiles
  profiles-dlq:
    cluster: my-cluster
    consumer_group: taskbroker-profiles-dlq
    produce_only: true

kafka_clusters:
  my-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let config = Config::from_args(&args).unwrap();
            let consumer_config = config.kafka_consumer_config_for("profiles");

            // No per-topic overrides, so the globals are used.
            assert_eq!(consumer_config.get("session.timeout.ms").unwrap(), "7000");
            assert_eq!(
                consumer_config.get("auto.commit.interval.ms").unwrap(),
                "2000"
            );
            assert_eq!(
                consumer_config.get("auto.offset.reset").unwrap(),
                "earliest"
            );

            Ok(())
        });
    }

    #[test]
    fn test_new_format_requires_retry_topic_to_be_declared() {
        // In the new format kafka_retry_topic must be declared in kafka_topics
        // so its cluster is known; otherwise retries would silently use the
        // deadletter producer's cluster.
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
kafka_deadletter_topic: profiles-dlq
kafka_retry_topic: profiles-retry

kafka_topics:
  profiles:
    cluster: my-cluster
    consumer_group: taskbroker-profiles
  profiles-dlq:
    cluster: my-cluster
    consumer_group: taskbroker-profiles-dlq
    produce_only: true

kafka_clusters:
  my-cluster:
    address: 10.0.0.1:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string()
                    .contains("kafka_retry_topic 'profiles-retry' is not defined in kafka_topics"),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_rejects_deadletter_topic_on_different_cluster() {
        // The upkeep producer is shared between deadletter publishing and retry
        // publishing (to the consumed topic), and a single producer can only
        // reach one cluster. Putting the deadletter topic on a different cluster
        // than the consumed topic would misroute retries, so it's rejected.
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
kafka_deadletter_topic: profiles-dlq

kafka_topics:
  profiles:
    cluster: main-cluster
    consumer_group: taskbroker-profiles
  profiles-dlq:
    cluster: dlq-cluster
    consumer_group: taskbroker-profiles-dlq
    produce_only: true

kafka_clusters:
  main-cluster:
    address: 10.0.0.1:9092
  dlq-cluster:
    address: 10.9.9.9:9092
"#,
            )?;

            let args = Args {
                run: Run::Broker,
                config: Some("config.yaml".to_owned()),
            };
            let err = Config::from_args(&args).unwrap_err();
            assert!(
                err.to_string()
                    .contains("they share a single producer and must be on the same cluster"),
                "unexpected error: {}",
                err
            );

            Ok(())
        });
    }

    #[test]
    fn test_legacy_retry_topic_uses_deadletter_cluster() {
        // Reproduces the `ingest-profiles-raw` pool: the main consumer topic
        // lives on a different cluster than the retry+DLQ topics. The retry
        // producer is the upkeep/deadletter producer, so a distinct legacy
        // retry topic must be registered on the deadletter cluster, not the
        // main consumer cluster. Otherwise the same-cluster validation compares
        // the retry topic against the wrong cluster and rejects this config.
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_KAFKA_CLUSTER", "kafka-profiles:9092");
            jail.set_env("TASKBROKER_KAFKA_TOPIC", "profiles");
            jail.set_env("TASKBROKER_KAFKA_RETRY_TOPIC", "taskworker-ingest");
            jail.set_env("TASKBROKER_KAFKA_DEADLETTER_TOPIC", "taskworker-ingest-dlq");
            jail.set_env("TASKBROKER_KAFKA_DEADLETTER_CLUSTER", "kafka-small:9092");

            let args = Args {
                run: Run::Broker,
                config: None,
            };
            let config = Config::from_args(&args).expect("legacy retry config should validate");

            // The retry topic resolves to the deadletter cluster (where the
            // upkeep producer actually publishes), not the main consumer cluster.
            let retry_topic = config
                .kafka_topics
                .get("taskworker-ingest")
                .expect("retry topic registered");
            assert!(retry_topic.produce_only);
            assert_eq!(
                config.cluster(&retry_topic.cluster).unwrap().address,
                "kafka-small:9092"
            );
            // And it matches the producer's cluster.
            assert_eq!(config.kafka_producer_cluster().address, "kafka-small:9092");

            Ok(())
        });
    }
}
