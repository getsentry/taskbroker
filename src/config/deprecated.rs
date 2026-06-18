use serde::{Deserialize, Serialize};

use crate::config::store::DatabaseAdapter;

macro_rules! map {
    () => {};

    // Two or more optional mappings separated by commans
    (
        $deprecated:expr => some($current_base:ident $(.$current_field:ident)+) if $provided:ident,
        $($rest:tt)+
    ) => {
        crate::config::deprecated::map! {
            $deprecated => some($current_base$(.$current_field)+) if $provided
        };

        crate::config::deprecated::map! {
            $($rest)+
        };
    };

    // Two or more mappings separated by commans
    (
        $deprecated:expr => $current_base:ident $(.$current_field:ident)+ if $provided:ident,
        $($rest:tt)+
    ) => {
        crate::config::deprecated::map! {
            $deprecated => $current_base$(.$current_field)+ if $provided
        };

        crate::config::deprecated::map! {
            $($rest)+
        };
    };

    // An optional deprecated value always wins
    ($deprecated:expr => some($current_base:ident $(.$current_field:ident)+) if $provided:ident) => {
        let key = stringify!($current_base$(.$current_field)+)
            .strip_prefix("self.")
            .unwrap_or(stringify!($current_base$(.$current_field)+));

        if !$provided(key) {
            $current_base$(.$current_field)+ = $deprecated.take();
        }
    };

    // A plain deprecated value only wins when it's provided
    ($deprecated:expr => $current_base:ident $(.$current_field:ident)+ if $provided:ident) => {
        let key = stringify!($current_base$(.$current_field)+)
            .strip_prefix("self.")
            .unwrap_or(stringify!($current_base$(.$current_field)+));

        if !$provided(key) {
            if let Some(v) = $deprecated.take() {
                $current_base$(.$current_field)+ = v;
            }
        }
    };
}

pub(crate) use map;

#[derive(PartialEq, Debug, Deserialize, Serialize, Default)]
pub struct DeprecatedConfig {
    /// The topic to fetch task messages from.
    /// Deprecated: use kafka_topics instead. Mutually exclusive with the new
    /// format; defaults to None (the historical "taskworker" default applies).
    pub kafka_topic: Option<String>,

    /// Comma separated list of kafka brokers to connect to.
    /// Deprecated: use kafka_clusters instead. Mutually exclusive with the new
    /// format; defaults to None (the historical "127.0.0.1:9092" default is
    /// applied during normalization when no kafka config is provided at all).
    pub kafka_cluster: Option<String>,

    /// The kafka consumer group name.
    /// Deprecated: use kafka_topics instead. Mutually exclusive with the new
    /// format; defaults to None (the historical "taskworker" default applies).
    pub kafka_consumer_group: Option<String>,

    /// The security method used for authentication eg. sasl_plaintext.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_security_protocol: Option<String>,

    /// The hashing algorithm used for authentication eg. scram-sha-256.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_sasl_mechanism: Option<String>,

    /// The sasl username for ingesting messages.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_sasl_username: Option<String>,

    /// The sasl password for ingesting messages.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_sasl_password: Option<String>,

    /// The location to the CA certificate file.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_ssl_ca_location: Option<String>,

    /// The location to the certificate file.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_ssl_certificate_location: Option<String>,

    /// The location to the private key file.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_ssl_key_location: Option<String>,

    /// Comma separated list of kafka brokers to publish dead letter messages on.
    /// Deprecated: declare the deadletter topic in kafka_topics (produce_only)
    /// with a cluster reference instead.
    pub kafka_deadletter_cluster: Option<String>,

    /// The security method used for authentication to the DLQ eg. sasl_plaintext.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_security_protocol: Option<String>,

    /// The hashing algorithm used for authentication to the DLQ eg. scram-sha-256.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_sasl_mechanism: Option<String>,

    /// The sasl username for DLQ publishing.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_sasl_username: Option<String>,

    /// The sasl password for DLQ publishing.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_sasl_password: Option<String>,

    /// The location to the DLQ CA certificate file.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_ssl_ca_location: Option<String>,

    /// The location to the DLQ certificate file.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_ssl_certificate_location: Option<String>,

    /// The location to the DLQ private key file.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_ssl_key_location: Option<String>,

    /// Enable raw mode for consuming unstructured Kafka messages.
    /// In raw mode, Kafka message bytes are wrapped into TaskActivation.
    pub raw_mode: Option<bool>,

    /// The database adapter to use for the activation store.
    pub database_adapter: Option<DatabaseAdapter>,

    /// Whether to run the migrations on the database.
    /// This is only used by the postgres database adapter, since
    /// in production the migrations shouldn't be run by the taskbroker.
    pub run_migrations: Option<bool>,

    /// The host of the postgres database to use for the activation store.
    pub pg_host: Option<String>,

    /// The port of the postgres database to use for the activation store.
    pub pg_port: Option<u16>,

    // User permitted to run DDL operations.
    pub pg_ddl_username: Option<String>,

    /// The username of the postgres database to use for the activation store.
    pub pg_username: Option<String>,

    /// The password of the postgres database to use for the activation store.
    pub pg_password: Option<String>,

    /// Password for the user permitted to run DDL operations.
    pub pg_ddl_password: Option<String>,

    /// The name of the postgres database to use for the activation store.
    pub pg_database_name: Option<String>,

    /// The default postgres database to use for migrations..
    pub pg_default_database_name: Option<String>,

    /// Extra query parameters that can be added to the postgres connection string. Should be in the format of "key=value&key2=value2".
    /// For example, "sslmode=require&sslrootcert=/path/to/root.crt".
    pub pg_extra_query_params: Option<String>,

    /// The path to the sqlite database
    pub db_path: Option<String>,

    /// The amount of time to wait before retrying writes to db when write fails.
    pub db_write_failure_backoff_ms: Option<u64>,

    /// The maximum number of times to retry a transient database query error
    /// before surfacing the error. When None, queries are not retried.
    pub db_query_max_retries: Option<u32>,

    /// The delay in milliseconds between query retry attempts.
    pub db_query_retry_delay_ms: Option<u64>,

    /// The maximum number of tasks that are buffered
    /// before being written to ActivationStore (sqlite).
    pub db_insert_batch_max_len: Option<usize>,

    /// The maximum number of bytes that are buffered
    /// before being written to ActivationStore (sqlite).
    pub db_insert_batch_max_size: Option<usize>,

    /// The time in milliseconds to buffer tasks
    /// before being written to ActivationStore (sqlite).
    pub db_insert_batch_max_time_ms: Option<u64>,

    /// The maximum size of the sqlite database in bytes.
    /// If the database reaches or exceeds this size, ingestion will
    /// pause until the database size is reduced.
    pub db_max_size: Option<u64>,

    /// The maximum number of pending records that can be
    /// in the ActivationStore (sqlite)
    pub max_pending_count: Option<usize>,

    /// The maximum number of delay records that can be
    /// in the ActivationStore (sqlite)
    pub max_delay_count: Option<usize>,

    /// The maximum number of processing records that can be
    /// in the ActivationStore (sqlite)
    pub max_processing_count: Option<usize>,

    /// The maximum number of times a task can be reset from
    /// processing back to pending. When this limit is reached,
    /// the activation will be discarded/deadlettered.
    pub max_processing_attempts: Option<usize>,

    /// The number of additional seconds that processing deadlines
    /// are extended by. This helps reduce broker deadline resets when
    /// brokers are under load, or there are small networking delays.
    pub processing_deadline_grace_sec: Option<u64>,

    /// The number of pages to vacuum from sqlite when vacuum is run.
    /// If None, all pages will be vacuumed.
    pub vacuum_page_count: Option<usize>,

    /// Enable additional metrics for the sqlite.
    pub enable_sqlite_status_metrics: Option<bool>,

    /// The number of concurrent push threads to run.
    pub push_threads: Option<usize>,

    /// Maximum time in milliseconds for a single push RPC to the worker service. This should be greater than the worker's internal timeout.
    pub push_timeout_ms: Option<u64>,

    /// The size of the push queue.
    pub push_queue_size: Option<usize>,

    /// Maximum time in milliseconds to wait when submitting an activation to the push pool.
    pub push_queue_timeout_ms: Option<u64>,

    /// Update claimed → processing updates in batches? Only applies in PUSH mode.
    pub batch_push_updates: Option<bool>,

    /// The size of a batch of dispatch updates.
    pub push_update_batch_size: Option<usize>,

    /// Maximum milliseconds to wait before flushing a batch of dispatch updates.
    pub push_update_interval_ms: Option<u32>,

    /// The number of concurrent fetch loops in push mode, which should be ≤ `MAX_FETCH_THREADS` and a power of two.
    pub fetch_threads: Option<usize>,

    /// Time in milliseconds to wait between fetch attempts when no pending activation is found.
    pub fetch_wait_ms: Option<u64>,

    /// The number of activations to claim with a single fetch query.
    pub fetch_batch_size: Option<i32>,
}
