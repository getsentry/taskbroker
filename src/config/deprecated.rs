use crate::config::store::DatabaseAdapter;
use serde::{Deserialize, Serialize};

/// Simplifies mapping deprecated configuration options to new options. Suppose the outgoing field
/// is `self.deprecated.foo` and the new version of that field is `self.config.bar`. You can use
/// this macro in the following way.
///
/// ```rs
/// let provided = |key: &str| {
///   builder
///     .find_metadata(key)
///     .is_some_and(|metadata| metadata.name != DEFAULT_CONFIG_PROVIDER)
/// };
///
/// map! {
///   self.deprecated.foo => self.config.bar if provided
/// };
/// ```
///
/// You can also apply a transformation, like turning the value into a `Duration` or an `Option`.
///
/// ```rs
/// fn duration<T: Into<u64>>(v: T) -> Duration {
///   Duration::from_millis(v.into())
/// }
///
/// fn optional<T>(v: T) -> Option<T> {
///   Some(v)
/// }
///
/// deprecated::map! {
///   self.deprecated.foo             => self.config.bar if provided,
///   self.deprecated.zap as duration => self.config.zip if provided
///   self.depreacted.uh as optional  => self.config.uh if provided
/// };
/// ```
macro_rules! map {
    () => {};

    // Two or more transformed mappings separated by commas
    (
        $deprecated_base:ident $(.$deprecated_field:ident)+ as $mapper:path => $current_base:ident $(.$current_field:ident)+ if $provided:ident,
        $($rest:tt)+
    ) => {
        crate::config::deprecated::map! {
            $deprecated_base$(.$deprecated_field)+ as $mapper => $current_base$(.$current_field)+ if $provided
        };

        crate::config::deprecated::map! {
            $($rest)+
        };
    };

    // Two or more mappings separated by commas
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

    // Syntax is "deprecated.field as something => new.field if provided"
    ($deprecated_base:ident $(.$deprecated_field:ident)+ as $mapper:path => $current_base:ident $(.$current_field:ident)+ if $provided:ident) => {
        let key = concat!(stringify!($current_base), $(".", stringify!($current_field)),+)
            .strip_prefix("self.")
            .unwrap();

        if !$provided(key) {
            if let Some(v) = $deprecated_base$(.$deprecated_field)+.take() {
                $current_base$(.$current_field)+ = $mapper(v);
            }
        }
    };

    // Syntax is "deprecated.field => new.field if provided"
    ($deprecated:expr => $current_base:ident $(.$current_field:ident)+ if $provided:ident) => {
        let key = concat!(stringify!($current_base), $(".", stringify!($current_field)),+)
            .strip_prefix("self.")
            .unwrap();

        if !$provided(key) {
            if let Some(v) = $deprecated.take() {
                $current_base$(.$current_field)+ = v;
            }
        }
    };
}

pub(crate) use map;

/// We must support two versions of the configuration - the "desired" or "current" version,
/// and the "deprecated" or "outgoing" version. Outgoing configuration options should be
/// moved here and then mapped to their new counterparts, if appropriate.
#[derive(PartialEq, Debug, Deserialize, Serialize, Default)]
pub struct DeprecatedConfig {
    /// The Kafka topic to fetch activations from. Defaults to `None`, but set
    /// to "taskworker" during normalization.
    pub kafka_topic: Option<String>,

    /// Comma separated list of Kafka brokers to use. Defaults to `None`, but set
    /// to "127.0.0.1:9092" during normalization.
    pub kafka_cluster: Option<String>,

    /// The Kafka consumer group name. Defaults to `None`, but set
    /// to "taskworker" during normalization.
    pub kafka_consumer_group: Option<String>,

    /// The security method used for authentication (like `sasl_plaintext`).
    pub kafka_security_protocol: Option<String>,

    /// The hashing algorithm used for authentication (like `scram-sha-256`).
    pub kafka_sasl_mechanism: Option<String>,

    /// The SASL username for ingesting messages.
    pub kafka_sasl_username: Option<String>,

    /// The SASL password for ingesting messages.
    pub kafka_sasl_password: Option<String>,

    /// The location to the CA certificate file.
    pub kafka_ssl_ca_location: Option<String>,

    /// The location to the certificate file.
    pub kafka_ssl_certificate_location: Option<String>,

    /// The location to the private key file.
    pub kafka_ssl_key_location: Option<String>,

    /// Comma separated list of Kafka brokers to publish dead letter messages on.
    pub kafka_deadletter_cluster: Option<String>,

    /// The security method used for authentication to the DLQ (like `sasl_plaintext`).
    pub kafka_deadletter_security_protocol: Option<String>,

    /// The hashing algorithm used for authentication to the DLQ (like `scram-sha-256`).
    pub kafka_deadletter_sasl_mechanism: Option<String>,

    /// The SASL username for DLQ publishing.
    pub kafka_deadletter_sasl_username: Option<String>,

    /// The SASL password for DLQ publishing.
    pub kafka_deadletter_sasl_password: Option<String>,

    /// The location to the DLQ CA certificate file.
    pub kafka_deadletter_ssl_ca_location: Option<String>,

    /// The location to the DLQ certificate file.
    pub kafka_deadletter_ssl_certificate_location: Option<String>,

    /// The location to the DLQ private key file.
    pub kafka_deadletter_ssl_key_location: Option<String>,

    /// Enable raw mode for consuming unstructured Kafka messages.
    /// In raw mode, Kafka message bytes are wrapped into `TaskActivation`s.
    pub raw_mode: Option<bool>,

    /// The database adapter to use for the activation store.
    pub database_adapter: Option<DatabaseAdapter>,

    /// Whether to run the migrations on the database.
    /// This is only used by the Postgres database adapter.
    pub run_migrations: Option<bool>,

    /// The host of the Postgres database to use for the activation store.
    pub pg_host: Option<String>,

    /// The port of the Postgres database to use for the activation store.
    pub pg_port: Option<u16>,

    // User permitted to run DDL operations on the Postgres database.
    pub pg_ddl_username: Option<String>,

    /// Password for the user permitted to run DDL operations on the Postgres database.
    pub pg_ddl_password: Option<String>,

    /// User for the Postgres database to use for the activation store.
    pub pg_username: Option<String>,

    /// Password for the Postgres database to use for the activation store.
    pub pg_password: Option<String>,

    /// The name of the Postgres database to use for the activation store.
    pub pg_database_name: Option<String>,

    /// The default Postgres database to use for migrations.
    pub pg_default_database_name: Option<String>,

    /// Extra query parameters that can be added to the Postgres connection string.
    /// Should be in the format of `key=value&key2=value2`.
    /// For example, `sslmode=require&sslrootcert=/path/to/root.crt`.
    pub pg_extra_query_params: Option<String>,

    /// The path to the SQLite database.
    pub db_path: Option<String>,

    /// The amount of time to wait before retrying failed DB writes (from the consumer).
    pub db_write_failure_backoff_ms: Option<u64>,

    /// The maximum number of times to retry a transient database error before surfacing the error.
    /// When `None`, queries are not retried.
    pub db_query_max_retries: Option<u32>,

    /// The delay in milliseconds between query retry attempts.
    pub db_query_retry_delay_ms: Option<u64>,

    /// The maximum number of tasks that are buffered before being written to the activation store.
    pub db_insert_batch_max_len: Option<usize>,

    /// The maximum number of bytes that are buffered before being written to the activation store.
    pub db_insert_batch_max_size: Option<usize>,

    /// The time in milliseconds to buffer tasks before being written to the activation store.
    pub db_insert_batch_max_time_ms: Option<u64>,

    /// The maximum size of the database in bytes. If the database reaches or exceeds this size,
    /// ingestion will pause until the database size is reduced.
    pub db_max_size: Option<u64>,

    /// The maximum number of pending activations that can be in the activation store.
    pub max_pending_count: Option<usize>,

    /// The maximum number of delayed activations that can be in the activation store.
    pub max_delay_count: Option<usize>,

    /// The maximum number of processing activations that can be in the activation store.
    pub max_processing_count: Option<usize>,

    /// The maximum number of times a task can be reset from processing back to pending.
    /// When this limit is reached, the activation will be discarded or deadlettered.
    pub max_processing_attempts: Option<usize>,

    /// The number of additional seconds that processing deadlines are extended by. This helps reduce
    /// broker deadline resets when brokers are under load, or there are small networking delays.
    pub processing_deadline_grace_sec: Option<u64>,

    /// The number of pages to vacuum from SQLite when vacuum is run.
    /// If `None`, all pages will be vacuumed.
    pub vacuum_page_count: Option<usize>,

    /// Enable additional SQLite metrics.
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

    /// The size of a batch of claimed → processing updates.
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
