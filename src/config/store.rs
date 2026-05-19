use serde::{Deserialize, Serialize};

/// Which specific database should we use?
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseAdapter {
    Sqlite,
    Postgres,
}

/// Configuration options specific to Postgres.
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct PostgresConfig {
    /// What host should we use to connect?
    pub host: String,

    /// What port should we use to connect?
    pub port: u16,

    /// Username for the database that stores activations.
    pub username: String,

    /// Password for the database that stores activations.
    pub password: String,

    /// Name of the database that stores activations.
    pub database_name: String,

    /// Name of the database to use for migrations.
    pub default_database_name: String,

    /// Whether to run the migrations on the database.
    pub run_migrations: bool,

    /// Query parameters added to the connection string. Should be in the format of "key=value&key2=value2".
    /// For example, "sslmode=require&sslrootcert=/path/to/root.crt".
    pub options: String,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "sentry-postgres-1".to_owned(),
            port: 5432,
            username: "postgres".to_owned(),
            password: "password".to_owned(),
            database_name: "default".to_owned(),
            default_database_name: "postgres".to_owned(),
            run_migrations: false,
            options: "".to_owned(),
        }
    }
}

/// Configuration options specific to SQLite.
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct SqliteConfig {
    /// The path to the database.
    pub path: String,

    /// Enable additional metrics.
    pub enable_status_metrics: bool,

    /// The number of pages to vacuum from SQLite when vacuum is run.
    /// If None, all pages will be vacuumed.
    pub vacuum_page_count: Option<usize>,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: "./taskbroker-inflight.sqlite".to_owned(),
            enable_status_metrics: true,
            vacuum_page_count: None,
        }
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct StoreConfig {
    /// Which specific database should we use?
    pub database: DatabaseAdapter,

    /// Configuration options specific to SQLite.
    pub sqlite: SqliteConfig,

    /// Configuration options specific to Postgres.
    pub pg: PostgresConfig,

    /// The amount of time to wait before retrying failed activation insertions.
    pub insert_failure_backoff_ms: u64,

    /// Maximum number of tasks to buffer before writing them to the store.
    pub insert_batch_length: usize,

    /// Maximum number of bytes to buffer before writing them to the store.
    pub insert_batch_size: usize,

    /// Maximum number of milliseconds to buffer tasks before writing them to the store.
    pub insert_batch_interval_ms: u64,

    /// Maximum size of the database in bytes. If the database reaches or exceeds this
    /// size, ingestion will pause until database size decreases.
    pub max_size: Option<u64>,

    /// Maximum number of pending records that can be in the store.
    pub max_pending_count: usize,

    /// Maximum number of delay records that can be in the store.
    pub max_delay_count: usize,

    /// Maximum number of processing records that can be in the store.
    pub max_processing_count: usize,

    /// The number of additional seconds that processing deadlines are extended by. This helps
    /// reduce deadline resets when brokers are under load, or there are small networking delays.
    pub processing_deadline_grace_sec: u64,

    /// How long a claim is valid for. Note that this option should NOT be set by the user, but
    /// rather calculated using various configuration options. It's only here for ergonomics.
    pub claim_lease_ms: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            database: DatabaseAdapter::Sqlite,
            sqlite: SqliteConfig::default(),
            pg: PostgresConfig::default(),
            insert_failure_backoff_ms: 4000,
            insert_batch_length: 256,
            insert_batch_size: 16_000_000,
            insert_batch_interval_ms: 1000,
            max_size: Some(3000000000),
            max_pending_count: 2048,
            max_delay_count: 8192,
            max_processing_count: 2048,
            processing_deadline_grace_sec: 3,
            claim_lease_ms: 0, // TODO
        }
    }
}
