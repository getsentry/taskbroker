use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::Config;
use crate::config::deserialize;
use crate::config::serialize;
use crate::store::adapters::postgres;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseAdapter {
    /// SQLite database adapter
    Sqlite,

    /// PostgreSQL database adapter
    Postgres,
}

impl DatabaseAdapter {
    pub async fn migrate(&self, config: &Config) -> Result<()> {
        match self {
            Self::Postgres => postgres::migrate(config).await,
            Self::Sqlite => {
                warn!("Standalone migration not supported for SQLite");
                Ok(())
            }
        }
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct PgConfig {
    /// Whether to run the migrations on the database.
    /// This is only used by the postgres database adapter, since
    /// in production the migrations shouldn't be run by the taskbroker.
    pub run_migrations: bool,

    /// The host of the postgres database to use for the activation store.
    pub host: String,

    /// The port of the postgres database to use for the activation store.
    pub port: u16,

    // User permitted to run DDL operations.
    pub ddl_username: String,

    /// The username of the postgres database to use for the activation store.
    pub username: String,

    /// The password of the postgres database to use for the activation store.
    pub password: String,

    /// Password for the user permitted to run DDL operations.
    pub ddl_password: String,

    /// The name of the postgres database to use for the activation store.
    pub database_name: String,

    /// The default postgres database to use for migrations..
    pub default_database_name: String,

    /// Extra query parameters that can be added to the postgres connection string. Should be in the format of "key=value&key2=value2".
    /// For example, "sslmode=require&sslrootcert=/path/to/root.crt".
    pub query_params: Option<String>,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            run_migrations: false,
            host: "sentry-postgres-1".to_owned(),
            port: 5432,
            ddl_username: "postgres".to_owned(),
            ddl_password: "password".to_owned(),
            username: "postgres".to_owned(),
            password: "password".to_owned(),
            database_name: "default".to_owned(),
            default_database_name: "postgres".to_owned(),
            query_params: None,
        }
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct SqliteConfig {
    /// The path to the sqlite database
    pub path: String,

    /// Enable additional metrics for the sqlite.
    pub enable_status_metrics: bool,

    /// The number of pages to vacuum from SQLite when vacuum is run.
    /// If None, all pages will be vacuumed.
    pub vacuum_page_count: Option<usize>,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: "./taskbroker-inflight.sqlite".to_owned(),
            vacuum_page_count: None,
            enable_status_metrics: true,
        }
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct StoreConfig {
    /// The database adapter to use for the activation store.
    pub database_adapter: DatabaseAdapter,

    /// Postgres configuration.
    pub pg: PgConfig,

    /// SQLite configuration.
    pub sqlite: SqliteConfig,

    /// The amount of time to wait before retrying writes to db when write fails.
    pub db_write_failure_backoff_ms: u64,

    /// The maximum number of times to retry a transient database query error
    /// before surfacing the error. When zero, queries are not retried.
    pub db_query_max_retries: u32,

    /// The delay between query retry attempts.
    #[serde(with = "crate::serde::duration")]
    pub db_query_retry_delay: Duration,

    /// The maximum number of tasks that are buffered
    /// before being written to ActivationStore (sqlite).
    pub db_insert_batch_max_len: usize,

    /// The maximum number of bytes that are buffered
    /// before being written to ActivationStore (sqlite).
    pub db_insert_batch_max_size: usize,

    /// The time in milliseconds to buffer tasks
    /// before being written to ActivationStore (sqlite).
    pub db_insert_batch_max_time_ms: u64,

    /// The maximum size of the sqlite database in bytes.
    /// If the database reaches or exceeds this size, ingestion will
    /// pause until the database size is reduced.
    pub db_max_size: Option<u64>,

    /// The maximum number of pending records that can be
    /// in the ActivationStore (sqlite)
    pub max_pending_count: usize,

    /// The maximum number of delay records that can be
    /// in the ActivationStore (sqlite)
    pub max_delay_count: usize,

    /// The maximum number of processing records that can be
    /// in the ActivationStore (sqlite)
    pub max_processing_count: usize,

    /// The maximum number of times a task can be reset from
    /// processing back to pending. When this limit is reached,
    /// the activation will be discarded/deadlettered.
    pub max_processing_attempts: usize,

    /// The number of additional seconds that processing deadlines
    /// are extended by. This helps reduce broker deadline resets when
    /// brokers are under load, or there are small networking delays.
    pub processing_deadline_grace_sec: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            database_adapter: DatabaseAdapter::Sqlite,
            pg: PgConfig::default(),
            sqlite: SqliteConfig::default(),
            db_write_failure_backoff_ms: 4000,
            db_query_max_retries: 3,
            db_query_retry_delay: Duration::from_millis(100),
            db_insert_batch_max_len: 256,
            db_insert_batch_max_size: 16_000_000,
            db_insert_batch_max_time_ms: 1000,
            db_max_size: Some(3000000000),
            max_pending_count: 2048,
            max_delay_count: 8192,
            max_processing_count: 2048,
            max_processing_attempts: 5,
            processing_deadline_grace_sec: 3,
        }
    }
}
