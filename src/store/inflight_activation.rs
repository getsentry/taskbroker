use anyhow::{Error, anyhow};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use libsqlite3_sys::{
    SQLITE_DBSTATUS_CACHE_HIT, SQLITE_DBSTATUS_CACHE_MISS, SQLITE_DBSTATUS_CACHE_SPILL,
    SQLITE_DBSTATUS_CACHE_USED, SQLITE_DBSTATUS_CACHE_USED_SHARED, SQLITE_DBSTATUS_CACHE_WRITE,
    SQLITE_DBSTATUS_DEFERRED_FKS, SQLITE_DBSTATUS_LOOKASIDE_HIT,
    SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL, SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE,
    SQLITE_DBSTATUS_LOOKASIDE_USED, SQLITE_DBSTATUS_SCHEMA_USED, SQLITE_DBSTATUS_STMT_USED,
    SQLITE_OK, sqlite3_db_status,
};
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivationStatus};
use sqlx::{
    ConnectOptions, FromRow, Pool, QueryBuilder, Row, Sqlite, Type,
    migrate::MigrateDatabase,
    pool::{PoolConnection, PoolOptions},
    sqlite::{
        SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqliteQueryResult,
        SqliteRow, SqliteSynchronous,
    },
};
use std::collections::HashMap;
use std::{str::FromStr, time::Instant};
use tracing::instrument;

use crate::config::Config;

/// The members of this enum should be synced with the members
/// of InflightActivationStatus in sentry_protos
#[derive(Clone, Copy, Debug, PartialEq, Eq, Type)]
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

    pub fn decode_from_str(value: String) -> Self {
        match value.as_str() {
            "Unspecified" => InflightActivationStatus::Unspecified,
            "Pending" => InflightActivationStatus::Pending,
            "Processing" => InflightActivationStatus::Processing,
            "Failure" => InflightActivationStatus::Failure,
            "Retry" => InflightActivationStatus::Retry,
            "Complete" => InflightActivationStatus::Complete,
            "Delay" => InflightActivationStatus::Delay,
            _ => InflightActivationStatus::Unspecified,
        }
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

#[derive(Clone, Debug, PartialEq)]
pub struct InflightActivation {
    pub id: String,
    /// The protobuf activation that was received from kafka
    pub activation: Vec<u8>,

    /// The current status of the activation
    pub status: InflightActivationStatus,

    /// The topic the activation was received from
    pub topic: String,

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
    /// and the provided datetime
    pub fn received_latency(&self, now: DateTime<Utc>) -> i64 {
        now.signed_duration_since(self.received_at)
            .num_milliseconds()
            - self.delay_until.map_or(0, |delay_until| {
                delay_until
                    .signed_duration_since(self.received_at)
                    .num_milliseconds()
            })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QueryResult {
    pub rows_affected: u64,
}

impl From<SqliteQueryResult> for QueryResult {
    fn from(value: SqliteQueryResult) -> Self {
        Self {
            rows_affected: value.rows_affected(),
        }
    }
}

pub struct FailedTasksForwarder {
    pub to_discard: Vec<(String, Vec<u8>)>,
    pub to_deadletter: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, FromRow)]
struct TableRow {
    id: String,
    activation: Vec<u8>,
    partition: i32,
    offset: i64,
    added_at: DateTime<Utc>,
    received_at: DateTime<Utc>,
    processing_attempts: i32,
    expires_at: Option<DateTime<Utc>>,
    delay_until: Option<DateTime<Utc>>,
    processing_deadline_duration: u32,
    processing_deadline: Option<DateTime<Utc>>,
    status: InflightActivationStatus,
    at_most_once: bool,
    namespace: String,
    taskname: String,
    #[sqlx(try_from = "i32")]
    on_attempts_exceeded: OnAttemptsExceeded,
}

impl TryFrom<InflightActivation> for TableRow {
    type Error = anyhow::Error;

    fn try_from(value: InflightActivation) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            activation: value.activation,
            partition: value.partition,
            offset: value.offset,
            added_at: value.added_at,
            received_at: value.received_at,
            processing_attempts: value.processing_attempts,
            expires_at: value.expires_at,
            delay_until: value.delay_until,
            processing_deadline_duration: value.processing_deadline_duration,
            processing_deadline: value.processing_deadline,
            status: value.status,
            at_most_once: value.at_most_once,
            namespace: value.namespace,
            taskname: value.taskname,
            on_attempts_exceeded: value.on_attempts_exceeded,
        })
    }
}

impl From<TableRow> for InflightActivation {
    fn from(value: TableRow) -> Self {
        Self {
            id: value.id,
            activation: value.activation,
            status: value.status,
            topic: "topic".to_string(),
            partition: value.partition,
            offset: value.offset,
            added_at: value.added_at,
            received_at: value.received_at,
            processing_attempts: value.processing_attempts,
            processing_deadline_duration: value.processing_deadline_duration,
            expires_at: value.expires_at,
            delay_until: value.delay_until,
            processing_deadline: value.processing_deadline,
            at_most_once: value.at_most_once,
            namespace: value.namespace,
            taskname: value.taskname,
            on_attempts_exceeded: value.on_attempts_exceeded,
        }
    }
}

impl From<HashMap<String, String>> for InflightActivation {
    fn from(value: HashMap<String, String>) -> Self {
        let decoded_activation = general_purpose::STANDARD
            .decode(value.get("activation").unwrap().clone())
            .unwrap();
        let expires_at = value.get("expires_at").map(|expires_at| {
            DateTime::from_timestamp_millis(expires_at.parse::<i64>().unwrap()).unwrap()
        });
        let delay_until = value.get("delay_until").map(|delay_until| {
            DateTime::from_timestamp_millis(delay_until.parse::<i64>().unwrap()).unwrap()
        });
        let processing_deadline = value.get("processing_deadline").map(|processing_deadline| {
            DateTime::from_timestamp_millis(processing_deadline.parse::<i64>().unwrap()).unwrap()
        });
        Self {
            id: value.get("id").unwrap().to_string(),
            activation: decoded_activation,
            status: InflightActivationStatus::decode_from_str(
                value.get("status").unwrap().to_string(),
            ),
            topic: value.get("topic").unwrap().to_string(),
            partition: value.get("partition").unwrap().parse::<i32>().unwrap(),
            offset: value.get("offset").unwrap().parse::<i64>().unwrap(),
            added_at: DateTime::from_timestamp_millis(
                value.get("added_at").unwrap().parse::<i64>().unwrap(),
            )
            .unwrap(),
            received_at: DateTime::from_timestamp_millis(
                value.get("received_at").unwrap().parse::<i64>().unwrap(),
            )
            .unwrap(),
            processing_attempts: value
                .get("processing_attempts")
                .unwrap()
                .parse::<i32>()
                .unwrap(),
            processing_deadline_duration: value
                .get("processing_deadline_duration")
                .unwrap()
                .parse::<u32>()
                .unwrap(),
            expires_at,
            delay_until,
            processing_deadline,
            at_most_once: value.get("at_most_once").unwrap().parse::<bool>().unwrap(),
            namespace: value.get("namespace").unwrap().to_string(),
            taskname: value.get("taskname").unwrap().to_string(),
            on_attempts_exceeded: OnAttemptsExceeded::from_str_name(
                value.get("on_attempts_exceeded").unwrap().as_str(),
            )
            .unwrap(),
        }
    }
}

pub async fn create_sqlite_pool(url: &str) -> Result<(Pool<Sqlite>, Pool<Sqlite>), Error> {
    if !Sqlite::database_exists(url).await? {
        Sqlite::create_database(url).await?
    }

    let read_pool = PoolOptions::<Sqlite>::new()
        .max_connections(64)
        .connect_with(
            SqliteConnectOptions::from_str(url)?
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .read_only(true)
                .disable_statement_logging(),
        )
        .await?;

    let write_pool = PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(url)?
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .auto_vacuum(SqliteAutoVacuum::Incremental)
                .disable_statement_logging(),
        )
        .await?;

    Ok((read_pool, write_pool))
}

pub struct InflightActivationStoreConfig {
    pub max_processing_attempts: usize,
    pub processing_deadline_grace_sec: u64,
    pub vacuum_page_count: Option<usize>,
    pub enable_sqlite_status_metrics: bool,
}

impl InflightActivationStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_processing_attempts: config.max_processing_attempts,
            vacuum_page_count: config.vacuum_page_count,
            processing_deadline_grace_sec: config.processing_deadline_grace_sec,
            enable_sqlite_status_metrics: config.enable_sqlite_status_metrics,
        }
    }
}

pub struct InflightActivationStore {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
    config: InflightActivationStoreConfig,
}

impl InflightActivationStore {
    async fn acquire_write_conn_metric(
        &self,
        caller: &'static str,
    ) -> Result<PoolConnection<Sqlite>, Error> {
        let start = Instant::now();
        let conn = self.write_pool.acquire().await?;
        metrics::histogram!("sqlite.write.acquire_conn", "fn" => caller).record(start.elapsed());
        Ok(conn)
    }
    pub async fn new(url: &str, config: InflightActivationStoreConfig) -> Result<Self, Error> {
        let (read_pool, write_pool) = create_sqlite_pool(url).await?;

        sqlx::migrate!("./migrations").run(&write_pool).await?;

        Ok(Self {
            read_pool,
            write_pool,
            config,
        })
    }

    /// Trigger incremental vacuum to reclaim free pages in the database.
    /// Depending on config data, will either vacuum a set number of
    /// pages or attempt to reclaim all free pages.
    #[instrument(skip_all)]
    pub async fn vacuum_db(&self) -> Result<(), Error> {
        let timer = Instant::now();

        if let Some(page_count) = self.config.vacuum_page_count {
            let mut conn = self.acquire_write_conn_metric("vacuum_db").await?;
            sqlx::query(format!("PRAGMA incremental_vacuum({page_count})").as_str())
                .execute(&mut *conn)
                .await?;
        } else {
            let mut conn = self.acquire_write_conn_metric("vacuum_db").await?;
            sqlx::query("PRAGMA incremental_vacuum")
                .execute(&mut *conn)
                .await?;
        }
        let freelist_count: i32 = sqlx::query("PRAGMA freelist_count")
            .fetch_one(&self.read_pool)
            .await?
            .get("freelist_count");

        metrics::histogram!("store.vacuum", "database" => "meta").record(timer.elapsed());
        metrics::gauge!("store.vacuum.freelist", "database" => "meta").set(freelist_count);
        Ok(())
    }

    /// Perform a full vacuum on the database.
    pub async fn full_vacuum_db(&self) -> Result<(), Error> {
        let mut conn = self.acquire_write_conn_metric("full_vacuum_db").await?;
        sqlx::query("VACUUM").execute(&mut *conn).await?;
        self.emit_db_status_metrics().await;
        Ok(())
    }

    async fn emit_db_status_metrics(&self) {
        if !self.config.enable_sqlite_status_metrics {
            return;
        }

        if let Ok(mut conn) = self.read_pool.acquire().await
            && let Ok(mut raw) = conn.lock_handle().await
        {
            let mut cur: i32 = 0;
            let mut hi: i32 = 0;
            unsafe {
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_CACHE_USED,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.cache_used_bytes").set(cur);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_CACHE_USED_SHARED,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.cache_used_shared_bytes").set(cur);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_CACHE_HIT,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.cache_hit_total").set(cur);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_CACHE_MISS,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.cache_miss_total").set(cur);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_CACHE_WRITE,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.cache_write_total").set(cur);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_CACHE_SPILL,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.cache_spill_total").set(cur);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_SCHEMA_USED,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.schema_used_bytes").set(cur);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_STMT_USED,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.stmt_used_bytes").set(cur);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_LOOKASIDE_USED,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.lookaside_used").set(cur);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_LOOKASIDE_HIT,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.lookaside_hit_highwater").set(hi);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.lookaside_miss_size_highwater").set(hi);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.lookaside_miss_full_highwater").set(hi);
                }
                if sqlite3_db_status(
                    raw.as_raw_handle().as_mut(),
                    SQLITE_DBSTATUS_DEFERRED_FKS,
                    &mut cur,
                    &mut hi,
                    0,
                ) == SQLITE_OK
                {
                    metrics::gauge!("sqlite.db.deferred_fks_unresolved").set(cur);
                }
            }
        }
    }

    /// Get the size of the database in bytes based on SQLite metadata queries.
    pub async fn db_size(&self) -> Result<u64, Error> {
        let result: u64 = sqlx::query(
            "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()",
        )
        .fetch_one(&self.read_pool)
        .await?
        .get(0);

        Ok(result)
    }

    /// Get an activation by id. Primarily used for testing
    pub async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
        let row_result: Option<TableRow> = sqlx::query_as(
            "
            SELECT id,
                activation,
                partition,
                offset,
                added_at,
                received_at,
                processing_attempts,
                expires_at,
                delay_until,
                processing_deadline_duration,
                processing_deadline,
                status,
                at_most_once,
                namespace,
                taskname,
                on_attempts_exceeded
            FROM inflight_taskactivations
            WHERE id = $1
            ",
        )
        .bind(id)
        .fetch_optional(&self.read_pool)
        .await?;

        let Some(row) = row_result else {
            return Ok(None);
        };

        Ok(Some(row.into()))
    }

    #[instrument(skip_all)]
    pub async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        if batch.is_empty() {
            return Ok(QueryResult { rows_affected: 0 });
        }
        let mut query_builder = QueryBuilder::<Sqlite>::new(
            "
            INSERT INTO inflight_taskactivations
                (
                    id,
                    activation,
                    partition,
                    offset,
                    added_at,
                    received_at,
                    processing_attempts,
                    expires_at,
                    delay_until,
                    processing_deadline_duration,
                    processing_deadline,
                    status,
                    at_most_once,
                    namespace,
                    taskname,
                    on_attempts_exceeded
                )
            ",
        );
        let rows = batch
            .into_iter()
            .map(TableRow::try_from)
            .collect::<Result<Vec<TableRow>, _>>()?;
        let query = query_builder
            .push_values(rows, |mut b, row| {
                b.push_bind(row.id);
                b.push_bind(row.activation);
                b.push_bind(row.partition);
                b.push_bind(row.offset);
                b.push_bind(row.added_at.timestamp());
                b.push_bind(row.received_at.timestamp());
                b.push_bind(row.processing_attempts);
                b.push_bind(row.expires_at.map(|t| Some(t.timestamp())));
                b.push_bind(row.delay_until.map(|t| Some(t.timestamp())));
                b.push_bind(row.processing_deadline_duration);
                if let Some(deadline) = row.processing_deadline {
                    b.push_bind(deadline.timestamp());
                } else {
                    // Add a literal null
                    b.push("null");
                }
                b.push_bind(row.status);
                b.push_bind(row.at_most_once);
                b.push_bind(row.namespace);
                b.push_bind(row.taskname);
                b.push_bind(row.on_attempts_exceeded as i32);
            })
            .push(" ON CONFLICT(id) DO NOTHING")
            .build();
        let mut conn = self.acquire_write_conn_metric("store").await?;
        let meta_result = Ok(query.execute(&mut *conn).await?.into());

        // Sync the WAL into the main database so we don't lose data on host failure.
        let checkpoint_timer = Instant::now();
        let checkpoint_result = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
            .fetch_one(&mut *conn)
            .await;
        match checkpoint_result {
            Ok(row) => {
                metrics::gauge!("store.passive_checkpoint_busy").set(row.get::<i32, _>("busy"));
                metrics::gauge!("store.pages_written_to_wal").set(row.get::<i32, _>("log"));
                metrics::gauge!("store.pages_committed_to_db")
                    .set(row.get::<i32, _>("checkpointed"));
                metrics::gauge!("store.checkpoint.failed").set(0);
            }
            Err(_e) => {
                metrics::gauge!("store.checkpoint.failed").set(1);
            }
        }
        metrics::histogram!("store.checkpoint.duration").record(checkpoint_timer.elapsed());

        meta_result
    }

    #[instrument(skip_all)]
    pub async fn get_pending_activation(
        &self,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
        // Convert single namespace to vector for internal use
        let namespaces = namespace.map(|ns| vec![ns.to_string()]);
        let result = self
            .get_pending_activations_from_namespaces(namespaces.as_deref(), Some(1))
            .await
            .unwrap();
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
        let now = Utc::now();

        let grace_period = self.config.processing_deadline_grace_sec;
        let mut query_builder = QueryBuilder::new(format!(
            "UPDATE inflight_taskactivations
            SET
                processing_deadline = unixepoch(
                    'now', '+' || (processing_deadline_duration + {grace_period}) || ' seconds'
                ),
                status = "
        ));
        query_builder.push_bind(InflightActivationStatus::Processing);
        query_builder.push(
            "
            WHERE id IN (
                SELECT id
                FROM inflight_taskactivations
                WHERE status = ",
        );
        query_builder.push_bind(InflightActivationStatus::Pending);
        query_builder.push(" AND (expires_at IS NULL OR expires_at > ");
        query_builder.push_bind(now.timestamp());
        query_builder.push(")");

        // Handle namespace filtering
        if let Some(namespaces) = namespaces
            && !namespaces.is_empty()
        {
            query_builder.push(" AND namespace IN (");
            let mut separated = query_builder.separated(", ");
            for namespace in namespaces.iter() {
                separated.push_bind(namespace);
            }
            query_builder.push(")");
        }
        query_builder.push(" ORDER BY added_at");
        if let Some(limit) = limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit);
        }
        query_builder.push(") RETURNING *");

        let mut conn = self
            .acquire_write_conn_metric("get_pending_activation")
            .await?;
        let rows: Vec<TableRow> = query_builder
            .build_query_as::<TableRow>()
            .fetch_all(&mut *conn)
            .await?;

        Ok(rows.into_iter().map(|row| row.into()).collect())
    }

    /// Get the age of the oldest pending activation in seconds.
    /// Only activations with status=pending and processing_attempts=0 are considered
    /// as we are interested in latency to the *first* attempt.
    /// Tasks with delay_until set, will have their age adjusted based on their
    /// delay time. No tasks = 0 lag
    pub async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> f64 {
        let result = sqlx::query(
            "SELECT received_at, delay_until
            FROM inflight_taskactivations
            WHERE status = $1
            AND processing_attempts = 0
            ORDER BY received_at ASC
            LIMIT 1
            ",
        )
        .bind(InflightActivationStatus::Pending)
        .fetch_one(&self.read_pool)
        .await;

        if let Ok(row) = result {
            let received_at: DateTime<Utc> = row.get("received_at");
            let delay_until: Option<DateTime<Utc>> = row.get("delay_until");
            let millis = now.signed_duration_since(received_at).num_milliseconds()
                - delay_until.map_or(0, |delay_time| {
                    delay_time
                        .signed_duration_since(received_at)
                        .num_milliseconds()
                });
            millis as f64 / 1000.0
        } else {
            // If we couldn't find a row, there is no latency.
            0.0
        }
    }

    #[instrument(skip_all)]
    pub async fn count_pending_activations(&self) -> Result<usize, Error> {
        self.count_by_status(InflightActivationStatus::Pending)
            .await
    }

    #[instrument(skip_all)]
    pub async fn count_by_status(&self, status: InflightActivationStatus) -> Result<usize, Error> {
        let result =
            sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations WHERE status = $1")
                .bind(status)
                .fetch_one(&self.read_pool)
                .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    pub async fn count(&self) -> Result<usize, Error> {
        let result = sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations")
            .fetch_one(&self.read_pool)
            .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    /// Update the status of a specific activation
    #[instrument(skip_all)]
    pub async fn set_status(
        &self,
        id: &str,
        status: InflightActivationStatus,
    ) -> Result<Option<InflightActivation>, Error> {
        let mut conn = self.acquire_write_conn_metric("set_status").await?;
        let result: Option<TableRow> = sqlx::query_as(
            "UPDATE inflight_taskactivations SET status = $1 WHERE id = $2 RETURNING *",
        )
        .bind(status)
        .bind(id)
        .fetch_optional(&mut *conn)
        .await?;

        let Some(row) = result else {
            return Ok(None);
        };

        Ok(Some(row.into()))
    }

    #[instrument(skip_all)]
    pub async fn set_processing_deadline(
        &self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let mut conn = self
            .acquire_write_conn_metric("set_processing_deadline")
            .await?;
        sqlx::query("UPDATE inflight_taskactivations SET processing_deadline = $1 WHERE id = $2")
            .bind(deadline.unwrap().timestamp())
            .bind(id)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn delete_activation(&self, id: &str) -> Result<(), Error> {
        let mut conn = self.acquire_write_conn_metric("delete_activation").await?;
        sqlx::query("DELETE FROM inflight_taskactivations WHERE id = $1")
            .bind(id)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        Ok(sqlx::query_as(
            "
            SELECT id,
                activation,
                partition,
                offset,
                added_at,
                received_at,
                processing_attempts,
                expires_at,
                delay_until,
                processing_deadline_duration,
                processing_deadline,
                status,
                at_most_once,
                namespace,
                taskname,
                on_attempts_exceeded
            FROM inflight_taskactivations
            WHERE status = $1
            ",
        )
        .bind(InflightActivationStatus::Retry)
        .fetch_all(&self.read_pool)
        .await?
        .into_iter()
        .map(|row: TableRow| row.into())
        .collect())
    }

    pub async fn clear(&self) -> Result<(), Error> {
        let mut conn = self.acquire_write_conn_metric("clear").await?;
        sqlx::query("DELETE FROM inflight_taskactivations")
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    /// Update tasks that are in processing and have exceeded their processing deadline
    /// Exceeding a processing deadline does not consume a retry as we don't know
    /// if a worker took the task and was killed, or failed.
    #[instrument(skip_all)]
    pub async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut atomic = self.write_pool.begin().await?;

        // Idempotent tasks that fail their processing deadlines go directly to failure
        // there are no retries, as the worker will reject the task due to idempotency keys.
        let most_once_result = sqlx::query(
            "UPDATE inflight_taskactivations
            SET processing_deadline = null, status = $1
            WHERE processing_deadline < $2 AND at_most_once = TRUE AND status = $3",
        )
        .bind(InflightActivationStatus::Failure)
        .bind(now.timestamp())
        .bind(InflightActivationStatus::Processing)
        .execute(&mut *atomic)
        .await;

        let mut processing_deadline_modified_rows = 0;
        if let Ok(query_res) = most_once_result {
            processing_deadline_modified_rows = query_res.rows_affected();
        }

        // Update non-idempotent tasks.
        // Increment processing_attempts by 1 and reset processing_deadline to null.
        let result = sqlx::query(
            "UPDATE inflight_taskactivations
            SET processing_deadline = null, status = $1, processing_attempts = processing_attempts + 1
            WHERE processing_deadline < $2 AND status = $3",
        )
        .bind(InflightActivationStatus::Pending)
        .bind(now.timestamp())
        .bind(InflightActivationStatus::Processing)
        .execute(&mut *atomic)
        .await;

        atomic.commit().await?;

        if let Ok(query_res) = result {
            processing_deadline_modified_rows += query_res.rows_affected();
            return Ok(processing_deadline_modified_rows);
        }

        Err(anyhow!("Could not update tasks past processing_deadline"))
    }

    /// Update tasks that have exceeded their max processing attempts.
    /// These tasks are set to status=failure and will be handled by handle_failed_tasks accordingly.
    #[instrument(skip_all)]
    pub async fn handle_processing_attempts(&self) -> Result<u64, Error> {
        let mut conn = self
            .acquire_write_conn_metric("handle_processing_attempts")
            .await?;
        let processing_attempts_result = sqlx::query(
            "UPDATE inflight_taskactivations
            SET status = $1
            WHERE processing_attempts >= $2 AND status = $3",
        )
        .bind(InflightActivationStatus::Failure)
        .bind(self.config.max_processing_attempts as i32)
        .bind(InflightActivationStatus::Pending)
        .execute(&mut *conn)
        .await;

        if let Ok(query_res) = processing_attempts_result {
            return Ok(query_res.rows_affected());
        }

        Err(anyhow!("Could not update tasks past processing_deadline"))
    }

    /// Perform upkeep work for tasks that are past expires_at deadlines
    ///
    /// Tasks that are pending and past their expires_at deadline are updated
    /// to have status=failure so that they can be discarded/deadlettered by handle_failed_tasks
    ///
    /// The number of impacted records is returned in a Result.
    #[instrument(skip_all)]
    pub async fn handle_expires_at(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut conn = self.acquire_write_conn_metric("handle_expires_at").await?;
        let query = sqlx::query(
            "DELETE FROM inflight_taskactivations WHERE status = $1 AND expires_at IS NOT NULL AND expires_at < $2",
        )
        .bind(InflightActivationStatus::Pending)
        .bind(now.timestamp())
        .execute(&mut *conn)
        .await?;

        Ok(query.rows_affected())
    }

    /// Perform upkeep work for tasks that are past delay_until deadlines
    ///
    /// Tasks that are delayed and past their delay_until deadline are updated
    /// to have status=pending so that they can be executed by workers
    ///
    /// The number of impacted records is returned in a Result.
    #[instrument(skip_all)]
    pub async fn handle_delay_until(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut conn = self.acquire_write_conn_metric("handle_delay_until").await?;
        let update_result = sqlx::query(
            r#"UPDATE inflight_taskactivations
            SET status = $1
            WHERE delay_until IS NOT NULL AND delay_until < $2 AND status = $3
            "#,
        )
        .bind(InflightActivationStatus::Pending)
        .bind(now.timestamp())
        .bind(InflightActivationStatus::Delay)
        .execute(&mut *conn)
        .await?;

        Ok(update_result.rows_affected())
    }

    /// Perform upkeep work related to status=failure
    ///
    /// Activations that are status=failure need to either be discarded by setting status=complete
    /// or need to be moved to deadletter and are returned in the Result.
    /// Once dead-lettered tasks have been added to Kafka those tasks can have their status set to
    /// complete.
    #[instrument(skip_all)]
    pub async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
        let mut atomic = self.write_pool.begin().await?;

        let failed_tasks: Vec<SqliteRow> =
            sqlx::query("SELECT id, activation, on_attempts_exceeded FROM inflight_taskactivations WHERE status = $1")
                .bind(InflightActivationStatus::Failure)
                .fetch_all(&mut *atomic)
                .await?
                .into_iter()
                .collect();

        let mut forwarder = FailedTasksForwarder {
            to_discard: vec![],
            to_deadletter: vec![],
        };

        for record in failed_tasks.iter() {
            let activation_data: &[u8] = record.get("activation");
            let id: String = record.get("id");
            // We could be deadlettering because of activation.expires
            // when a task expires we still deadletter if configured.
            let on_attempts_exceeded_val: i32 = record.get("on_attempts_exceeded");
            let on_attempts_exceeded: OnAttemptsExceeded =
                on_attempts_exceeded_val.try_into().unwrap();
            if on_attempts_exceeded == OnAttemptsExceeded::Discard
                || on_attempts_exceeded == OnAttemptsExceeded::Unspecified
            {
                forwarder.to_discard.push((id, activation_data.to_vec()))
            } else if on_attempts_exceeded == OnAttemptsExceeded::Deadletter {
                forwarder.to_deadletter.push((id, activation_data.to_vec()))
            }
        }

        if !forwarder.to_discard.is_empty() {
            let mut query_builder = QueryBuilder::new("UPDATE inflight_taskactivations ");
            query_builder
                .push("SET status = ")
                .push_bind(InflightActivationStatus::Complete)
                .push(" WHERE id IN (");

            let mut separated = query_builder.separated(", ");
            for (id, _body) in forwarder.to_discard.iter() {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");

            query_builder.build().execute(&mut *atomic).await?;
        }

        atomic.commit().await?;

        Ok(forwarder)
    }

    /// Mark a collection of tasks as complete by id
    #[instrument(skip_all)]
    pub async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        let mut query_builder = QueryBuilder::new("UPDATE inflight_taskactivations ");
        query_builder
            .push("SET status = ")
            .push_bind(InflightActivationStatus::Complete)
            .push(" WHERE id IN (");

        let mut separated = query_builder.separated(", ");
        for id in ids.iter() {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
        let mut conn = self.acquire_write_conn_metric("mark_completed").await?;
        let result = query_builder.build().execute(&mut *conn).await?;

        Ok(result.rows_affected())
    }

    /// Remove completed tasks.
    /// This method is a garbage collector for the inflight task store.
    #[instrument(skip_all)]
    pub async fn remove_completed(&self) -> Result<u64, Error> {
        let mut conn = self.acquire_write_conn_metric("remove_completed").await?;
        let query = sqlx::query("DELETE FROM inflight_taskactivations WHERE status = $1")
            .bind(InflightActivationStatus::Complete)
            .execute(&mut *conn)
            .await?;

        Ok(query.rows_affected())
    }

    /// Remove killswitched tasks.
    #[instrument(skip_all)]
    pub async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        let mut query_builder =
            QueryBuilder::new("DELETE FROM inflight_taskactivations WHERE taskname IN (");
        let mut separated = query_builder.separated(", ");
        for taskname in killswitched_tasks.iter() {
            separated.push_bind(taskname);
        }
        separated.push_unseparated(")");
        let mut conn = self
            .acquire_write_conn_metric("remove_killswitched")
            .await?;
        let query = query_builder.build().execute(&mut *conn).await?;

        Ok(query.rows_affected())
    }
}
