use std::str::FromStr;
use std::time::Instant;

use sqlx::migrate::MigrateDatabase;
use sqlx::pool::{PoolConnection, PoolOptions};
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqliteRow,
    SqliteSynchronous,
};
use sqlx::{ConnectOptions, FromRow, Pool, QueryBuilder, Row, Sqlite};

use anyhow::{Error, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use libsqlite3_sys::{
    SQLITE_DBSTATUS_CACHE_HIT, SQLITE_DBSTATUS_CACHE_MISS, SQLITE_DBSTATUS_CACHE_SPILL,
    SQLITE_DBSTATUS_CACHE_USED, SQLITE_DBSTATUS_CACHE_USED_SHARED, SQLITE_DBSTATUS_CACHE_WRITE,
    SQLITE_DBSTATUS_DEFERRED_FKS, SQLITE_DBSTATUS_LOOKASIDE_HIT,
    SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL, SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE,
    SQLITE_DBSTATUS_LOOKASIDE_USED, SQLITE_DBSTATUS_SCHEMA_USED, SQLITE_DBSTATUS_STMT_USED,
    SQLITE_OK, sqlite3_db_status,
};
use sentry_protos::taskbroker::v1::OnAttemptsExceeded;
use tracing::{instrument, warn};

use crate::config::Config;
use crate::store::activation::{InflightActivation, InflightActivationStatus};
use crate::store::traits::InflightActivationStore;
use crate::store::types::{BucketRange, FailedTasksForwarder};

#[derive(Debug, FromRow)]
pub struct TableRow {
    pub id: String,
    pub activation: Vec<u8>,
    pub partition: i32,
    pub offset: i64,
    pub added_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub processing_attempts: i32,
    pub expires_at: Option<DateTime<Utc>>,
    pub delay_until: Option<DateTime<Utc>>,
    pub processing_deadline_duration: i32,
    pub processing_deadline: Option<DateTime<Utc>>,
    pub claim_expires_at: Option<DateTime<Utc>>,
    pub status: String,
    pub at_most_once: bool,
    pub application: String,
    pub namespace: String,
    pub taskname: String,
    #[sqlx(try_from = "i32")]
    pub on_attempts_exceeded: OnAttemptsExceeded,
    pub bucket: i16,
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
            claim_expires_at: value.claim_expires_at,
            status: value.status.to_string(),
            at_most_once: value.at_most_once,
            application: value.application,
            namespace: value.namespace,
            taskname: value.taskname,
            on_attempts_exceeded: value.on_attempts_exceeded,
            bucket: value.bucket,
        })
    }
}

impl From<TableRow> for InflightActivation {
    fn from(value: TableRow) -> Self {
        Self {
            id: value.id,
            activation: value.activation,
            status: InflightActivationStatus::from_str(&value.status).unwrap(),
            partition: value.partition,
            offset: value.offset,
            added_at: value.added_at,
            received_at: value.received_at,
            processing_attempts: value.processing_attempts,
            processing_deadline_duration: value.processing_deadline_duration,
            expires_at: value.expires_at,
            delay_until: value.delay_until,
            processing_deadline: value.processing_deadline,
            claim_expires_at: value.claim_expires_at,
            at_most_once: value.at_most_once,
            application: value.application,
            namespace: value.namespace,
            taskname: value.taskname,
            on_attempts_exceeded: value.on_attempts_exceeded,
            bucket: value.bucket,
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
    /// Milliseconds added to `claim_expires_at` before grace: `fetch_batch_size * push_queue_timeout_ms`.
    pub claim_lease_ms: u64,
    pub vacuum_page_count: Option<usize>,
    pub enable_sqlite_status_metrics: bool,
}

impl InflightActivationStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_processing_attempts: config.max_processing_attempts,
            vacuum_page_count: config.vacuum_page_count,
            processing_deadline_grace_sec: config.processing_deadline_grace_sec,
            claim_lease_ms: config.fetch_batch_size.max(1) as u64 * config.push_queue_timeout_ms,
            enable_sqlite_status_metrics: config.enable_sqlite_status_metrics,
        }
    }
}

pub struct SqliteActivationStore {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
    config: InflightActivationStoreConfig,
}

impl SqliteActivationStore {
    pub async fn new(url: &str, config: InflightActivationStoreConfig) -> Result<Self, Error> {
        let (read_pool, write_pool) = create_sqlite_pool(url).await?;

        sqlx::migrate!("./migrations/sqlite")
            .run(&write_pool)
            .await?;

        Ok(Self {
            read_pool,
            write_pool,
            config,
        })
    }

    async fn acquire_write_conn_metric(
        &self,
        caller: &'static str,
    ) -> Result<PoolConnection<Sqlite>, Error> {
        let start = Instant::now();
        let conn = self.write_pool.acquire().await?;
        metrics::histogram!("sqlite.write.acquire_conn", "fn" => caller).record(start.elapsed());
        Ok(conn)
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
}

#[async_trait]
impl InflightActivationStore for SqliteActivationStore {
    /// Trigger incremental vacuum to reclaim free pages in the database.
    /// Depending on config data, will either vacuum a set number of
    /// pages or attempt to reclaim all free pages.
    #[instrument(skip_all)]
    async fn vacuum_db(&self) -> Result<(), Error> {
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
    async fn full_vacuum_db(&self) -> Result<(), Error> {
        let mut conn = self.acquire_write_conn_metric("full_vacuum_db").await?;
        sqlx::query("VACUUM").execute(&mut *conn).await?;
        self.emit_db_status_metrics().await;
        Ok(())
    }

    /// Get the size of the database in bytes based on SQLite metadata queries.
    async fn db_size(&self) -> Result<u64, Error> {
        let result: u64 = sqlx::query(
            "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()",
        )
        .fetch_one(&self.read_pool)
        .await?
        .get(0);

        Ok(result)
    }

    /// Get an activation by id. Primarily used for testing
    async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
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
                claim_expires_at,
                status,
                at_most_once,
                application,
                namespace,
                taskname,
                on_attempts_exceeded,
                bucket
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

    fn assign_partitions(&self, partitions: Vec<i32>) -> Result<(), Error> {
        warn!("assign_partitions: {:?}", partitions);
        Ok(())
    }

    #[instrument(skip_all)]
    async fn store(&self, batch: Vec<InflightActivation>) -> Result<u64, Error> {
        if batch.is_empty() {
            return Ok(0);
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
                    claim_expires_at,
                    status,
                    at_most_once,
                    application,
                    namespace,
                    taskname,
                    on_attempts_exceeded,
                    bucket
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
                    b.push("null");
                }

                if let Some(exp) = row.claim_expires_at {
                    b.push_bind(exp.timestamp());
                } else {
                    b.push("null");
                }

                b.push_bind(row.status);
                b.push_bind(row.at_most_once);
                b.push_bind(row.application);
                b.push_bind(row.namespace);
                b.push_bind(row.taskname);
                b.push_bind(row.on_attempts_exceeded as i32);
                b.push_bind(row.bucket);
            })
            .push(" ON CONFLICT(id) DO NOTHING")
            .build();
        let mut conn = self.acquire_write_conn_metric("store").await?;
        let result = query.execute(&mut *conn).await?;
        let rows_affected = Ok(result.rows_affected());

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

        rows_affected
    }

    #[instrument(skip_all)]
    async fn claim_activations(
        &self,
        application: Option<&str>,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
        bucket: Option<BucketRange>,
    ) -> Result<Vec<InflightActivation>, Error> {
        let now = Utc::now();
        let grace_period = self.config.processing_deadline_grace_sec;

        let mut query_builder = QueryBuilder::new("UPDATE inflight_taskactivations SET ");

        query_builder.push(format!(
            "processing_deadline = unixepoch('now', '+' || (processing_deadline_duration + {grace_period}) || ' seconds'), claim_expires_at = NULL, status = "
        ));

        query_builder.push_bind(InflightActivationStatus::Processing);

        query_builder.push(" WHERE id IN (SELECT id FROM inflight_taskactivations WHERE status = ");
        query_builder.push_bind(InflightActivationStatus::Pending);
        query_builder.push(" AND (expires_at IS NULL OR expires_at > ");
        query_builder.push_bind(now.timestamp());
        query_builder.push(")");

        if let Some(value) = application {
            query_builder.push(" AND application =");
            query_builder.push_bind(value);
        }
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
        if let Some((min, max)) = bucket {
            query_builder.push(" AND bucket >= ");
            query_builder.push_bind(min);
            query_builder.push(" AND bucket <= ");
            query_builder.push_bind(max);
        }
        query_builder.push(" ORDER BY added_at");
        if let Some(limit) = limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit);
        }
        query_builder.push(") RETURNING *");

        let mut conn = self.acquire_write_conn_metric("claim_activations").await?;
        let rows: Vec<TableRow> = query_builder
            .build_query_as::<TableRow>()
            .fetch_all(&mut *conn)
            .await?;

        Ok(rows.into_iter().map(|row| row.into()).collect())
    }

    /// Revert a `Processing` activation back to `Pending` without incrementing processing attempts.
    #[instrument(skip_all)]
    async fn undo_claim_activation(&self, id: &str) -> Result<(), Error> {
        let mut conn = self
            .acquire_write_conn_metric("undo_claim_activation")
            .await?;

        let result = sqlx::query(
            "UPDATE inflight_taskactivations SET
                status = $1,
                processing_deadline = NULL,
                claim_expires_at = NULL
            WHERE id = $2
              AND status = $3",
        )
        .bind(InflightActivationStatus::Pending)
        .bind(id)
        .bind(InflightActivationStatus::Processing)
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            metrics::counter!("store.undo_claim_activation", "result" => "not_found").increment(1);

            warn!(
                task_id = %id,
                "Activation could not be unclaimed, it may be missing or not processing"
            );
        } else {
            metrics::counter!("store.undo_claim_activation", "result" => "ok").increment(1);
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn mark_activation_processing(&self, id: &str) -> Result<(), Error> {
        let mut conn = self
            .acquire_write_conn_metric("mark_activation_processing")
            .await?;

        let grace_period = self.config.processing_deadline_grace_sec;
        let result = sqlx::query(&format!(
            "UPDATE inflight_taskactivations SET
                status = $1,
                processing_deadline = unixepoch('now', '+' || (processing_deadline_duration + {grace_period}) || ' seconds'),
                claim_expires_at = NULL
            WHERE id = $2 AND status = $3",
        ))
        .bind(InflightActivationStatus::Processing)
        .bind(id)
        .bind(InflightActivationStatus::Claimed)
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            metrics::counter!("push.mark_activation_processing", "result" => "not_found")
                .increment(1);

            warn!(
                task_id = %id,
                "Activation could not be marked as sent, it may be missing or its status may have already changed"
            );
        } else {
            metrics::counter!("push.mark_activation_processing", "result" => "ok").increment(1);
        }

        Ok(())
    }

    /// Get the age of the oldest pending activation in seconds.
    /// Only activations with status=pending and processing_attempts=0 are considered
    /// as we are interested in latency to the *first* attempt.
    /// Tasks with delay_until set, will have their age adjusted based on their
    /// delay time. No tasks = 0 lag
    async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> f64 {
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
    async fn count_by_status(&self, status: InflightActivationStatus) -> Result<usize, Error> {
        let result =
            sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations WHERE status = $1")
                .bind(status)
                .fetch_one(&self.read_pool)
                .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    async fn count(&self) -> Result<usize, Error> {
        let result = sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations")
            .fetch_one(&self.read_pool)
            .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    /// Update the status of a specific activation
    #[instrument(skip_all)]
    async fn set_status(
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
    async fn set_processing_deadline(
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
    async fn delete_activation(&self, id: &str) -> Result<(), Error> {
        let mut conn = self.acquire_write_conn_metric("delete_activation").await?;
        sqlx::query("DELETE FROM inflight_taskactivations WHERE id = $1")
            .bind(id)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
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
                claim_expires_at,
                status,
                at_most_once,
                application,
                namespace,
                taskname,
                on_attempts_exceeded,
                bucket
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

    async fn clear(&self) -> Result<(), Error> {
        let mut conn = self.acquire_write_conn_metric("clear").await?;
        sqlx::query("DELETE FROM inflight_taskactivations")
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    /// Expired push claims (`Claimed` + past `claim_expires_at`).
    #[instrument(skip_all)]
    async fn handle_claim_expiration(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut conn = self
            .acquire_write_conn_metric("handle_claim_expiration")
            .await?;

        let released = sqlx::query(
            "UPDATE inflight_taskactivations
             SET claim_expires_at = null,
                 status = $1
             WHERE claim_expires_at IS NOT NULL
                 AND claim_expires_at < $2
                 AND status = $3",
        )
        .bind(InflightActivationStatus::Pending)
        .bind(now.timestamp())
        .bind(InflightActivationStatus::Claimed)
        .execute(&mut *conn)
        .await?;

        Ok(released.rows_affected())
    }

    /// Update tasks that are in processing and have exceeded their processing deadline
    /// Exceeding a processing deadline does not consume a retry as we don't know
    /// if a worker took the task and was killed, or failed.
    #[instrument(skip_all)]
    async fn handle_processing_deadline(&self) -> Result<u64, Error> {
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
    async fn handle_processing_attempts(&self) -> Result<u64, Error> {
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
    async fn handle_expires_at(&self) -> Result<u64, Error> {
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
    async fn handle_delay_until(&self) -> Result<u64, Error> {
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
    async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
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
    async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
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
    async fn remove_completed(&self) -> Result<u64, Error> {
        let mut conn = self.acquire_write_conn_metric("remove_completed").await?;
        let query = sqlx::query("DELETE FROM inflight_taskactivations WHERE status = $1")
            .bind(InflightActivationStatus::Complete)
            .execute(&mut *conn)
            .await?;

        Ok(query.rows_affected())
    }

    /// Remove killswitched tasks.
    #[instrument(skip_all)]
    async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error> {
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
