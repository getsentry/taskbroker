use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use prost::Message;
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivation, TaskActivationStatus};
use sqlx::{
    ConnectOptions, FromRow, Pool, QueryBuilder, Row, Sqlite, Type,
    migrate::MigrateDatabase,
    pool::PoolOptions,
    sqlite::{
        SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqliteQueryResult,
        SqliteRow, SqliteSynchronous,
    },
};
use tracing::instrument;

use crate::config::{BlobTableMode, Config};

pub struct InflightActivationStoreConfig {
    pub max_processing_attempts: usize,
    pub blob_delete_delay_ms: u64,
    pub blob_dual_write: BlobTableMode,
}

impl InflightActivationStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_processing_attempts: config.max_processing_attempts,
            blob_delete_delay_ms: config.blob_delete_delay_ms,
            blob_dual_write: config.blob_dual_write.try_into().expect("mode is required"),
        }
    }
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Type)]
pub enum InflightOnAttemptsExceeded {
    /// Unused but necessary to align with sentry-protos
    Unspecified,
    Discard,
    Deadletter,
}

impl From<InflightOnAttemptsExceeded> for OnAttemptsExceeded {
    fn from(val: InflightOnAttemptsExceeded) -> Self {
        match val {
            InflightOnAttemptsExceeded::Unspecified => OnAttemptsExceeded::Unspecified,
            InflightOnAttemptsExceeded::Discard => OnAttemptsExceeded::Discard,
            InflightOnAttemptsExceeded::Deadletter => OnAttemptsExceeded::Deadletter,
        }
    }
}

impl TryFrom<i32> for InflightOnAttemptsExceeded {
    type Error = ();

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == InflightOnAttemptsExceeded::Unspecified as i32 => {
                Ok(InflightOnAttemptsExceeded::Unspecified)
            }
            x if x == InflightOnAttemptsExceeded::Discard as i32 => {
                Ok(InflightOnAttemptsExceeded::Discard)
            }
            x if x == InflightOnAttemptsExceeded::Deadletter as i32 => {
                Ok(InflightOnAttemptsExceeded::Deadletter)
            }
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InflightActivation {
    /// The unique id of the activation
    pub id: String,

    /// The protobuf activation that was received from kafka
    pub activation: Option<TaskActivation>,

    /// The current status of the activation
    pub status: InflightActivationStatus,

    /// The partition the activation was received from
    pub partition: i32,

    /// The offset the activation had
    pub offset: i64,

    /// The timestamp when the activation was stored in activation store.
    pub added_at: DateTime<Utc>,

    /// The number of times the activation has been attempted to be processed. This counter is
    /// incremented everytime a task is reset from processing back to pending. When this
    /// exceeds max_processing_attempts, the task is discarded/deadlettered.
    pub processing_attempts: i32,

    /// If the task has specified an expiry, this is the timestamp after which the task should be removed from inflight store
    pub expires_at: Option<DateTime<Utc>>,

    /// If the task has specified a delay, this is the timestamp after which the task can be sent to workers
    pub delay_until: Option<DateTime<Utc>>,

    /// The duration of the processing deadline
    pub processing_deadline_duration: u32,

    /// The timestamp for when processing should be complete
    pub processing_deadline: Option<DateTime<Utc>>,

    /// Whether or not the activation uses at_most_once.
    /// When enabled activations are not retried when processing_deadlines
    /// are exceeded.
    pub at_most_once: bool,

    pub namespace: String,

    /// What to do when the maximum number of attempts to complete a task is exceeded
    pub on_attempts_exceeded: InflightOnAttemptsExceeded,
}

impl InflightActivation {
    /// A convenience function to get the TaskActivation. This will panic if the
    /// activation isn't set. Mostly used in tests.
    pub fn get_activation(&self) -> TaskActivation {
        self.activation.clone().unwrap()
    }
}

/// The number of milliseconds between an activation's received timestamp
/// and the provided datetime
pub fn received_latency(
    activation: &TaskActivation,
    delay_until: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> i64 {
    let activation_received = activation.received_at.unwrap();
    let received_datetime = DateTime::from_timestamp(
        activation_received.seconds,
        activation_received.nanos as u32,
    );

    received_datetime.map_or(0, |received| {
        now.signed_duration_since(received).num_milliseconds()
            - delay_until.map_or(0, |delay_until| {
                delay_until
                    .signed_duration_since(received)
                    .num_milliseconds()
            })
    })
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
    pub to_discard: Vec<String>,
    pub to_deadletter: Vec<String>,
}

#[derive(Debug, FromRow)]
struct MetaTableRow {
    id: String,
    activation: Vec<u8>,
    partition: i32,
    offset: i64,
    added_at: DateTime<Utc>,
    processing_attempts: i32,
    expires_at: Option<DateTime<Utc>>,
    delay_until: Option<DateTime<Utc>>,
    processing_deadline_duration: u32,
    processing_deadline: Option<DateTime<Utc>>,
    status: InflightActivationStatus,
    at_most_once: bool,
    namespace: String,
    on_attempts_exceeded: InflightOnAttemptsExceeded,
}

impl TryFrom<InflightActivation> for MetaTableRow {
    type Error = anyhow::Error;

    fn try_from(value: InflightActivation) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            activation: value.activation.unwrap_or_default().encode_to_vec(),
            partition: value.partition,
            offset: value.offset,
            added_at: value.added_at,
            processing_attempts: value.processing_attempts,
            expires_at: value.expires_at,
            delay_until: value.delay_until,
            processing_deadline_duration: value.processing_deadline_duration,
            processing_deadline: value.processing_deadline,
            status: value.status,
            at_most_once: value.at_most_once,
            namespace: value.namespace.clone(),
            on_attempts_exceeded: value.on_attempts_exceeded,
        })
    }
}

impl From<MetaTableRow> for InflightActivation {
    fn from(value: MetaTableRow) -> Self {
        Self {
            id: value.id,
            activation: Some(TaskActivation::decode(&value.activation as &[u8]).expect(
                "Decode should always be successful as we only store encoded data in this column",
            )),
            status: value.status,
            partition: value.partition,
            offset: value.offset,
            added_at: value.added_at,
            processing_attempts: value.processing_attempts,
            expires_at: value.expires_at,
            delay_until: value.delay_until,
            processing_deadline_duration: value.processing_deadline_duration,
            processing_deadline: value.processing_deadline,
            at_most_once: value.at_most_once,
            namespace: value.namespace,
            on_attempts_exceeded: value.on_attempts_exceeded,
        }
    }
}

#[derive(Debug, FromRow)]
struct BlobTableRow {
    id: String,
    activation: Vec<u8>,
    added_at: DateTime<Utc>,
}

impl TryFrom<InflightActivation> for BlobTableRow {
    type Error = anyhow::Error;

    fn try_from(value: InflightActivation) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            activation: value.activation.unwrap().encode_to_vec(),
            added_at: value.added_at,
        })
    }
}

async fn create_sqlite_pool(url: &str) -> Result<(Pool<Sqlite>, Pool<Sqlite>), Error> {
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

pub struct InflightActivationStore {
    meta_read_pool: SqlitePool,
    meta_write_pool: SqlitePool,
    blob_write_pool: SqlitePool,
    blob_read_pool: SqlitePool,
    config: InflightActivationStoreConfig,
}

impl InflightActivationStore {
    pub async fn new(
        url: &str,
        blob_url: &str,
        config: InflightActivationStoreConfig,
    ) -> Result<Self, Error> {
        // let meta_result = create_sqlite_pool(url);
        let (meta_read_pool, meta_write_pool) = create_sqlite_pool(url).await?;
        let (blob_read_pool, blob_write_pool) = create_sqlite_pool(blob_url).await?;

        sqlx::migrate!("./migrations").run(&meta_write_pool).await?;
        sqlx::migrate!("./migrations/blob")
            .run(&blob_write_pool)
            .await?;

        Ok(Self {
            meta_read_pool,
            meta_write_pool,
            blob_read_pool,
            blob_write_pool,
            config,
        })
    }

    /// Trigger incremental vacuum to reclaim free pages in the database.
    #[instrument(skip_all)]
    pub async fn vacuum_db(&self) -> Result<(), Error> {
        let mut timer = Instant::now();
        sqlx::query("PRAGMA incremental_vacuum")
            .execute(&self.meta_write_pool)
            .await?;
        metrics::histogram!("store.vacuum", "database" => "meta").record(timer.elapsed());

        timer = Instant::now();
        sqlx::query("PRAGMA incremental_vacuum")
            .execute(&self.blob_write_pool)
            .await?;
        metrics::histogram!("store.vacuum", "database" => "blob").record(timer.elapsed());
        Ok(())
    }

    /// Convenience function to determine whether activation blobs should be read from the new db
    fn is_new_read(&self) -> bool {
        self.config.blob_dual_write == BlobTableMode::DualWriteNewRead
            || self.config.blob_dual_write == BlobTableMode::NewWriteNewRead
    }

    /// Convenience function to determine whether activation blobs should be written to the new db
    fn is_new_write(&self) -> bool {
        self.config.blob_dual_write == BlobTableMode::NewWriteNewRead
            || self.config.blob_dual_write == BlobTableMode::DualWriteOrigRead
            || self.config.blob_dual_write == BlobTableMode::DualWriteNewRead
    }

    /// Convenience function to determine whether activation blobs should be written to the original db
    fn is_orig_write(&self) -> bool {
        self.config.blob_dual_write == BlobTableMode::OrigWriteOrigRead
            || self.config.blob_dual_write == BlobTableMode::DualWriteOrigRead
            || self.config.blob_dual_write == BlobTableMode::DualWriteNewRead
    }

    /// Get an activation by id. Primarily used for testing
    pub async fn get_by_id(
        &self,
        id: &str,
        with_data: bool,
    ) -> Result<Option<InflightActivation>, Error> {
        let result = self.get_meta_by_id(id).await?;
        if result.is_none() {
            return Ok(result);
        }

        let mut inflight = result.unwrap();

        if self.is_new_read() && with_data && self.is_new_read() {
            let activation = self.get_activation_blob(id).await?;
            inflight.activation = Some(activation);
        }

        Ok(Some(inflight))
    }

    pub async fn get_meta_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
        let row_result: Option<MetaTableRow> = sqlx::query_as(
            "
            SELECT id,
                activation,
                partition,
                offset,
                added_at,
                processing_attempts,
                expires_at,
                delay_until,
                processing_deadline_duration,
                processing_deadline,
                on_attempts_exceeded,
                status,
                at_most_once,
                namespace
            FROM inflight_taskactivations
            WHERE id = $1
            ",
        )
        .bind(id)
        .fetch_optional(&self.meta_read_pool)
        .await?;

        let Some(row) = row_result else {
            return Ok(None);
        };

        Ok(Some(row.into()))
    }

    pub async fn get_activation_raw_blob(&self, id: &str) -> Result<Vec<u8>, Error> {
        if self.is_new_read() {
            let blob_result: Option<BlobTableRow> = sqlx::query_as(
                "
                SELECT id, activation, added_at
                FROM taskactivation_blobs 
                WHERE id = $1
                ",
            )
            .bind(id)
            .fetch_optional(&self.blob_read_pool)
            .await?;

            match blob_result {
                Some(blob_row) => Ok(blob_row.activation),
                None => Err(anyhow!("could not find blob for activation")),
            }
        } else {
            let inflight = self.get_meta_by_id(id).await?.unwrap();
            Ok(inflight.activation.unwrap().encode_to_vec())
        }
    }

    pub async fn get_activation_blob(&self, id: &str) -> Result<TaskActivation, Error> {
        let activation = self.get_activation_raw_blob(id).await?;
        let activation_data: &[u8] = &activation;
        Ok(TaskActivation::decode(activation_data)?)
    }

    #[instrument(skip_all)]
    pub async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        if batch.is_empty() {
            return Ok(QueryResult { rows_affected: 0 });
        }

        // A note about the consistency of the data in the two databases:
        // If there is data in the blob database that isn't in the meta database, that is not
        // a consistency issue. No extra tasks will get run or failed. The orphaned blobs
        // will get cleaned up. However if there is data in the meta database that is not reflected
        // in the blob database, that is a consistency error. As such, write the blobs first and
        // that succeeds, then attempt to write the meta. A failure on either write will cause the
        // entire batch to be failed.
        if self.is_new_write() {
            let mut blob_query_builder = QueryBuilder::<Sqlite>::new(
                "
                INSERT INTO taskactivation_blobs
                    (
                        id,
                        activation,
                        added_at
                    )
                ",
            );
            let blob_rows = batch
                .clone()
                .into_iter()
                .map(BlobTableRow::try_from)
                .collect::<Result<Vec<BlobTableRow>, _>>()?;
            let blob_query = blob_query_builder
                .push_values(blob_rows, |mut b, row| {
                    b.push_bind(row.id);
                    b.push_bind(row.activation);
                    b.push_bind(row.added_at.timestamp());
                })
                .push(" ON CONFLICT(id) DO NOTHING")
                .build();
            blob_query.execute(&self.blob_write_pool).await?;

            // Sync the WAL into the blob database so we don't lose data on host failure.
            let blob_checkpoint_result = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
                .fetch_one(&self.blob_write_pool)
                .await?;

            metrics::gauge!("store.pages_written_to_wal", "database" => "blob")
                .set(blob_checkpoint_result.get::<i32, _>("log"));
            metrics::gauge!("store.pages_committed_to_db", "database" => "blob")
                .set(blob_checkpoint_result.get::<i32, _>("checkpointed"));
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
                    processing_attempts,
                    expires_at,
                    delay_until,
                    processing_deadline_duration,
                    processing_deadline,
                    on_attempts_exceeded,
                    status,
                    at_most_once,
                    namespace
                )
            ",
        );
        let rows = batch
            .into_iter()
            .map(MetaTableRow::try_from)
            .collect::<Result<Vec<MetaTableRow>, _>>()?;
        let query = query_builder
            .push_values(rows, |mut b, row| {
                b.push_bind(row.id);
                if self.is_orig_write() {
                    b.push_bind(row.activation);
                } else {
                    b.push_bind(vec![]);
                }
                b.push_bind(row.partition);
                b.push_bind(row.offset);
                b.push_bind(row.added_at.timestamp());
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
                b.push_bind(row.on_attempts_exceeded);
                b.push_bind(row.status);
                b.push_bind(row.at_most_once);
                b.push_bind(row.namespace);
            })
            .push(" ON CONFLICT(id) DO NOTHING")
            .build();
        let meta_result = Ok(query.execute(&self.meta_write_pool).await?.into());

        // Sync the WAL into the main database so we don't lose data on host failure.
        let checkpoint_result = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
            .fetch_one(&self.meta_write_pool)
            .await?;

        metrics::gauge!("store.pages_written_to_wal", "database" => "meta")
            .set(checkpoint_result.get::<i32, _>("log"));
        metrics::gauge!("store.pages_committed_to_db", "database" => "meta")
            .set(checkpoint_result.get::<i32, _>("checkpointed"));

        meta_result
    }

    #[instrument(skip_all)]
    pub async fn get_pending_activation(
        &self,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
        let now = Utc::now();

        let mut query_builder = QueryBuilder::new(
            "
            UPDATE inflight_taskactivations
            SET
                processing_deadline = unixepoch(
                    'now', '+' || processing_deadline_duration || ' seconds'
                ),
                status = ",
        );
        query_builder.push_bind(InflightActivationStatus::Processing);
        query_builder.push(
            "
            WHERE id = (
                SELECT id
                FROM inflight_taskactivations
                WHERE status = ",
        );
        query_builder.push_bind(InflightActivationStatus::Pending);
        query_builder.push(" AND (expires_at IS NULL OR expires_at > ");
        query_builder.push_bind(now.timestamp());
        query_builder.push(")");

        if let Some(namespace) = namespace {
            query_builder.push(" AND namespace = ");
            query_builder.push_bind(namespace);
        }
        query_builder.push(" ORDER BY added_at LIMIT 1) RETURNING *");

        let result: Option<MetaTableRow> = query_builder
            .build_query_as::<MetaTableRow>()
            .fetch_optional(&self.meta_write_pool)
            .await?;
        let Some(row) = result else { return Ok(None) };

        Ok(Some(row.into()))
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
                .fetch_one(&self.meta_read_pool)
                .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    pub async fn count(&self) -> Result<usize, Error> {
        let result = sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations")
            .fetch_one(&self.meta_read_pool)
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
        let result: Option<MetaTableRow> = sqlx::query_as(
            "UPDATE inflight_taskactivations SET status = $1 WHERE id = $2 RETURNING *",
        )
        .bind(status)
        .bind(id)
        .fetch_optional(&self.meta_write_pool)
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
        sqlx::query("UPDATE inflight_taskactivations SET processing_deadline = $1 WHERE id = $2")
            .bind(deadline.unwrap().timestamp())
            .bind(id)
            .execute(&self.meta_write_pool)
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn delete_activation(&self, id: &str) -> Result<(), Error> {
        sqlx::query("DELETE FROM inflight_taskactivations WHERE id = $1")
            .bind(id)
            .execute(&self.meta_write_pool)
            .await?;

        sqlx::query("DELETE FROM taskactivation_blobs WHERE id = $1")
            .bind(id)
            .execute(&self.blob_write_pool)
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
                processing_attempts,
                expires_at,
                delay_until,
                processing_deadline_duration,
                processing_deadline,
                on_attempts_exceeded,
                status,
                at_most_once,
                namespace
            FROM inflight_taskactivations
            WHERE status = $1
            ",
        )
        .bind(InflightActivationStatus::Retry)
        .fetch_all(&self.meta_read_pool)
        .await?
        .into_iter()
        .map(|row: MetaTableRow| row.into())
        .collect())
    }

    pub async fn clear(&self) -> Result<(), Error> {
        sqlx::query("DELETE FROM inflight_taskactivations")
            .execute(&self.meta_write_pool)
            .await?;

        sqlx::query("DELETE FROM taskactivation_blobs")
            .execute(&self.blob_write_pool)
            .await?;
        Ok(())
    }

    /// Update tasks that are in processing and have exceeded their processing deadline
    /// Exceeding a processing deadline does not consume a retry as we don't know
    /// if a worker took the task and was killed, or failed.
    #[instrument(skip_all)]
    pub async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut atomic = self.meta_write_pool.begin().await?;

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
        let processing_attempts_result = sqlx::query(
            "UPDATE inflight_taskactivations 
            SET status = $1 
            WHERE processing_attempts >= $2 AND status = $3",
        )
        .bind(InflightActivationStatus::Failure)
        .bind(self.config.max_processing_attempts as i32)
        .bind(InflightActivationStatus::Pending)
        .execute(&self.meta_write_pool)
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
        let query = sqlx::query(
            "DELETE FROM inflight_taskactivations WHERE status = $1 AND expires_at IS NOT NULL AND expires_at < $2",
        )
        .bind(InflightActivationStatus::Pending)
        .bind(now.timestamp())
        .execute(&self.meta_write_pool)
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
        let update_result = sqlx::query(
            r#"UPDATE inflight_taskactivations
            SET status = $1
            WHERE delay_until IS NOT NULL AND delay_until < $2 AND status = $3
            "#,
        )
        .bind(InflightActivationStatus::Pending)
        .bind(now.timestamp())
        .bind(InflightActivationStatus::Delay)
        .execute(&self.meta_write_pool)
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
        let mut atomic = self.meta_write_pool.begin().await?;

        let failed_tasks: Vec<SqliteRow> = sqlx::query(
            "SELECT id, on_attempts_exceeded FROM inflight_taskactivations WHERE status = $1",
        )
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
            // We could be deadlettering because of activation.expires
            // when a task expires we still deadletter if configured.
            // let retry_state = &activation.retry_state.as_ref().unwrap();
            let id: &str = record.get("id");
            let infl_on_attempts_exceeded: InflightOnAttemptsExceeded =
                record.get("on_attempts_exceeded");
            let on_attempts_exceeded: OnAttemptsExceeded = infl_on_attempts_exceeded.into();

            if on_attempts_exceeded == OnAttemptsExceeded::Discard
                || on_attempts_exceeded == OnAttemptsExceeded::Unspecified
            {
                forwarder.to_discard.push(id.to_string())
            } else if on_attempts_exceeded == OnAttemptsExceeded::Deadletter {
                forwarder.to_deadletter.push(id.to_string())
            }
        }

        if !forwarder.to_discard.is_empty() {
            let mut query_builder = QueryBuilder::new("UPDATE inflight_taskactivations ");
            query_builder
                .push("SET status = ")
                .push_bind(InflightActivationStatus::Complete)
                .push(" WHERE id IN (");

            let mut separated = query_builder.separated(", ");
            for id in forwarder.to_discard.iter() {
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
        let result = query_builder.build().execute(&self.meta_write_pool).await?;

        Ok(result.rows_affected())
    }

    /// Remove completed tasks.
    /// This method is a garbage collector for the inflight task store.
    #[instrument(skip_all)]
    pub async fn remove_completed(&self) -> Result<u64, Error> {
        let query = sqlx::query("DELETE FROM inflight_taskactivations WHERE status = $1")
            .bind(InflightActivationStatus::Complete)
            .execute(&self.meta_write_pool)
            .await?;

        Ok(query.rows_affected())
    }

    /// Remove orphaned blobs that don't have a corresponding ID in the meta table.
    /// This method is a garbage collector for the blob store.
    #[instrument(skip_all)]
    pub async fn remove_orphaned_blobs(&self) -> Result<u64, Error> {
        let active_ids: Vec<SqliteRow> = sqlx::query("SELECT id FROM inflight_taskactivations")
            .fetch_all(&self.meta_read_pool)
            .await?
            .into_iter()
            .collect();

        let expiry_cutoff = Utc::now() - Duration::from_millis(self.config.blob_delete_delay_ms);
        let mut query_builder = QueryBuilder::new("DELETE FROM taskactivation_blobs ");
        query_builder
            .push(" WHERE added_at <= ")
            .push_bind(expiry_cutoff.timestamp())
            .push("AND id NOT IN (");

        let mut separated = query_builder.separated(", ");
        for record in active_ids.iter() {
            let id: &str = record.get("id");
            separated.push_bind(id.to_string());
        }
        separated.push_unseparated(")");

        let result = query_builder.build().execute(&self.blob_write_pool).await?;

        Ok(result.rows_affected())
    }
}
