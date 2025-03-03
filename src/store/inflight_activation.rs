use std::{str::FromStr, time::Instant};

use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use prost::Message;
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivation, TaskActivationStatus};
use sqlx::{
    ConnectOptions, FromRow, QueryBuilder, Row, Sqlite, Type,
    migrate::MigrateDatabase,
    pool::PoolOptions,
    sqlite::{
        SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqliteQueryResult,
        SqliteRow, SqliteSynchronous,
    },
};
use tracing::info;

use crate::config::Config;

pub struct InflightActivationStoreConfig {
    pub max_processing_attempts: usize,
    pub enable_wal_checkpoint_log: bool,
}

impl InflightActivationStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_processing_attempts: config.max_processing_attempts,
            enable_wal_checkpoint_log: config.enable_wal_checkpoint_log,
        }
    }
}

pub struct InflightActivationStore {
    sqlite_pool: SqlitePool,
    config: InflightActivationStoreConfig,
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

#[derive(Clone, Debug, PartialEq)]
pub struct InflightActivation {
    /// The protobuf activation that was received from kafka
    pub activation: TaskActivation,

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

    /// The timestamp for when processing should be complete
    pub processing_deadline: Option<DateTime<Utc>>,

    /// Whether or not the activation uses at_most_once.
    /// When enabled activations are not retried when processing_deadlines
    /// are exceeded.
    pub at_most_once: bool,
    pub namespace: String,
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
    pub to_deadletter: Vec<TaskActivation>,
}

#[derive(Debug, FromRow)]
struct TableRow {
    id: String,
    activation: Vec<u8>,
    partition: i32,
    offset: i64,
    added_at: DateTime<Utc>,
    processing_attempts: i32,
    expires_at: Option<DateTime<Utc>>,
    processing_deadline_duration: u32,
    processing_deadline: Option<DateTime<Utc>>,
    status: InflightActivationStatus,
    at_most_once: bool,
    namespace: String,
}

impl TryFrom<InflightActivation> for TableRow {
    type Error = anyhow::Error;

    fn try_from(value: InflightActivation) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.activation.id.clone(),
            activation: value.activation.encode_to_vec(),
            partition: value.partition,
            offset: value.offset,
            added_at: value.added_at,
            processing_attempts: value.processing_attempts,
            expires_at: value.expires_at,
            processing_deadline_duration: value.activation.processing_deadline_duration as u32,
            processing_deadline: value.processing_deadline,
            status: value.status,
            at_most_once: value.at_most_once,
            namespace: value.namespace.clone(),
        })
    }
}

impl From<TableRow> for InflightActivation {
    fn from(value: TableRow) -> Self {
        Self {
            activation: TaskActivation::decode(&value.activation as &[u8]).expect(
                "Decode should always be successful as we only store encoded data in this column",
            ),
            status: value.status,
            partition: value.partition,
            offset: value.offset,
            added_at: value.added_at,
            processing_attempts: value.processing_attempts,
            expires_at: value.expires_at,
            processing_deadline: value.processing_deadline,
            at_most_once: value.at_most_once,
            namespace: value.namespace,
        }
    }
}

impl InflightActivationStore {
    pub async fn new(url: &str, config: InflightActivationStoreConfig) -> Result<Self, Error> {
        if !Sqlite::database_exists(url).await? {
            Sqlite::create_database(url).await?
        }

        let conn_options = SqliteConnectOptions::from_str(url)?
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .auto_vacuum(SqliteAutoVacuum::Incremental)
            .disable_statement_logging();

        let sqlite_pool = PoolOptions::<Sqlite>::new()
            .max_connections(1)
            .connect_with(conn_options)
            .await?;

        sqlx::migrate!("./migrations").run(&sqlite_pool).await?;

        Ok(Self {
            sqlite_pool,
            config,
        })
    }

    /// Trigger incremental vacuum to reclaim free pages in the database.
    pub async fn vacuum_db(&self) -> Result<(), Error> {
        let timer = Instant::now();
        sqlx::query("PRAGMA incremental_vacuum")
            .execute(&self.sqlite_pool)
            .await?;
        metrics::histogram!("store.vacuum").record(timer.elapsed());
        Ok(())
    }

    /// Get an activation by id. Primarily used for testing
    pub async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
        let row_result: Option<TableRow> =
            sqlx::query_as("SELECT * FROM inflight_taskactivations WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.sqlite_pool)
                .await?;

        let Some(row) = row_result else {
            return Ok(None);
        };

        Ok(Some(row.into()))
    }

    pub async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        if batch.is_empty() {
            return Ok(QueryResult { rows_affected: 0 });
        }
        let mut query_builder = QueryBuilder::<Sqlite>::new(
            "INSERT INTO inflight_taskactivations \
            (id, activation, partition, offset, added_at, processing_attempts, expires_at, processing_deadline_duration, processing_deadline, status, at_most_once, namespace)",
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
                b.push_bind(row.processing_attempts);
                b.push_bind(row.expires_at.map(|t| Some(t.timestamp())));
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
            })
            .push(" ON CONFLICT(id) DO NOTHING")
            .build();
        let result = Ok(query.execute(&self.sqlite_pool).await?.into());

        // Sync the WAL into the main database so we don't lose data on host failure.
        let wal_checkpoint_result = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
            .fetch_one(&self.sqlite_pool)
            .await?;

        if self.config.enable_wal_checkpoint_log {
            let results = [
                wal_checkpoint_result.get::<i32, _>("busy"),
                wal_checkpoint_result.get::<i32, _>("log"),
                wal_checkpoint_result.get::<i32, _>("checkpointed"),
            ];
            info!("PRAGMA wal_checkpoint(PASSIVE) results: {:?}", results);
        }

        result
    }

    pub async fn get_pending_activation(
        &self,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
        let now = Utc::now();

        let mut query_builder = QueryBuilder::new(
            "UPDATE inflight_taskactivations
            SET processing_deadline = unixepoch('now', '+' || processing_deadline_duration || ' seconds'), status = ",
        );
        query_builder.push_bind(InflightActivationStatus::Processing);
        query_builder.push(
            " WHERE id = (
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

        let result: Option<TableRow> = query_builder
            .build_query_as::<TableRow>()
            .fetch_optional(&self.sqlite_pool)
            .await?;
        let Some(row) = result else { return Ok(None) };

        Ok(Some(row.into()))
    }

    pub async fn count_pending_activations(&self) -> Result<usize, Error> {
        self.count_by_status(InflightActivationStatus::Pending)
            .await
    }

    pub async fn count_by_status(&self, status: InflightActivationStatus) -> Result<usize, Error> {
        let result =
            sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations WHERE status = $1")
                .bind(status)
                .fetch_one(&self.sqlite_pool)
                .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    pub async fn count(&self) -> Result<usize, Error> {
        let result = sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations")
            .fetch_one(&self.sqlite_pool)
            .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    /// Update the status of a specific activation
    pub async fn set_status(
        &self,
        id: &str,
        status: InflightActivationStatus,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE inflight_taskactivations SET status = $1 WHERE id = $2")
            .bind(status)
            .bind(id)
            .execute(&self.sqlite_pool)
            .await?;
        Ok(())
    }

    pub async fn set_processing_deadline(
        &self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE inflight_taskactivations SET processing_deadline = $1 WHERE id = $2")
            .bind(deadline.unwrap().timestamp())
            .bind(id)
            .execute(&self.sqlite_pool)
            .await?;
        Ok(())
    }

    pub async fn delete_activation(&self, id: &str) -> Result<(), Error> {
        sqlx::query("DELETE FROM inflight_taskactivations WHERE id = $1")
            .bind(id)
            .execute(&self.sqlite_pool)
            .await?;
        Ok(())
    }

    pub async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        Ok(
            sqlx::query_as("SELECT * FROM inflight_taskactivations WHERE status = $1")
                .bind(InflightActivationStatus::Retry)
                .fetch_all(&self.sqlite_pool)
                .await?
                .into_iter()
                .map(|row: TableRow| row.into())
                .collect(),
        )
    }

    pub async fn clear(&self) -> Result<(), Error> {
        sqlx::query("DELETE FROM inflight_taskactivations")
            .execute(&self.sqlite_pool)
            .await?;
        Ok(())
    }

    /// Update tasks that are in processing and have exceeded their processing deadline
    /// Exceeding a processing deadline does not consume a retry as we don't know
    /// if a worker took the task and was killed, or failed.
    pub async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        let now = Utc::now();

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
        .execute(&self.sqlite_pool)
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
        .execute(&self.sqlite_pool)
        .await;

        if let Ok(query_res) = result {
            processing_deadline_modified_rows += query_res.rows_affected();
            return Ok(processing_deadline_modified_rows);
        }

        Err(anyhow!("Could not update tasks past processing_deadline"))
    }

    /// Update tasks that have exceeded their max processing attempts.
    /// These tasks are set to status=failure and will be handled by handle_failed_tasks accordingly.
    pub async fn handle_processing_attempts(&self) -> Result<u64, Error> {
        let processing_attempts_result = sqlx::query(
            "UPDATE inflight_taskactivations 
            SET status = $1 
            WHERE processing_attempts >= $2 AND status = $3",
        )
        .bind(InflightActivationStatus::Failure)
        .bind(self.config.max_processing_attempts as i32)
        .bind(InflightActivationStatus::Pending)
        .execute(&self.sqlite_pool)
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
    pub async fn handle_expires_at(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let update_result = sqlx::query(
            r#"UPDATE inflight_taskactivations
            SET status = $1
            WHERE expires_at IS NOT NULL AND expires_at < $2 AND status = $3
            "#,
        )
        .bind(InflightActivationStatus::Failure)
        .bind(now.timestamp())
        .bind(InflightActivationStatus::Pending)
        .execute(&self.sqlite_pool)
        .await?;

        Ok(update_result.rows_affected())
    }

    /// Perform upkeep work related to status=failure
    ///
    /// Activations that are status=failure need to either be discarded by setting status=complete
    /// or need to be moved to deadletter and are returned in the Result.
    /// Once dead-lettered tasks have been added to Kafka those tasks can have their status set to
    /// complete.
    pub async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
        let mut atomic = self.sqlite_pool.begin().await?;

        let failed_tasks: Vec<SqliteRow> =
            sqlx::query("SELECT activation FROM inflight_taskactivations WHERE status = $1")
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
            let activation = TaskActivation::decode(activation_data)?;

            // Without a retry state, tasks are discarded
            if activation.retry_state.as_ref().is_none() {
                forwarder.to_discard.push(activation.id);
                continue;
            }
            // We could be deadlettering because of activation.expires
            // when a task expires we still deadletter if configured.
            let retry_state = &activation.retry_state.as_ref().unwrap();
            if retry_state.on_attempts_exceeded == OnAttemptsExceeded::Discard as i32
                || retry_state.on_attempts_exceeded == OnAttemptsExceeded::Unspecified as i32
            {
                forwarder.to_discard.push(activation.id)
            } else if retry_state.on_attempts_exceeded == OnAttemptsExceeded::Deadletter as i32 {
                forwarder.to_deadletter.push(activation)
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
        let result = query_builder.build().execute(&self.sqlite_pool).await?;

        Ok(result.rows_affected())
    }

    /// Remove completed tasks.
    /// This method is a garbage collector for the inflight task store.
    pub async fn remove_completed(&self) -> Result<u64, Error> {
        let query = sqlx::query("DELETE FROM inflight_taskactivations WHERE status = $1")
            .bind(InflightActivationStatus::Complete)
            .execute(&self.sqlite_pool)
            .await?;

        Ok(query.rows_affected())
    }
}
