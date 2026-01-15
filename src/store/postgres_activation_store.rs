use crate::store::inflight_activation::{
    FailedTasksForwarder, InflightActivation, InflightActivationStatus, InflightActivationStore,
    QueryResult, TableRow,
};
use anyhow::{Error, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sentry_protos::taskbroker::v1::OnAttemptsExceeded;
use sqlx::{
    Pool, Postgres, QueryBuilder, Row,
    pool::PoolConnection,
    postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow},
};
use std::{str::FromStr, time::Instant};
use tracing::{instrument, warn};

use crate::config::Config;

pub async fn create_postgres_pool(
    url: &str,
    database_name: &str,
) -> Result<(Pool<Postgres>, Pool<Postgres>), Error> {
    let conn_str = url.to_owned() + "/" + database_name;
    let read_pool = PgPoolOptions::new()
        .max_connections(64)
        .connect_with(PgConnectOptions::from_str(&conn_str)?)
        .await?;

    let write_pool = PgPoolOptions::new()
        .max_connections(64)
        .connect_with(PgConnectOptions::from_str(&conn_str)?)
        .await?;
    Ok((read_pool, write_pool))
}

pub async fn create_default_postgres_pool(url: &str) -> Result<Pool<Postgres>, Error> {
    let conn_str = url.to_owned() + "/postgres";
    let default_pool = PgPoolOptions::new()
        .max_connections(64)
        .connect_with(PgConnectOptions::from_str(&conn_str)?)
        .await?;
    Ok(default_pool)
}

pub struct PostgresActivationStoreConfig {
    pub pg_url: String,
    pub pg_database_name: String,
    pub run_migrations: bool,
    pub max_processing_attempts: usize,
    pub processing_deadline_grace_sec: u64,
    pub vacuum_page_count: Option<usize>,
    pub enable_sqlite_status_metrics: bool,
}

impl PostgresActivationStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            pg_url: config.pg_url.clone(),
            pg_database_name: config.pg_database_name.clone(),
            run_migrations: config.run_migrations,
            max_processing_attempts: config.max_processing_attempts,
            vacuum_page_count: config.vacuum_page_count,
            processing_deadline_grace_sec: config.processing_deadline_grace_sec,
            enable_sqlite_status_metrics: config.enable_sqlite_status_metrics,
        }
    }
}

pub struct PostgresActivationStore {
    read_pool: PgPool,
    write_pool: PgPool,
    config: PostgresActivationStoreConfig,
}

impl PostgresActivationStore {
    async fn acquire_write_conn_metric(
        &self,
        caller: &'static str,
    ) -> Result<PoolConnection<Postgres>, Error> {
        let start = Instant::now();
        let conn = self.write_pool.acquire().await?;
        metrics::histogram!("postgres.write.acquire_conn", "fn" => caller).record(start.elapsed());
        Ok(conn)
    }

    pub async fn new(config: PostgresActivationStoreConfig) -> Result<Self, Error> {
        if config.run_migrations {
            let default_pool = create_default_postgres_pool(&config.pg_url).await?;

            // Create the database if it doesn't exist
            let row: (bool,) = sqlx::query_as(
                "SELECT EXISTS ( SELECT 1 FROM pg_catalog.pg_database WHERE datname = $1 )",
            )
            .bind(&config.pg_database_name)
            .fetch_one(&default_pool)
            .await?;

            if !row.0 {
                println!("Creating database {}", &config.pg_database_name);
                sqlx::query(format!("CREATE DATABASE {}", &config.pg_database_name).as_str())
                    .bind(&config.pg_database_name)
                    .execute(&default_pool)
                    .await?;
            }
            // Close the default pool
            default_pool.close().await;
        }

        let (read_pool, write_pool) =
            create_postgres_pool(&config.pg_url, &config.pg_database_name).await?;

        if config.run_migrations {
            println!("Running migrations on database");
            sqlx::migrate!("./pg_migrations").run(&write_pool).await?;
        }

        Ok(Self {
            read_pool,
            write_pool,
            config,
        })
    }
}

#[async_trait]
impl InflightActivationStore for PostgresActivationStore {
    /// Trigger incremental vacuum to reclaim free pages in the database.
    /// Depending on config data, will either vacuum a set number of
    /// pages or attempt to reclaim all free pages.
    #[instrument(skip_all)]
    async fn vacuum_db(&self) -> Result<(), Error> {
        // TODO: Remove
        Ok(())
    }

    /// Perform a full vacuum on the database.
    async fn full_vacuum_db(&self) -> Result<(), Error> {
        // TODO: Remove
        Ok(())
    }

    /// Get the size of the database in bytes based on SQLite metadata queries.
    async fn db_size(&self) -> Result<u64, Error> {
        let row_result: (i64,) = sqlx::query_as("SELECT pg_database_size($1) as size")
            .bind(&self.config.pg_database_name)
            .fetch_one(&self.read_pool)
            .await?;
        if row_result.0 < 0 {
            return Ok(0);
        }
        Ok(row_result.0 as u64)
    }

    /// Get an activation by id. Primarily used for testing
    async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
        let row_result: Option<TableRow> = sqlx::query_as(
            "
            SELECT id,
                activation,
                partition,
                kafka_offset AS offset,
                added_at,
                received_at,
                processing_attempts,
                expires_at,
                delay_until,
                processing_deadline_duration,
                processing_deadline,
                status,
                at_most_once,
                application,
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
    async fn store(&self, batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
        if batch.is_empty() {
            return Ok(QueryResult { rows_affected: 0 });
        }
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "
            INSERT INTO inflight_taskactivations
                (
                    id,
                    activation,
                    partition,
                    kafka_offset,
                    added_at,
                    received_at,
                    processing_attempts,
                    expires_at,
                    delay_until,
                    processing_deadline_duration,
                    processing_deadline,
                    status,
                    at_most_once,
                    application,
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
                b.push_bind(row.added_at);
                b.push_bind(row.received_at);
                b.push_bind(row.processing_attempts);
                b.push_bind(row.expires_at);
                b.push_bind(row.delay_until);
                b.push_bind(row.processing_deadline_duration);
                if let Some(deadline) = row.processing_deadline {
                    b.push_bind(deadline);
                } else {
                    // Add a literal null
                    b.push("null");
                }
                b.push_bind(row.status);
                b.push_bind(row.at_most_once);
                b.push_bind(row.application);
                b.push_bind(row.namespace);
                b.push_bind(row.taskname);
                b.push_bind(row.on_attempts_exceeded as i32);
            })
            .push(" ON CONFLICT(id) DO NOTHING")
            .build();
        let mut conn = self.acquire_write_conn_metric("store").await?;
        Ok(query.execute(&mut *conn).await?.into())
    }

    #[instrument(skip_all)]
    async fn get_pending_activation(
        &self,
        application: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<Option<InflightActivation>, Error> {
        // Convert single namespace to vector for internal use
        let namespaces = namespace.map(|ns| vec![ns.to_string()]);

        // If a namespace filter is used, an application must also be used.
        if namespaces.is_some() && application.is_none() {
            warn!(
                "Received request for namespaced task without application. namespaces = {namespaces:?}"
            );
            return Ok(None);
        }
        let result = self
            .get_pending_activations_from_namespaces(application, namespaces.as_deref(), Some(1))
            .await?;
        if result.is_empty() {
            return Ok(None);
        }
        Ok(Some(result[0].clone()))
    }

    /// Get a pending activation from specified namespaces
    /// If namespaces is None, gets from any namespace
    /// If namespaces is Some(&[...]), gets from those namespaces
    #[instrument(skip_all)]
    async fn get_pending_activations_from_namespaces(
        &self,
        application: Option<&str>,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
    ) -> Result<Vec<InflightActivation>, Error> {
        let now = Utc::now();

        let grace_period = self.config.processing_deadline_grace_sec;
        let mut query_builder = QueryBuilder::new(
            "WITH selected_activations AS (
                SELECT id
                FROM inflight_taskactivations
                WHERE status = ",
        );
        query_builder.push_bind(InflightActivationStatus::Pending.to_string());
        query_builder.push(" AND (expires_at IS NULL OR expires_at > ");
        query_builder.push_bind(now);
        query_builder.push(")");

        // Handle application & namespace filtering
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
        query_builder.push(" ORDER BY added_at");
        if let Some(limit) = limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit);
        }
        query_builder.push(" FOR UPDATE SKIP LOCKED)");
        query_builder.push(format!(
            "UPDATE inflight_taskactivations
            SET
                processing_deadline = now() + (processing_deadline_duration * interval '1 second') + (interval '{grace_period} seconds'),
                status = "
        ));
        query_builder.push_bind(InflightActivationStatus::Processing.to_string());
        query_builder.push(" FROM selected_activations ");
        query_builder.push(" WHERE inflight_taskactivations.id = selected_activations.id");
        query_builder.push(" RETURNING *, kafka_offset AS offset");

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
        .bind(InflightActivationStatus::Pending.to_string())
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
    async fn count_pending_activations(&self) -> Result<usize, Error> {
        self.count_by_status(InflightActivationStatus::Pending)
            .await
    }

    #[instrument(skip_all)]
    async fn count_by_status(&self, status: InflightActivationStatus) -> Result<usize, Error> {
        let result =
            sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations WHERE status = $1")
                .bind(status.to_string())
                .fetch_one(&self.read_pool)
                .await?;
        Ok(result.get::<i64, _>("count") as usize)
    }

    async fn count(&self) -> Result<usize, Error> {
        let result = sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations")
            .fetch_one(&self.read_pool)
            .await?;
        Ok(result.get::<i64, _>("count") as usize)
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
            "UPDATE inflight_taskactivations SET status = $1 WHERE id = $2 RETURNING *, kafka_offset AS offset",
        )
        .bind(status.to_string())
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
            .bind(deadline.unwrap())
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
                kafka_offset AS offset,
                added_at,
                received_at,
                processing_attempts,
                expires_at,
                delay_until,
                processing_deadline_duration,
                processing_deadline,
                status,
                at_most_once,
                application,
                namespace,
                taskname,
                on_attempts_exceeded
            FROM inflight_taskactivations
            WHERE status = $1
            ",
        )
        .bind(InflightActivationStatus::Retry.to_string())
        .fetch_all(&self.read_pool)
        .await?
        .into_iter()
        .map(|row: TableRow| row.into())
        .collect())
    }

    // Used in tests
    async fn clear(&self) -> Result<(), Error> {
        let mut conn = self.acquire_write_conn_metric("clear").await?;
        sqlx::query("TRUNCATE TABLE inflight_taskactivations")
            .execute(&mut *conn)
            .await?;

        Ok(())
    }

    /// Update tasks that are in processing and have exceeded their processing deadline
    /// Exceeding a processing deadline does not consume a retry as we don't know
    /// if a worker took the task and was killed, or failed.
    #[instrument(skip_all)]
    async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut atomic = self.write_pool.begin().await?;

        // At-most-once tasks that fail their processing deadlines go directly to failure
        // there are no retries, as the worker will reject the task due to at_most_once keys.
        let most_once_result = sqlx::query(
            "UPDATE inflight_taskactivations
            SET processing_deadline = null, status = $1
            WHERE processing_deadline < $2 AND at_most_once = TRUE AND status = $3",
        )
        .bind(InflightActivationStatus::Failure.to_string())
        .bind(now)
        .bind(InflightActivationStatus::Processing.to_string())
        .execute(&mut *atomic)
        .await;

        let mut processing_deadline_modified_rows = 0;
        if let Ok(query_res) = most_once_result {
            processing_deadline_modified_rows = query_res.rows_affected();
        }

        // Update regular tasks.
        // Increment processing_attempts by 1 and reset processing_deadline to null.
        let result = sqlx::query(
            "UPDATE inflight_taskactivations
            SET processing_deadline = null, status = $1, processing_attempts = processing_attempts + 1
            WHERE processing_deadline < $2 AND status = $3",
        )
        .bind(InflightActivationStatus::Pending.to_string())
        .bind(now)
        .bind(InflightActivationStatus::Processing.to_string())
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
        .bind(InflightActivationStatus::Failure.to_string())
        .bind(self.config.max_processing_attempts as i32)
        .bind(InflightActivationStatus::Pending.to_string())
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
        .bind(InflightActivationStatus::Pending.to_string())
        .bind(now)
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
        .bind(InflightActivationStatus::Pending.to_string())
        .bind(now)
        .bind(InflightActivationStatus::Delay.to_string())
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

        let failed_tasks: Vec<PgRow> =
            sqlx::query("SELECT id, activation, on_attempts_exceeded FROM inflight_taskactivations WHERE status = $1")
                .bind(InflightActivationStatus::Failure.to_string())
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
                .push_bind(InflightActivationStatus::Complete.to_string())
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
            .push_bind(InflightActivationStatus::Complete.to_string())
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
            .bind(InflightActivationStatus::Complete.to_string())
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

    // Used in tests
    async fn remove_db(&self) -> Result<(), Error> {
        self.read_pool.close().await;
        self.write_pool.close().await;
        let default_pool = create_default_postgres_pool(&self.config.pg_url).await?;
        let _ = sqlx::query(format!("DROP DATABASE {}", &self.config.pg_database_name).as_str())
            .bind(&self.config.pg_database_name)
            .execute(&default_pool)
            .await;
        let _ = default_pool.close().await;
        Ok(())
    }
}
