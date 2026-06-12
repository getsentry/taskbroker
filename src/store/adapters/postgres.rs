use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::RwLock;
use std::time::Instant;

use sqlx::ConnectOptions;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};
use sqlx::{FromRow, Pool, Postgres, QueryBuilder, Row, Transaction};

use anyhow::{Error, Result, anyhow};
use async_backtrace::framed;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use prost::Message;
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivation};
use tracing::{instrument, warn};

use crate::config::Config;
use crate::store::activation::{Activation, ActivationStatus};
use crate::store::retry::{RetryConfig, retry_query};
use crate::store::traits::ActivationStore;
use crate::store::types::{BucketRange, DepthCounts, FailedTasksForwarder};

/// Run migrations.
pub async fn migrate(config: &Config) -> Result<()> {
    let mut conn_opts = PgConnectOptions::new()
        .username(&config.store.pg.ddl_username)
        .password(&config.store.pg.ddl_password)
        .host(&config.store.pg.host)
        .port(config.store.pg.port);

    if let Some(extra_query_params) = config.store.pg.query_params.as_ref() {
        let url = conn_opts.to_url_lossy();
        let new_url =
            url.as_ref().split('?').next().unwrap().to_string() + "?" + extra_query_params;
        conn_opts = PgConnectOptions::from_str(&new_url).unwrap();
    }

    let default_pool =
        create_default_postgres_pool(&conn_opts, &config.store.pg.default_database_name).await?;

    // Create the database if it doesn't exist
    let row: (bool,) =
        sqlx::query_as("SELECT EXISTS ( SELECT 1 FROM pg_catalog.pg_database WHERE datname = $1 )")
            .bind(&config.store.pg.database_name)
            .fetch_one(&default_pool)
            .await?;

    if !row.0 {
        println!("Creating database {}", &config.store.pg.database_name);
        sqlx::query(format!("CREATE DATABASE {}", &config.store.pg.database_name).as_str())
            .execute(&default_pool)
            .await?;
    }

    default_pool.close().await;

    let migration_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(conn_opts.database(&config.store.pg.database_name))
        .await?;

    println!("Running migrations on database");
    sqlx::migrate!("./migrations/postgres")
        .run(&migration_pool)
        .await?;

    migration_pool.close().await;

    Ok(())
}

/// Database representation of an [`Activation`], used for both reads and
/// writes.
///
/// On the write path it is built with `TableRow::from(&Activation)` and
/// borrows the activation's strings and payload, so storing a batch does not
/// copy it. On the read path sqlx decodes a fully owned `TableRow<'static>`,
/// which is converted into an [`Activation`] without further copies.
#[derive(Debug)]
struct TableRow<'a> {
    pub id: Cow<'a, str>,
    pub activation: Cow<'a, [u8]>,
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
    pub status: Cow<'a, str>,
    pub at_most_once: bool,
    pub application: Cow<'a, str>,
    pub namespace: Cow<'a, str>,
    pub taskname: Cow<'a, str>,
    pub on_attempts_exceeded: OnAttemptsExceeded,
    pub bucket: i16,
}

impl<'a> From<&'a Activation> for TableRow<'a> {
    fn from(value: &'a Activation) -> Self {
        Self {
            id: Cow::Borrowed(&value.id),
            activation: Cow::Borrowed(&value.activation),
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
            status: Cow::Owned(value.status.to_string()),
            at_most_once: value.at_most_once,
            application: Cow::Borrowed(&value.application),
            namespace: Cow::Borrowed(&value.namespace),
            taskname: Cow::Borrowed(&value.taskname),
            on_attempts_exceeded: value.on_attempts_exceeded,
            bucket: value.bucket,
        }
    }
}

impl From<TableRow<'_>> for Activation {
    fn from(value: TableRow<'_>) -> Self {
        // On the read path we're using TableRow<'static>, which already has
        // owned strings inside. Therefore into_owned() does nothing.
        Self {
            id: value.id.into_owned(),
            activation: value.activation.into_owned(),
            status: ActivationStatus::from_str(&value.status).unwrap(),
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
            application: value.application.into_owned(),
            namespace: value.namespace.into_owned(),
            taskname: value.taskname.into_owned(),
            on_attempts_exceeded: value.on_attempts_exceeded,
            bucket: value.bucket,
        }
    }
}

/// Decode an owned row. Hand-written rather than derived because the derive
/// would bind the struct's lifetime parameter to the row's, which
/// `query_as`/`fetch_all` (which drop the row) cannot express.
impl FromRow<'_, PgRow> for TableRow<'static> {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: Cow::Owned(row.try_get::<String, _>("id")?),
            activation: Cow::Owned(row.try_get::<Vec<u8>, _>("activation")?),
            partition: row.try_get("partition")?,
            offset: row.try_get("offset")?,
            added_at: row.try_get("added_at")?,
            received_at: row.try_get("received_at")?,
            processing_attempts: row.try_get("processing_attempts")?,
            expires_at: row.try_get("expires_at")?,
            delay_until: row.try_get("delay_until")?,
            processing_deadline_duration: row.try_get("processing_deadline_duration")?,
            processing_deadline: row.try_get("processing_deadline")?,
            claim_expires_at: row.try_get("claim_expires_at")?,
            status: Cow::Owned(row.try_get::<String, _>("status")?),
            at_most_once: row.try_get("at_most_once")?,
            application: Cow::Owned(row.try_get::<String, _>("application")?),
            namespace: Cow::Owned(row.try_get::<String, _>("namespace")?),
            taskname: Cow::Owned(row.try_get::<String, _>("taskname")?),
            on_attempts_exceeded: row
                .try_get::<i32, _>("on_attempts_exceeded")?
                .try_into()
                .map_err(|err| sqlx::Error::ColumnDecode {
                    index: "on_attempts_exceeded".into(),
                    source: Box::new(err),
                })?,
            bucket: row.try_get("bucket")?,
        })
    }
}

#[framed]
pub async fn create_postgres_pool(
    connection: &PgConnectOptions,
    database_name: &str,
) -> Result<(Pool<Postgres>, Pool<Postgres>), Error> {
    let conn_opts = connection.clone().database(database_name);
    let read_pool = PgPoolOptions::new()
        .max_connections(64)
        .connect_with(conn_opts.clone())
        .await?;

    let write_pool = PgPoolOptions::new()
        .max_connections(64)
        .connect_with(conn_opts)
        .await?;
    Ok((read_pool, write_pool))
}

#[framed]
pub async fn create_default_postgres_pool(
    connection: &PgConnectOptions,
    default_database_name: &str,
) -> Result<Pool<Postgres>, Error> {
    let conn_opts = connection.clone().database(default_database_name);
    let default_pool = PgPoolOptions::new()
        .max_connections(64)
        .connect_with(conn_opts)
        .await?;
    Ok(default_pool)
}

pub struct PostgresStoreConfig {
    pub pg_connection: PgConnectOptions,
    pub pg_database_name: String,
    pub pg_default_database_name: String,
    pub run_migrations: bool,
    pub max_processing_attempts: usize,
    pub processing_deadline_grace_sec: u64,
    pub claim_lease_ms: u64,
    pub retry_config: RetryConfig,
}

impl PostgresStoreConfig {
    pub fn from_config(config: &Config) -> Self {
        let mut conn_opts = PgConnectOptions::new()
            .username(&config.store.pg.username)
            .password(&config.store.pg.password)
            .host(&config.store.pg.host)
            .port(config.store.pg.port);

        if let Some(extra_query_params) = config.store.pg.query_params.as_ref() {
            let url = conn_opts.to_url_lossy();
            let new_url =
                url.as_ref().split('?').next().unwrap().to_string() + "?" + extra_query_params;
            conn_opts = PgConnectOptions::from_str(&new_url).unwrap();
        }

        Self {
            pg_connection: conn_opts,
            pg_database_name: config.store.pg.database_name.clone(),
            pg_default_database_name: config.store.pg.default_database_name.clone(),
            run_migrations: config.store.pg.run_migrations,
            max_processing_attempts: config.store.max_processing_attempts,
            processing_deadline_grace_sec: config.store.processing_deadline_grace_sec,
            claim_lease_ms: config.store.claim_lease_ms,
            retry_config: RetryConfig::from_config(config),
        }
    }
}

pub struct PostgresStore {
    read_pool: PgPool,
    write_pool: PgPool,
    config: PostgresStoreConfig,
    partitions: RwLock<Vec<i32>>,
}

impl PostgresStore {
    #[framed]
    async fn acquire_write_conn_metric(
        &self,
        caller: &'static str,
    ) -> Result<PoolConnection<Postgres>, Error> {
        let start = Instant::now();
        let conn = self.write_pool.acquire().await?;
        metrics::histogram!("postgres.write.acquire_conn", "fn" => caller, "mode" => "conn")
            .record(start.elapsed());
        Ok(conn)
    }

    #[framed]
    async fn begin_write_tx_metric(
        &self,
        caller: &'static str,
    ) -> Result<Transaction<'_, Postgres>, Error> {
        let start = Instant::now();
        let tx = self.write_pool.begin().await?;
        metrics::histogram!("postgres.write.acquire_conn", "fn" => caller, "mode" => "begin")
            .record(start.elapsed());
        Ok(tx)
    }

    #[framed]
    pub async fn new(config: PostgresStoreConfig) -> Result<Self, Error> {
        let (read_pool, write_pool) =
            create_postgres_pool(&config.pg_connection, &config.pg_database_name).await?;

        Ok(Self {
            read_pool,
            write_pool,
            config,
            partitions: RwLock::new(vec![]),
        })
    }

    /// Add the partition condition to the query builder in a thread-safe manner
    fn add_partition_condition(
        &self,
        query_builder: &mut QueryBuilder<Postgres>,
        first_condition: bool,
    ) {
        let partitions = self.partitions.read().unwrap();
        let condition = if first_condition { "WHERE" } else { "AND" };
        if !partitions.is_empty() {
            query_builder.push(" ");
            query_builder.push(condition);
            query_builder.push(" partition IN (");
            let mut separated = query_builder.separated(", ");
            for partition in partitions.iter() {
                separated.push_bind(*partition);
            }
            query_builder.push(")");
        }
    }
}

#[async_trait]
impl ActivationStore for PostgresStore {
    /// Trigger incremental vacuum to reclaim free pages in the database.
    /// Depending on config data, will either vacuum a set number of
    /// pages or attempt to reclaim all free pages.
    #[instrument(skip_all)]
    #[framed]
    async fn vacuum_db(&self) -> Result<(), Error> {
        // TODO: Remove
        Ok(())
    }

    /// Perform a full vacuum on the database.
    #[framed]
    async fn full_vacuum_db(&self) -> Result<(), Error> {
        // TODO: Remove
        Ok(())
    }

    /// Get the size of the database in bytes based on SQLite metadata queries.
    #[framed]
    async fn db_size(&self) -> Result<u64, Error> {
        let row_result: (i64,) = retry_query(&self.config.retry_config, "db_size", || async {
            Ok(sqlx::query_as("SELECT pg_database_size($1) as size")
                .bind(&self.config.pg_database_name)
                .fetch_one(&self.read_pool)
                .await?)
        })
        .await?;
        if row_result.0 < 0 {
            return Ok(0);
        }
        Ok(row_result.0 as u64)
    }

    /// Get an activation by id. Primarily used for testing
    #[framed]
    async fn get_by_id(&self, id: &str) -> Result<Option<Activation>, Error> {
        let row_result: Option<TableRow> =
            retry_query(&self.config.retry_config, "get_by_id", || async {
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
                .await?)
            })
            .await?;

        let Some(row) = row_result else {
            return Ok(None);
        };

        Ok(Some(row.into()))
    }

    fn assign_partitions(&self, partitions: Vec<i32>) -> Result<(), Error> {
        let mut write_guard = self.partitions.write().unwrap();
        write_guard.clear();
        write_guard.extend(partitions);
        Ok(())
    }

    #[instrument(skip_all)]
    #[framed]
    async fn store(&self, batch: &[Activation]) -> Result<u64, Error> {
        if batch.is_empty() {
            return Ok(0);
        }

        retry_query(&self.config.retry_config, "store", || async {
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
            let query = query_builder
                .push_values(batch.iter().map(TableRow::from), |mut b, row| {
                    b.push_bind(row.id);
                    // Cow<[u8]> has no Encode impl, so bind the variants directly.
                    match row.activation {
                        Cow::Borrowed(bytes) => b.push_bind(bytes),
                        Cow::Owned(bytes) => b.push_bind(bytes),
                    };
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
                        b.push("null");
                    }
                    if let Some(exp) = row.claim_expires_at {
                        b.push_bind(exp);
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

            Ok(result.rows_affected())
        })
        .await
    }

    #[instrument(skip_all)]
    #[framed]
    async fn claim_activations(
        &self,
        application: Option<&str>,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
        bucket: Option<BucketRange>,
        mark_processing: bool,
    ) -> Result<Vec<Activation>, Error> {
        let grace_period = self.config.processing_deadline_grace_sec;
        let claim_lease_ms = self.config.claim_lease_ms as i64;

        retry_query(&self.config.retry_config, "claim_activations", || async {
            let now = Utc::now();

            let mut query_builder = QueryBuilder::<Postgres>::new(
                "WITH selected_activations AS (
                    SELECT id
                    FROM inflight_taskactivations
                    WHERE status = ",
            );
            query_builder.push_bind(ActivationStatus::Pending.to_string());
            query_builder.push(" AND (expires_at IS NULL OR expires_at > ");
            query_builder.push_bind(now);
            query_builder.push(")");

            self.add_partition_condition(&mut query_builder, false);

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
            query_builder.push(" FOR UPDATE SKIP LOCKED)");

            if mark_processing {
                query_builder.push(format!(
                    "UPDATE inflight_taskactivations
                     SET processing_deadline = now() + (processing_deadline_duration * interval '1 second') + (interval '{grace_period} seconds'),
                         claim_expires_at = NULL,
                         status = "
                ));

                query_builder.push_bind(ActivationStatus::Processing.to_string());
            } else {
                query_builder.push(format!(
                    "UPDATE inflight_taskactivations
                     SET claim_expires_at = now() + ({claim_lease_ms} * interval '1 millisecond'),
                         processing_deadline = NULL,
                         status = "
                ));

                query_builder.push_bind(ActivationStatus::Claimed.to_string());
            }

            query_builder.push(" FROM selected_activations ");
            query_builder.push(" WHERE inflight_taskactivations.id = selected_activations.id");
            query_builder.push(" RETURNING *, kafka_offset AS offset");

            let mut conn = self.acquire_write_conn_metric("claim_activations").await?;
            let rows: Vec<TableRow> = query_builder
                .build_query_as::<TableRow>()
                .fetch_all(&mut *conn)
                .await?;

            Ok(rows.into_iter().map(|row| row.into()).collect())
        })
        .await
    }

    #[instrument(skip_all)]
    #[framed]
    async fn mark_activation_processing(&self, id: &str) -> Result<(), Error> {
        let grace_period = self.config.processing_deadline_grace_sec;

        retry_query(
            &self.config.retry_config,
            "mark_activation_processing",
            || async {
                let mut conn = self
                    .acquire_write_conn_metric("mark_activation_processing")
                    .await?;

                let result = sqlx::query(&format!(
                    "UPDATE inflight_taskactivations SET
                        status = $1,
                        processing_deadline = now() + (processing_deadline_duration * interval '1 second') + (interval '{grace_period} seconds'),
                        claim_expires_at = NULL
                    WHERE id = $2 AND status = $3",
                ))
                .bind(ActivationStatus::Processing.to_string())
                .bind(id)
                .bind(ActivationStatus::Claimed.to_string())
                .execute(&mut *conn)
                .await?;

                if result.rows_affected() == 0 {
                    metrics::counter!("push.mark_activation_processing", "result" => "not_found")
                        .increment(1);

                    warn!(
                        task_id = %id,
                        "Activation could not be marked as processing, it may be missing or its status may have already changed"
                    );
                } else {
                    metrics::counter!("push.mark_activation_processing", "result" => "ok")
                        .increment(1);
                }

                Ok(())
            },
        )
        .await
    }

    #[instrument(skip_all)]
    #[framed]
    async fn mark_processing_batch(&self, ids: &[String]) -> Result<u64, Error> {
        if ids.is_empty() {
            return Ok(0);
        }

        let grace_period = self.config.processing_deadline_grace_sec;
        retry_query(
            &self.config.retry_config,
            "mark_processing_batch",
            || async {
                let mut conn = self
                    .acquire_write_conn_metric("mark_processing_batch")
                    .await?;

                let result = sqlx::query(&format!(
                    "UPDATE inflight_taskactivations SET
                        status = $1,
                        processing_deadline = now() + (processing_deadline_duration * interval '1 second') + (interval '{grace_period} seconds'),
                        claim_expires_at = NULL
                    WHERE id = ANY($2) AND status = $3",
                ))
                .bind(ActivationStatus::Processing.to_string())
                .bind(ids)
                .bind(ActivationStatus::Claimed.to_string())
                .execute(&mut *conn)
                .await?;

                Ok(result.rows_affected())
            },
        )
        .await
    }

    /// Get the age of the oldest pending/claimed activation in seconds.
    /// Only activations with status=pending/claimed and processing_attempts=0 are considered
    /// as we are interested in latency to the *first* attempt.
    /// Tasks with delay_until set, will have their age adjusted based on their
    /// delay time. No tasks = 0 lag
    #[framed]
    async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> f64 {
        let mut query_builder = QueryBuilder::new(
            "SELECT received_at, delay_until
            FROM inflight_taskactivations
            WHERE status IN (",
        );
        let mut separated = query_builder.separated(", ");
        separated.push_bind(ActivationStatus::Pending.to_string());
        separated.push_bind(ActivationStatus::Claimed.to_string());
        query_builder.push(")");
        query_builder.push(" AND processing_attempts = 0");

        self.add_partition_condition(&mut query_builder, false);

        query_builder.push(" ORDER BY received_at ASC LIMIT 1");

        let result = match query_builder
            .build_query_as::<(DateTime<Utc>, Option<DateTime<Utc>>)>()
            .fetch_optional(&self.read_pool)
            .await
        {
            Ok(row) => row,
            Err(e) => {
                warn!("pending_activation_max_lag query failed: {e}");
                return 0.0;
            }
        };

        if let Some(row) = result {
            let received_at: DateTime<Utc> = row.0;
            let delay_until: Option<DateTime<Utc>> = row.1;
            let millis = now.signed_duration_since(received_at).num_milliseconds()
                - delay_until.map_or(0, |delay_time| {
                    delay_time
                        .signed_duration_since(received_at)
                        .num_milliseconds()
                });
            millis as f64 / 1000.0
        } else {
            // No pending activations means no latency
            0.0
        }
    }

    #[instrument(skip_all)]
    #[framed]
    async fn count_by_status(&self, status: ActivationStatus) -> Result<usize, Error> {
        retry_query(&self.config.retry_config, "count_by_status", || async {
            let mut query_builder = QueryBuilder::new(
                "SELECT COUNT(*) as count FROM inflight_taskactivations WHERE status = ",
            );
            query_builder.push_bind(status.to_string());
            self.add_partition_condition(&mut query_builder, false);
            let result = query_builder
                .build_query_as::<(i64,)>()
                .fetch_one(&self.read_pool)
                .await?;
            Ok(result.0 as usize)
        })
        .await
    }

    #[framed]
    async fn count(&self) -> Result<usize, Error> {
        retry_query(&self.config.retry_config, "count", || async {
            let mut query_builder =
                QueryBuilder::new("SELECT COUNT(*) as count FROM inflight_taskactivations");
            self.add_partition_condition(&mut query_builder, true);
            let result = query_builder
                .build_query_as::<(i64,)>()
                .fetch_one(&self.read_pool)
                .await?;
            Ok(result.0 as usize)
        })
        .await
    }

    #[instrument(skip_all)]
    #[framed]
    async fn count_depths(&self) -> Result<DepthCounts, Error> {
        retry_query(&self.config.retry_config, "count_depths", || async {
            // Notice that statuses are embedded into the query for simplicity - if the enum is every changed, this must change too!
            let mut query_builder = QueryBuilder::new(
                "SELECT COUNT(*) FILTER (WHERE status = 'Pending'),
                        COUNT(*) FILTER (WHERE status = 'Delay'),
                        COUNT(*) FILTER (WHERE status = 'Claimed'),
                        COUNT(*) FILTER (WHERE status = 'Processing')
                 FROM inflight_taskactivations",
            );

            self.add_partition_condition(&mut query_builder, true);

            let row: (i64, i64, i64, i64) = query_builder
                .build_query_as()
                .fetch_one(&self.read_pool)
                .await?;

            Ok(DepthCounts {
                pending: row.0 as usize,
                delay: row.1 as usize,
                claimed: row.2 as usize,
                processing: row.3 as usize,
            })
        })
        .await
    }

    #[instrument(skip_all)]
    #[framed]
    async fn count_depths_per_partition(&self) -> Result<HashMap<i32, DepthCounts>, Error> {
        let assigned: Vec<i32> = self.partitions.read().unwrap().clone();
        if assigned.is_empty() {
            return Ok(HashMap::new());
        }

        let mut query_builder = QueryBuilder::new(
            "SELECT partition,
                    COUNT(*) FILTER (WHERE status = 'Pending'),
                    COUNT(*) FILTER (WHERE status = 'Delay'),
                    COUNT(*) FILTER (WHERE status = 'Claimed'),
                    COUNT(*) FILTER (WHERE status = 'Processing')
             FROM inflight_taskactivations WHERE partition IN (",
        );
        let mut separated = query_builder.separated(", ");
        for partition in &assigned {
            separated.push_bind(*partition);
        }
        query_builder.push(") GROUP BY partition");

        let rows: Vec<(i32, i64, i64, i64, i64)> = query_builder
            .build_query_as()
            .fetch_all(&self.read_pool)
            .await?;

        let mut counts: HashMap<i32, DepthCounts> = rows
            .into_iter()
            .map(|(partition, pending, delay, claimed, processing)| {
                (
                    partition,
                    DepthCounts {
                        pending: pending as usize,
                        delay: delay as usize,
                        claimed: claimed as usize,
                        processing: processing as usize,
                    },
                )
            })
            .collect();

        for partition in &assigned {
            counts.entry(*partition).or_insert(DepthCounts {
                pending: 0,
                delay: 0,
                claimed: 0,
                processing: 0,
            });
        }

        Ok(counts)
    }

    /// Update the status of a specific activation.
    /// If max_attempts is provided (for Retry status), also updates the activation's retry_state.
    #[instrument(skip_all)]
    #[framed]
    async fn set_status(
        &self,
        id: &str,
        status: ActivationStatus,
        max_attempts: Option<u32>,
        delay_on_retry: Option<u64>,
    ) -> Result<Option<Activation>, Error> {
        retry_query(&self.config.retry_config, "set_status", || async {
            let mut tx = self.begin_write_tx_metric("set_status").await?;

            let result: Option<TableRow> = sqlx::query_as(
                "UPDATE inflight_taskactivations SET status = $1 WHERE id = $2 RETURNING *, kafka_offset AS offset",
            )
            .bind(status.to_string())
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;

            let Some(mut row) = result else {
                return Ok(None);
            };

            let mut activation = TaskActivation::decode(&row.activation as &[u8])?;
            let mut needs_update = false;
            let task_retry_state = activation.retry_state.get_or_insert_default();

            // Only update the blob if max_attempts actually changed. This should rarely
            // happen after the first retry, since max_attempts comes from the task's
            // retry decorator which stays constant across retries.
            // For raw topics, retry_state starts as None so we create it on first retry.
            if let Some(max_attempts) = max_attempts
                && task_retry_state.max_attempts != max_attempts
            {
                task_retry_state.max_attempts = max_attempts;
                needs_update = true;
            }

            if let Some(delay_on_retry) = delay_on_retry
                && task_retry_state.delay_on_retry != Some(delay_on_retry)
            {
                task_retry_state.delay_on_retry = Some(delay_on_retry);
                needs_update = true;
            }

            if needs_update {
                let updated_activation = activation.encode_to_vec();
                sqlx::query(
                    "UPDATE inflight_taskactivations SET activation = $1 WHERE id = $2",
                )
                .bind(&updated_activation)
                .bind(id)
                .execute(&mut *tx)
                .await?;

                row.activation = Cow::Owned(updated_activation);
            }

            tx.commit().await?;

            Ok(Some(row.into()))
        })
        .await
    }

    #[instrument(skip_all)]
    #[framed]
    async fn set_status_batch(
        &self,
        ids: &[String],
        status: ActivationStatus,
    ) -> Result<u64, Error> {
        if ids.is_empty() {
            return Ok(0);
        }

        retry_query(&self.config.retry_config, "set_status_batch", || async {
            let mut conn = self.acquire_write_conn_metric("set_status_batch").await?;

            let result =
                sqlx::query("UPDATE inflight_taskactivations SET status = $1 WHERE id = ANY($2)")
                    .bind(status.to_string())
                    .bind(ids)
                    .execute(&mut *conn)
                    .await?;

            Ok(result.rows_affected())
        })
        .await
    }

    #[instrument(skip_all)]
    #[framed]
    async fn set_processing_deadline(
        &self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        retry_query(
            &self.config.retry_config,
            "set_processing_deadline",
            || async {
                let mut conn = self
                    .acquire_write_conn_metric("set_processing_deadline")
                    .await?;
                sqlx::query(
                    "UPDATE inflight_taskactivations SET processing_deadline = $1 WHERE id = $2",
                )
                .bind(deadline.unwrap())
                .bind(id)
                .execute(&mut *conn)
                .await?;
                Ok(())
            },
        )
        .await
    }

    #[instrument(skip_all)]
    #[framed]
    async fn delete_activation(&self, id: &str) -> Result<(), Error> {
        retry_query(&self.config.retry_config, "delete_activation", || async {
            let mut conn = self.acquire_write_conn_metric("delete_activation").await?;
            sqlx::query("DELETE FROM inflight_taskactivations WHERE id = $1")
                .bind(id)
                .execute(&mut *conn)
                .await?;
            Ok(())
        })
        .await
    }

    #[instrument(skip_all)]
    #[framed]
    async fn get_retry_activations(&self) -> Result<Vec<Activation>, Error> {
        retry_query(
            &self.config.retry_config,
            "get_retry_activations",
            || async {
                let mut query_builder = QueryBuilder::new(
                    "SELECT id,
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
                        claim_expires_at,
                        status,
                        at_most_once,
                        application,
                        namespace,
                        taskname,
                        on_attempts_exceeded,
                        bucket
                    FROM inflight_taskactivations
                    WHERE status = ",
                );
                query_builder.push_bind(ActivationStatus::Retry.to_string());
                self.add_partition_condition(&mut query_builder, false);

                Ok(query_builder
                    .build_query_as::<TableRow>()
                    .fetch_all(&self.read_pool)
                    .await?
                    .into_iter()
                    .map(|row: TableRow| row.into())
                    .collect())
            },
        )
        .await
    }

    // Used in tests
    #[framed]
    async fn clear(&self) -> Result<(), Error> {
        let mut conn = self.acquire_write_conn_metric("clear").await?;
        sqlx::query("TRUNCATE TABLE inflight_taskactivations")
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    /// Revert expired push claims back to pending status.
    #[instrument(skip_all)]
    #[framed]
    async fn handle_claim_expiration(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut conn = self
            .acquire_write_conn_metric("handle_claim_expiration")
            .await?;

        let mut query_builder = QueryBuilder::new(
            "UPDATE inflight_taskactivations
             SET claim_expires_at = null,
                 status = ",
        );
        query_builder.push_bind(ActivationStatus::Pending.to_string());
        query_builder.push(
            " WHERE claim_expires_at IS NOT NULL
                 AND claim_expires_at < ",
        );
        query_builder.push_bind(now);
        query_builder.push(" AND status = ");
        query_builder.push_bind(ActivationStatus::Claimed.to_string());
        self.add_partition_condition(&mut query_builder, false);

        let released = query_builder.build().execute(&mut *conn).await?;

        Ok(released.rows_affected())
    }

    /// Update tasks that are in processing and have exceeded their processing deadline
    /// Exceeding a processing deadline does not consume a retry as we don't know
    /// if a worker took the task and was killed, or failed.
    #[instrument(skip_all)]
    #[framed]
    async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut atomic = self.write_pool.begin().await?;

        // At-most-once tasks that fail their processing deadlines go directly to failure
        // there are no retries, as the worker will reject the task due to at_most_once keys.
        let mut query_builder = QueryBuilder::new(
            "UPDATE inflight_taskactivations
            SET processing_deadline = null, status = ",
        );
        query_builder.push_bind(ActivationStatus::Failure.to_string());
        query_builder.push(" WHERE processing_deadline < ");
        query_builder.push_bind(now);
        query_builder.push(" AND at_most_once = TRUE AND status = ");
        query_builder.push_bind(ActivationStatus::Processing.to_string());

        self.add_partition_condition(&mut query_builder, false);

        let most_once_result = query_builder.build().execute(&mut *atomic).await;

        let mut processing_deadline_modified_rows = 0;
        if let Ok(query_res) = most_once_result {
            processing_deadline_modified_rows = query_res.rows_affected();
        }

        // Update regular tasks.
        // Increment processing_attempts by 1 and reset processing_deadline to null.
        let mut query_builder = QueryBuilder::new(
            "UPDATE inflight_taskactivations
            SET processing_deadline = null, status = ",
        );
        query_builder.push_bind(ActivationStatus::Pending.to_string());
        query_builder.push(", processing_attempts = processing_attempts + 1");
        query_builder.push(" WHERE processing_deadline < ");
        query_builder.push_bind(now);
        query_builder.push(" AND status = ");
        query_builder.push_bind(ActivationStatus::Processing.to_string());
        self.add_partition_condition(&mut query_builder, false);

        let result = query_builder.build().execute(&mut *atomic).await;

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
    #[framed]
    async fn handle_processing_attempts(&self) -> Result<u64, Error> {
        let mut conn = self
            .acquire_write_conn_metric("handle_processing_attempts")
            .await?;
        let mut query_builder = QueryBuilder::new(
            "UPDATE inflight_taskactivations
            SET status = ",
        );
        query_builder.push_bind(ActivationStatus::Failure.to_string());
        query_builder.push(" WHERE processing_attempts >= ");
        query_builder.push_bind(self.config.max_processing_attempts as i32);
        query_builder.push(" AND status = ");
        query_builder.push_bind(ActivationStatus::Pending.to_string());
        self.add_partition_condition(&mut query_builder, false);
        let processing_attempts_result = query_builder.build().execute(&mut *conn).await;

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
    #[framed]
    async fn handle_expires_at(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut conn = self.acquire_write_conn_metric("handle_expires_at").await?;
        let mut query_builder =
            QueryBuilder::new("DELETE FROM inflight_taskactivations WHERE status = ");
        query_builder.push_bind(ActivationStatus::Pending.to_string());
        query_builder.push(" AND expires_at IS NOT NULL AND expires_at < ");
        query_builder.push_bind(now);
        self.add_partition_condition(&mut query_builder, false);
        let result = query_builder.build().execute(&mut *conn).await?;

        Ok(result.rows_affected())
    }

    /// Perform upkeep work for tasks that are past delay_until deadlines
    ///
    /// Tasks that are delayed and past their delay_until deadline are updated
    /// to have status=pending so that they can be executed by workers
    ///
    /// The number of impacted records is returned in a Result.
    #[instrument(skip_all)]
    #[framed]
    async fn handle_delay_until(&self) -> Result<u64, Error> {
        let now = Utc::now();
        let mut conn = self.acquire_write_conn_metric("handle_delay_until").await?;

        let mut query_builder = QueryBuilder::new(
            "UPDATE inflight_taskactivations
                SET status = ",
        );
        query_builder.push_bind(ActivationStatus::Pending.to_string());
        query_builder.push(" WHERE delay_until IS NOT NULL AND delay_until < ");
        query_builder.push_bind(now);
        query_builder.push(" AND status = ");
        query_builder.push_bind(ActivationStatus::Delay.to_string());
        self.add_partition_condition(&mut query_builder, false);
        let update_result = query_builder.build().execute(&mut *conn).await?;

        Ok(update_result.rows_affected())
    }

    /// Perform upkeep work related to status=failure
    ///
    /// Activations that are status=failure need to either be discarded by setting status=complete
    /// or need to be moved to deadletter and are returned in the Result.
    /// Once dead-lettered tasks have been added to Kafka those tasks can have their status set to
    /// complete.
    #[instrument(skip_all)]
    #[framed]
    async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
        let mut atomic = self.write_pool.begin().await?;

        let mut query_builder = QueryBuilder::new(
            "SELECT id, activation, on_attempts_exceeded FROM inflight_taskactivations WHERE status = ",
        );
        query_builder.push_bind(ActivationStatus::Failure.to_string());
        self.add_partition_condition(&mut query_builder, false);
        let failed_tasks = query_builder
            .build_query_as::<(String, Vec<u8>, i32)>()
            .fetch_all(&mut *atomic)
            .await?;

        let mut forwarder = FailedTasksForwarder {
            to_discard: vec![],
            to_deadletter: vec![],
        };

        for record in failed_tasks.iter() {
            let activation_data: &[u8] = record.1.as_slice();
            let id: String = record.0.clone();
            // We could be deadlettering because of activation.expires
            // when a task expires we still deadletter if configured.
            let on_attempts_exceeded: OnAttemptsExceeded = record.2.try_into().unwrap();
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
                .push_bind(ActivationStatus::Complete.to_string())
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
    #[framed]
    async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        retry_query(&self.config.retry_config, "mark_completed", || async {
            let mut query_builder = QueryBuilder::new("UPDATE inflight_taskactivations ");
            query_builder
                .push("SET status = ")
                .push_bind(ActivationStatus::Complete.to_string())
                .push(" WHERE id IN (");

            let mut separated = query_builder.separated(", ");
            for id in ids.iter() {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");
            let mut conn = self.acquire_write_conn_metric("mark_completed").await?;
            let result = query_builder.build().execute(&mut *conn).await?;

            Ok(result.rows_affected())
        })
        .await
    }

    /// Remove completed tasks.
    /// This method is a garbage collector for the activation store.
    #[instrument(skip_all)]
    #[framed]
    async fn remove_completed(&self) -> Result<u64, Error> {
        let mut conn = self.acquire_write_conn_metric("remove_completed").await?;
        let mut query_builder =
            QueryBuilder::new("DELETE FROM inflight_taskactivations WHERE status = ");
        query_builder.push_bind(ActivationStatus::Complete.to_string());
        self.add_partition_condition(&mut query_builder, false);
        let result = query_builder.build().execute(&mut *conn).await?;

        Ok(result.rows_affected())
    }

    /// Remove killswitched tasks.
    #[instrument(skip_all)]
    #[framed]
    async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        let mut query_builder =
            QueryBuilder::new("DELETE FROM inflight_taskactivations WHERE taskname IN (");
        let mut separated = query_builder.separated(", ");
        for taskname in killswitched_tasks.iter() {
            separated.push_bind(taskname);
        }
        separated.push_unseparated(")");
        self.add_partition_condition(&mut query_builder, false);
        let mut conn = self
            .acquire_write_conn_metric("remove_killswitched")
            .await?;
        let query = query_builder.build().execute(&mut *conn).await?;

        Ok(query.rows_affected())
    }

    // Used in tests
    #[framed]
    async fn remove_db(&self) -> Result<(), Error> {
        self.read_pool.close().await;
        self.write_pool.close().await;
        let default_pool = create_default_postgres_pool(
            &self.config.pg_connection,
            &self.config.pg_default_database_name,
        )
        .await?;
        let _ = sqlx::query(format!("DROP DATABASE {}", &self.config.pg_database_name).as_str())
            .bind(&self.config.pg_database_name)
            .execute(&default_pool)
            .await;
        let _ = default_pool.close().await;
        Ok(())
    }
}
