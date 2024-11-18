use std::str::FromStr;

use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use prost::Message;
use sentry_protos::sentry::v1::TaskActivation;
use sqlx::{
    migrate::MigrateDatabase,
    sqlite::{SqliteConnectOptions, SqlitePool, SqliteQueryResult, SqliteRow},
    ConnectOptions, FromRow, QueryBuilder, Row, Sqlite, Type,
};

pub struct InflightActivationStore {
    sqlite_pool: SqlitePool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Type)]
pub enum TaskActivationStatus {
    Pending,
    Processing,
    Failure,
    Retry,
    Complete,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InflightActivation {
    pub activation: TaskActivation,
    pub status: TaskActivationStatus,
    pub partition: i32,
    pub offset: i64,
    pub added_at: DateTime<Utc>,
    pub deadletter_at: Option<DateTime<Utc>>,
    pub processing_deadline: Option<DateTime<Utc>>,
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

#[derive(FromRow)]
struct TableRow {
    id: String,
    activation: Vec<u8>,
    partition: i32,
    offset: i64,
    added_at: DateTime<Utc>,
    deadletter_at: Option<DateTime<Utc>>,
    processing_deadline_duration: u32,
    processing_deadline: Option<DateTime<Utc>>,
    status: TaskActivationStatus,
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
            deadletter_at: value.deadletter_at,
            processing_deadline_duration: value.activation.processing_deadline_duration as u32,
            processing_deadline: value.processing_deadline,
            status: value.status,
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
            deadletter_at: value.deadletter_at,
            processing_deadline: value.processing_deadline,
        }
    }
}

impl InflightActivationStore {
    pub async fn new(url: &str) -> Result<Self, Error> {
        if !Sqlite::database_exists(url).await? {
            Sqlite::create_database(url).await?
        }
        let conn_options = SqliteConnectOptions::from_str(url)?.disable_statement_logging();

        let sqlite_pool = SqlitePool::connect_with(conn_options).await?;

        sqlx::migrate!("./migrations").run(&sqlite_pool).await?;

        Ok(Self { sqlite_pool })
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
            (id, activation, partition, offset, added_at, deadletter_at, processing_deadline_duration, processing_deadline, status)",
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
                b.push_bind(row.deadletter_at);
                b.push_bind(row.processing_deadline_duration);
                if let Some(deadline) = row.processing_deadline {
                    b.push_bind(deadline.format("%Y-%m-%D %H:%M:%S").to_string());
                } else {
                    // Add a literal null
                    b.push("null");
                }
                b.push_bind(row.status);
            })
            .build();
        Ok(query.execute(&self.sqlite_pool).await?.into())
    }

    pub async fn get_pending_activation(&self) -> Result<Option<InflightActivation>, Error> {
        let result = sqlx::query(
            "UPDATE inflight_taskactivations
            
                SET 
                    processing_deadline = datetime('now', '+' || processing_deadline_duration || ' seconds'),
                    status = $1
                WHERE
                    id = (
                        SELECT id FROM inflight_taskactivations WHERE status = $2 LIMIT 1
                    )
                RETURNING id",
        )
        .bind(TaskActivationStatus::Processing)
        .bind(TaskActivationStatus::Pending)
        .fetch_optional(&self.sqlite_pool)
        .await?;

        let Some(row) = result else { return Ok(None) };

        let row: TableRow = sqlx::query_as("SELECT * FROM inflight_taskactivations WHERE id = ?")
            .bind(row.get::<String, _>("id"))
            .fetch_one(&self.sqlite_pool)
            .await?;

        Ok(Some(row.into()))
    }

    pub async fn count_pending_activations(&self) -> Result<usize, Error> {
        let result =
            sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations WHERE status = $1")
                .bind(TaskActivationStatus::Pending)
                .fetch_one(&self.sqlite_pool)
                .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    pub async fn set_status(&self, id: &str, status: TaskActivationStatus) -> Result<(), Error> {
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
            .bind(deadline)
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
                .bind(TaskActivationStatus::Retry)
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
    pub async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        // Find rows past processing deadlines
        let now = Utc::now();

        let mut atomic = self.sqlite_pool.begin().await?;

        let expired: Vec<SqliteRow> = sqlx::query(
            "SELECT id, activation
            FROM inflight_taskactivations
            WHERE processing_deadline < $1 AND status = $2
            ",
        )
        .bind(now.format("%Y-%m-%d %H:%M:%S").to_string())
        .bind(TaskActivationStatus::Processing)
        .fetch_all(&mut *atomic)
        .await?
        .into_iter()
        .collect();

        let mut to_update: Vec<String> = vec![];
        for record in expired {
            let activation_data: &[u8] = record.get("activation");
            let row_id: String = record.get("id");

            let activation = TaskActivation::decode(activation_data)?;
            let retry_state_option = activation.retry_state;
            if retry_state_option.is_none() {
                to_update.push(row_id);
                continue;
            }
            // Determine if there are retries remaining. Exceeding a processing deadline
            // does not increment the number of retry attempts.
            let retry_state = retry_state_option.unwrap();
            let mut has_retries_remaining = false;
            if let Some(deadletter_after_attempt) = retry_state.deadletter_after_attempt {
                if deadletter_after_attempt < retry_state.attempts {
                    has_retries_remaining = true;
                }
            }
            if let Some(discard_after_attempt) = retry_state.discard_after_attempt {
                if discard_after_attempt < retry_state.attempts {
                    has_retries_remaining = true;
                }
            }
            if has_retries_remaining {
                to_update.push(row_id);
            }
        }

        if to_update.is_empty() {
            return Ok(0);
        }

        // Clear processing deadlines and make tasks available again.
        let mut query_builder = QueryBuilder::new(
            "UPDATE inflight_taskactivations
            SET status = $1, processing_deadline = null
            WHERE id IN (",
        );
        let mut separated = query_builder.separated(", ");
        for id in to_update.iter() {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");

        let query = query_builder.build();
        let result = query.execute(&mut *atomic).await;

        atomic.commit().await?;

        if let Ok(query_res) = result {
            return Ok(query_res.rows_affected());
        }

        Err(anyhow!("Could not update tasks past processing_deadline"))
    }

    /// Perform upkeep work related to status=failure
    ///
    /// Activations that are status=failure need to either be discarded by setting status=complete
    /// or need to be moved to deadletter and are returned in the Result.
    /// Once dead-lettered tasks have been added to Kafka those tasks can have their status set to
    /// complete.
    pub async fn handle_failed_tasks(&self) -> Result<Vec<TaskActivation>, Error> {
        let mut atomic = self.sqlite_pool.begin().await?;

        let failed_tasks: Vec<SqliteRow> =
            sqlx::query("SELECT activation FROM inflight_taskactivations WHERE status = $1")
                .bind(TaskActivationStatus::Failure)
                .fetch_all(&mut *atomic)
                .await?
                .into_iter()
                .collect();

        let mut to_discard: Vec<String> = vec![];
        let mut to_deadletter: Vec<TaskActivation> = vec![];

        for record in failed_tasks.iter() {
            let activation_data: &[u8] = record.get("activation");
            let activation = TaskActivation::decode(activation_data)?;

            // Without a retry state, tasks are discarded
            if activation.retry_state.as_ref().is_none() {
                to_discard.push(activation.id);
                continue;
            }
            let retry_state = &activation.retry_state.as_ref().unwrap();
            if retry_state.discard_after_attempt.is_some() {
                to_discard.push(activation.id.clone());
            }
            if retry_state.deadletter_after_attempt.is_some() {
                to_deadletter.push(activation);
            }
        }

        if !to_discard.is_empty() {
            let mut query_builder = QueryBuilder::new(
                "UPDATE inflight_taskactivations
                SET status = $1
                WHERE id IN (",
            );
            let mut separated = query_builder.separated(", ");
            for id in to_discard.iter() {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");
            query_builder.build().execute(&mut *atomic).await?;
        }

        atomic.commit().await?;

        Ok(to_deadletter)
    }

    /// Mark a collection of tasks as complete by id
    pub async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        let mut query_builder =
            QueryBuilder::new("UPDATE inflight_taskactivations SET status = $1 WHERE id IN (");
        let mut separated = query_builder.separated(", ");
        for id in ids.iter() {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
        let result = query_builder.build().execute(&self.sqlite_pool).await?;

        Ok(result.rows_affected())
    }

    /// Remove completed tasks that do not have an incomplete record following
    /// them in offset order.
    ///
    /// This method is a garbage collector for the inflight task store.
    pub async fn remove_completed(&self) -> Result<u64, Error> {
        let mut atomic = self.sqlite_pool.begin().await?;

        let incomplete_query = sqlx::query(
            r#"
            SELECT "offset"
            FROM inflight_taskactivations
            WHERE status != $1
            ORDER BY "offset"
            LIMIT 1
            "#,
        )
        .bind(TaskActivationStatus::Complete)
        .fetch_optional(&mut *atomic)
        .await?;

        let lowest_incomplete_offset: i64 = if let Some(query_result) = incomplete_query {
            query_result.get("offset")
        } else {
            return Ok(0);
        };

        let cleanup_query = sqlx::query(
            r#"DELETE FROM inflight_taskactivations WHERE status = $1 AND "offset" < $2"#,
        )
        .bind(TaskActivationStatus::Complete)
        .bind(lowest_incomplete_offset)
        .execute(&mut *atomic)
        .await?;

        atomic.commit().await?;

        Ok(cleanup_query.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{DateTime, TimeZone, Utc};
    use rand::Rng;
    use sentry_protos::sentry::v1::{RetryState, TaskActivation};
    use sqlx::{Row, SqlitePool};

    use crate::inflight_activation_store::{
        InflightActivation, InflightActivationStore, TaskActivationStatus,
    };

    fn generate_temp_filename() -> String {
        let mut rng = rand::thread_rng();
        format!("/var/tmp/{}-{}.sqlite", Utc::now(), rng.gen::<u64>())
    }

    fn make_activations(count: u32) -> Vec<InflightActivation> {
        let mut records: Vec<InflightActivation> = vec![];
        for i in 0..count {
            #[allow(deprecated)]
            let item = InflightActivation {
                activation: TaskActivation {
                    id: format!("id_{}", i),
                    namespace: "namespace".into(),
                    taskname: "taskname".into(),
                    parameters: "{}".into(),
                    headers: HashMap::new(),
                    received_at: Some(prost_types::Timestamp {
                        seconds: Utc::now().timestamp(),
                        nanos: 0,
                    }),
                    deadline: None,
                    retry_state: None,
                    processing_deadline_duration: 10,
                    expires: None,
                },
                status: TaskActivationStatus::Pending,
                partition: 0,
                offset: i as i64,
                added_at: Utc::now(),
                deadletter_at: None,
                processing_deadline: None,
            };
            records.push(item);
        }
        records
    }

    #[tokio::test]
    async fn test_create_db() {
        assert!(InflightActivationStore::new(&generate_temp_filename())
            .await
            .is_ok())
    }

    #[tokio::test]
    async fn test_store() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let batch = make_activations(2);
        assert!(store.store(batch).await.is_ok());

        let result = sqlx::query(
            "SELECT count() as count
             FROM inflight_taskactivations;",
        )
        .fetch_one(&SqlitePool::connect(&url).await.unwrap())
        .await
        .unwrap();

        assert_eq!(result.get::<u32, &str>("count"), 2);
    }

    #[tokio::test]
    async fn test_get_pending_activation() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();
        let added_at = Utc::now();

        #[allow(deprecated)]
        let batch = vec![InflightActivation {
            activation: TaskActivation {
                id: "id_0".into(),
                namespace: "namespace".into(),
                taskname: "taskname".into(),
                parameters: "{some_param: 123}".into(),
                headers: HashMap::new(),
                received_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadline: None,
                retry_state: None,
                processing_deadline_duration: 10,
                expires: Some(1),
            },
            status: TaskActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at,
            deadletter_at: None,
            processing_deadline: None,
        }];
        assert!(store.store(batch.clone()).await.is_ok());

        let result = store.get_pending_activation().await.unwrap();
        #[allow(deprecated)]
        let expected = Some(InflightActivation {
            activation: TaskActivation {
                id: "id_0".into(),
                namespace: "namespace".into(),
                taskname: "taskname".into(),
                parameters: "{some_param: 123}".into(),
                headers: HashMap::new(),
                received_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadline: None,
                retry_state: None,
                processing_deadline_duration: 10,
                expires: Some(1),
            },
            status: TaskActivationStatus::Processing,
            partition: 0,
            offset: 0,
            added_at,
            deadletter_at: None,
            processing_deadline: result.as_ref().unwrap().processing_deadline,
        });

        assert_eq!(result, expected);

        let deadline = result.as_ref().unwrap().processing_deadline;
        assert!(deadline.unwrap() > Utc::now());
    }

    #[tokio::test]
    async fn test_count_pending_activations() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let mut batch = make_activations(3);
        batch[0].status = TaskActivationStatus::Processing;
        assert!(store.store(batch).await.is_ok());

        assert_eq!(store.count_pending_activations().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn set_activation_status() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let batch = make_activations(2);
        assert!(store.store(batch).await.is_ok());

        assert_eq!(store.count_pending_activations().await.unwrap(), 2);
        assert!(store
            .set_status("id_0", TaskActivationStatus::Failure)
            .await
            .is_ok());
        assert_eq!(store.count_pending_activations().await.unwrap(), 1);
        assert!(store
            .set_status("id_0", TaskActivationStatus::Pending)
            .await
            .is_ok());
        assert_eq!(store.count_pending_activations().await.unwrap(), 2);
        assert!(store
            .set_status("id_0", TaskActivationStatus::Failure)
            .await
            .is_ok());
        assert!(store
            .set_status("id_1", TaskActivationStatus::Failure)
            .await
            .is_ok());
        assert_eq!(store.count_pending_activations().await.unwrap(), 0);
        assert!(store.get_pending_activation().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_set_processing_deadline() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let batch = make_activations(1);
        assert!(store.store(batch.clone()).await.is_ok());

        let deadline = Utc::now();
        assert!(store
            .set_processing_deadline("id_0", Some(deadline))
            .await
            .is_ok());

        let result = sqlx::query(
            "SELECT processing_deadline
            FROM inflight_taskactivations
            WHERE id = $1;",
        )
        .bind("id_0")
        .fetch_one(&SqlitePool::connect(&url).await.unwrap())
        .await
        .unwrap();

        assert_eq!(
            result.get::<Option<DateTime<Utc>>, _>("processing_deadline"),
            Some(deadline)
        );
    }

    #[tokio::test]
    async fn test_delete_activation() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let batch = make_activations(2);
        assert!(store.store(batch).await.is_ok());

        let result = sqlx::query(
            "SELECT count() as count
             FROM inflight_taskactivations;",
        )
        .fetch_one(&SqlitePool::connect(&url).await.unwrap())
        .await
        .unwrap();
        assert_eq!(result.get::<u32, &str>("count"), 2);

        assert!(store.delete_activation("id_0").await.is_ok());
        let result = sqlx::query(
            "SELECT count() as count
             FROM inflight_taskactivations;",
        )
        .fetch_one(&SqlitePool::connect(&url).await.unwrap())
        .await
        .unwrap();
        assert_eq!(result.get::<u32, &str>("count"), 1);

        assert!(store.delete_activation("id_0").await.is_ok());
        let result = sqlx::query(
            "SELECT count() as count
             FROM inflight_taskactivations;",
        )
        .fetch_one(&SqlitePool::connect(&url).await.unwrap())
        .await
        .unwrap();
        assert_eq!(result.get::<u32, &str>("count"), 1);

        assert!(store.delete_activation("id_1").await.is_ok());
        let result = sqlx::query(
            "SELECT count() as count
             FROM inflight_taskactivations;",
        )
        .fetch_one(&SqlitePool::connect(&url).await.unwrap())
        .await
        .unwrap();
        assert_eq!(result.get::<u32, &str>("count"), 0);
    }

    #[tokio::test]
    async fn test_get_retry_activations() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let batch = make_activations(2);
        assert!(store.store(batch.clone()).await.is_ok());

        assert_eq!(store.count_pending_activations().await.unwrap(), 2);
        assert!(store
            .set_status("id_0", TaskActivationStatus::Retry)
            .await
            .is_ok());
        assert_eq!(store.count_pending_activations().await.unwrap(), 1);
        assert!(store
            .set_status("id_1", TaskActivationStatus::Retry)
            .await
            .is_ok());

        let retries = store.get_retry_activations().await.unwrap();
        assert_eq!(retries.len(), 2);
        for record in retries.iter() {
            assert_eq!(record.status, TaskActivationStatus::Retry);
        }
    }

    #[tokio::test]
    async fn test_handle_processing_deadline() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let mut batch = make_activations(2);
        batch[1].status = TaskActivationStatus::Processing;
        batch[1].processing_deadline =
            Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());

        assert!(store.store(batch).await.is_ok());

        let past_deadline = store.handle_processing_deadline().await;
        assert!(past_deadline.is_ok());
        assert_eq!(past_deadline.unwrap(), 1);

        // Run again to check early return
        let past_deadline = store.handle_processing_deadline().await;
        assert!(past_deadline.is_ok());
        assert_eq!(past_deadline.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_remove_completed() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let mut records = make_activations(3);
        // record 1 & 2 should not be removed.
        records[0].status = TaskActivationStatus::Complete;
        records[1].status = TaskActivationStatus::Pending;
        records[2].status = TaskActivationStatus::Complete;

        assert!(store.store(records.clone()).await.is_ok());

        let result = store.remove_completed().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert!(store
            .get_by_id(&records[0].activation.id)
            .await
            .expect("no error")
            .is_none());
        assert!(store
            .get_by_id(&records[1].activation.id)
            .await
            .expect("no error")
            .is_some());
        assert!(store
            .get_by_id(&records[2].activation.id)
            .await
            .expect("no error")
            .is_some());
    }

    #[tokio::test]
    async fn test_remove_completed_multiple_gaps() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let mut records = make_activations(4);
        // only record 1 can be removed
        records[0].status = TaskActivationStatus::Complete;
        records[1].status = TaskActivationStatus::Failure;
        records[2].status = TaskActivationStatus::Complete;
        records[3].status = TaskActivationStatus::Processing;

        assert!(store.store(records.clone()).await.is_ok());

        let result = store.remove_completed().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert!(store
            .get_by_id(&records[0].activation.id)
            .await
            .expect("no error")
            .is_none());
        assert!(store
            .get_by_id(&records[1].activation.id)
            .await
            .expect("no error")
            .is_some());
        assert!(store
            .get_by_id(&records[2].activation.id)
            .await
            .expect("no error")
            .is_some());
        assert!(store
            .get_by_id(&records[3].activation.id)
            .await
            .expect("no error")
            .is_some());
    }

    #[tokio::test]
    async fn test_handle_failed_tasks() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let mut records = make_activations(4);
        // deadletter
        records[0].status = TaskActivationStatus::Failure;
        records[0].activation.retry_state = Some(RetryState {
            attempts: 1,
            kind: "".into(),
            discard_after_attempt: None,
            deadletter_after_attempt: Some(1),
        });
        // discard
        records[1].status = TaskActivationStatus::Failure;
        records[1].activation.retry_state = Some(RetryState {
            attempts: 1,
            kind: "".into(),
            discard_after_attempt: Some(1),
            deadletter_after_attempt: None,
        });
        // no retry state = discard
        records[2].status = TaskActivationStatus::Failure;
        assert!(records[2].activation.retry_state.is_none());

        // Another deadletter
        records[3].status = TaskActivationStatus::Failure;
        records[3].activation.retry_state = Some(RetryState {
            attempts: 1,
            kind: "".into(),
            discard_after_attempt: None,
            deadletter_after_attempt: Some(1),
        });
        assert!(store.store(records.clone()).await.is_ok());

        let result = store.handle_failed_tasks().await;
        assert!(result.is_ok(), "handle_failed_tasks should be ok");
        let deadletter = result.unwrap();

        assert_eq!(deadletter.len(), 2);
        assert_eq!(deadletter[0].id, records[0].activation.id);
        assert_eq!(deadletter[1].id, records[3].activation.id);
    }

    #[tokio::test]
    async fn test_mark_completed() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        let records = make_activations(3);
        assert!(store.store(records.clone()).await.is_ok());
        assert_eq!(store.count_pending_activations().await.unwrap(), 3);
        let ids = records
            .iter()
            .map(|item| item.activation.id.clone())
            .collect();
        let result = store.mark_completed(ids).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3, "three records removed");
        assert_eq!(
            store.count_pending_activations().await.unwrap(),
            0,
            "no pending tasks left"
        );
    }

    #[tokio::test]
    async fn test_clear() {
        let url = generate_temp_filename();
        let store = InflightActivationStore::new(&url).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![InflightActivation {
            activation: TaskActivation {
                id: "id_0".into(),
                namespace: "namespace".into(),
                taskname: "taskname".into(),
                parameters: "{}".into(),
                headers: HashMap::new(),
                received_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadline: None,
                retry_state: None,
                processing_deadline_duration: 0,
                expires: Some(1),
            },
            status: TaskActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            deadletter_at: None,
            processing_deadline: None,
        }];
        assert!(store.store(batch).await.is_ok());

        let result = sqlx::query(
            "SELECT count() as count
             FROM inflight_taskactivations;",
        )
        .fetch_one(&SqlitePool::connect(&url).await.unwrap())
        .await
        .unwrap();

        assert_eq!(result.get::<u32, &str>("count"), 1);

        assert!(store.clear().await.is_ok());

        let result = sqlx::query(
            "SELECT count() as count
             FROM inflight_taskactivations;",
        )
        .fetch_one(&SqlitePool::connect(&url).await.unwrap())
        .await
        .unwrap();

        assert_eq!(result.get::<u32, &str>("count"), 0);
    }
}
