use anyhow::Error;
use chrono::{DateTime, Utc};
use prost::Message;
use sentry_protos::sentry::v1::TaskActivation;
use sqlx::{migrate::MigrateDatabase, sqlite::SqlitePool, QueryBuilder, Row, Sqlite};

pub struct InflightTaskStore {
    sqlite_pool: SqlitePool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TaskActivationStatus {
    Unspecified,
    Pending,
    Processing,
    Failure,
    Retry,
    Complete,
}

impl TaskActivationStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    fn as_str_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "TASK_ACTIVATION_STATUS_UNSPECIFIED",
            Self::Pending => "TASK_ACTIVATION_STATUS_PENDING",
            Self::Processing => "TASK_ACTIVATION_STATUS_PROCESSING",
            Self::Failure => "TASK_ACTIVATION_STATUS_FAILURE",
            Self::Retry => "TASK_ACTIVATION_STATUS_RETRY",
            Self::Complete => "TASK_ACTIVATION_STATUS_COMPLETE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "TASK_ACTIVATION_STATUS_UNSPECIFIED" => Some(Self::Unspecified),
            "TASK_ACTIVATION_STATUS_PENDING" => Some(Self::Pending),
            "TASK_ACTIVATION_STATUS_PROCESSING" => Some(Self::Processing),
            "TASK_ACTIVATION_STATUS_FAILURE" => Some(Self::Failure),
            "TASK_ACTIVATION_STATUS_RETRY" => Some(Self::Retry),
            "TASK_ACTIVATION_STATUS_COMPLETE" => Some(Self::Complete),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InflightActivation {
    activation: TaskActivation,
    status: TaskActivationStatus,
    offset: i64,
    added_at: DateTime<Utc>,
    deadletter_at: Option<DateTime<Utc>>,
    processing_deadline: Option<DateTime<Utc>>,
}

pub struct TableRow {
    id: String,
    activation: Vec<u8>,
    offset: i64,
    added_at: DateTime<Utc>,
    deadletter_at: Option<DateTime<Utc>>,
    processing_deadline_duration: u32,
    processing_deadline: Option<DateTime<Utc>>,
    status: String,
}

impl TryFrom<InflightActivation> for TableRow {
    type Error = anyhow::Error;

    fn try_from(value: InflightActivation) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.activation.id.clone(),
            activation: value.activation.encode_to_vec(),
            offset: value.offset,
            added_at: value.added_at,
            deadletter_at: value.deadletter_at,
            processing_deadline_duration: value.activation.processing_deadline_duration as u32,
            processing_deadline: None,
            status: TaskActivationStatus::Pending.as_str_name().into(),
        })
    }
}

impl From<TableRow> for InflightActivation {
    fn from(value: TableRow) -> Self {
        Self {
            activation: TaskActivation::decode(&value.activation as &[u8]).expect(
                "Decode should always be successful as we only store encoded data in this column",
            ),
            status: TaskActivationStatus::from_str_name(&value.status).unwrap(),
            offset: value.offset,
            added_at: value.added_at,
            deadletter_at: value.deadletter_at,
            processing_deadline: value.processing_deadline,
        }
    }
}

impl InflightTaskStore {
    pub async fn new(url: &str) -> Result<Self, Error> {
        if !Sqlite::database_exists(url).await? {
            Sqlite::create_database(url).await?
        }
        let sqlite_pool = SqlitePool::connect(url).await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS inflight_taskactivations (
                    id UUID NOT NULL PRIMARY KEY,
                    activation BLOB NOT NULL,
                    offset BIGINTEGER NOT NULL,
                    added_at DATETIME NOT NULL,
                    deadletter_at DATETIME,
                    processing_deadline_duration INTEGER NOT NULL,
                    processing_deadline DATETIME,
                    status INTEGER NOT NULL
                );",
        )
        .execute(&sqlite_pool)
        .await?;

        Ok(Self { sqlite_pool })
    }

    pub async fn store(&self, batch: Vec<InflightActivation>) -> Result<(), Error> {
        let mut query_builder = QueryBuilder::<Sqlite>::new(
            "INSERT INTO inflight_taskactivations \
            (id, activation, offset, added_at, deadletter_at, processing_deadline_duration, status)",
        );
        let rows = batch
            .into_iter()
            .map(TableRow::try_from)
            .collect::<Result<Vec<TableRow>, _>>()?;
        let query = query_builder
            .push_values(rows, |mut b, row| {
                b.push_bind(row.id);
                b.push_bind(row.activation);
                b.push_bind(row.offset);
                b.push_bind(row.added_at);
                b.push_bind(row.deadletter_at);
                b.push_bind(row.processing_deadline_duration);
                b.push_bind(row.status);
            })
            .build();
        query.execute(&self.sqlite_pool).await?;
        Ok(())
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
        .bind(TaskActivationStatus::Processing.as_str_name())
        .bind(TaskActivationStatus::Pending.as_str_name())
        .fetch_optional(&self.sqlite_pool)
        .await?;

        let Some(row) = result else { return Ok(None) };

        let row = sqlx::query("SELECT * FROM inflight_taskactivations WHERE id = ?")
            .bind(row.get::<String, _>("id"))
            .fetch_one(&self.sqlite_pool)
            .await
            .expect("This row should always be available as we just fetched the ID via the previous query");

        Ok(Some(
            TableRow {
                id: row.get("id"),
                activation: row.get("activation"),
                offset: row.get("offset"),
                added_at: row.get("added_at"),
                deadletter_at: row.get("deadletter_at"),
                processing_deadline_duration: row.get("processing_deadline_duration"),
                processing_deadline: row.get("processing_deadline"),
                status: row.get("status"),
            }
            .into(),
        ))
    }

    pub async fn count_pending_activations(&self) -> Result<usize, Error> {
        let result =
            sqlx::query("SELECT COUNT(*) as count FROM inflight_taskactivations WHERE status = $1")
                .bind(TaskActivationStatus::Pending.as_str_name())
                .fetch_one(&self.sqlite_pool)
                .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    pub async fn set_status(&self, id: &str, status: TaskActivationStatus) -> Result<(), Error> {
        sqlx::query("UPDATE inflight_taskactivations SET status = $1 WHERE id = $2")
            .bind(status.as_str_name())
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
            sqlx::query("SELECT * FROM inflight_taskactivations WHERE status = $1")
                .bind(TaskActivationStatus::Retry.as_str_name())
                .fetch_all(&self.sqlite_pool)
                .await?
                .into_iter()
                .map(|row| {
                    TableRow {
                        id: row.get("id"),
                        activation: row.get("activation"),
                        offset: row.get("offset"),
                        added_at: row.get("added_at"),
                        deadletter_at: row.get("deadletter_at"),
                        processing_deadline_duration: row.get("processing_deadline_duration"),
                        processing_deadline: row.get("processing_deadline"),
                        status: row.get("status"),
                    }
                    .into()
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{DateTime, Utc};
    use rand::Rng;
    use sentry_protos::sentry::v1::TaskActivation;
    use sqlx::{Row, SqlitePool};

    use crate::inflight_task_store::{InflightActivation, InflightTaskStore, TaskActivationStatus};

    fn generate_temp_filename() -> String {
        let mut rng = rand::thread_rng();
        format!("/var/tmp/{}-{}.sqlite", Utc::now(), rng.gen::<u64>())
    }

    #[tokio::test]
    async fn test_create_db() {
        assert!(InflightTaskStore::new(&generate_temp_filename())
            .await
            .is_ok())
    }

    #[tokio::test]
    async fn test_store() {
        let url = generate_temp_filename();
        let store = InflightTaskStore::new(&url).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
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
                status: TaskActivationStatus::Unspecified,
                offset: 0,
                added_at: Utc::now(),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: TaskActivation {
                    id: "id_1".into(),
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
                status: TaskActivationStatus::Unspecified,
                offset: 1,
                added_at: Utc::now(),
                deadletter_at: None,
                processing_deadline: None,
            },
        ];
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
        let store = InflightTaskStore::new(&url).await.unwrap();
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
            status: TaskActivationStatus::Unspecified,
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
            status: TaskActivationStatus::Processing.into(),
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
        let store = InflightTaskStore::new(&url).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
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
                status: TaskActivationStatus::Unspecified,
                offset: 0,
                added_at: Utc::now(),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: TaskActivation {
                    id: "id_1".into(),
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
                status: TaskActivationStatus::Unspecified,
                offset: 1,
                added_at: Utc::now(),
                deadletter_at: None,
                processing_deadline: None,
            },
        ];
        assert!(store.store(batch).await.is_ok());

        assert_eq!(store.count_pending_activations().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn set_activation_status() {
        let url = generate_temp_filename();
        let store = InflightTaskStore::new(&url).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
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
                status: TaskActivationStatus::Unspecified,
                offset: 0,
                added_at: Utc::now(),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: TaskActivation {
                    id: "id_1".into(),
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
                status: TaskActivationStatus::Unspecified,
                offset: 1,
                added_at: Utc::now(),
                deadletter_at: None,
                processing_deadline: None,
            },
        ];
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
        let store = InflightTaskStore::new(&url).await.unwrap();

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
                processing_deadline_duration: 0,
                expires: Some(1),
            },
            status: TaskActivationStatus::Unspecified,
            offset: 0,
            added_at: Utc::now(),
            deadletter_at: None,
            processing_deadline: None,
        }];
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
        let store = InflightTaskStore::new(&url).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
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
                status: TaskActivationStatus::Unspecified,
                offset: 0,
                added_at: Utc::now(),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: TaskActivation {
                    id: "id_1".into(),
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
                status: TaskActivationStatus::Unspecified,
                offset: 1,
                added_at: Utc::now(),
                deadletter_at: None,
                processing_deadline: None,
            },
        ];
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
        let store = InflightTaskStore::new(&url).await.unwrap();
        let added_at = Utc::now();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
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
                status: TaskActivationStatus::Unspecified,
                offset: 0,
                added_at,
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: TaskActivation {
                    id: "id_1".into(),
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
                status: TaskActivationStatus::Unspecified,
                offset: 1,
                added_at,
                deadletter_at: None,
                processing_deadline: None,
            },
        ];
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

        #[allow(deprecated)]
        let expected = vec![
            InflightActivation {
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
                status: TaskActivationStatus::Retry,
                offset: 0,
                added_at,
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: TaskActivation {
                    id: "id_1".into(),
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
                status: TaskActivationStatus::Retry,
                offset: 1,
                added_at,
                deadletter_at: None,
                processing_deadline: None,
            },
        ];

        assert_eq!(store.get_retry_activations().await.unwrap(), expected);
    }
}
