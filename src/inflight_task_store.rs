use std::sync::Arc;

use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use prost::Message;
use rdkafka::{
    error::KafkaError,
    message::OwnedMessage,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use sentry_protos::sentry::v1::{InflightActivation, TaskActivation, TaskActivationStatus};
use sqlx::{migrate::MigrateDatabase, sqlite::SqlitePool, QueryBuilder, Row, Sqlite};
use tokio::task::JoinSet;

pub struct InflightTaskStore<T> {
    sqlite_pool: SqlitePool,
    kafka_producer: Arc<T>,
}

pub struct TableRow {
    id: String,
    activation: Vec<u8>,
    offset: i64,
    added_at: DateTime<Utc>,
    deadletter_at: Option<DateTime<Utc>>,
    processing_deadline_duration: u32,
    processing_deadline: Option<DateTime<Utc>>,
    status: i32,
}

impl TryFrom<InflightActivation> for TableRow {
    type Error = anyhow::Error;

    fn try_from(value: InflightActivation) -> Result<Self, Self::Error> {
        let activation = value.activation.as_ref().ok_or(anyhow!(
            "Failed to convert InflightActivation to TableRow: missing activation"
        ))?;

        Ok(Self {
            id: activation.id.clone(),
            activation: activation.encode_to_vec(),
            offset: value.offset,
            added_at: DateTime::from_timestamp(
                value
                    .added_at
                    .ok_or(anyhow!(
                        "Failed to convert InflightActivation to TableRow: missing added_at"
                    ))?
                    .seconds,
                0,
            )
            .ok_or(anyhow!(
                "Failed to convert InflightActivation to TableRow: invalid added_at timestamp"
            ))?,
            deadletter_at: value
                .deadletter_at
                .map(|timestamp| {
                    DateTime::from_timestamp(timestamp.seconds, 0).ok_or(anyhow!(
                    "Failed to convert InflightActivation to TableRow: invalid added_at timestamp"
                ))
                })
                .transpose()?,
            processing_deadline_duration: activation.processing_deadline_duration as u32,
            processing_deadline: None,
            status: TaskActivationStatus::Pending.into(),
        })
    }
}

impl From<TableRow> for InflightActivation {
    fn from(value: TableRow) -> Self {
        Self {
            activation: Some(TaskActivation::decode(&value.activation as &[u8]).expect(
                "Decode should always be successful as we only store encoded data in this column",
            )),
            status: value.status,
            offset: value.offset,
            added_at: Some(prost_types::Timestamp {
                seconds: value.added_at.timestamp(),
                nanos: 0,
            }),
            deadletter_at: value.deadletter_at.map(|date_time| prost_types::Timestamp {
                seconds: date_time.timestamp(),
                nanos: 0,
            }),
            processing_deadline: value.processing_deadline.map(|date_time| {
                prost_types::Timestamp {
                    seconds: date_time.timestamp(),
                    nanos: 0,
                }
            }),
        }
    }
}

impl<T> InflightTaskStore<T> {
    pub async fn new(url: &str, kafka_producer: T) -> Result<Self, Error> {
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

        Ok(Self {
            sqlite_pool,
            kafka_producer: Arc::new(kafka_producer),
        })
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
        .bind(TaskActivationStatus::Processing as i32)
        .bind(TaskActivationStatus::Pending as i32)
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
                .bind(TaskActivationStatus::Pending as i32)
                .fetch_one(&self.sqlite_pool)
                .await?;
        Ok(result.get::<u64, _>("count") as usize)
    }

    pub async fn set_status(&self, id: &str, status: TaskActivationStatus) -> Result<(), Error> {
        sqlx::query("UPDATE inflight_taskactivations SET status = $1 WHERE id = $2")
            .bind(status as i32)
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
}

pub trait Producer {
    fn send<T>(
        &self,
        payload: T,
    ) -> impl std::future::Future<Output = Result<(i32, i64), (KafkaError, OwnedMessage)>> + Send
    where
        T: rdkafka::message::ToBytes + Send + Sync;
}

pub struct KafkaProducer {
    producer: FutureProducer,
    topic: String,
    timeout: Timeout,
}

impl Producer for KafkaProducer {
    async fn send<T>(&self, payload: T) -> Result<(i32, i64), (KafkaError, OwnedMessage)>
    where
        T: rdkafka::message::ToBytes + Send + Sync,
    {
        self.producer
            .clone()
            .send(
                FutureRecord::<(), T>::to(&self.topic).payload(&payload),
                self.timeout,
            )
            .await
    }
}

impl<T> InflightTaskStore<T>
where
    T: Producer + Send + Sync + 'static,
{
    pub async fn upkeep(&self) -> Result<(), Error> {
        todo!()
    }

    async fn produce_retry_activations(&self) -> Result<(), Error> {
        let mut inflight_activations: Vec<InflightActivation> =
            sqlx::query("SELECT * FROM inflight_taskactivations WHERE status = $1")
                .bind(TaskActivationStatus::Retry as i32)
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
                .collect();

        inflight_activations
            .iter_mut()
            .for_each(|inflight_activation| {
                inflight_activation
                    .activation
                    .as_mut()
                    .expect("InflightActivation should always have a TaskActivation")
                    .retry_state
                    .as_mut()
                    .expect("InflightActivation should always have a retry state")
                    .attempts += 1;
            });

        let retry_activation_ids: Vec<_> = inflight_activations
            .iter()
            .map(|inflight_activation| {
                inflight_activation
                    .activation
                    .as_ref()
                    .expect("InflightActivation should always have a TaskActivation")
                    .id
                    .clone()
            })
            .collect();

        let kafka_acks: JoinSet<_> = inflight_activations
            .into_iter()
            .map(|inflight_activation| {
                let kafka_producer = self.kafka_producer.clone();
                tokio::spawn(async move {
                    kafka_producer
                        .send(
                            inflight_activation
                                .activation
                                .expect("InflightActivation should always have a TaskActivation")
                                .encode_to_vec(),
                        )
                        .await
                })
            })
            .collect();

        kafka_acks.join_all().await;

        let mut query_builder =
            QueryBuilder::<Sqlite>::new("DELETE FROM inflight_taskactivations WHERE id in (");
        let mut separated = query_builder.separated(", ");
        for id in retry_activation_ids.into_iter() {
            separated.push_bind(id);
        }
        separated.push_unseparated(") ");
        query_builder.build().execute(&self.sqlite_pool).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock},
        time::Duration,
    };

    use chrono::{DateTime, Utc};
    use prost::Message;
    use rand::Rng;
    use rdkafka::{
        error::KafkaError, message::OwnedMessage, producer::FutureProducer, ClientConfig,
    };
    use sentry_protos::sentry::v1::{
        InflightActivation, RetryState, TaskActivation, TaskActivationStatus,
    };
    use sqlx::{Row, SqlitePool};
    use tokio::time::sleep;

    use crate::inflight_task_store::{InflightTaskStore, Producer};

    use super::KafkaProducer;

    fn generate_temp_filename() -> String {
        let mut rng = rand::thread_rng();
        format!("/var/tmp/{}-{}.sqlite", Utc::now(), rng.gen::<u64>())
    }

    #[tokio::test]
    async fn test_create_db() {
        assert!(InflightTaskStore::new(&generate_temp_filename(), ())
            .await
            .is_ok())
    }

    #[tokio::test]
    async fn test_store() {
        let url = generate_temp_filename();
        let store = InflightTaskStore::new(&url, ()).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
                activation: Some(TaskActivation {
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
                }),
                status: 0,
                offset: 0,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: Some(TaskActivation {
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
                }),
                status: 0,
                offset: 1,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
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
        let store = InflightTaskStore::new(&url, ()).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![InflightActivation {
            activation: Some(TaskActivation {
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
            }),
            status: 0,
            offset: 0,
            added_at: Some(prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            deadletter_at: None,
            processing_deadline: None,
        }];
        assert!(store.store(batch.clone()).await.is_ok());

        let result = store.get_pending_activation().await.unwrap();
        #[allow(deprecated)]
        let expected = Some(InflightActivation {
            activation: Some(TaskActivation {
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
            }),
            status: TaskActivationStatus::Processing.into(),
            offset: 0,
            added_at: Some(prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            deadletter_at: None,
            processing_deadline: result.as_ref().unwrap().processing_deadline,
        });

        assert_eq!(result, expected);

        let deadline = result.as_ref().unwrap().processing_deadline;
        assert!(deadline.unwrap().seconds > Utc::now().timestamp());
    }

    #[tokio::test]
    async fn test_count_pending_activations() {
        let url = generate_temp_filename();
        let store = InflightTaskStore::new(&url, ()).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
                activation: Some(TaskActivation {
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
                }),
                status: 0,
                offset: 0,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: Some(TaskActivation {
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
                }),
                status: 0,
                offset: 1,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
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
        let store = InflightTaskStore::new(&url, ()).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
                activation: Some(TaskActivation {
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
                }),
                status: 0,
                offset: 0,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: Some(TaskActivation {
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
                }),
                status: 0,
                offset: 1,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
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
        let store = InflightTaskStore::new(&url, ()).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![InflightActivation {
            activation: Some(TaskActivation {
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
            }),
            status: 0,
            offset: 0,
            added_at: Some(prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
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
        let store = InflightTaskStore::new(&url, ()).await.unwrap();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
                activation: Some(TaskActivation {
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
                }),
                status: 0,
                offset: 0,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: Some(TaskActivation {
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
                }),
                status: 0,
                offset: 1,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
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
    async fn test_store_can_accept_kafka_producer() {
        let url = generate_temp_filename();
        let store = InflightTaskStore::new(
            &url,
            KafkaProducer {
                producer: ClientConfig::new()
                    .set("bootstrap.servers", "127.0.0.1:9092")
                    .set("message.timeout.ms", "5000")
                    .create::<FutureProducer>()
                    .expect("Producer creation error"),
                topic: "topic".into(),
                timeout: rdkafka::util::Timeout::After(Duration::from_secs(1)),
            },
        )
        .await
        .unwrap();

        assert!(store.produce_retry_activations().await.is_ok());
    }

    struct TestProducer {
        data: Arc<RwLock<Vec<Vec<u8>>>>,
    }

    impl Producer for TestProducer {
        async fn send<T>(&self, payload: T) -> Result<(i32, i64), (KafkaError, OwnedMessage)>
        where
            T: rdkafka::message::ToBytes + Send + Sync,
        {
            self.data.write().unwrap().push(payload.to_bytes().to_vec());
            sleep(Duration::from_secs(1)).await;
            Ok((0, self.data.read().unwrap().len().try_into().unwrap()))
        }
    }

    #[tokio::test]
    async fn test_produce_retry_activations() {
        let url = generate_temp_filename();
        let producer_queue = Arc::new(RwLock::new(Vec::new()));
        let store = InflightTaskStore::new(
            &url,
            TestProducer {
                data: producer_queue.clone(),
            },
        )
        .await
        .unwrap();

        #[allow(deprecated)]
        let batch = vec![
            InflightActivation {
                activation: Some(TaskActivation {
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
                    retry_state: Some(RetryState {
                        attempts: 1,
                        kind: "".into(),
                        discard_after_attempt: None,
                        deadletter_after_attempt: None,
                    }),
                    processing_deadline_duration: 0,
                    expires: Some(1),
                }),
                status: 0,
                offset: 0,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: Some(TaskActivation {
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
                    retry_state: Some(RetryState {
                        attempts: 2,
                        kind: "".into(),
                        discard_after_attempt: None,
                        deadletter_after_attempt: None,
                    }),
                    processing_deadline_duration: 0,
                    expires: Some(1),
                }),
                status: 0,
                offset: 1,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadletter_at: None,
                processing_deadline: None,
            },
            InflightActivation {
                activation: Some(TaskActivation {
                    id: "id_2".into(),
                    namespace: "namespace".into(),
                    taskname: "taskname".into(),
                    parameters: "{}".into(),
                    headers: HashMap::new(),
                    received_at: Some(prost_types::Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    deadline: None,
                    retry_state: Some(RetryState {
                        attempts: 3,
                        kind: "".into(),
                        discard_after_attempt: None,
                        deadletter_after_attempt: None,
                    }),
                    processing_deadline_duration: 0,
                    expires: Some(1),
                }),
                status: 0,
                offset: 1,
                added_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                deadletter_at: None,
                processing_deadline: None,
            },
        ];

        assert!(store.store(batch.clone()).await.is_ok());
        assert!(store.produce_retry_activations().await.is_ok());
        assert_eq!(producer_queue.read().unwrap().len(), 0);

        assert!(store
            .set_status("id_0", TaskActivationStatus::Retry)
            .await
            .is_ok());
        assert!(store
            .set_status("id_1", TaskActivationStatus::Retry)
            .await
            .is_ok());
        assert!(store.produce_retry_activations().await.is_ok());
        assert_eq!(producer_queue.read().unwrap().len(), 2);
        let mut expected_activation = batch[1].activation.as_ref().unwrap().clone();
        expected_activation.retry_state.as_mut().unwrap().attempts += 1;
        assert_eq!(
            TaskActivation::decode(&producer_queue.write().unwrap().pop().unwrap() as &[u8])
                .unwrap(),
            expected_activation
        );
        let mut expected_activation = batch[0].activation.as_ref().unwrap().clone();
        expected_activation.retry_state.as_mut().unwrap().attempts += 1;
        assert_eq!(
            TaskActivation::decode(&producer_queue.write().unwrap().pop().unwrap() as &[u8])
                .unwrap(),
            expected_activation
        );
    }
}
