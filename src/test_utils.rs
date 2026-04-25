use std::collections::HashMap;
use std::env::var;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Error, anyhow};
use async_trait::async_trait;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use prost::Message as ProstMessage;
use prost_types::Timestamp;
use sentry_protos::taskbroker::v1::{self, OnAttemptsExceeded, RetryState, TaskActivation};
use uuid::Uuid;

use crate::config::Config;
use crate::store::activation::{
    InflightActivation, InflightActivationBuilder, InflightActivationStatus,
};
use crate::store::adapters::postgres::{PostgresActivationStore, PostgresActivationStoreConfig};
use crate::store::adapters::sqlite::{InflightActivationStoreConfig, SqliteActivationStore};
use crate::store::traits::InflightActivationStore;
use crate::store::types::{BucketRange, FailedTasksForwarder};

/// Builder for `TaskActivation`. We cannot generate a builder automatically because `TaskActivation` is defined in `sentry-protos`.
pub struct TaskActivationBuilder {
    pub id: Option<String>,
    pub application: Option<String>,
    pub namespace: Option<String>,
    pub taskname: Option<String>,
    pub parameters: Option<String>,
    pub parameters_bytes: Option<Vec<u8>>,
    pub headers: Option<HashMap<String, String>>,
    pub received_at: Option<Timestamp>,
    pub retry_state: Option<v1::RetryState>,
    pub processing_deadline_duration: Option<u64>,
    pub expires: Option<u64>,
    pub delay: Option<u64>,
}

impl TaskActivationBuilder {
    pub fn new() -> Self {
        Self {
            id: None,
            application: None,
            namespace: None,
            taskname: None,
            parameters: None,
            parameters_bytes: None,
            headers: None,
            received_at: None,
            retry_state: None,
            processing_deadline_duration: None,
            expires: None,
            delay: None,
        }
    }

    pub fn id<T: Into<String>>(mut self, id: T) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn application<T: Into<String>>(mut self, application: T) -> Self {
        self.application = Some(application.into());
        self
    }

    pub fn namespace<T: Into<String>>(mut self, namespace: T) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn taskname<T: Into<String>>(mut self, taskname: T) -> Self {
        self.taskname = Some(taskname.into());
        self
    }

    #[allow(deprecated)]
    pub fn parameters<T: Into<String>>(mut self, parameters: T) -> Self {
        self.parameters = Some(parameters.into());
        self
    }

    pub fn parameters_bytes(mut self, parameters_bytes: Vec<u8>) -> Self {
        self.parameters_bytes = Some(parameters_bytes);
        self
    }

    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn received_at(mut self, received_at: Timestamp) -> Self {
        self.received_at = Some(received_at);
        self
    }

    pub fn retry_state(mut self, retry_state: v1::RetryState) -> Self {
        self.retry_state = Some(retry_state);
        self
    }

    pub fn processing_deadline_duration(mut self, processing_deadline_duration: u64) -> Self {
        self.processing_deadline_duration = Some(processing_deadline_duration);
        self
    }

    pub fn expires(mut self, expires: u64) -> Self {
        self.expires = Some(expires);
        self
    }

    pub fn delay(mut self, delay: u64) -> Self {
        self.delay = Some(delay);
        self
    }

    #[allow(deprecated)]
    pub fn build(self) -> v1::TaskActivation {
        v1::TaskActivation {
            id: self.id.expect("id is required"),
            application: Some(self.application.expect("application is required")),
            namespace: self.namespace.expect("namespace is required"),
            taskname: self.taskname.expect("taskname is required"),
            parameters: self.parameters.unwrap_or_else(|| "{}".to_string()),
            parameters_bytes: self.parameters_bytes.unwrap_or_default(),
            headers: self.headers.unwrap_or_default(),
            processing_deadline_duration: self.processing_deadline_duration.unwrap_or(0),
            received_at: self.received_at,
            retry_state: self.retry_state,
            expires: self.expires,
            delay: self.delay,
        }
    }
}

impl Default for TaskActivationBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InflightActivationBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(mut self, builder: TaskActivationBuilder) -> InflightActivation {
        // Grab required fields
        let id = self.id.as_ref().expect("field 'id' is required");

        let namespace = self
            .namespace
            .as_ref()
            .expect("field 'namespace' is required");

        let taskname = self
            .taskname
            .as_ref()
            .expect("field 'taskname' is required");

        // Grab fields with defaults
        let application = self.application.clone().unwrap_or_else(|| "sentry".into());
        let received_at = self.received_at.unwrap_or_default();
        let processing_deadline_duration = self.processing_deadline_duration.unwrap_or_default();

        // Infer 'expires' field
        let expires = self
            .expires_at
            .flatten()
            .map(|date_time| (date_time - received_at).num_seconds() as u64);

        // Infer 'delay' field
        let delay = self
            .delay_until
            .flatten()
            .map(|date_time| (date_time - received_at).num_seconds() as u64);

        // Build the activation
        let mut activation = builder
            .id(id)
            .application(application)
            .taskname(taskname)
            .namespace(namespace)
            .received_at(Timestamp::from(SystemTime::from(received_at)))
            .processing_deadline_duration(processing_deadline_duration as u64)
            .build();

        // Set 'expiration' and 'delay' fields manually after activation has been build
        activation.expires = expires;
        activation.delay = delay;

        self.activation = Some(activation.encode_to_vec());

        match self._build() {
            Ok(activation) => activation,
            Err(e) => panic!("Failed to build InflightActivation - {}", e),
        }
    }
}

pub fn get_pg_host() -> String {
    var("TASKBROKER_PG_HOST").unwrap_or("localhost".to_string())
}

pub fn get_pg_port() -> u16 {
    var("TASKBROKER_PG_PORT")
        .unwrap_or("5432".to_string())
        .parse()
        .unwrap()
}

pub fn get_pg_username() -> String {
    var("TASKBROKER_PG_USERNAME").unwrap_or("postgres".to_string())
}

pub fn get_pg_password() -> String {
    var("TASKBROKER_PG_PASSWORD").unwrap_or("password".to_string())
}

pub fn get_pg_database_name() -> String {
    let random_name = format!("a{}", Uuid::new_v4().to_string().replace("-", ""));
    var("TASKBROKER_PG_DATABASE_NAME").unwrap_or(random_name)
}

pub fn generate_temp_filename() -> String {
    format!(
        "/tmp/taskbroker-test-{}",
        Uuid::new_v4().to_string().replace("-", "")
    )
}

/// Generate a unique alphanumeric string for namespaces (and possibly other purposes).
pub fn generate_unique_namespace() -> String {
    Uuid::new_v4().to_string()
}

/// Create a collection of `count` pending unsaved activations in a particular `namespace`. If you do not want to provide a namespace, use `make_activations`.
pub fn make_activations_with_namespace(namespace: String, count: u32) -> Vec<InflightActivation> {
    let mut records: Vec<InflightActivation> = vec![];

    for i in 0..count {
        let now = Utc::now();

        let item = InflightActivationBuilder::new()
            .id(format!("id_{i}"))
            .taskname("taskname")
            .namespace(&namespace)
            .added_at(now)
            .received_at(now)
            .offset(i as i64)
            .processing_deadline_duration(10)
            .build(TaskActivationBuilder::new());

        records.push(item);
    }
    records
}

/// Create a collection of `count` pending unsaved activations in a unique namespace. If you want to provide the namespace, use `make_activations_with_namespace`.
pub fn make_activations(count: u32) -> Vec<InflightActivation> {
    let namespace = generate_unique_namespace();
    make_activations_with_namespace(namespace, count)
}

/// Create a basic default [`Config`]
pub fn create_config() -> Arc<Config> {
    Arc::new(Config::default())
}

/// Create an InflightActivationStore instance
pub async fn create_test_store(adapter: &str) -> Arc<dyn InflightActivationStore> {
    match adapter {
        "sqlite" => Arc::new(
            SqliteActivationStore::new(
                &generate_temp_filename(),
                InflightActivationStoreConfig::from_config(&create_integration_config()),
            )
            .await
            .unwrap(),
        ) as Arc<dyn InflightActivationStore>,
        "postgres" => {
            let store = Arc::new(
                PostgresActivationStore::new(PostgresActivationStoreConfig::from_config(
                    &create_integration_config(),
                ))
                .await
                .unwrap(),
            ) as Arc<dyn InflightActivationStore>;
            store.assign_partitions(vec![0]).unwrap();
            store
        }
        _ => panic!("Invalid adapter: {}", adapter),
    }
}

struct FailingSetStatusStore {
    inner: Arc<dyn InflightActivationStore>,
}

#[async_trait]
impl InflightActivationStore for FailingSetStatusStore {
    async fn store(&self, batch: Vec<InflightActivation>) -> Result<u64, Error> {
        self.inner.store(batch).await
    }

    fn assign_partitions(&self, partitions: Vec<i32>) -> Result<(), Error> {
        self.inner.assign_partitions(partitions)
    }

    async fn claim_activations(
        &self,
        application: Option<&str>,
        namespaces: Option<&[String]>,
        limit: Option<i32>,
        bucket: Option<BucketRange>,
        mark_processing: bool,
    ) -> Result<Vec<InflightActivation>, Error> {
        self.inner
            .claim_activations(application, namespaces, limit, bucket, mark_processing)
            .await
    }

    async fn mark_activation_processing(&self, id: &str) -> Result<(), Error> {
        self.inner.mark_activation_processing(id).await
    }

    async fn set_status(
        &self,
        _id: &str,
        _status: InflightActivationStatus,
    ) -> Result<Option<InflightActivation>, Error> {
        Err(anyhow!("injected set_status failure for test"))
    }

    async fn pending_activation_max_lag(&self, now: &DateTime<Utc>) -> f64 {
        self.inner.pending_activation_max_lag(now).await
    }

    async fn count_by_status(&self, status: InflightActivationStatus) -> Result<usize, Error> {
        self.inner.count_by_status(status).await
    }

    async fn count(&self) -> Result<usize, Error> {
        self.inner.count().await
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<InflightActivation>, Error> {
        self.inner.get_by_id(id).await
    }

    async fn set_processing_deadline(
        &self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.inner.set_processing_deadline(id, deadline).await
    }

    async fn delete_activation(&self, id: &str) -> Result<(), Error> {
        self.inner.delete_activation(id).await
    }

    async fn vacuum_db(&self) -> Result<(), Error> {
        self.inner.vacuum_db().await
    }

    async fn full_vacuum_db(&self) -> Result<(), Error> {
        self.inner.full_vacuum_db().await
    }

    async fn db_size(&self) -> Result<u64, Error> {
        self.inner.db_size().await
    }

    async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
        self.inner.get_retry_activations().await
    }

    async fn handle_claim_expiration(&self) -> Result<u64, Error> {
        self.inner.handle_claim_expiration().await
    }

    async fn handle_processing_deadline(&self) -> Result<u64, Error> {
        self.inner.handle_processing_deadline().await
    }

    async fn handle_processing_attempts(&self) -> Result<u64, Error> {
        self.inner.handle_processing_attempts().await
    }

    async fn handle_expires_at(&self) -> Result<u64, Error> {
        self.inner.handle_expires_at().await
    }

    async fn handle_delay_until(&self) -> Result<u64, Error> {
        self.inner.handle_delay_until().await
    }

    async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
        self.inner.handle_failed_tasks().await
    }

    async fn mark_completed(&self, ids: Vec<String>) -> Result<u64, Error> {
        self.inner.mark_completed(ids).await
    }

    async fn remove_completed(&self) -> Result<u64, Error> {
        self.inner.remove_completed().await
    }

    async fn remove_killswitched(&self, killswitched_tasks: Vec<String>) -> Result<u64, Error> {
        self.inner.remove_killswitched(killswitched_tasks).await
    }

    async fn clear(&self) -> Result<(), Error> {
        self.inner.clear().await
    }
}

pub async fn make_failing_store() -> Arc<dyn InflightActivationStore> {
    let inner = create_test_store("sqlite").await;
    Arc::new(FailingSetStatusStore { inner }) as Arc<dyn InflightActivationStore>
}

/// Create a Config instance that uses a testing topic
/// and earliest auto_offset_reset. This is intended to be combined
/// with [`reset_topic`]
pub fn create_integration_config() -> Arc<Config> {
    let config = Config {
        pg_host: get_pg_host(),
        pg_port: get_pg_port(),
        pg_username: get_pg_username(),
        pg_password: get_pg_password(),
        pg_database_name: get_pg_database_name(),
        run_migrations: true,
        kafka_topic: "taskbroker-test".into(),
        kafka_auto_offset_reset: "earliest".into(),
        ..Config::default()
    };

    Arc::new(config)
}

/// Create a Config instance that uses SSL
/// and earliest auto_offset_reset. This is intended to be combined
/// with [`reset_topic`]
pub fn create_integration_config_with_ssl() -> Arc<Config> {
    let config = Config {
        pg_host: get_pg_host(),
        pg_port: get_pg_port(),
        pg_username: get_pg_username(),
        pg_password: get_pg_password(),
        pg_database_name: get_pg_database_name(),
        pg_extra_query_params: Some("sslmode=require".to_string()),
        run_migrations: true,
        kafka_topic: "taskbroker-test".into(),
        kafka_auto_offset_reset: "earliest".into(),
        ..Config::default()
    };

    Arc::new(config)
}

pub fn create_integration_config_with_topic(topic: String) -> Arc<Config> {
    let config = Config {
        pg_host: get_pg_host(),
        pg_port: get_pg_port(),
        pg_username: get_pg_username(),
        pg_password: get_pg_password(),
        pg_database_name: get_pg_database_name(),
        run_migrations: true,
        kafka_topic: topic,
        kafka_auto_offset_reset: "earliest".into(),
        ..Config::default()
    };

    Arc::new(config)
}

/// Create a kafka producer for a given config
pub fn create_producer(config: Arc<Config>) -> Arc<FutureProducer> {
    let producer: FutureProducer = config
        .kafka_producer_config()
        .create()
        .expect("Could not create kafka producer");

    Arc::new(producer)
}

/// Reset a kafka topic by destroying it and recreating it.
pub async fn reset_topic(config: Arc<Config>) {
    let admin_client: AdminClient<_> = config
        .kafka_consumer_config()
        .create()
        .expect("Could not create admin client");

    let options = AdminOptions::default();
    admin_client
        .delete_topics(
            &[config.kafka_topic.as_ref(), &config.kafka_deadletter_topic],
            &options,
        )
        .await
        .expect("Could not delete topic");
    let new_topic = NewTopic::new(&config.kafka_topic, 1, TopicReplication::Fixed(0));
    let new_dlq_topic = NewTopic::new(
        &config.kafka_deadletter_topic,
        1,
        TopicReplication::Fixed(0),
    );
    admin_client
        .create_topics([&new_topic, &new_dlq_topic], &options)
        .await
        .expect("Could not create topic");
}

/// Consume a number of messages from topic based on [`Config`].
/// Will wait up to 30s for messages to arrive before panicing.
pub async fn consume_topic(
    config: Arc<Config>,
    topic: &str,
    num_records: usize,
) -> Vec<TaskActivation> {
    let consumer: StreamConsumer = config
        .kafka_consumer_config()
        .create()
        .expect("could not create consumer");
    consumer.subscribe(&[topic]).expect("could not subscribe");

    let mut stream = consumer.stream();
    let mut results: Vec<TaskActivation> = vec![];
    let mut last_message = None;
    let start = Utc::now();
    loop {
        let current = Utc::now();
        if current.timestamp() - start.timestamp() > 30 {
            panic!("Timed out waiting for messages from consumer");
        }
        if results.len() == num_records {
            break;
        }
        let message_opt = stream.next().await;
        let message_res = match message_opt {
            Some(result) => result,
            None => panic!("No message received"),
        };
        let message = match message_res {
            Ok(msg) => msg,
            Err(err) => panic!("Message is an error {err:?}"),
        };
        let payload = message.payload().expect("Could not fetch message payload");
        let activation = TaskActivation::decode(payload).unwrap();
        results.push(activation);
        last_message = Some(message);
    }
    // Commit the last message's offset so subsequent calls start from the next message
    if let Some(msg) = last_message {
        consumer.commit_message(&msg, CommitMode::Sync).unwrap();
    }
    results
}

pub fn replace_retry_state(inflight: &mut InflightActivation, retry: Option<RetryState>) {
    let mut activation = TaskActivation::decode(&inflight.activation as &[u8]).unwrap();
    activation.retry_state = retry;
    inflight.activation = activation.encode_to_vec();
    if let Some(retry) = retry {
        inflight.on_attempts_exceeded =
            retry.on_attempts_exceeded.try_into().expect("invalid enum");
    } else {
        inflight.on_attempts_exceeded = OnAttemptsExceeded::Discard;
    }
}

/// Helper struct for asserting counts on the InflightActivationStore.
#[derive(Default)]
pub struct StatusCount {
    pub pending: usize,
    pub claimed: usize,
    pub processing: usize,
    pub retry: usize,
    pub delayed: usize,
    pub complete: usize,
    pub failure: usize,
}

/// Assert the state of all counts in the inflight activation store.
pub async fn assert_counts(expected: StatusCount, store: &dyn InflightActivationStore) {
    assert_eq!(
        expected.pending,
        store
            .count_by_status(InflightActivationStatus::Pending)
            .await
            .unwrap(),
        "difference in pending count",
    );
    assert_eq!(
        expected.claimed,
        store
            .count_by_status(InflightActivationStatus::Claimed)
            .await
            .unwrap(),
        "difference in claimed count",
    );
    assert_eq!(
        expected.processing,
        store
            .count_by_status(InflightActivationStatus::Processing)
            .await
            .unwrap(),
        "difference in processing count",
    );
    assert_eq!(
        expected.retry,
        store
            .count_by_status(InflightActivationStatus::Retry)
            .await
            .unwrap(),
        "difference in retry count",
    );
    assert_eq!(
        expected.delayed,
        store
            .count_by_status(InflightActivationStatus::Delay)
            .await
            .unwrap(),
        "difference in delay count",
    );
    assert_eq!(
        expected.complete,
        store
            .count_by_status(InflightActivationStatus::Complete)
            .await
            .unwrap(),
        "difference in complete count",
    );
    assert_eq!(
        expected.failure,
        store
            .count_by_status(InflightActivationStatus::Failure)
            .await
            .unwrap(),
        "difference in failure count",
    );
}

/// Store a single inflight activation with a caller-provided task name and
/// namespace, then return its task id.
///
/// This helper is used by gRPC server tests that need a real inflight row in
/// the store before calling `set_task_status(...)`.
pub async fn seed_inflight(
    store: &Arc<dyn InflightActivationStore>,
    taskname: &str,
    namespace: &str,
) -> String {
    let mut activations = make_activations(1);

    let mut payload = TaskActivation::decode(&activations[0].activation as &[u8]).unwrap();
    payload.taskname = taskname.to_string();
    payload.namespace = namespace.to_string();

    activations[0].taskname = taskname.to_string();
    activations[0].namespace = namespace.to_string();
    activations[0].activation = payload.encode_to_vec();

    let id = activations[0].id.clone();
    store.store(activations).await.unwrap();
    id
}
