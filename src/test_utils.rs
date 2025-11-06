use futures::StreamExt;
use prost::Message as ProstMessage;
use rdkafka::{
    Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
};
use std::{collections::HashMap, env::var, sync::Arc};

use crate::{
    config::Config,
    store::inflight_activation::{
        InflightActivation, InflightActivationStatus, InflightActivationStore,
        InflightActivationStoreConfig,
    },
};
use chrono::{Timelike, Utc};
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, RetryState, TaskActivation};

pub fn get_pg_url() -> String {
    var("TASKBROKER_PG_URL").unwrap_or("postgres://postgres:password@localhost:5432/".to_string())
}

pub fn get_pg_database_name() -> String {
    var("TASKBROKER_PG_DATABASE_NAME").unwrap_or("taskbroker".to_string())
}

/// Create a collection of pending unsaved activations.
pub fn make_activations(count: u32) -> Vec<InflightActivation> {
    let mut records: Vec<InflightActivation> = vec![];
    for i in 0..count {
        let now = Utc::now();
        #[allow(deprecated)]
        let item = InflightActivation {
            id: format!("id_{i}"),
            activation: TaskActivation {
                id: format!("id_{i}"),
                namespace: "namespace".into(),
                taskname: "taskname".into(),
                parameters: "{}".into(),
                headers: HashMap::new(),
                received_at: Some(prost_types::Timestamp {
                    seconds: now.timestamp(),
                    nanos: now.nanosecond() as i32,
                }),
                retry_state: None,
                processing_deadline_duration: 10,
                expires: None,
                delay: None,
            }
            .encode_to_vec(),
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: i as i64,
            added_at: now,
            received_at: now,
            processing_attempts: 0,
            processing_deadline_duration: 10,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".into(),
            taskname: "taskname".into(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
        };
        records.push(item);
    }
    records
}

/// Create a basic default [`Config`]
pub fn create_config() -> Arc<Config> {
    Arc::new(Config::default())
}

/// Create an InflightActivationStore instance
pub async fn create_test_store() -> Arc<InflightActivationStore> {
    let store = Arc::new(
        InflightActivationStore::new(InflightActivationStoreConfig::from_config(
            &create_integration_config(),
        ))
        .await
        .unwrap(),
    );
    store.clear().await.unwrap();
    store
}

/// Create a Config instance that uses a testing topic
/// and earliest auto_offset_reset. This is intended to be combined
/// with [`reset_topic`]
pub fn create_integration_config() -> Arc<Config> {
    let config = Config {
        pg_url: get_pg_url(),
        pg_database_name: get_pg_database_name(),
        kafka_topic: "taskbroker-test".into(),
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
    pub processing: usize,
    pub retry: usize,
    pub delayed: usize,
    pub complete: usize,
    pub failure: usize,
}

/// Assert the state of all counts in the inflight activation store.
pub async fn assert_counts(expected: StatusCount, store: &InflightActivationStore) {
    assert_eq!(
        expected.pending,
        store
            .count_by_status(InflightActivationStatus::Pending)
            .await
            .unwrap(),
        "difference in pending count",
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
