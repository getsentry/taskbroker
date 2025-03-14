use futures::StreamExt;
use prost::Message as ProstMessage;
use rand::Rng;
use rdkafka::{
    Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
};
use std::{collections::HashMap, sync::Arc};

use crate::{
    config::Config,
    store::inflight_activation::{
        InflightActivation, InflightActivationStatus, InflightActivationStore,
        InflightActivationStoreConfig,
    },
};
use chrono::{Timelike, Utc};
use sentry_protos::taskbroker::v1::TaskActivation;

/// Generate a unique filename for isolated SQLite databases.
pub fn generate_temp_path() -> String {
    let mut rng = rand::thread_rng();
    format!("/var/tmp/{}-{}", Utc::now(), rng.r#gen::<u64>())
}

/// Create a collection of pending unsaved activations.
pub fn make_activations(count: u32) -> Vec<InflightActivation> {
    let mut records: Vec<InflightActivation> = vec![];
    for i in 0..count {
        let now = Utc::now();
        #[allow(deprecated)]
        let item = InflightActivation {
            activation: TaskActivation {
                id: format!("id_{}", i),
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
            },
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: i as i64,
            added_at: Utc::now(),
            processing_attempts: 0,
            expires_at: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".into(),
        };
        records.push(item);
    }
    records
}

/// Assert that the inflight store has a given number of records with a status.
pub async fn assert_count_by_status(
    store: &InflightActivationStore,
    status: InflightActivationStatus,
    expected: usize,
) {
    let count = store.count_by_status(status).await.unwrap();
    assert_eq!(
        count, expected,
        "Incorrect number of activations with {status:?}"
    );
}

/// Create a basic default [`Config`]
pub fn create_config() -> Arc<Config> {
    Arc::new(Config::default())
}

/// Create an InflightActivationStore instance
pub async fn create_test_store() -> InflightActivationStore {
    let url = generate_temp_path();

    InflightActivationStore::new(
        &url,
        InflightActivationStoreConfig::from_config(&create_integration_config()),
    )
    .await
    .unwrap()
}

/// Create a Config instance that uses a testing topic
/// and earliest auto_offset_reset. This is intended to be combined
/// with [`reset_topic`]
pub fn create_integration_config() -> Arc<Config> {
    let config = Config {
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
        .kafka_producer_config()
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
