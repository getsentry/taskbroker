use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Error, anyhow};
use futures::future::join_all;
use rdkafka::Message as _;
use rdkafka::Timestamp;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::reduce::Reduce;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskFunc, RunTaskInThreads,
};
use sentry_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory, StrategyError,
    SubmitError, merge_commit_request,
};
use sentry_arroyo::types::{InnerMessage, Message, Partition, Topic};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, info};

use crate::config::Config;
use crate::kafka::activation_batcher::ActivationBatcherConfig;
use crate::kafka::activation_writer::{ActivationWriter, ActivationWriterConfig};
use crate::kafka::deserialize::{self, DeserializeConfig};
use crate::killswitch;
use crate::runtime_config::RuntimeConfigManager;
use crate::store::activation::Activation;
use crate::store::traits::ActivationStore;
use crate::store::types::TopicPartition;

const DEFAULT_MAX_POLL_INTERVAL_MS: usize = 300_000;
const STORE_BACKPRESSURE_REPOLL: Duration = Duration::from_millis(250);

type ActivationDeserializer =
    dyn Fn(&OwnedMessage) -> Result<Option<Activation>, Error> + Send + Sync + 'static;
type BatchAccumulator = dyn Fn(
        ActivationBatch,
        Message<BatchInput>,
    ) -> Result<ActivationBatch, (SubmitError<BatchInput>, ActivationBatch)>
    + Send
    + Sync;

#[derive(Clone)]
struct BatchInput {
    activation: Option<Box<Activation>>,
    weight: usize,
}

#[derive(Clone, Default)]
struct ActivationBatch {
    activations: Vec<Activation>,
    activation_bytes: usize,
    forward_payloads: Vec<Vec<u8>>,
}

pub async fn start_consumer(
    topic: &str,
    config: Arc<Config>,
    activation_store: Arc<dyn ActivationStore>,
    runtime_config_manager: Arc<RuntimeConfigManager>,
) -> Result<(), Error> {
    let topic = topic.to_owned();
    let topics_tag = topic.clone();
    let kafka_config = arroyo_kafka_config_for(config.as_ref(), &topic)?;
    let topic_ref = Topic::new(&topic);
    let assigned_partitions = Arc::new(Mutex::new(BTreeSet::new()));
    let runtime = Handle::current();

    let factory = TaskbrokerStrategyFactory {
        config,
        activation_store: activation_store.clone(),
        runtime_config_manager,
        topic: topic.clone(),
        topics_tag,
        runtime,
        assigned_partitions: assigned_partitions.clone(),
    };

    let processor = StreamProcessor::with_kafka(kafka_config, factory, topic_ref, None);
    let mut processor_handle = processor.get_handle();

    let mut run_handle =
        tokio::task::spawn_blocking(move || processor.run().map_err(|err| err.to_string()));
    let shutdown_guard = elegant_departure::get_shutdown_guard();

    tokio::select! {
        result = &mut run_handle => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(anyhow!("arroyo consumer failed: {err}")),
                Err(err) => Err(anyhow!("arroyo consumer task panicked: {err}")),
            }
        }
        _ = shutdown_guard.wait() => {
            info!("Cancellation token received, shutting down Arroyo consumer...");
            processor_handle.signal_shutdown();
            let result = run_handle.await;
            revoke_remaining_partitions(&activation_store, &assigned_partitions);
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(anyhow!("arroyo consumer failed during shutdown: {err}")),
                Err(err) => Err(anyhow!("arroyo consumer task panicked during shutdown: {err}")),
            }
        }
    }
}

fn arroyo_kafka_config_for(config: &Config, topic: &str) -> Result<KafkaConfig, Error> {
    let rdkafka_config = config.kafka_consumer_config_for(topic);
    let config_map = rdkafka_config.config_map();

    let bootstrap_servers = config_map
        .get("bootstrap.servers")
        .ok_or_else(|| anyhow!("missing bootstrap.servers for topic {topic}"))?
        .split(',')
        .map(str::to_owned)
        .collect();
    let group_id = config_map
        .get("group.id")
        .ok_or_else(|| anyhow!("missing group.id for topic {topic}"))?
        .to_owned();
    let auto_offset_reset = config_map
        .get("auto.offset.reset")
        .map(|value| InitialOffset::from_str(value))
        .transpose()
        .map_err(|_| anyhow!("invalid auto.offset.reset for topic {topic}"))?
        .unwrap_or(InitialOffset::Earliest);
    let max_poll_interval_ms = config_map
        .get("max.poll.interval.ms")
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_MAX_POLL_INTERVAL_MS);

    let mut overrides: HashMap<String, String> = config_map.clone();
    for key in [
        "bootstrap.servers",
        "group.id",
        "auto.offset.reset",
        "enable.auto.commit",
        "enable.auto.offset.store",
        "max.poll.interval.ms",
    ] {
        overrides.remove(key);
    }

    Ok(KafkaConfig::new_consumer_config(
        bootstrap_servers,
        group_id,
        auto_offset_reset,
        false,
        max_poll_interval_ms,
        Some(overrides),
    ))
}

#[derive(Clone)]
struct TaskbrokerStrategyFactory {
    config: Arc<Config>,
    activation_store: Arc<dyn ActivationStore>,
    runtime_config_manager: Arc<RuntimeConfigManager>,
    topic: String,
    topics_tag: String,
    runtime: Handle,
    assigned_partitions: Arc<Mutex<BTreeSet<TopicPartition>>>,
}

impl ProcessingStrategyFactory<KafkaPayload> for TaskbrokerStrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        let commit = CommitOffsets::new(Duration::from_millis(
            self.config.kafka_auto_commit_interval_ms as u64,
        ));
        let writer = ActivationWriter::new(
            self.activation_store.clone(),
            ActivationWriterConfig::from_topic(&self.config, &self.topic),
        );
        let accumulator_config = ActivationBatcherConfig::from_topic(&self.config, &self.topic);
        let forwarder_config = ActivationBatcherConfig::from_topic(&self.config, &self.topic);
        let row_weight = accumulator_config
            .max_batch_size
            .saturating_add(accumulator_config.max_batch_len.saturating_sub(1))
            .checked_div(accumulator_config.max_batch_len.max(1))
            .unwrap_or(1)
            .max(1);
        let max_batch_size = accumulator_config.max_batch_size;
        let max_batch_time_ms = accumulator_config.max_batch_time_ms;
        let store = ActivationStoreStrategy::new(
            commit,
            writer,
            ActivationForwarder::new(forwarder_config, self.runtime_config_manager.clone()),
            self.runtime.clone(),
        );
        let batch = Reduce::new(
            store,
            activation_batch_accumulator(
                accumulator_config,
                self.runtime_config_manager.clone(),
                self.runtime.clone(),
            ),
            Arc::new(ActivationBatch::default),
            max_batch_size,
            Duration::from_millis(max_batch_time_ms),
            batch_input_weight,
        )
        .flush_empty_batches(true);
        let deserializer_config = DeserializeConfig::from_topic(&self.config, &self.topic);
        let deserializer = Arc::new(deserialize::new(deserializer_config));
        let deserializer = Arc::new(move |message: &OwnedMessage| deserializer(message).map(Some));
        let deserialize_row_weight = row_weight;
        let map_deserializer = deserializer.clone();
        let map_topic = self.topic.clone();
        let concurrency = ConcurrencyConfig::with_runtime(
            self.assigned_partitions.lock().unwrap().len().max(1),
            self.runtime.clone(),
        );
        let map = RunTaskInThreads::new(
            batch,
            move |message: Message<KafkaPayload>| {
                deserialize_message(
                    message,
                    map_deserializer.clone(),
                    deserialize_row_weight,
                    map_topic.clone(),
                )
            },
            &concurrency,
            Some("taskbroker_deserialize"),
        );

        Box::new(PartitionOwnershipStrategy::new(
            map,
            self.activation_store.clone(),
            self.assigned_partitions.clone(),
            self.topics_tag.clone(),
        ))
    }

    fn update_partitions(&self, partitions: &HashMap<Partition, u64>) {
        let new_partitions: BTreeSet<TopicPartition> = partitions
            .keys()
            .map(|partition| {
                TopicPartition::new(partition.topic.as_str(), i32::from(partition.index))
            })
            .collect();

        metrics::gauge!("arroyo.consumer.current_partitions", "topic" => self.topics_tag.clone())
            .set(new_partitions.len() as f64);
        self.activation_store
            .assign_partitions(&mut new_partitions.iter().cloned());

        *self.assigned_partitions.lock().unwrap() = new_partitions;
    }
}

fn deserialize_message(
    message: Message<KafkaPayload>,
    deserializer: Arc<ActivationDeserializer>,
    row_weight: usize,
    configured_topic: String,
) -> RunTaskFunc<BatchInput, String> {
    Box::pin(async move {
        let owned_message = match message_to_owned(&message) {
            Ok(owned_message) => owned_message,
            Err(err) => {
                error!("Failed to convert Arroyo Kafka message, skipping: {err:?}");
                return Ok(message.replace(BatchInput {
                    activation: None,
                    weight: row_weight,
                }));
            }
        };
        match deserializer(&owned_message) {
            Ok(activation) => {
                let weight = activation
                    .as_ref()
                    .map(|activation| activation.activation.len().max(row_weight))
                    .unwrap_or(row_weight);
                Ok(message.replace(BatchInput {
                    activation: activation.map(Box::new),
                    weight,
                }))
            }
            Err(err) => {
                // Explicit poison-message policy for this prototype: malformed
                // payloads are logged and skipped by committing their offsets.
                // Task execution failures still flow through normal task status
                // handling after a valid activation has been stored.
                error!(
                    topic = ?owned_message.topic(),
                    partition = ?owned_message.partition(),
                    offset = ?owned_message.offset(),
                    configured_topic = ?configured_topic,
                    "Failed to deserialize message, skipping: {err:?}",
                );
                Ok(message.replace(BatchInput {
                    activation: None,
                    weight: row_weight,
                }))
            }
        }
    })
}

fn batch_input_weight(input: &BatchInput) -> usize {
    input.weight
}

#[allow(clippy::result_large_err)]
fn activation_batch_accumulator(
    config: ActivationBatcherConfig,
    runtime_config_manager: Arc<RuntimeConfigManager>,
    runtime: Handle,
) -> Arc<BatchAccumulator> {
    Arc::new(move |mut batch, message| {
        let BatchInput { activation, .. } = message.into_payload();
        let Some(activation) = activation else {
            return Ok(batch);
        };

        let runtime_config = runtime.block_on(runtime_config_manager.read());
        let forward_topic = runtime_config
            .demoted_topic
            .clone()
            .unwrap_or(config.kafka_long_topic.clone());
        let task_name = &activation.taskname;
        let namespace = &activation.namespace;

        if !runtime_config.drop_task_killswitch.is_empty() {
            let should_drop_start = Instant::now();
            let match_kind = killswitch::should_drop(
                &runtime_config.drop_task_killswitch,
                task_name,
                &activation.activation,
            );
            metrics::histogram!(
                "filter.drop_task_killswitch.duration",
                "topic" => config.kafka_topic.clone(),
            )
            .record(should_drop_start.elapsed());
            if let Some(match_kind) = match_kind {
                metrics::counter!(
                    "filter.drop_task_killswitch",
                    "topic" => config.kafka_topic.clone(),
                    "taskname" => task_name.clone(),
                    "match" => match_kind,
                )
                .increment(1);
                return Ok(batch);
            }
        }

        if let Some(expires_at) = activation.expires_at
            && chrono::Utc::now() > expires_at
        {
            metrics::counter!(
                "filter.expired_at_consumer",
                "topic" => config.kafka_topic.clone(),
            )
            .increment(1);
            return Ok(batch);
        }

        if runtime_config.demoted_namespaces.contains(namespace) {
            if forward_topic == config.kafka_topic {
                metrics::counter!(
                    "filter.forward_task_demoted_namespace.skipped",
                    "topic" => config.kafka_topic.clone(),
                    "namespace" => namespace.clone(),
                    "taskname" => task_name.clone(),
                )
                .increment(1);
            } else {
                metrics::counter!(
                    "filter.forward_task_demoted_namespace",
                    "topic" => config.kafka_topic.clone(),
                    "namespace" => namespace.clone(),
                    "taskname" => task_name.clone(),
                )
                .increment(1);
                batch.forward_payloads.push(activation.activation);
                return Ok(batch);
            }
        }

        batch.activation_bytes += activation.activation.len();
        batch.activations.push(*activation);
        Ok(batch)
    })
}

fn message_to_owned(message: &Message<KafkaPayload>) -> Result<OwnedMessage, Error> {
    let payload = message.payload();
    match &message.inner_message {
        InnerMessage::BrokerMessage(broker) => Ok(OwnedMessage::new(
            payload.payload().cloned(),
            payload.key().cloned(),
            broker.partition.topic.as_str().to_owned(),
            Timestamp::CreateTime(broker.timestamp.timestamp_millis()),
            i32::from(broker.partition.index),
            broker.offset as i64,
            payload.headers().cloned().map(Into::into),
        )),
        InnerMessage::AnyMessage(_) => Err(anyhow!("expected broker message")),
    }
}

struct PartitionOwnershipStrategy<N> {
    inner: N,
    activation_store: Arc<dyn ActivationStore>,
    assigned_partitions: Arc<Mutex<BTreeSet<TopicPartition>>>,
    topics_tag: String,
    revoked: bool,
}

impl<N> PartitionOwnershipStrategy<N> {
    fn new(
        inner: N,
        activation_store: Arc<dyn ActivationStore>,
        assigned_partitions: Arc<Mutex<BTreeSet<TopicPartition>>>,
        topics_tag: String,
    ) -> Self {
        Self {
            inner,
            activation_store,
            assigned_partitions,
            topics_tag,
            revoked: false,
        }
    }

    fn revoke_once(&mut self) {
        if self.revoked {
            return;
        }
        revoke_remaining_partitions(&self.activation_store, &self.assigned_partitions);
        metrics::gauge!("arroyo.consumer.current_partitions", "topic" => self.topics_tag.clone())
            .set(0.0);
        self.revoked = true;
    }
}

impl<T, N> ProcessingStrategy<T> for PartitionOwnershipStrategy<N>
where
    T: Send + Sync + 'static,
    N: ProcessingStrategy<T>,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
        self.inner.submit(message)
    }

    fn terminate(&mut self) {
        self.inner.terminate();
        self.revoke_once();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let result = self.inner.join(timeout);
        self.revoke_once();
        result
    }
}

fn revoke_remaining_partitions(
    activation_store: &Arc<dyn ActivationStore>,
    assigned_partitions: &Arc<Mutex<BTreeSet<TopicPartition>>>,
) {
    let mut partitions = assigned_partitions.lock().unwrap();
    if partitions.is_empty() {
        return;
    }
    let revoked: Vec<_> = partitions.iter().cloned().collect();
    activation_store.revoke_partitions(&mut revoked.into_iter());
    partitions.clear();
}

struct ActivationForwarder {
    topic: String,
    producer_config: ClientConfig,
    kafka_long_topic: String,
    send_timeout_ms: u64,
    runtime_config_manager: Arc<RuntimeConfigManager>,
    producer: Arc<FutureProducer>,
    producer_cluster: String,
}

impl ActivationForwarder {
    fn new(
        config: ActivationBatcherConfig,
        runtime_config_manager: Arc<RuntimeConfigManager>,
    ) -> Self {
        let producer: Arc<FutureProducer> = Arc::new(
            config
                .producer_config
                .create()
                .expect("Could not create kafka producer in activation forwarder"),
        );
        let producer_cluster = config
            .producer_config
            .get("bootstrap.servers")
            .expect("producer config always sets bootstrap.servers")
            .to_owned();

        Self {
            topic: config.kafka_topic,
            producer_config: config.producer_config,
            kafka_long_topic: config.kafka_long_topic,
            send_timeout_ms: config.send_timeout_ms,
            runtime_config_manager,
            producer,
            producer_cluster,
        }
    }

    async fn forward(&mut self, payloads: Vec<Vec<u8>>) -> Result<(), Error> {
        if payloads.is_empty() {
            return Ok(());
        }

        let runtime_config = self.runtime_config_manager.read().await;
        let forward_cluster = runtime_config
            .demoted_topic_cluster
            .clone()
            .unwrap_or_else(|| {
                self.producer_config
                    .get("bootstrap.servers")
                    .expect("producer config always sets bootstrap.servers")
                    .to_string()
            });
        if self.producer_cluster != forward_cluster {
            let mut new_config = self.producer_config.clone();
            new_config.set("bootstrap.servers", &forward_cluster);
            self.producer = Arc::new(
                new_config
                    .create()
                    .expect("Could not create kafka producer in activation forwarder"),
            );
            self.producer_cluster = forward_cluster;
        }
        let forward_topic = runtime_config
            .demoted_topic
            .clone()
            .unwrap_or(self.kafka_long_topic.clone());
        drop(runtime_config);

        let sends = payloads.iter().map(|payload| {
            self.producer.send(
                FutureRecord::<(), Vec<u8>>::to(&forward_topic).payload(payload),
                Timeout::After(Duration::from_millis(self.send_timeout_ms)),
            )
        });

        let results = join_all(sends).await;
        let success_count = results.iter().filter(|result| result.is_ok()).count();

        metrics::histogram!("consumer.forward_attempts", "topic" => self.topic.clone())
            .record(results.len() as f64);
        metrics::histogram!("consumer.forward_successes", "topic" => self.topic.clone())
            .record(success_count as f64);
        metrics::histogram!("consumer.forward_failures", "topic" => self.topic.clone())
            .record((results.len() - success_count) as f64);

        Ok(())
    }
}

struct ActivationStoreStrategy<N> {
    next_step: N,
    writer: Option<ActivationWriter>,
    forwarder: Option<ActivationForwarder>,
    runtime: Handle,
    write_task: Option<JoinHandle<StoreWriteResult>>,
    carried_message: Option<Message<()>>,
    commit_request_carried_over: Option<CommitRequest>,
}

struct StoreWriteResult {
    writer: ActivationWriter,
    forwarder: ActivationForwarder,
    message: Message<ActivationBatch>,
    result: Result<(), Error>,
}

impl<N> ActivationStoreStrategy<N> {
    fn new(
        next_step: N,
        writer: ActivationWriter,
        forwarder: ActivationForwarder,
        runtime: Handle,
    ) -> Self {
        Self {
            next_step,
            writer: Some(writer),
            forwarder: Some(forwarder),
            runtime,
            write_task: None,
            carried_message: None,
            commit_request_carried_over: None,
        }
    }

    fn retry_carried_message(&mut self) -> Result<(), StrategyError>
    where
        N: ProcessingStrategy<()>,
    {
        let Some(message) = self.carried_message.take() else {
            return Ok(());
        };
        match self.next_step.submit(message) {
            Ok(()) => Ok(()),
            Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                self.carried_message = Some(message);
                Ok(())
            }
            Err(SubmitError::InvalidMessage(invalid)) => Err(invalid.into()),
        }
    }

    fn finish_write(&mut self) -> Result<(), StrategyError>
    where
        N: ProcessingStrategy<()>,
    {
        let Some(task) = self.write_task.as_ref() else {
            return Ok(());
        };
        if !task.is_finished() {
            return Ok(());
        }

        let task = self.write_task.take().unwrap();
        let StoreWriteResult {
            writer,
            forwarder,
            message,
            result,
        } = self.runtime.block_on(task).map_err(|err| {
            StrategyError::Other(anyhow!("activation store write task failed: {err}").into())
        })?;
        self.writer = Some(writer);
        self.forwarder = Some(forwarder);
        result.map_err(|err| StrategyError::Other(err.into()))?;

        match self.next_step.submit(message.replace(())) {
            Ok(()) => Ok(()),
            Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                self.carried_message = Some(message);
                Ok(())
            }
            Err(SubmitError::InvalidMessage(invalid)) => Err(invalid.into()),
        }
    }
}

impl<N> ProcessingStrategy<ActivationBatch> for ActivationStoreStrategy<N>
where
    N: ProcessingStrategy<()>,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let commit_request = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), commit_request);

        self.retry_carried_message()?;
        self.finish_write()?;

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(
        &mut self,
        message: Message<ActivationBatch>,
    ) -> Result<(), SubmitError<ActivationBatch>> {
        if self.writer.is_none()
            || self.forwarder.is_none()
            || self.write_task.is_some()
            || self.carried_message.is_some()
        {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        let writer = self.writer.take().unwrap();
        let mut forwarder = self.forwarder.take().unwrap();
        self.write_task = Some(self.runtime.spawn(async move {
            let batch = message.payload().clone();
            let result = async {
                metrics::histogram!("consumer.batch_rows", "topic" => forwarder.topic.clone())
                    .record(batch.activations.len() as f64);
                metrics::histogram!("consumer.batch_bytes", "topic" => forwarder.topic.clone())
                    .record(batch.activation_bytes as f64);

                forwarder.forward(batch.forward_payloads).await?;
                loop {
                    if writer.write_batch(&batch.activations).await? {
                        return Ok(());
                    }
                    sleep(STORE_BACKPRESSURE_REPOLL).await;
                }
            }
            .await;

            StoreWriteResult {
                writer,
                forwarder,
                message,
                result,
            }
        }));

        Ok(())
    }

    fn terminate(&mut self) {
        if let Some(task) = &self.write_task {
            task.abort();
        }
        self.write_task = None;
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        while self.write_task.is_some() || self.carried_message.is_some() {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                break;
            }
            let commit_request = self.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit_request);
            std::thread::sleep(Duration::from_millis(10));
        }

        let next_commit = self.next_step.join(timeout)?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}
