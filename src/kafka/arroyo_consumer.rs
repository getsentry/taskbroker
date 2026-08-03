use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Error, anyhow};
use rdkafka::Message as _;
use rdkafka::Timestamp;
use rdkafka::message::OwnedMessage;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskFunc, RunTaskInThreads,
};
use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, InvalidMessageReason, MessageRejected, ProcessingStrategy,
    ProcessingStrategyFactory, StrategyError, SubmitError, merge_commit_request,
};
use sentry_arroyo::types::{InnerMessage, Message, Partition, Topic};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, info};

use crate::config::Config;
use crate::kafka::activation_batcher::{ActivationBatcher, ActivationBatcherConfig};
use crate::kafka::activation_writer::{ActivationWriter, ActivationWriterConfig};
use crate::kafka::consumer::Reducer;
use crate::kafka::deserialize::{self, DeserializeConfig};
use crate::runtime_config::RuntimeConfigManager;
use crate::store::activation::Activation;
use crate::store::traits::ActivationStore;
use crate::store::types::TopicPartition;

const DEFAULT_MAX_POLL_INTERVAL_MS: usize = 300_000;
const STORE_BACKPRESSURE_REPOLL: Duration = Duration::from_millis(250);

type ActivationDeserializer =
    dyn Fn(&OwnedMessage) -> Result<Activation, Error> + Send + Sync + 'static;

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
        let store = ActivationStoreStrategy::new(commit, writer, self.runtime.clone());
        let batcher = ActivationBatcher::new(
            ActivationBatcherConfig::from_topic(&self.config, &self.topic),
            self.runtime_config_manager.clone(),
        );
        let batch = ActivationBatchStrategy::new(store, batcher, self.runtime.clone());
        let concurrency = ConcurrencyConfig::with_runtime(
            self.assigned_partitions.lock().unwrap().len().max(1),
            self.runtime.clone(),
        );
        let deserializer = Arc::new(deserialize::new(DeserializeConfig::from_topic(
            &self.config,
            &self.topic,
        )));
        let map = RunTaskInThreads::new(
            batch,
            move |message: Message<KafkaPayload>| {
                deserialize_message(message, deserializer.clone())
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
) -> RunTaskFunc<Option<Activation>, String> {
    Box::pin(async move {
        let owned_message = match message_to_owned(&message) {
            Ok(owned_message) => owned_message,
            Err(err) => {
                error!("Failed to convert Arroyo Kafka message, skipping: {err:?}");
                return Ok(message.replace(None));
            }
        };
        match deserializer(&owned_message) {
            Ok(activation) => Ok(message.replace(Some(activation))),
            Err(err) => {
                // Explicit poison-message policy for this prototype: malformed
                // payloads are logged and skipped by committing their offsets.
                // Task execution failures still flow through normal task status
                // handling after a valid activation has been stored.
                error!(
                    topic = ?owned_message.topic(),
                    partition = ?owned_message.partition(),
                    offset = ?owned_message.offset(),
                    "Failed to deserialize message, skipping: {err:?}",
                );
                Ok(message.replace(None))
            }
        }
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

struct ActivationBatchStrategy<N> {
    next_step: N,
    batcher: Option<ActivationBatcher>,
    runtime: Handle,
    committable: BTreeMap<Partition, u64>,
    flush_task: Option<JoinHandle<BatchFlushResult>>,
    carried_message: Option<Message<Vec<Activation>>>,
    commit_request_carried_over: Option<CommitRequest>,
    flush_interval: Option<Duration>,
    last_flush: Instant,
}

struct BatchFlushResult {
    batcher: ActivationBatcher,
    committable: BTreeMap<Partition, u64>,
    result: Result<Option<Vec<Activation>>, Error>,
}

impl<N> ActivationBatchStrategy<N> {
    fn new(next_step: N, batcher: ActivationBatcher, runtime: Handle) -> Self {
        let flush_interval = batcher.get_reduce_config().flush_interval;
        Self {
            next_step,
            batcher: Some(batcher),
            runtime,
            committable: BTreeMap::new(),
            flush_task: None,
            carried_message: None,
            commit_request_carried_over: None,
            flush_interval,
            last_flush: Instant::now(),
        }
    }

    fn retry_carried_message(&mut self) -> Result<(), StrategyError>
    where
        N: ProcessingStrategy<Vec<Activation>>,
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

    fn finish_flush(&mut self) -> Result<(), StrategyError>
    where
        N: ProcessingStrategy<Vec<Activation>>,
    {
        let Some(task) = self.flush_task.as_ref() else {
            return Ok(());
        };
        if !task.is_finished() {
            return Ok(());
        }

        let task = self.flush_task.take().unwrap();
        let BatchFlushResult {
            batcher,
            committable,
            result,
        } = self.runtime.block_on(task).map_err(|err| {
            StrategyError::Other(anyhow!("activation batch flush task failed: {err}").into())
        })?;
        self.batcher = Some(batcher);
        self.last_flush = Instant::now();

        match result {
            Ok(Some(batch)) => {
                let message = Message::new_any_message(batch, committable);
                match self.next_step.submit(message) {
                    Ok(()) => Ok(()),
                    Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                        self.carried_message = Some(message);
                        Ok(())
                    }
                    Err(SubmitError::InvalidMessage(invalid)) => Err(invalid.into()),
                }
            }
            Ok(None) => {
                self.committable.extend(committable);
                Ok(())
            }
            Err(err) => Err(StrategyError::Other(err.into())),
        }
    }

    fn maybe_start_flush(&mut self, force: bool)
    where
        N: ProcessingStrategy<Vec<Activation>>,
    {
        if self.flush_task.is_some()
            || self.carried_message.is_some()
            || self.committable.is_empty()
        {
            return;
        }

        let should_flush = force
            || self
                .flush_interval
                .is_some_and(|interval| self.last_flush.elapsed() >= interval)
            || self
                .batcher
                .as_ref()
                .is_some_and(|batcher| self.runtime.block_on(batcher.is_full()));

        if !should_flush {
            return;
        }

        let mut batcher = self.batcher.take().unwrap();
        let committable = std::mem::take(&mut self.committable);
        self.flush_task = Some(self.runtime.spawn(async move {
            let result = batcher.flush().await;
            BatchFlushResult {
                batcher,
                committable,
                result,
            }
        }));
    }
}

impl<N> ProcessingStrategy<Option<Activation>> for ActivationBatchStrategy<N>
where
    N: ProcessingStrategy<Vec<Activation>>,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let commit_request = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), commit_request);

        self.retry_carried_message()?;
        self.finish_flush()?;
        self.maybe_start_flush(false);

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(
        &mut self,
        message: Message<Option<Activation>>,
    ) -> Result<(), SubmitError<Option<Activation>>> {
        if self.batcher.is_none() || self.flush_task.is_some() || self.carried_message.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        let committable: Vec<_> = message.committable().collect();
        let Some(activation) = message.into_payload() else {
            let skipped = Message::new_any_message(Vec::new(), committable.into_iter().collect());
            match self.next_step.submit(skipped) {
                Ok(()) => return Ok(()),
                Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                    self.carried_message = Some(message);
                    return Ok(());
                }
                Err(SubmitError::InvalidMessage(invalid)) => {
                    return Err(SubmitError::InvalidMessage(invalid));
                }
            }
        };
        let Some(batcher) = self.batcher.as_mut() else {
            unreachable!("checked above");
        };

        self.runtime
            .block_on(batcher.reduce(activation))
            .map_err(|err| {
                error!("Failed to batch activation: {err}");
                SubmitError::InvalidMessage(InvalidMessage {
                    partition: committable
                        .first()
                        .map(|(partition, _)| *partition)
                        .unwrap_or_else(|| Partition::new(Topic::new("unknown"), 0)),
                    offset: committable
                        .first()
                        .map(|(_, offset)| offset.saturating_sub(1))
                        .unwrap_or(0),
                    reason: InvalidMessageReason::Invalid,
                })
            })?;

        self.committable.extend(committable);
        self.maybe_start_flush(false);
        Ok(())
    }

    fn terminate(&mut self) {
        if let Some(task) = &self.flush_task {
            task.abort();
        }
        self.flush_task = None;
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        self.maybe_start_flush(true);

        while self.flush_task.is_some() || self.carried_message.is_some() {
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

struct ActivationStoreStrategy<N> {
    next_step: N,
    writer: Option<ActivationWriter>,
    runtime: Handle,
    write_task: Option<JoinHandle<StoreWriteResult>>,
    carried_message: Option<Message<()>>,
    commit_request_carried_over: Option<CommitRequest>,
}

struct StoreWriteResult {
    writer: ActivationWriter,
    message: Message<Vec<Activation>>,
    result: Result<(), Error>,
}

impl<N> ActivationStoreStrategy<N> {
    fn new(next_step: N, writer: ActivationWriter, runtime: Handle) -> Self {
        Self {
            next_step,
            writer: Some(writer),
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
            message,
            result,
        } = self.runtime.block_on(task).map_err(|err| {
            StrategyError::Other(anyhow!("activation store write task failed: {err}").into())
        })?;
        self.writer = Some(writer);
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

impl<N> ProcessingStrategy<Vec<Activation>> for ActivationStoreStrategy<N>
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
        message: Message<Vec<Activation>>,
    ) -> Result<(), SubmitError<Vec<Activation>>> {
        if self.writer.is_none() || self.write_task.is_some() || self.carried_message.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        let mut writer = self.writer.take().unwrap();
        self.write_task = Some(self.runtime.spawn(async move {
            let batch = message.payload().clone();
            let result = async {
                writer.reduce(batch).await?;
                loop {
                    match writer.flush().await? {
                        Some(()) => return Ok(()),
                        None => sleep(STORE_BACKPRESSURE_REPOLL).await,
                    }
                }
            }
            .await;

            StoreWriteResult {
                writer,
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
