use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::future::Future;
use std::mem::take;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::{cmp, iter};

use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;

use sentry_arroyo::backends::kafka::KafkaConsumer;
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::{AssignmentCallbacks, CommitOffsets, Consumer as ArroyoConsumer};
use sentry_arroyo::types::{BrokerMessage, Partition, Topic};

use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{MissedTickBehavior, sleep};
use tokio::{select, task, time};

use anyhow::{Error, anyhow};
use futures::{Stream, StreamExt, future, pin_mut};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::either::Either;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::store::traits::ActivationStore;
use crate::store::types::TopicPartition;

use super::message::ConsumerMessage;

pub async fn start_no_consume_mode(
    topics: &[&str],
    kafka_client_config: &ClientConfig,
) -> Result<(), Error> {
    let consumer: StreamConsumer = kafka_client_config
        .create()
        .expect("Consumer creation failed");

    // Validates: broker reachability, SASL/SSL auth handshake,
    // topic existence, and partition count — no group join.
    // Retry on transient errors like broker transport failure.
    let topic = topics[0];
    let mut retries = 0;
    let mut metadata = None;
    while retries < 3 {
        match consumer.fetch_metadata(Some(topic), Duration::from_secs(10)) {
            Ok(md) => {
                metadata = Some(md);
                break;
            }
            Err(KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure)) => {
                error!("Failed to connect to broker, retrying...");
                retries += 1;
                continue;
            }
            Err(e) => {
                return Err(anyhow!("failed to fetch metadata: {e:?}"));
            }
        };
    }

    if metadata.is_none() {
        return Err(anyhow!("failed to fetch metadata after 3 retries"));
    }

    let md = metadata.unwrap();
    let topic_md = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow!("topic {topic} not found"))?;

    if let Some(err) = topic_md.error() {
        return Err(anyhow!("topic {topic} error: {err:?}"));
    }
    info!("Topic {topic} configuration validated");
    let guard = elegant_departure::get_shutdown_guard();
    guard.wait().await;
    Ok(())
}

pub async fn start_consumer(
    topics: &[&str],
    kafka_config: KafkaConfig,
    activation_store: Arc<dyn ActivationStore>,
    spawn_actors: impl FnMut(Arc<ConsumerPipeline>, &AssignedPartitions) -> ActorHandles,
) -> Result<(), Error> {
    let (event_sender, event_receiver) = unbounded_channel();
    let topics_tag = topics.join(",");
    let pipeline = Arc::new(ConsumerPipeline::new());
    let callbacks = ArroyoCallbacks::new(event_sender.clone(), topics_tag.clone());
    let topics: Vec<Topic> = topics.iter().map(|topic| Topic::new(topic)).collect();
    let consumer =
        KafkaConsumer::new(kafka_config, &topics, callbacks).expect("Consumer creation failed");

    handle_shutdown_signals(event_sender.clone());
    poll_consumer_client(consumer, pipeline.clone());
    metrics::gauge!("arroyo.consumer.current_partitions", "topic" => topics_tag.clone()).set(0);
    handle_events(
        pipeline,
        event_receiver,
        activation_store,
        spawn_actors,
        topics_tag,
    )
    .await
}

/// Sends rebalance and shutdown events from sync callbacks into the async event loop.
type EventSender = UnboundedSender<(Event, oneshot::Sender<OffsetMap>)>;

/// Receives rebalance and shutdown events in the async event loop.
type EventReceiver = UnboundedReceiver<(Event, oneshot::Sender<OffsetMap>)>;

/// Forwards the process shutdown signal into the consumer event loop.
pub fn handle_shutdown_signals(event_sender: EventSender) {
    let guard = elegant_departure::get_shutdown_guard();
    tokio::spawn(async move {
        let _ = guard.wait().await;
        info!("Cancellation token received, shutting down consumer...");
        let (rendezvous_sender, _) = oneshot::channel();
        let _ = event_sender.send((Event::Shutdown, rendezvous_sender));
    });
}

/// Runs the synchronous Arroyo consumer in a blocking task, routing polled messages into the async pipeline and applying committed offsets.
#[instrument(skip_all)]
pub fn poll_consumer_client(
    mut consumer: KafkaConsumer<ArroyoCallbacks>,
    pipeline: Arc<ConsumerPipeline>,
) {
    task::spawn_blocking(move || {
        let _guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();

        while !pipeline.is_stopped() {
            while let Some(offsets) = pipeline.try_recv_commit_offsets() {
                if let Err(err) = consumer.commit_offsets(offsets) {
                    error!(?err, "Failed to commit offsets");
                    return;
                }
            }

            match consumer.poll(Some(Duration::from_millis(100))) {
                Ok(None) => {}

                Ok(Some(message)) => pipeline.route_message(message),

                Err(err) => {
                    error!(?err, "Got unexpected status from consumer client");
                    return;
                }
            }
        }

        let offsets = pipeline.latest_offsets();

        if !offsets.is_empty()
            && let Err(err) = consumer.commit_offsets(offsets)
        {
            error!(?err, "Failed to commit offsets during shutdown");
        }

        debug!("Shutdown complete");
    });
}

#[derive(Debug, Clone)]
pub struct ArroyoCallbacks {
    event_sender: EventSender,
    topics_tag: String,
}

impl ArroyoCallbacks {
    pub fn new(event_sender: EventSender, topics_tag: String) -> Self {
        Self {
            event_sender,
            topics_tag,
        }
    }
}

impl AssignmentCallbacks for ArroyoCallbacks {
    #[instrument(skip_all)]
    fn on_assign(&self, partitions: HashMap<Partition, u64>) {
        let (rendezvous_sender, rendezvous_receiver) = oneshot::channel();
        debug!("Got assignment callback");
        if partitions.is_empty() {
            warn!(
                "Got partition assignment with no partitions, \
                this is likely due to there being more consumers than partitions"
            );
            return;
        }

        let partitions: BTreeSet<_> = partitions.keys().copied().collect();
        let partition_count = partitions.len();
        let _ = self
            .event_sender
            .send((Event::Assign(partitions), rendezvous_sender));
        info!("Partition assignment event sent, waiting for rendezvous...");
        let _ = rendezvous_receiver.blocking_recv();
        info!("Rendezvous complete");
        metrics::counter!(
            "arroyo.consumer.partitions_assigned.count",
            "topic" => self.topics_tag.clone(),
        )
        .increment(partition_count as u64);
    }

    #[instrument(skip_all)]
    fn on_revoke<C: CommitOffsets>(&self, commit_offsets: C, partitions: Vec<Partition>) {
        let (rendezvous_sender, rendezvous_receiver) = oneshot::channel();
        debug!("Got revocation callback");
        if partitions.is_empty() {
            warn!(
                "Got partition revocation with no partitions, \
                this is likely due to there being more consumers than partitions"
            );
            return;
        }

        let partition_count = partitions.len();
        let revoked = partitions.into_iter().collect();
        let _ = self
            .event_sender
            .send((Event::Revoke(revoked), rendezvous_sender));
        info!("Partition revocation event sent, waiting for rendezvous...");
        let offsets = rendezvous_receiver.blocking_recv().unwrap_or_default();
        if !offsets.is_empty()
            && let Err(err) = commit_offsets.commit(offsets)
        {
            error!("Failed to commit offsets during revocation: {:?}", err);
        }
        info!("Rendezvous complete");
        metrics::counter!(
            "arroyo.consumer.partitions_revoked.count",
            "topic" => self.topics_tag.clone(),
        )
        .increment(partition_count as u64);
    }
}

/// Set of topic partitions currently assigned to or revoked from this consumer.
pub type AssignedPartitions = BTreeSet<Partition>;

/// Maps each assigned Arroyo partition to the next offset the consumer should read after commit.
type OffsetMap = HashMap<Partition, u64>;

/// Runtime state for the processing pipeline attached to one Arroyo Kafka consumer.
/// This type owns the partition queues, high water offsets, and shared shutdown signal.
#[derive(Debug)]
pub struct ConsumerPipeline {
    /// Routes messages to different map actors based on partition.
    queues: RwLock<HashMap<Partition, Sender<ConsumerMessage>>>,

    /// Communicates the next offset to read for every partition back to the blocking poll loop.
    commit_sender: UnboundedSender<OffsetMap>,

    /// Receives the next offset to read for every partition so the poll loop can commit them through Arroyo.
    commit_receiver: Mutex<UnboundedReceiver<OffsetMap>>,

    /// Tracks the latest observed high water offset per partition for revoke and shutdown commits.
    latest_offsets: RwLock<OffsetMap>,

    /// Signals the blocking poll loop to stop after shutdown has reached the pipeline.
    stopped: AtomicBool,
}

impl ConsumerPipeline {
    fn new() -> Self {
        let (commit_sender, commit_receiver) = unbounded_channel();

        Self {
            queues: RwLock::new(HashMap::new()),
            commit_sender,
            commit_receiver: Mutex::new(commit_receiver),
            latest_offsets: RwLock::new(HashMap::new()),
            stopped: AtomicBool::new(false),
        }
    }

    /// Registers an assigned partition with the pipeline and returns the queue its map actor reads from.
    pub fn register_partition_queue(&self, partition: Partition) -> Result<PartitionQueue, Error> {
        let (sender, receiver) = mpsc::channel(1);

        self.queues.write().unwrap().insert(partition, sender);

        Ok(PartitionQueue { receiver })
    }

    /// Routes one polled Kafka message into the async queue for its assigned partition.
    fn route_message(&self, message: BrokerMessage<KafkaPayload>) {
        let message = ConsumerMessage::new(message);
        let key = message.partition_key();

        let sender = self.queues.read().unwrap().get(&key).cloned();
        let Some(sender) = sender else {
            warn!(
                topic = key.topic.as_str(),
                partition = key.index,
                "Dropping message for unassigned partition"
            );

            return;
        };

        if let Err(err) = sender.blocking_send(message) {
            warn!(?err, "Unable to route message to partition queue");
        }
    }

    /// Records newly processed offsets and asks the blocking poll loop to commit them.
    fn track_offsets(&self, high_watermark: HighWatermark) {
        let offsets = high_watermark.into_arroyo_offsets();
        if offsets.is_empty() {
            return;
        }

        self.latest_offsets.write().unwrap().extend(offsets.clone());
        if let Err(err) = self.commit_sender.send(offsets) {
            error!(?err, "Unable to send commit request to consumer loop");
        }
    }

    /// Returns the latest processed offsets known for all partitions.
    fn latest_offsets(&self) -> OffsetMap {
        self.latest_offsets.read().unwrap().clone()
    }

    /// Returns the latest processed offsets known for the selected assigned partitions.
    fn latest_offsets_for(&self, partitions: &AssignedPartitions) -> OffsetMap {
        let latest_offsets = self.latest_offsets.read().unwrap();

        partitions
            .iter()
            .filter_map(|partition| {
                latest_offsets
                    .get(partition)
                    .copied()
                    .map(|offset| (*partition, offset))
            })
            .collect()
    }

    /// Removes partition queues after their actors have stopped consuming from them.
    fn remove_queues(&self, partitions: &AssignedPartitions) {
        let mut queues = self.queues.write().unwrap();
        for partition in partitions {
            queues.remove(partition);
        }
    }

    /// Pulls the next pending offset commit request for the blocking poll loop, if any.
    fn try_recv_commit_offsets(&self) -> Option<OffsetMap> {
        match self.commit_receiver.lock().unwrap().try_recv() {
            Ok(offsets) => Some(offsets),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => None,
        }
    }

    /// Requests that the blocking poll loop exit.
    fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    /// Returns whether shutdown has asked the blocking poll loop to exit.
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct PartitionQueue {
    receiver: Receiver<ConsumerMessage>,
}

#[derive(Debug)]
pub enum Event {
    Assign(AssignedPartitions),
    Revoke(AssignedPartitions),
    Shutdown,
}

#[derive(Debug)]
pub struct ActorHandles {
    pub join_set: JoinSet<Result<(), Error>>,
    pub shutdown: CancellationToken,
    pub rendezvous: oneshot::Receiver<()>,
}

impl ActorHandles {
    #[instrument(skip(self))]
    async fn shutdown(mut self, deadline: Duration) {
        debug!("Signaling shutdown to actors...");
        self.shutdown.cancel();
        info!("Actor shutdown signaled, waiting for rendezvous...");

        select! {
            _ = self.rendezvous => {
                info!("Rendezvous complete within callback deadline");
            }
            _ = sleep(deadline) => {
                error!(
                    "Unable to rendezvous within callback deadline, \
                    aborting all tasks within JoinSet"
                );
                self.join_set.abort_all();
            }
        }
    }

    async fn join_next(&mut self) -> Option<Result<Result<(), Error>, JoinError>> {
        self.join_set.join_next().await
    }
}

#[macro_export]
macro_rules! processing_strategy {
    (
        @reducers,
        (),
        $prev_receiver:ident,
        $err_sender:ident,
        $shutdown_signal:ident,
        $handles:ident,
    ) => {{
        $prev_receiver
    }};
    (
        @reducers,
        ($reduce_first:expr $(,$reduce_rest:expr)*),
        $prev_receiver:ident,
        $err_sender:ident,
        $shutdown_signal:ident,
        $handles:ident,
    ) => {{
        let (sender, receiver) = tokio::sync::mpsc::channel(1);

        $handles.spawn($crate::kafka::consumer::reduce(
            $reduce_first,
            $prev_receiver,
            sender.clone(),
            $err_sender.clone(),
            $shutdown_signal.clone(),
        ));

        processing_strategy!(
            @reducers,
            ($($reduce_rest),*),
            receiver,
            $err_sender,
            $shutdown_signal,
            $handles,
        )
    }};
    (
        {
            err: $reduce_err:expr,
            map: $map_fn:expr,
            reduce: $reduce_first:expr $(,$reduce_rest:expr)*,
        }
    ) => {{
        |consumer: Arc<$crate::kafka::consumer::ConsumerPipeline>,
         tpl: &$crate::kafka::consumer::AssignedPartitions|
         -> $crate::kafka::consumer::ActorHandles {
            let start = std::time::Instant::now();

            let mut handles = tokio::task::JoinSet::new();
            let shutdown_signal = tokio_util::sync::CancellationToken::new();

            let (rendezvous_sender, rendezvous_receiver) = tokio::sync::oneshot::channel();

            let (map_sender, reduce_receiver) = tokio::sync::mpsc::channel(1);
            let (err_sender, err_receiver) = tokio::sync::mpsc::channel(1);

            for partition in tpl.iter() {
                let queue = consumer
                    .register_partition_queue(*partition)
                    .expect("Unable to register partition queue");

                handles.spawn($crate::kafka::consumer::map(
                    queue,
                    $map_fn,
                    map_sender.clone(),
                    err_sender.clone(),
                    shutdown_signal.clone(),
                ));
            }

            let commit_receiver = $crate::processing_strategy!(
                @reducers,
                ($reduce_first $(,$reduce_rest)*),
                reduce_receiver,
                err_sender,
                shutdown_signal,
                handles,
            );

            handles.spawn($crate::kafka::consumer::commit(
                commit_receiver,
                consumer.clone(),
                rendezvous_sender,
            ));

            handles.spawn($crate::kafka::consumer::reduce_err(
                $reduce_err,
                err_receiver,
                shutdown_signal.clone(),
            ));

            tracing::debug!("Creating actors took {:?}", start.elapsed());

            $crate::kafka::consumer::ActorHandles {
                join_set: handles,
                shutdown: shutdown_signal,
                rendezvous: rendezvous_receiver,
            }
        }
    }};
}

#[derive(Debug)]
enum ConsumerState {
    Ready,
    Consuming(ActorHandles, AssignedPartitions),
    Stopped,
}

#[instrument(skip_all)]
pub async fn handle_events(
    pipeline: Arc<ConsumerPipeline>,
    mut event_receiver: EventReceiver,
    activation_store: Arc<dyn ActivationStore>,
    mut spawn_actors: impl FnMut(Arc<ConsumerPipeline>, &AssignedPartitions) -> ActorHandles,
    topics_tag: String,
) -> Result<(), anyhow::Error> {
    const CALLBACK_DURATION: Duration = Duration::from_secs(4);

    let _guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    let mut state = ConsumerState::Ready;

    while let ConsumerState::Ready | ConsumerState::Consuming { .. } = state {
        select! {
            res = match state {
                ConsumerState::Consuming(ref mut handles, _) => Either::Left(handles.join_next()),
                _ => Either::Right(future::pending::<_>()),
            } => {
                error!("Actor exited unexpectedly with {:?}, shutting down...", res);
                drop(elegant_departure::shutdown());
            }

            event = event_receiver.recv() => {
                let Some((event, rendezvous_guard)) = event else {
                    unreachable!("Unexpected end to event stream");
                };
                info!("Received event: {:?}", event);
                state = match (state, event) {
                    (ConsumerState::Ready, Event::Assign(tpl)) => {
                        metrics::gauge!("arroyo.consumer.current_partitions", "topic" => topics_tag.clone())
                            .set(tpl.len() as f64);
                        activation_store.assign_partitions(&mut tpl.iter().map(TopicPartition::from));
                        let handles = spawn_actors(pipeline.clone(), &tpl);
                        let _ = rendezvous_guard.send(OffsetMap::default());

                        ConsumerState::Consuming(handles, tpl)
                    }
                    (ConsumerState::Ready, Event::Revoke(_)) => {
                        unreachable!("Got partition revocation before the consumer has started")
                    }
                    (ConsumerState::Ready, Event::Shutdown) => {
                        pipeline.stop();
                        let _ = rendezvous_guard.send(OffsetMap::default());
                        ConsumerState::Stopped
                    }
                    (ConsumerState::Consuming(_, _), Event::Assign(_)) => {
                        unreachable!("Got partition assignment after the consumer has started")
                    }
                    (ConsumerState::Consuming(handles, tpl), Event::Revoke(revoked)) => {
                        assert!(
                            tpl == revoked,
                            "Revoked TPL should be equal to the subset of TPL we're consuming from"
                        );

                        activation_store.revoke_partitions(&mut revoked.iter().map(TopicPartition::from));
                        handles.shutdown(CALLBACK_DURATION).await;
                        pipeline.remove_queues(&revoked);
                        let _ = rendezvous_guard.send(pipeline.latest_offsets_for(&revoked));
                        metrics::gauge!("arroyo.consumer.current_partitions", "topic" => topics_tag.clone()).set(0);
                        ConsumerState::Ready
                    }
                    (ConsumerState::Consuming(handles, tpl), Event::Shutdown) => {
                        activation_store.revoke_partitions(&mut tpl.iter().map(TopicPartition::from));
                        handles.shutdown(CALLBACK_DURATION).await;
                        pipeline.remove_queues(&tpl);
                        pipeline.stop();
                        let _ = rendezvous_guard.send(OffsetMap::default());
                        metrics::gauge!("arroyo.consumer.current_partitions", "topic" => topics_tag.clone()).set(0);
                        ConsumerState::Stopped
                    }
                    (ConsumerState::Stopped, _) => {
                        unreachable!("Got event after consumer has stopped")
                    }
                }
            }
        }
    }
    debug!("Shutdown complete");
    Ok(())
}

pub trait MessageQueue {
    fn stream(self) -> impl Stream<Item = ConsumerMessage>;
}

impl MessageQueue for PartitionQueue {
    fn stream(self) -> impl Stream<Item = ConsumerMessage> {
        ReceiverStream::new(self.receiver)
    }
}

#[instrument(skip_all)]
pub async fn map<T>(
    queue: impl MessageQueue,
    transform: impl Fn(&ConsumerMessage) -> Result<T, Error>,
    ok: Sender<(iter::Once<ConsumerMessage>, T)>,
    err: Sender<ConsumerMessage>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let stream = queue.stream();
    pin_mut!(stream);

    loop {
        select! {
            biased;

            _ = shutdown.cancelled() => {
                debug!("Receive shutdown signal, shutting down...");
                break;
            }

            val = stream.next() => {
                let Some(msg) = val else {
                    break;
                };

                match transform(&msg) {
                    Ok(transformed) => {
                        if ok.send((iter::once(msg), transformed)).await.is_err() {
                            debug!("Receive half of ok channel is closed, shutting down...");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(
                            topic = ?msg.topic(),
                            partition = ?msg.partition(),
                            offset = ?msg.offset(),
                            "Failed to map message: {:?}",
                            e,
                        );
                        err.send(msg)
                            .await
                            .expect("reduce_err is not available");
                    }
                }
            }
        }
    }
    debug!("Shutdown complete");
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ReduceConfig {
    pub shutdown_condition: ReduceShutdownCondition,
    pub shutdown_behaviour: ReduceShutdownBehaviour,
    pub when_full_behaviour: ReducerWhenFullBehaviour,
    pub flush_interval: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReduceShutdownCondition {
    Signal,
    Drain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReduceShutdownBehaviour {
    Flush,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReducerWhenFullBehaviour {
    Flush,
    Backpressure,
}

pub trait Reducer {
    type Input;
    type Output;

    fn reduce(&mut self, t: Self::Input) -> impl Future<Output = Result<(), anyhow::Error>> + Send;
    fn flush(&mut self)
    -> impl Future<Output = Result<Option<Self::Output>, anyhow::Error>> + Send;
    fn reset(&mut self);
    fn is_full(&self) -> impl Future<Output = bool> + Send;
    fn get_reduce_config(&self) -> ReduceConfig;
}

async fn handle_reducer_failure<T>(
    reducer: &mut impl Reducer<Input = T>,
    inflight_msgs: &mut Vec<ConsumerMessage>,
    err: &Sender<ConsumerMessage>,
) {
    for msg in take(inflight_msgs).into_iter() {
        err.send(msg).await.expect("reduce_err is not available");
    }
    reducer.reset();
}

#[instrument(skip_all)]
async fn flush_reducer<T, U>(
    reducer: &mut impl Reducer<Input = T, Output = U>,
    inflight_msgs: &mut Vec<ConsumerMessage>,
    ok: &Sender<(Vec<ConsumerMessage>, U)>,
    err: &Sender<ConsumerMessage>,
) -> Result<(), Error> {
    match reducer.flush().await {
        Err(e) => {
            error!("Failed to flush reducer, reason: {}", e);
            handle_reducer_failure(reducer, inflight_msgs, err).await;
        }
        Ok(None) => {}
        Ok(Some(result)) => {
            ok.send((take(inflight_msgs), result))
                .await
                .map_err(|err| anyhow!("{}", err))?;
        }
    }
    Ok(())
}

#[instrument(skip_all)]
pub async fn reduce<T, U>(
    mut reducer: impl Reducer<Input = T, Output = U>,
    mut receiver: Receiver<(impl IntoIterator<Item = ConsumerMessage>, T)>,
    ok: Sender<(Vec<ConsumerMessage>, U)>,
    err: Sender<ConsumerMessage>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let config = reducer.get_reduce_config();
    let mut flush_timer = config.flush_interval.map(time::interval);
    let mut repoll_timer = time::interval(Duration::from_millis(250));
    repoll_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut inflight_msgs = Vec::new();

    loop {
        select! {
            biased;

            _ = if config.shutdown_condition == ReduceShutdownCondition::Signal {
                Either::Left(shutdown.cancelled())
            } else {
                Either::Right(future::pending::<_>())
            } => {
                match config.shutdown_behaviour {
                    ReduceShutdownBehaviour::Flush => {
                        debug!("Received shutdown signal, flushing reducer...");
                        flush_reducer(&mut reducer, &mut inflight_msgs, &ok, &err).await?;
                    }
                    ReduceShutdownBehaviour::Drop => {
                        debug!("Received shutdown signal, dropping reducer...");
                        drop(reducer);
                    }
                };
                break;
            }

            _ = if let Some(ref mut flush_timer) = flush_timer {
                Either::Left(flush_timer.tick())
            } else {
                Either::Right(future::pending::<_>())
            } => {
                flush_reducer(&mut reducer, &mut inflight_msgs, &ok, &err).await?;
            }

            val = receiver.recv(), if !reducer.is_full().await => {
                let Some((msg, value)) = val else {
                    assert_eq!(
                        config.shutdown_condition,
                        ReduceShutdownCondition::Drain,
                        "Got end of stream without shutdown signal"
                    );
                    match config.shutdown_behaviour {
                        ReduceShutdownBehaviour::Flush => {
                            debug!("Received end of stream, flushing reducer...");
                            flush_reducer(&mut reducer, &mut inflight_msgs, &ok, &err).await?;
                        }
                        ReduceShutdownBehaviour::Drop => {
                            debug!("Received end of stream, dropping reducer...");
                            drop(reducer);
                        }
                    };
                    break;
                };

                inflight_msgs.extend(msg);

                if let Err(e) = reducer.reduce(value).await {
                    error!(
                        "Failed to reduce message at \
                        (topic: {}, partition: {}, offset: {}), reason: {}",
                        inflight_msgs.last().unwrap().topic(),
                        inflight_msgs.last().unwrap().partition(),
                        inflight_msgs.last().unwrap().offset(),
                        e,
                    );
                    handle_reducer_failure(&mut reducer, &mut inflight_msgs, &err).await;
                }
            }

            _ = repoll_timer.tick() => {}
        }

        if config.when_full_behaviour == ReducerWhenFullBehaviour::Flush && reducer.is_full().await
        {
            flush_reducer(&mut reducer, &mut inflight_msgs, &ok, &err).await?;
        }
    }

    debug!("Shutdown complete");
    Ok(())
}

#[instrument(skip_all)]
pub async fn reduce_err(
    mut reducer: impl Reducer<Input = ConsumerMessage, Output = ()>,
    mut receiver: Receiver<ConsumerMessage>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let config = reducer.get_reduce_config();
    let mut flush_timer = config.flush_interval.map(time::interval);
    let mut repoll_timer = time::interval(Duration::from_secs(1));
    repoll_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        select! {
            biased;

            _ = if config.shutdown_condition == ReduceShutdownCondition::Signal {
                Either::Left(shutdown.cancelled())
            } else {
                Either::Right(future::pending::<_>())
            } => {
                match config.shutdown_behaviour {
                    ReduceShutdownBehaviour::Flush => {
                        debug!("Received shutdown signal, flushing reducer...");
                        reducer
                            .flush()
                            .await
                            .expect("Failed to flush error reducer");
                    },
                    ReduceShutdownBehaviour::Drop => {
                        debug!("Received shutdown signal, dropping reducer...");
                        drop(reducer);
                    },
                }
                break;
            }

            _ = if let Some(ref mut flush_timer) = flush_timer {
                Either::Left(flush_timer.tick())
            } else {
                Either::Right(future::pending::<_>())
            } => {
                reducer
                    .flush()
                    .await
                    .expect("Failed to flush error reducer");
            }

            val = receiver.recv(), if !reducer.is_full().await => {
                let Some(msg) = val else {
                    assert_eq!(
                        config.shutdown_condition,
                        ReduceShutdownCondition::Drain,
                        "Got end of stream without shutdown signal"
                    );
                    match config.shutdown_behaviour {
                        ReduceShutdownBehaviour::Flush => {
                            debug!("Received end of stream, flushing reducer...");
                            reducer
                                .flush()
                                .await
                                .expect("Failed to flush error reducer");
                        }
                        ReduceShutdownBehaviour::Drop => {
                            debug!("Received end of stream, dropping reducer...");
                            drop(reducer);
                        }
                    };
                    break;
                };

                reducer
                    .reduce(msg)
                    .await
                    .expect("Failed to reduce error reducer");
            }

            _ = repoll_timer.tick() => {}
        }

        if config.when_full_behaviour == ReducerWhenFullBehaviour::Flush && reducer.is_full().await
        {
            reducer
                .flush()
                .await
                .expect("Failed to flush error reducer");
        }
    }

    debug!("Shutdown complete");
    Ok(())
}

#[derive(Default)]
struct HighWatermark {
    data: HashMap<(String, i32), i64>,
}

impl HighWatermark {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    fn track(&mut self, msg: &ConsumerMessage) {
        let cur_offset = self
            .data
            .entry((msg.topic().to_string(), msg.partition()))
            .or_insert(msg.offset() + 1);
        *cur_offset = cmp::max(*cur_offset, msg.offset() + 1);
    }

    fn into_arroyo_offsets(self) -> OffsetMap {
        self.data
            .into_iter()
            .map(|((topic, partition), offset)| {
                (
                    Partition::new(Topic::new(&topic), partition as u16),
                    offset as u64,
                )
            })
            .collect()
    }
}

#[instrument(skip_all)]
pub async fn commit(
    mut receiver: Receiver<(Vec<ConsumerMessage>, ())>,
    consumer: Arc<ConsumerPipeline>,
    _rendezvous_guard: oneshot::Sender<()>,
) -> Result<(), Error> {
    while let Some(msgs) = receiver.recv().await {
        let mut highwater_mark = HighWatermark::new();
        msgs.0.iter().for_each(|msg| highwater_mark.track(msg));
        consumer.track_offsets(highwater_mark);
    }
    debug!("Shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::iter;
    use std::marker::PhantomData;
    use std::mem::take;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use anyhow::anyhow;
    use chrono::Utc;
    use futures::{Stream, StreamExt};
    use sentry_arroyo::backends::kafka::types::{
        Headers as ArroyoHeaders, KafkaPayload as ArroyoKafkaPayload,
    };
    use sentry_arroyo::types::BrokerMessage;
    use sentry_arroyo::types::{Partition, Topic};
    use tokio::sync::{broadcast, mpsc, oneshot};
    use tokio::time::sleep;
    use tokio_stream::wrappers::BroadcastStream;
    use tokio_util::sync::CancellationToken;

    use crate::kafka::consumer::{
        ConsumerPipeline, MessageQueue, ReduceConfig, ReduceShutdownBehaviour,
        ReduceShutdownCondition, Reducer, ReducerWhenFullBehaviour, commit, map, reduce,
        reduce_err,
    };
    use crate::kafka::message::ConsumerMessage;
    use crate::kafka::os_stream_writer::{OsStream, OsStreamWriter};

    fn test_message(
        payload: Option<Vec<u8>>,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> ConsumerMessage {
        ConsumerMessage::new(BrokerMessage::new(
            ArroyoKafkaPayload::new(None, None, payload),
            Partition::new(Topic::new(topic), partition as u16),
            offset as u64,
            Utc::now(),
        ))
    }

    struct StreamingReducer<T> {
        data: Option<T>,
        pipe: Arc<RwLock<Vec<T>>>,
        error_on_idx: Option<usize>,
    }

    impl<T> StreamingReducer<T> {
        fn new(error_on_idx: Option<usize>) -> Self {
            Self {
                data: None,
                pipe: Arc::new(RwLock::new(Vec::new())),
                error_on_idx,
            }
        }

        fn get_pipe(&self) -> Arc<RwLock<Vec<T>>> {
            self.pipe.clone()
        }
    }

    impl<T> Reducer for StreamingReducer<T>
    where
        T: Send + Sync + Clone,
    {
        type Input = T;

        type Output = ();

        async fn reduce(&mut self, t: Self::Input) -> Result<(), anyhow::Error> {
            if let Some(idx) = self.error_on_idx
                && idx == self.pipe.read().unwrap().len()
            {
                self.error_on_idx.take();
                return Err(anyhow!("err"));
            }
            assert!(self.data.is_none());
            self.data = Some(t);
            Ok(())
        }

        async fn flush(&mut self) -> Result<Option<()>, anyhow::Error> {
            if self.data.is_none() {
                return Ok(None);
            }
            self.pipe.write().unwrap().push(self.data.take().unwrap());
            Ok(Some(()))
        }

        fn reset(&mut self) {
            self.data.take();
        }

        async fn is_full(&self) -> bool {
            self.data.is_some()
        }

        fn get_reduce_config(&self) -> ReduceConfig {
            ReduceConfig {
                shutdown_condition: ReduceShutdownCondition::Signal,
                shutdown_behaviour: ReduceShutdownBehaviour::Drop,
                when_full_behaviour: ReducerWhenFullBehaviour::Flush,
                flush_interval: None,
            }
        }
    }

    struct BatchingReducer<T> {
        buffer: Arc<RwLock<Vec<T>>>,
        pipe: Arc<RwLock<Vec<T>>>,
        error_on_nth_reduce: Option<usize>,
        error_on_nth_flush: Option<usize>,
        shutdown_condition: ReduceShutdownCondition,
    }

    impl<T> BatchingReducer<T> {
        fn new(
            error_on_reduce: Option<usize>,
            error_on_flush: Option<usize>,
            shutdown_condition: ReduceShutdownCondition,
        ) -> Self {
            Self {
                buffer: Arc::new(RwLock::new(Vec::new())),
                pipe: Arc::new(RwLock::new(Vec::new())),
                error_on_nth_reduce: error_on_reduce,
                error_on_nth_flush: error_on_flush,
                shutdown_condition,
            }
        }

        fn get_buffer(&self) -> Arc<RwLock<Vec<T>>> {
            self.buffer.clone()
        }

        fn get_pipe(&self) -> Arc<RwLock<Vec<T>>> {
            self.pipe.clone()
        }
    }

    impl<T> Reducer for BatchingReducer<T>
    where
        T: Send + Sync + Clone,
    {
        type Input = T;
        type Output = ();

        async fn reduce(&mut self, t: Self::Input) -> Result<(), anyhow::Error> {
            if let Some(idx) = self.error_on_nth_reduce {
                if idx == 0 {
                    self.error_on_nth_reduce.take();
                    return Err(anyhow!("err"));
                } else {
                    self.error_on_nth_reduce = Some(idx - 1);
                }
            }
            self.buffer.write().unwrap().push(t);
            Ok(())
        }

        async fn flush(&mut self) -> Result<Option<()>, anyhow::Error> {
            if let Some(idx) = self.error_on_nth_flush {
                if idx == 0 {
                    self.error_on_nth_flush.take();
                    return Err(anyhow!("err"));
                } else {
                    self.error_on_nth_flush = Some(idx - 1);
                }
            }
            if self.buffer.read().unwrap().is_empty() {
                return Ok(None);
            }
            self.pipe
                .write()
                .unwrap()
                .extend(take(&mut self.buffer.write().unwrap() as &mut Vec<T>));
            Ok(Some(()))
        }

        fn reset(&mut self) {
            self.buffer.write().unwrap().clear();
        }

        async fn is_full(&self) -> bool {
            self.buffer.read().unwrap().len() >= 32
        }

        fn get_reduce_config(&self) -> ReduceConfig {
            ReduceConfig {
                shutdown_condition: self.shutdown_condition,
                shutdown_behaviour: ReduceShutdownBehaviour::Flush,
                when_full_behaviour: ReducerWhenFullBehaviour::Backpressure,
                flush_interval: Some(Duration::from_secs(1)),
            }
        }
    }

    #[tokio::test]
    async fn test_commit() {
        let consumer = Arc::new(ConsumerPipeline::new());
        let (sender, receiver) = mpsc::channel(1);
        let (rendezvou_sender, rendezvou_receiver) = oneshot::channel();

        let msg = vec![
            test_message(None, "topic", 0, 1),
            test_message(None, "topic", 1, 0),
        ];

        assert!(sender.send((msg.clone(), ())).await.is_ok());

        tokio::spawn(commit(receiver, consumer.clone(), rendezvou_sender));

        drop(sender);
        let _ = rendezvou_receiver.await;

        assert_eq!(
            consumer.latest_offsets(),
            HashMap::from([
                (Partition::new(Topic::new("topic"), 0), 2),
                (Partition::new(Topic::new("topic"), 1), 1),
            ])
        );
    }

    #[test]
    fn test_arroyo_message_exposes_consumer_message_fields() {
        let headers = ArroyoHeaders::new().insert("compression-type", Some(b"zstd".to_vec()));
        let payload = ArroyoKafkaPayload::new(None, Some(headers), Some(vec![1, 2, 3]));
        let message = BrokerMessage::new(
            payload,
            Partition::new(Topic::new("topic"), 1),
            42,
            Utc::now(),
        );
        let message = ConsumerMessage::new(message);

        assert_eq!(message.payload(), Some(&[1, 2, 3][..]));
        assert_eq!(message.topic(), "topic");
        assert_eq!(message.partition(), 1);
        assert_eq!(message.offset(), 42);
        assert_eq!(message.headers()[0].value, Some(&b"zstd"[..]));
    }

    #[tokio::test]
    async fn test_consumer_pipeline_routes_to_partition_queue() {
        let consumer = Arc::new(ConsumerPipeline::new());
        let queue = consumer
            .register_partition_queue(Partition::new(Topic::new("topic"), 1))
            .unwrap();
        let payload = ArroyoKafkaPayload::new(None, None, Some(vec![9]));
        let message = BrokerMessage::new(
            payload,
            Partition::new(Topic::new("topic"), 1),
            10,
            Utc::now(),
        );

        let route_consumer = consumer.clone();
        tokio::task::spawn_blocking(move || route_consumer.route_message(message))
            .await
            .unwrap();

        let routed = queue.stream().next().await.unwrap();
        assert_eq!(routed.payload(), Some(&[9][..]));
        assert_eq!(routed.offset(), 10);
    }

    #[tokio::test]
    async fn test_reduce_err_without_flush_interval() {
        let reducer = StreamingReducer::new(None);
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let msg = test_message(Some(vec![0, 1, 2, 3, 4, 5, 6, 7]), "topic", 0, 0);

        tokio::spawn(reduce_err(reducer, receiver, shutdown.clone()));

        assert!(sender.send(msg.clone()).await.is_ok());
        sleep(Duration::from_secs(1)).await;
        assert_eq!(
            pipe.read().unwrap().last().unwrap().payload().unwrap(),
            &[0, 1, 2, 3, 4, 5, 6, 7]
        );

        drop(sender);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_reduce_without_flush_interval() {
        let reducer = StreamingReducer::new(None);
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(2);
        let (ok_sender, mut ok_receiver) = mpsc::channel(2);
        let (err_sender, err_receiver) = mpsc::channel(2);
        let shutdown = CancellationToken::new();

        let msg_0 = test_message(Some(vec![0, 2, 4, 6]), "topic", 0, 0);
        let msg_1 = test_message(Some(vec![1, 3, 5, 7]), "topic", 0, 1);

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((iter::once(msg_0.clone()), 1)).await.is_ok());
        assert!(sender.send((iter::once(msg_1.clone()), 2)).await.is_ok());

        assert_eq!(
            ok_receiver.recv().await.unwrap().0[0].payload(),
            msg_0.payload()
        );
        assert_eq!(
            ok_receiver.recv().await.unwrap().0[0].payload(),
            msg_1.payload()
        );
        assert_eq!(pipe.read().unwrap().as_slice(), &[1, 2]);
        assert!(err_receiver.is_empty());

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_fail_on_reduce_without_flush_interval() {
        let reducer = StreamingReducer::new(Some(1));
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(2);
        let (ok_sender, mut ok_receiver) = mpsc::channel(2);
        let (err_sender, mut err_receiver) = mpsc::channel(2);
        let shutdown = CancellationToken::new();

        let msg_0 = test_message(Some(vec![0, 2, 4, 6]), "topic", 0, 0);
        let msg_1 = test_message(Some(vec![1, 3, 5, 7]), "topic", 0, 1);
        let msg_2 = test_message(Some(vec![0, 0, 0, 0]), "topic", 0, 2);

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((iter::once(msg_0.clone()), 1)).await.is_ok());
        assert_eq!(
            ok_receiver.recv().await.unwrap().0[0].payload(),
            msg_0.payload(),
        );
        assert_eq!(pipe.read().unwrap().as_slice(), &[1]);

        assert!(sender.send((iter::once(msg_1.clone()), 2)).await.is_ok());
        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_1.payload()
        );
        assert_eq!(pipe.read().unwrap().as_slice(), &[1]);

        assert!(sender.send((iter::once(msg_2.clone()), 3)).await.is_ok());
        assert_eq!(
            ok_receiver.recv().await.unwrap().0[0].payload(),
            msg_2.payload(),
        );
        assert_eq!(pipe.read().unwrap().as_slice(), &[1, 3]);

        assert!(ok_receiver.is_empty());
        assert!(err_receiver.is_empty());

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_reduce_err_with_flush_interval() {
        let reducer = BatchingReducer::new(None, None, ReduceShutdownCondition::Signal);
        let buffer = reducer.get_buffer();
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let msg = test_message(Some(vec![0, 1, 2, 3, 4, 5, 6, 7]), "topic", 0, 0);

        tokio::spawn(reduce_err(reducer, receiver, shutdown.clone()));

        assert!(sender.send(msg.clone()).await.is_ok());
        sleep(Duration::from_secs(1)).await;
        assert_eq!(pipe.read().unwrap()[0].payload(), msg.payload());
        assert!(buffer.read().unwrap().is_empty());

        drop(sender);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_reduce_with_flush_interval() {
        let reducer = BatchingReducer::new(None, None, ReduceShutdownCondition::Signal);
        let buffer = reducer.get_buffer();
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(2);
        let (ok_sender, mut ok_receiver) = mpsc::channel(2);
        let (err_sender, err_receiver) = mpsc::channel(2);
        let shutdown = CancellationToken::new();

        let msg_0 = test_message(Some(vec![0, 2, 4, 6]), "topic", 0, 0);
        let msg_1 = test_message(Some(vec![1, 3, 5, 7]), "topic", 0, 1);

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((iter::once(msg_0.clone()), 1)).await.is_ok());
        assert!(sender.send((iter::once(msg_1.clone()), 2)).await.is_ok());

        let ok_msgs = ok_receiver.recv().await.unwrap().0;
        assert_eq!(ok_msgs.len(), 2);
        assert_eq!(ok_msgs[0].payload(), msg_0.payload());
        assert_eq!(ok_msgs[1].payload(), msg_1.payload());
        assert!(buffer.read().unwrap().is_empty());
        assert_eq!(pipe.read().unwrap().as_slice(), &[1, 2]);
        assert!(err_receiver.is_empty());

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_fail_on_reduce_with_flush_interval() {
        let reducer = BatchingReducer::new(Some(1), None, ReduceShutdownCondition::Signal);
        let buffer = reducer.get_buffer();
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(3);
        let (ok_sender, mut ok_receiver) = mpsc::channel(3);
        let (err_sender, mut err_receiver) = mpsc::channel(3);
        let shutdown = CancellationToken::new();

        let msg_0 = test_message(Some(vec![0, 3, 6]), "topic", 0, 0);
        let msg_1 = test_message(Some(vec![1, 4, 7]), "topic", 0, 1);
        let msg_2 = test_message(Some(vec![2, 5, 8]), "topic", 0, 2);

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((iter::once(msg_0.clone()), 0)).await.is_ok());
        let ok_msgs = ok_receiver.recv().await.unwrap().0;
        assert_eq!(ok_msgs.len(), 1);
        assert_eq!(ok_msgs[0].payload(), msg_0.payload());
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0]);

        assert!(sender.send((iter::once(msg_1.clone()), 1)).await.is_ok());
        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_1.payload()
        );
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0] as &[i32]);

        assert!(sender.send((iter::once(msg_2.clone()), 2)).await.is_ok());
        let ok_msgs = ok_receiver.recv().await.unwrap().0;
        assert_eq!(ok_msgs.len(), 1);
        assert_eq!(ok_msgs[0].payload(), msg_2.payload());
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0, 2] as &[i32]);

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_empty());
        assert!(err_receiver.is_empty());
    }

    #[tokio::test]
    async fn test_fail_on_flush() {
        let reducer = BatchingReducer::new(None, Some(1), ReduceShutdownCondition::Signal);
        let buffer = reducer.get_buffer();
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(1);
        let (ok_sender, mut ok_receiver) = mpsc::channel(1);
        let (err_sender, mut err_receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let msg_0 = test_message(Some(vec![0, 3, 6]), "topic", 0, 0);
        let msg_1 = test_message(Some(vec![1, 4, 7]), "topic", 0, 1);
        let msg_2 = test_message(Some(vec![2, 5, 8]), "topic", 0, 2);
        let msg_3 = test_message(Some(vec![0, 0, 0]), "topic", 0, 3);

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        assert!(sender.send((iter::once(msg_0.clone()), 0)).await.is_ok());
        let ok_msgs = ok_receiver.recv().await.unwrap().0;
        assert_eq!(ok_msgs.len(), 1);
        assert_eq!(ok_msgs[0].payload(), msg_0.payload());

        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0]);

        assert!(sender.send((iter::once(msg_1.clone()), 1)).await.is_ok());
        assert!(sender.send((iter::once(msg_2.clone()), 2)).await.is_ok());
        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_1.payload()
        );
        assert_eq!(
            err_receiver.recv().await.unwrap().payload(),
            msg_2.payload()
        );
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0]);

        assert!(sender.send((iter::once(msg_3.clone()), 3)).await.is_ok());
        let ok_msgs = ok_receiver.recv().await.unwrap().0;
        assert_eq!(ok_msgs.len(), 1);
        assert_eq!(ok_msgs[0].payload(), msg_3.payload());
        assert_eq!(buffer.read().unwrap().as_slice(), &[] as &[i32]);
        assert_eq!(pipe.read().unwrap().as_slice(), &[0, 3]);

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_empty());
        assert!(err_receiver.is_empty());
    }

    #[tokio::test]
    async fn test_sequential_reducers() {
        let reducer_0 = BatchingReducer::new(None, None, ReduceShutdownCondition::Signal);
        let buffer_0 = reducer_0.get_buffer();
        let pipe_0 = reducer_0.get_pipe();

        let reducer_1 = BatchingReducer::new(None, None, ReduceShutdownCondition::Signal);
        let buffer_1 = reducer_1.get_buffer();
        let pipe_1 = reducer_1.get_pipe();

        let shutdown = CancellationToken::new();

        let (sender, receiver) = mpsc::channel(1);
        let (ok_sender_0, ok_receiver_0) = mpsc::channel(2);
        let (err_sender_0, err_receiver_0) = mpsc::channel(1);

        let (ok_sender_1, mut ok_receiver_1) = mpsc::channel(1);
        let (err_sender_1, err_receiver_1) = mpsc::channel(1);

        let msg_0 = test_message(Some(vec![0, 2, 4, 6]), "topic", 0, 0);
        let msg_1 = test_message(Some(vec![1, 3, 5, 7]), "topic", 0, 1);

        tokio::spawn(reduce(
            reducer_0,
            receiver,
            ok_sender_0,
            err_sender_0,
            shutdown.clone(),
        ));

        tokio::spawn(reduce(
            reducer_1,
            ok_receiver_0,
            ok_sender_1,
            err_sender_1,
            shutdown.clone(),
        ));

        assert!(sender.send((iter::once(msg_0.clone()), 1)).await.is_ok());
        assert!(sender.send((iter::once(msg_1.clone()), 2)).await.is_ok());

        let mut ok_msgs = Vec::new();
        while ok_msgs.len() < 2 {
            let batch = tokio::time::timeout(Duration::from_secs(2), ok_receiver_1.recv())
                .await
                .unwrap()
                .unwrap()
                .0;
            ok_msgs.extend(batch);
        }
        assert_eq!(ok_msgs.len(), 2);
        assert_eq!(ok_msgs[0].payload(), msg_0.payload());
        assert_eq!(ok_msgs[1].payload(), msg_1.payload());

        assert!(buffer_0.read().unwrap().is_empty());
        assert_eq!(pipe_0.read().unwrap().as_slice(), &[1, 2]);

        assert!(buffer_1.read().unwrap().is_empty());
        assert!(!pipe_1.read().unwrap().is_empty());

        assert!(err_receiver_0.is_empty());
        assert!(err_receiver_1.is_empty());

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(err_receiver_0.is_closed());
        assert!(ok_receiver_1.is_closed());
        assert!(err_receiver_1.is_closed());
    }

    #[tokio::test]
    async fn test_reduce_shutdown_from_drain() {
        let reducer = BatchingReducer::new(None, None, ReduceShutdownCondition::Drain);
        let buffer = reducer.get_buffer();
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(2);
        let (ok_sender, mut ok_receiver) = mpsc::channel(2);
        let (err_sender, err_receiver) = mpsc::channel(2);
        let shutdown = CancellationToken::new();

        let msg_0 = test_message(Some(vec![0, 2, 4, 6]), "topic", 0, 0);
        let msg_1 = test_message(Some(vec![1, 3, 5, 7]), "topic", 0, 1);

        tokio::spawn(reduce(
            reducer,
            receiver,
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));

        shutdown.cancel();

        assert!(sender.send((iter::once(msg_0.clone()), 1)).await.is_ok());
        assert!(sender.send((iter::once(msg_1.clone()), 2)).await.is_ok());

        let ok_msgs = ok_receiver.recv().await.unwrap().0;
        assert_eq!(ok_msgs.len(), 2);
        assert_eq!(ok_msgs[0].payload(), msg_0.payload());
        assert_eq!(ok_msgs[1].payload(), msg_1.payload());
        assert!(buffer.read().unwrap().is_empty());
        assert_eq!(pipe.read().unwrap().as_slice(), &[1, 2]);
        assert!(err_receiver.is_empty());

        drop(sender);
        shutdown.cancel();

        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    impl MessageQueue for broadcast::Receiver<ConsumerMessage> {
        fn stream(self) -> impl Stream<Item = ConsumerMessage> {
            BroadcastStream::new(self).map(|msg| msg.expect("Cannot receive broadcast message"))
        }
    }

    #[tokio::test]
    async fn test_map() {
        let (sender, receiver) = broadcast::channel(1);
        let (ok_sender, mut ok_receiver) = mpsc::channel(1);
        let (err_sender, err_receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        tokio::spawn(map(
            receiver,
            |msg| Ok(msg.payload().unwrap()[0] * 2),
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));
        sleep(Duration::from_secs(1)).await;

        let msg_0 = test_message(Some(vec![0]), "topic", 0, 0);
        let msg_1 = test_message(Some(vec![1]), "topic", 0, 1);
        assert!(sender.send(msg_0.clone()).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(res.0.collect::<Vec<_>>()[0].payload(), msg_0.payload());
        assert_eq!(res.1, msg_0.payload().unwrap()[0] * 2);

        assert!(sender.send(msg_1.clone()).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(res.0.collect::<Vec<_>>()[0].payload(), msg_1.payload());
        assert_eq!(res.1, msg_1.payload().unwrap()[0] * 2);

        shutdown.cancel();
        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    #[tokio::test]
    async fn test_fail_on_map() {
        let (sender, receiver) = broadcast::channel(1);
        let (ok_sender, mut ok_receiver) = mpsc::channel(1);
        let (err_sender, mut err_receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        tokio::spawn(map(
            receiver,
            |msg| {
                if msg.payload().unwrap()[0] == 1 {
                    Err(anyhow!("Oh no"))
                } else {
                    Ok(msg.payload().unwrap()[0] * 2)
                }
            },
            ok_sender,
            err_sender,
            shutdown.clone(),
        ));
        sleep(Duration::from_secs(1)).await;

        let msg_0 = test_message(Some(vec![0]), "topic", 0, 0);
        let msg_1 = test_message(Some(vec![1]), "topic", 0, 1);
        let msg_2 = test_message(Some(vec![2]), "topic", 0, 2);

        assert!(sender.send(msg_0.clone()).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(res.0.collect::<Vec<_>>()[0].payload(), msg_0.payload());
        assert_eq!(res.1, 0);

        assert!(sender.send(msg_1.clone()).is_ok());
        assert!(ok_receiver.is_empty());
        let res = err_receiver.recv().await.unwrap();
        assert_eq!(res.payload(), msg_1.payload());

        assert!(sender.send(msg_2.clone()).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(res.0.collect::<Vec<_>>()[0].payload(), msg_2.payload());
        assert_eq!(res.1, 4);

        shutdown.cancel();
        sleep(Duration::from_secs(1)).await;
        assert!(ok_receiver.is_closed());
        assert!(err_receiver.is_closed());
    }

    pub struct NoopReducer<T> {
        phantom: PhantomData<T>,
    }

    impl<T> NoopReducer<T> {
        pub fn new() -> Self {
            Self {
                phantom: PhantomData,
            }
        }
    }

    impl<T> Reducer for NoopReducer<T>
    where
        T: Send + Sync,
    {
        type Input = T;
        type Output = ();

        async fn reduce(&mut self, _t: Self::Input) -> Result<(), anyhow::Error> {
            Ok(())
        }

        async fn flush(&mut self) -> Result<Option<()>, anyhow::Error> {
            Ok(Some(()))
        }

        fn reset(&mut self) {}

        async fn is_full(&self) -> bool {
            false
        }

        fn get_reduce_config(&self) -> ReduceConfig {
            ReduceConfig {
                shutdown_condition: ReduceShutdownCondition::Drain,
                shutdown_behaviour: ReduceShutdownBehaviour::Flush,
                when_full_behaviour: ReducerWhenFullBehaviour::Flush,
                flush_interval: Some(Duration::from_secs(1)),
            }
        }
    }

    #[tokio::test]
    async fn test_processing_strategy_can_compile() {
        let _ = processing_strategy!({
            err:
                OsStreamWriter::new(
                    Duration::from_secs(1),
                    OsStream::StdErr,
                ),

            map:
                |_: &ConsumerMessage| Ok(()),
            reduce:
                NoopReducer::new(),
                NoopReducer::new(),
                NoopReducer::new(),
                NoopReducer::new(),
        });

        let _ = processing_strategy!({

            err:
                OsStreamWriter::new(
                    Duration::from_secs(1),
                    OsStream::StdErr,
                ),

            map:
                |_: &ConsumerMessage| Ok(()),
            reduce:
                NoopReducer::new(),
        });
    }
}
