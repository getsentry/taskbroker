use anyhow::{Error, anyhow};
use futures::{
    Stream, StreamExt,
    future::{self},
    pin_mut,
};
use rdkafka::{
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
    consumer::{
        BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer,
        stream_consumer::StreamPartitionQueue,
    },
    error::{KafkaError, KafkaResult},
    message::{BorrowedMessage, OwnedMessage},
    types::RDKafkaErrorCode,
};
use std::{
    cmp,
    collections::{BTreeSet, HashMap},
    fmt::Debug,
    future::Future,
    iter,
    mem::take,
    sync::{
        Arc,
        mpsc::{SyncSender, sync_channel},
    },
    time::Duration,
};
use tokio::{
    runtime::Handle,
    select,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
    task::{self, JoinError, JoinSet},
    time::{self, MissedTickBehavior, sleep},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::{either::Either, sync::CancellationToken};
use tracing::{debug, error, info, instrument, warn};

pub async fn start_consumer(
    topics: &[&str],
    kafka_client_config: &ClientConfig,
    spawn_actors: impl FnMut(
        Arc<StreamConsumer<KafkaContext>>,
        &BTreeSet<(String, i32)>,
    ) -> ActorHandles,
) -> Result<(), Error> {
    let (client_shutdown_sender, client_shutdown_receiver) = oneshot::channel();
    let (event_sender, event_receiver) = unbounded_channel();
    let context = KafkaContext::new(event_sender.clone());
    let consumer: Arc<StreamConsumer<KafkaContext>> = Arc::new(
        kafka_client_config
            .create_with_context(context)
            .expect("Consumer creation failed"),
    );

    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");

    handle_shutdown_signals(event_sender.clone());
    poll_consumer_client(consumer.clone(), client_shutdown_receiver);
    handle_events(
        consumer,
        event_receiver,
        client_shutdown_sender,
        spawn_actors,
    )
    .await
}

pub fn handle_shutdown_signals(event_sender: UnboundedSender<(Event, SyncSender<()>)>) {
    let guard = elegant_departure::get_shutdown_guard();
    tokio::spawn(async move {
        let _ = guard.wait().await;
        info!("Cancellation token received, shutting down consumer...");
        let (rendezvous_sender, _) = sync_channel(0);
        let _ = event_sender.send((Event::Shutdown, rendezvous_sender));
    });
}

#[instrument(skip_all)]
pub fn poll_consumer_client(
    consumer: Arc<StreamConsumer<KafkaContext>>,
    mut shutdown: oneshot::Receiver<()>,
) {
    task::spawn_blocking(|| {
        Handle::current().block_on(async move {
            let _guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
            loop {
                select! {
                    biased;
                    _ = &mut shutdown => {
                        debug!("Received shutdown signal, commiting state in sync mode...");
                        let _ = consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Sync);
                        break;
                    }
                    msg = consumer.recv() => {
                        if let Err(KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure)) = msg {
                            error!("Failed to connect to broker, retrying...")
                        } else {
                            error!("Got unexpected status from consumer client: {:?}", msg);
                            break
                        }
                    }
                }
            }
            debug!("Shutdown complete");
        });
    });
}

#[derive(Debug)]
pub struct KafkaContext {
    event_sender: UnboundedSender<(Event, SyncSender<()>)>,
}

impl KafkaContext {
    pub fn new(event_sender: UnboundedSender<(Event, SyncSender<()>)>) -> Self {
        Self { event_sender }
    }
}

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
    #[instrument(skip_all)]
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        let (rendezvous_sender, rendezvous_receiver) = sync_channel(0);
        match rebalance {
            Rebalance::Assign(tpl) => {
                debug!("Got pre-rebalance callback, kind: Assign");
                if tpl.count() == 0 {
                    warn!(
                        "Got partition assignment with no partitions, \
                        this is likely due to there being more consumers than partitions"
                    );
                    return;
                }
                let _ = self.event_sender.send((
                    Event::Assign(tpl.to_topic_map().keys().cloned().collect()),
                    rendezvous_sender,
                ));
                info!("Partition assignment event sent, waiting for rendezvous...");
                let _ = rendezvous_receiver.recv();
                info!("Rendezvous complete");
            }
            Rebalance::Revoke(tpl) => {
                debug!("Got pre-rebalance callback, kind: Revoke");
                if tpl.count() == 0 {
                    warn!(
                        "Got partition revocation with no partitions, \
                        this is likely due to there being more consumers than partitions"
                    );
                    return;
                }
                let _ = self.event_sender.send((
                    Event::Revoke(tpl.to_topic_map().keys().cloned().collect()),
                    rendezvous_sender,
                ));
                info!("Partition assignment event sent, waiting for rendezvous...");
                let _ = rendezvous_receiver.recv();
                info!("Rendezvous complete");
            }
            Rebalance::Error(err) => {
                debug!("Got pre-rebalance callback, kind: Error");
                error!("Got rebalance error: {}", err);
            }
        }
    }

    #[instrument(skip(self))]
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        debug!("Got commit callback");
    }
}

#[derive(Debug)]
pub enum Event {
    Assign(BTreeSet<(String, i32)>),
    Revoke(BTreeSet<(String, i32)>),
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
        |consumer: Arc<rdkafka::consumer::StreamConsumer<$crate::kafka::consumer::KafkaContext>>,
         tpl: &std::collections::BTreeSet<(String, i32)>|
         -> $crate::kafka::consumer::ActorHandles {
            let start = std::time::Instant::now();

            let mut handles = tokio::task::JoinSet::new();
            let shutdown_signal = tokio_util::sync::CancellationToken::new();

            let (rendezvous_sender, rendezvous_receiver) = tokio::sync::oneshot::channel();

            let (map_sender, reduce_receiver) = tokio::sync::mpsc::channel(1);
            let (err_sender, err_receiver) = tokio::sync::mpsc::channel(1);

            for (topic, partition) in tpl.iter() {
                let queue = consumer
                    .split_partition_queue(topic, *partition)
                    .expect("Unable to split topic by Partition");

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
    Consuming(ActorHandles, BTreeSet<(String, i32)>),
    Stopped,
}

#[instrument(skip_all)]
pub async fn handle_events(
    consumer: Arc<StreamConsumer<KafkaContext>>,
    events: UnboundedReceiver<(Event, SyncSender<()>)>,
    shutdown_client: oneshot::Sender<()>,
    mut spawn_actors: impl FnMut(
        Arc<StreamConsumer<KafkaContext>>,
        &BTreeSet<(String, i32)>,
    ) -> ActorHandles,
) -> Result<(), anyhow::Error> {
    const CALLBACK_DURATION: Duration = Duration::from_secs(4);

    let _guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    let mut shutdown_client = Some(shutdown_client);
    let mut events_stream = UnboundedReceiverStream::new(events);

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

            event = events_stream.next() => {
                let Some((event, _rendezvous_guard)) = event else {
                    unreachable!("Unexpected end to event stream");
                };
                info!("Received event: {:?}", event);
                state = match (state, event) {
                    (ConsumerState::Ready, Event::Assign(tpl)) => {
                        ConsumerState::Consuming(spawn_actors(consumer.clone(), &tpl), tpl)
                    }
                    (ConsumerState::Ready, Event::Revoke(_)) => {
                        unreachable!("Got partition revocation before the consumer has started")
                    }
                    (ConsumerState::Ready, Event::Shutdown) => ConsumerState::Stopped,
                    (ConsumerState::Consuming(_, _), Event::Assign(_)) => {
                        unreachable!("Got partition assignment after the consumer has started")
                    }
                    (ConsumerState::Consuming(handles, tpl), Event::Revoke(revoked)) => {
                        assert!(
                            tpl == revoked,
                            "Revoked TPL should be equal to the subset of TPL we're consuming from"
                        );
                        handles.shutdown(CALLBACK_DURATION).await;
                        ConsumerState::Ready
                    }
                    (ConsumerState::Consuming(handles, _), Event::Shutdown) => {
                        handles.shutdown(CALLBACK_DURATION).await;
                        debug!("Signaling shutdown to client...");
                        shutdown_client.take();
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

pub trait KafkaMessage {
    fn detach(&self) -> Result<OwnedMessage, Error>;
}

impl KafkaMessage for Result<BorrowedMessage<'_>, KafkaError> {
    fn detach(&self) -> Result<OwnedMessage, Error> {
        match self {
            Ok(borrowed_msg) => Ok(borrowed_msg.detach()),
            Err(err) => Err(anyhow!(
                "Cannot detach message, got error from kafka: {:?}",
                err
            )),
        }
    }
}

pub trait MessageQueue {
    fn stream(&self) -> impl Stream<Item = impl KafkaMessage>;
}

impl MessageQueue for StreamPartitionQueue<KafkaContext> {
    fn stream(&self) -> impl Stream<Item = impl KafkaMessage> {
        self.stream()
    }
}

#[instrument(skip_all)]
pub async fn map<T>(
    queue: impl MessageQueue,
    transform: impl Fn(Arc<OwnedMessage>) -> Result<T, Error>,
    ok: mpsc::Sender<(iter::Once<OwnedMessage>, T)>,
    err: mpsc::Sender<OwnedMessage>,
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
                let msg = Arc::new(msg.detach()?);
                match transform(msg.clone()) {
                    Ok(transformed) => {
                        if ok.send((
                            iter::once(
                                Arc::try_unwrap(msg)
                                    .expect("msg should only have a single strong ref"),
                            ),
                            transformed,
                        )).await.is_err() {
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
                        err.send(
                            Arc::try_unwrap(msg).expect("msg should only have a single strong ref"),
                        )
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
    inflight_msgs: &mut Vec<OwnedMessage>,
    err: &mpsc::Sender<OwnedMessage>,
) {
    for msg in take(inflight_msgs).into_iter() {
        err.send(msg).await.expect("reduce_err is not available");
    }
    reducer.reset();
}

#[instrument(skip_all)]
async fn flush_reducer<T, U>(
    reducer: &mut impl Reducer<Input = T, Output = U>,
    inflight_msgs: &mut Vec<OwnedMessage>,
    ok: &mpsc::Sender<(Vec<OwnedMessage>, U)>,
    err: &mpsc::Sender<OwnedMessage>,
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
    mut receiver: mpsc::Receiver<(impl IntoIterator<Item = OwnedMessage>, T)>,
    ok: mpsc::Sender<(Vec<OwnedMessage>, U)>,
    err: mpsc::Sender<OwnedMessage>,
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
    mut reducer: impl Reducer<Input = OwnedMessage, Output = ()>,
    mut receiver: mpsc::Receiver<OwnedMessage>,
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

trait CommitClient {
    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()>;
}

impl CommitClient for StreamConsumer<KafkaContext> {
    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        Consumer::store_offsets(self, tpl)
    }
}

#[derive(Default)]
struct HighwaterMark {
    data: HashMap<(String, i32), i64>,
}

impl HighwaterMark {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    fn track(&mut self, msg: &OwnedMessage) {
        let cur_offset = self
            .data
            .entry((msg.topic().to_string(), msg.partition()))
            .or_insert(msg.offset() + 1);
        *cur_offset = cmp::max(*cur_offset, msg.offset() + 1);
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

impl From<HighwaterMark> for TopicPartitionList {
    fn from(val: HighwaterMark) -> Self {
        let mut tpl = TopicPartitionList::with_capacity(val.len());
        for ((topic, partition), offset) in val.data.iter() {
            tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset))
                .expect("Invalid partition offset");
        }
        tpl
    }
}

#[instrument(skip_all)]
pub async fn commit(
    mut receiver: mpsc::Receiver<(Vec<OwnedMessage>, ())>,
    consumer: Arc<impl CommitClient>,
    _rendezvous_guard: oneshot::Sender<()>,
) -> Result<(), Error> {
    while let Some(msgs) = receiver.recv().await {
        let mut highwater_mark = HighwaterMark::new();
        msgs.0.iter().for_each(|msg| highwater_mark.track(msg));
        consumer.store_offsets(&highwater_mark.into()).unwrap();
    }
    debug!("Shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        iter,
        marker::PhantomData,
        mem::take,
        sync::{Arc, RwLock},
        time::Duration,
    };

    use anyhow::{Error, anyhow};
    use futures::Stream;
    use rdkafka::{
        Message, Offset, Timestamp, TopicPartitionList,
        error::{KafkaError, KafkaResult},
        message::OwnedMessage,
    };
    use tokio::{
        sync::{broadcast, mpsc, oneshot},
        time::sleep,
    };
    use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
    use tokio_util::sync::CancellationToken;

    use crate::kafka::{
        consumer::{
            CommitClient, KafkaMessage, MessageQueue, ReduceConfig, ReduceShutdownBehaviour,
            ReduceShutdownCondition, Reducer, ReducerWhenFullBehaviour, commit, map, reduce,
            reduce_err,
        },
        os_stream_writer::{OsStream, OsStreamWriter},
    };

    struct MockCommitClient {
        offsets: Arc<RwLock<Vec<TopicPartitionList>>>,
    }

    impl CommitClient for MockCommitClient {
        fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
            self.offsets.write().unwrap().push(tpl.clone());
            Ok(())
        }
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
            if let Some(idx) = self.error_on_idx {
                if idx == self.pipe.read().unwrap().len() {
                    self.error_on_idx.take();
                    return Err(anyhow!("err"));
                }
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
                .extend(take(&mut self.buffer.write().unwrap() as &mut Vec<T>).into_iter());
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
        let offsets = Arc::new(RwLock::new(Vec::new()));

        let commit_client = Arc::new(MockCommitClient {
            offsets: offsets.clone(),
        });
        let (sender, receiver) = mpsc::channel(1);
        let (rendezvou_sender, rendezvou_receiver) = oneshot::channel();

        let msg = vec![
            OwnedMessage::new(
                None,
                None,
                "topic".to_string(),
                Timestamp::NotAvailable,
                0,
                1,
                None,
            ),
            OwnedMessage::new(
                None,
                None,
                "topic".to_string(),
                Timestamp::NotAvailable,
                1,
                0,
                None,
            ),
        ];

        assert!(sender.send((msg.clone(), ())).await.is_ok());

        tokio::spawn(commit(receiver, commit_client, rendezvou_sender));

        drop(sender);
        let _ = rendezvou_receiver.await;

        assert_eq!(offsets.read().unwrap().len(), 1);
        assert_eq!(
            offsets.read().unwrap()[0],
            TopicPartitionList::from_topic_map(&HashMap::from([
                (("topic".to_string(), 0), Offset::Offset(2)),
                (("topic".to_string(), 1), Offset::Offset(1))
            ]))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_reduce_err_without_flush_interval() {
        let reducer = StreamingReducer::new(None);
        let pipe = reducer.get_pipe();

        let (sender, receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let msg = OwnedMessage::new(
            Some(vec![0, 1, 2, 3, 4, 5, 6, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );

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

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 2, 4, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 3, 5, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );

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

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 2, 4, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 3, 5, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );
        let msg_2 = OwnedMessage::new(
            Some(vec![0, 0, 0, 0]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            2,
            None,
        );

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

        let msg = OwnedMessage::new(
            Some(vec![0, 1, 2, 3, 4, 5, 6, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );

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

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 2, 4, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 3, 5, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );

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

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 3, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 4, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );
        let msg_2 = OwnedMessage::new(
            Some(vec![2, 5, 8]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            2,
            None,
        );

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

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 3, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 4, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );
        let msg_2 = OwnedMessage::new(
            Some(vec![2, 5, 8]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            2,
            None,
        );
        let msg_3 = OwnedMessage::new(
            Some(vec![0, 0, 0]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            3,
            None,
        );

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

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 2, 4, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 3, 5, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );

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

        let ok_msgs = ok_receiver_1.recv().await.unwrap().0;
        assert_eq!(ok_msgs.len(), 2);
        assert_eq!(ok_msgs[0].payload(), msg_0.payload());
        assert_eq!(ok_msgs[1].payload(), msg_1.payload());

        assert!(buffer_0.read().unwrap().is_empty());
        assert_eq!(pipe_0.read().unwrap().as_slice(), &[1, 2]);

        assert!(buffer_1.read().unwrap().is_empty());
        assert_eq!(pipe_1.read().unwrap().as_slice(), &[()]);

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

        let msg_0 = OwnedMessage::new(
            Some(vec![0, 2, 4, 6]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let msg_1 = OwnedMessage::new(
            Some(vec![1, 3, 5, 7]),
            None,
            "topic".to_string(),
            Timestamp::now(),
            0,
            1,
            None,
        );

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

    #[derive(Clone)]
    struct MockMessage {
        payload: Vec<u8>,
        topic: String,
        partition: i32,
        offset: i64,
    }

    impl KafkaMessage for Result<Result<MockMessage, KafkaError>, BroadcastStreamRecvError> {
        fn detach(&self) -> Result<OwnedMessage, Error> {
            let clone = self.clone().unwrap().unwrap();
            Ok(OwnedMessage::new(
                Some(clone.payload),
                None,
                clone.topic,
                Timestamp::now(),
                clone.partition,
                clone.offset,
                None,
            ))
        }
    }

    impl MessageQueue for broadcast::Receiver<Result<MockMessage, KafkaError>> {
        fn stream(&self) -> impl Stream<Item = impl KafkaMessage> {
            BroadcastStream::new(self.resubscribe())
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

        let msg_0 = MockMessage {
            payload: vec![0],
            topic: "topic".to_string(),
            partition: 0,
            offset: 0,
        };
        let msg_1 = MockMessage {
            payload: vec![1],
            topic: "topic".to_string(),
            partition: 0,
            offset: 1,
        };
        assert!(sender.send(Ok(msg_0.clone())).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(
            res.0.collect::<Vec<_>>()[0].payload(),
            Some(msg_0.payload.clone()).as_deref()
        );
        assert_eq!(res.1, msg_0.payload[0] * 2);

        assert!(sender.send(Ok(msg_1.clone())).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(
            res.0.collect::<Vec<_>>()[0].payload(),
            Some(msg_1.payload.clone()).as_deref()
        );
        assert_eq!(res.1, msg_1.payload[0] * 2);

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

        let msg_0 = MockMessage {
            payload: vec![0],
            topic: "topic".to_string(),
            partition: 0,
            offset: 0,
        };
        let msg_1 = MockMessage {
            payload: vec![1],
            topic: "topic".to_string(),
            partition: 0,
            offset: 1,
        };
        let msg_2 = MockMessage {
            payload: vec![2],
            topic: "topic".to_string(),
            partition: 0,
            offset: 2,
        };

        assert!(sender.send(Ok(msg_0.clone())).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(
            res.0.collect::<Vec<_>>()[0].payload(),
            Some(msg_0.payload).as_deref()
        );
        assert_eq!(res.1, 0);

        assert!(sender.send(Ok(msg_1.clone())).is_ok());
        assert!(ok_receiver.is_empty());
        let res = err_receiver.recv().await.unwrap();
        assert_eq!(res.payload(), Some(msg_1.payload).as_deref());

        assert!(sender.send(Ok(msg_2.clone())).is_ok());
        assert!(err_receiver.is_empty());
        let res = ok_receiver.recv().await.unwrap();
        assert_eq!(
            res.0.collect::<Vec<_>>()[0].payload(),
            Some(msg_2.payload).as_deref()
        );
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
                |_: Arc<OwnedMessage>| Ok(()),
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
                |_: Arc<OwnedMessage>| Ok(()),
            reduce:
                NoopReducer::new(),
        });
    }
}
