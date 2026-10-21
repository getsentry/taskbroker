use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::mpsc::sync_channel;

use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, TopicPartitionList};

use anyhow::{Context, Error};
use futures::{Stream, StreamExt};
use sentry_arroyo::backends::kafka::async_consumer::{
    AsyncAssignmentCallbacks, AsyncKafkaConsumer, AsyncPartitionQueue,
};
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::types::{Partition, Topic};
use tracing::{info, warn};

use super::consumer::{CommitClient, Event, KafkaContext, KafkaMessage, MessageQueue};
use super::message::MessageBackend;

pub enum ConsumerBackend {
    Rdkafka {
        consumer: Arc<StreamConsumer<KafkaContext>>,
    },
    Arroyo {
        consumer: AsyncKafkaConsumer,
    },
}

impl ConsumerBackend {
    pub fn is_arroyo(&self) -> bool {
        matches!(self, Self::Arroyo { .. })
    }

    pub fn new(
        topics: &[&str],
        config: &ClientConfig,
        context: KafkaContext,
        use_arroyo: bool,
    ) -> Result<Self, Error> {
        if use_arroyo {
            let arroyo_config = KafkaConfig::new_config(
                vec![
                    config
                        .get("bootstrap.servers")
                        .context("Missing brokers")?
                        .to_owned(),
                ],
                Some(config.config_map().clone()),
            );
            let topics: Vec<_> = topics.iter().map(|topic| Topic::new(topic)).collect();
            Ok(Self::Arroyo {
                consumer: AsyncKafkaConsumer::new(arroyo_config, &topics, context)
                    .context("Could not create Arroyo consumer")?,
            })
        } else {
            let consumer: StreamConsumer<KafkaContext> = config
                .create_with_context(context)
                .expect("Consumer creation failed");
            consumer
                .subscribe(topics)
                .expect("Can't subscribe to specified topics");
            Ok(Self::Rdkafka {
                consumer: Arc::new(consumer),
            })
        }
    }

    pub fn partition_queues(
        &self,
        assignment: Assignment,
    ) -> Result<Vec<PartitionQueueBackend>, Error> {
        match self {
            Self::Rdkafka { consumer } => assignment
                .partitions
                .iter()
                .map(|(topic, partition)| {
                    consumer
                        .split_partition_queue(topic, *partition)
                        .map(PartitionQueueBackend::Rdkafka)
                        .context("Unable to split partition queue")
                })
                .collect(),
            Self::Arroyo { .. } => Ok(assignment
                .arroyo_queues
                .into_iter()
                .map(PartitionQueueBackend::Arroyo)
                .collect()),
        }
    }
}

impl CommitClient for ConsumerBackend {
    fn store_offsets(&self, tpl: &TopicPartitionList) -> Result<(), Error> {
        match self {
            Self::Rdkafka { consumer } => consumer.store_offsets(tpl).map_err(Error::from),
            Self::Arroyo { consumer } => {
                let mut offsets = HashMap::with_capacity(tpl.count());
                for element in tpl.elements() {
                    let offset = element
                        .offset()
                        .to_raw()
                        .and_then(|offset| u64::try_from(offset).ok())
                        .context("Cannot stage an invalid offset")?;
                    offsets.insert(
                        arroyo_partition(element.topic(), element.partition())?,
                        offset,
                    );
                }
                consumer
                    .store_offsets(offsets)
                    .context("Arroyo consumer offset storage failed")
            }
        }
    }
}

fn arroyo_partition(topic: &str, partition: i32) -> Result<Partition, Error> {
    Ok(Partition::new(
        Topic::new(topic),
        u16::try_from(partition).context("Partition index does not fit Arroyo's partition type")?,
    ))
}

pub enum PartitionQueueBackend {
    Rdkafka(StreamPartitionQueue<KafkaContext>),
    Arroyo(AsyncPartitionQueue),
}

impl MessageQueue for PartitionQueueBackend {
    fn stream(&self) -> impl Stream<Item = impl KafkaMessage> {
        match self {
            Self::Rdkafka(queue) => MessageQueue::stream(queue)
                .map(KafkaMessage::into_message)
                .left_stream(),
            Self::Arroyo(queue) => futures::stream::unfold(queue, |queue| async move {
                let message = queue
                    .recv()
                    .await?
                    .map(MessageBackend::from)
                    .context("Arroyo partition receive failed");
                Some((message, queue))
            })
            .right_stream(),
        }
    }
}

impl KafkaContext {
    /// Arroyo's counterpart to `pre_rebalance`: blocks until `handle_events` has processed
    /// the event.
    fn handle_rebalance(&self, event: Event) {
        let (partition_count, metric) = match &event {
            Event::Assign(assignment) => (
                assignment.partitions.len(),
                "arroyo.consumer.partitions_assigned.count",
            ),
            Event::Revoke(partitions) => {
                (partitions.len(), "arroyo.consumer.partitions_revoked.count")
            }
            Event::Shutdown => unreachable!("Shutdown is not a rebalance event"),
        };
        if partition_count == 0 {
            warn!("Got rebalance event with no partitions");
            return;
        }
        let (sender, receiver) = sync_channel(0);
        let _ = self.event_sender.send((event, sender));
        info!("Partition rebalance event sent, waiting for rendezvous...");
        let _ = receiver.recv();
        info!("Rendezvous complete");
        metrics::counter!(
            metric,
            "topic" => self.topics_tag.clone(),
            "application" => "taskbroker",
        )
        .increment(partition_count as u64);
    }
}

impl AsyncAssignmentCallbacks for KafkaContext {
    fn on_assign(&self, queues: Vec<AsyncPartitionQueue>) {
        let partitions = queues
            .iter()
            .map(|queue| {
                let partition = queue.partition();
                (
                    partition.topic.as_str().to_owned(),
                    i32::from(partition.index),
                )
            })
            .collect();
        self.handle_rebalance(Event::Assign(Assignment {
            partitions,
            arroyo_queues: queues,
        }));
    }

    fn on_revoke(&self, partitions: Vec<Partition>) {
        let partitions = partitions
            .iter()
            .map(|partition| {
                (
                    partition.topic.as_str().to_owned(),
                    i32::from(partition.index),
                )
            })
            .collect();
        self.handle_rebalance(Event::Revoke(partitions));
    }

    /// Arroyo has already logged the error. Shut down like the rdkafka client does on
    /// unexpected consumer errors.
    fn on_error(&self, _error: KafkaError) {
        drop(elegant_departure::shutdown());
    }
}

/// The assigned partitions, with their queues when Arroyo has already split them.
pub struct Assignment {
    pub partitions: BTreeSet<(String, i32)>,
    pub(super) arroyo_queues: Vec<AsyncPartitionQueue>,
}

impl Debug for Assignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Assignment").field(&self.partitions).finish()
    }
}

impl KafkaMessage for Result<MessageBackend, Error> {
    fn into_message(self) -> Result<MessageBackend, Error> {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};

    use tokio::runtime::Handle;
    use tokio::sync::{mpsc, oneshot};
    use tokio::task;
    use tokio::time::timeout;

    use anyhow::{Context, Error, ensure};
    use futures::{StreamExt, pin_mut};

    use crate::kafka::consumer::{CommitClient, Event, KafkaContext, KafkaMessage, MessageQueue};

    use super::ConsumerBackend;

    #[rstest::rstest]
    #[case::rdkafka(false)]
    #[case::arroyo(true)]
    #[tokio::test]
    async fn test_partition_receive_and_commit(#[case] use_arroyo: bool) {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("test", 1, 1).unwrap();
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .set("group.id", "test-consumer-backend")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .create()
            .unwrap();
        producer
            .send(
                FutureRecord::to("test").key("key").payload("payload"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        let (sender, mut events) = mpsc::unbounded_channel();
        let consumer = Arc::new(
            ConsumerBackend::new(
                &["test"],
                &config,
                KafkaContext::new(sender, "test".to_owned()),
                use_arroyo,
            )
            .unwrap(),
        );
        let (shutdown_sender, shutdown) = oneshot::channel::<()>();
        let poller = task::spawn_blocking({
            let consumer = consumer.clone();
            move || {
                Handle::current().block_on(async {
                    // Arroyo polls the consumer on its own thread.
                    let ConsumerBackend::Rdkafka { consumer } = consumer.as_ref() else {
                        return None;
                    };
                    tokio::select! {
                        _ = shutdown => None,
                        message = consumer.recv() => Some(message.map(|message| message.detach())),
                    }
                })
            }
        });
        let result = timeout(Duration::from_secs(30), async {
            let (event, rendezvous) = events.recv().await.context("Missing assignment")?;
            let Event::Assign(assignment) = event else {
                anyhow::bail!("Expected assignment, got {event:?}");
            };
            let queues = consumer.partition_queues(assignment)?;
            drop(rendezvous);
            ensure!(queues.len() == 1);
            let stream = queues[0].stream();
            pin_mut!(stream);
            let message = stream
                .next()
                .await
                .context("Queue closed")?
                .into_message()?;
            ensure!(message.topic() == "test" && message.partition() == 0);
            ensure!(message.key() == Some(b"key".as_slice()));
            ensure!(message.payload() == Some(b"payload".as_slice()));

            let next_offset = Offset::Offset(message.offset() + 1);
            let mut offsets = TopicPartitionList::new();
            offsets.add_partition_offset("test", 0, next_offset)?;
            consumer.store_offsets(&offsets)?;
            let committer = consumer.clone();
            task::spawn_blocking(move || match committer.as_ref() {
                ConsumerBackend::Rdkafka { consumer } => {
                    consumer.commit_consumer_state(CommitMode::Sync)
                }
                ConsumerBackend::Arroyo { consumer } => consumer.commit_consumer_state(),
            })
            .await??;
            task::spawn_blocking(move || -> Result<(), Error> {
                let observer: BaseConsumer = config.create()?;
                let committed = observer.committed_offsets(offsets, Duration::from_secs(5))?;
                ensure!(committed.find_partition("test", 0).unwrap().offset() == next_offset);
                Ok(())
            })
            .await?
        })
        .await;

        drop(events);
        drop(shutdown_sender);
        let poll_result = poller.await.unwrap();
        task::spawn_blocking(move || drop(consumer)).await.unwrap();
        assert!(
            poll_result.is_none(),
            "Unexpected main queue result: {poll_result:?}"
        );
        result.unwrap().unwrap();
    }

    #[rstest::rstest]
    #[case::smallest("smallest")]
    #[case::beginning("beginning")]
    #[case::largest("largest")]
    #[case::end("end")]
    #[tokio::test]
    async fn test_consumer_accepts_offset_reset_aliases(#[case] policy: &str) {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("group.id", "test-offset-reset-aliases")
            .set("auto.offset.reset", policy);
        for use_arroyo in [false, true] {
            let (events, _) = mpsc::unbounded_channel();
            let context = KafkaContext::new(events, "test".to_owned());
            let consumer = ConsumerBackend::new(&["test"], &config, context, use_arroyo)
                .unwrap_or_else(|error| panic!("{policy}, use_arroyo={use_arroyo}: {error}"));
            tokio::task::spawn_blocking(move || drop(consumer))
                .await
                .unwrap();
        }
    }
}
