use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::sync::mpsc::{SyncSender, sync_channel};

use anyhow::{Context, Error};
use futures::Stream;
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{ClientConfig, TopicPartitionList};
use sentry_arroyo::backends::kafka::async_consumer::{
    AsyncAssignmentCallbacks, AsyncKafkaConsumer, AsyncPartitionQueue,
};
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::types::{Partition, Topic};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

use super::consumer::{CommitClient, Event, KafkaMessage, MessageQueue};
use super::message::MessageBackend;

pub fn create_consumer(
    topics: &[&str],
    config: &ClientConfig,
    context: KafkaContext,
) -> Result<AsyncKafkaConsumer, Error> {
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
    AsyncKafkaConsumer::new(arroyo_config, &topics, context)
        .context("Could not create Arroyo consumer")
}

impl CommitClient for AsyncKafkaConsumer {
    fn store_offsets(&self, tpl: &TopicPartitionList) -> Result<(), Error> {
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
        // The inherent method, not this trait method.
        AsyncKafkaConsumer::store_offsets(self, offsets)
            .context("Arroyo consumer offset storage failed")
    }
}

fn arroyo_partition(topic: &str, partition: i32) -> Result<Partition, Error> {
    Ok(Partition::new(
        Topic::new(topic),
        u16::try_from(partition).context("Partition index does not fit Arroyo's partition type")?,
    ))
}

impl MessageQueue for AsyncPartitionQueue {
    fn stream(&self) -> impl Stream<Item = impl KafkaMessage> {
        futures::stream::unfold(self, |queue| async move {
            let message = queue
                .recv()
                .await?
                .map(MessageBackend::from)
                .context("Arroyo partition receive failed");
            Some((message, queue))
        })
    }
}

#[derive(Debug)]
pub struct KafkaContext {
    event_sender: UnboundedSender<(Event, SyncSender<()>)>,
    /// The topic(s) this consumer is subscribed to, used as a metric tag.
    topics_tag: String,
}

impl KafkaContext {
    pub fn new(event_sender: UnboundedSender<(Event, SyncSender<()>)>, topics_tag: String) -> Self {
        Self {
            event_sender,
            topics_tag,
        }
    }

    /// Called from Arroyo's assignment callbacks: blocks until `handle_events` has processed the
    /// event.
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

fn partition_key(partition: Partition) -> (String, i32) {
    (
        partition.topic.as_str().to_owned(),
        i32::from(partition.index),
    )
}

impl AsyncAssignmentCallbacks for KafkaContext {
    fn on_assign(&self, queues: Vec<AsyncPartitionQueue>) {
        let partitions = queues
            .iter()
            .map(|queue| partition_key(queue.partition()))
            .collect();
        self.handle_rebalance(Event::Assign(Assignment { partitions, queues }));
    }

    fn on_revoke(&self, partitions: Vec<Partition>) {
        let partitions = partitions.into_iter().map(partition_key).collect();
        self.handle_rebalance(Event::Revoke(partitions));
    }

    /// Arroyo has already logged the error. Shut down on unexpected consumer errors.
    fn on_error(&self, error: KafkaError) {
        if error.rdkafka_error_code() == Some(RDKafkaErrorCode::InconsistentGroupProtocol) {
            // for switching to cooperative-sticky
            warn!("Waiting for group members on the other rebalance protocol to leave");
            metrics::counter!(
                "arroyo.consumer.inconsistent_group_protocol",
                "topic" => self.topics_tag.clone(),
                "application" => "taskbroker",
            )
            .increment(1);
            return;
        }
        drop(elegant_departure::shutdown());
    }
}

/// The assigned partitions and their queues.
pub struct Assignment {
    pub partitions: BTreeSet<(String, i32)>,
    pub queues: Vec<AsyncPartitionQueue>,
}

impl Assignment {
    pub fn split(self) -> impl Iterator<Item = ((String, i32), Assignment)> {
        self.queues.into_iter().map(|queue| {
            let partition = partition_key(queue.partition());
            let assignment = Assignment {
                partitions: BTreeSet::from([partition.clone()]),
                queues: vec![queue],
            };
            (partition, assignment)
        })
    }
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

    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};

    use anyhow::{Context, Error, ensure};
    use futures::{StreamExt, pin_mut};
    use tokio::sync::mpsc;
    use tokio::task;
    use tokio::time::timeout;

    use crate::kafka::consumer::{CommitClient, Event, KafkaMessage, MessageQueue};

    use super::{KafkaContext, create_consumer};

    #[tokio::test]
    async fn test_partition_receive_and_commit() {
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
            create_consumer(
                &["test"],
                &config,
                KafkaContext::new(sender, "test".to_owned()),
            )
            .unwrap(),
        );
        let result = timeout(Duration::from_secs(30), async {
            let (event, rendezvous) = events.recv().await.context("Missing assignment")?;
            let Event::Assign(assignment) = event else {
                anyhow::bail!("Expected assignment, got {event:?}");
            };
            drop(rendezvous);
            ensure!(assignment.queues.len() == 1);
            let stream = assignment.queues[0].stream();
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
            CommitClient::store_offsets(consumer.as_ref(), &offsets)?;
            let committer = consumer.clone();
            task::spawn_blocking(move || committer.commit_consumer_state()).await??;
            task::spawn_blocking(move || -> Result<(), Error> {
                let observer: BaseConsumer = config.create()?;
                let committed = observer.committed_offsets(offsets, Duration::from_secs(5))?;
                ensure!(committed.find_partition("test", 0).unwrap().offset() == next_offset);
                Ok(())
            })
            .await?
        })
        .await;

        // Dropping the consumer revokes its partitions, which would wait on `events` forever.
        drop(events);
        task::spawn_blocking(move || drop(consumer)).await.unwrap();
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
        let (events, _) = mpsc::unbounded_channel();
        let context = KafkaContext::new(events, "test".to_owned());
        let consumer = create_consumer(&["test"], &config, context)
            .unwrap_or_else(|error| panic!("{policy}: {error}"));
        tokio::task::spawn_blocking(move || drop(consumer))
            .await
            .unwrap();
    }
}
