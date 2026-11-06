use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::config_blob::ProducerSelector;
use sentry_arroyo::backends::kafka::producer::AsyncKafkaProducer;
use sentry_arroyo::backends::kafka::reloading_producer::{
    ReloadConfig, ReloadError, ReloadingKafkaProducer,
};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::{AsyncProducer, ProducerError};
use sentry_arroyo::types::{Topic, TopicOrPartition};

use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

/// An Arroyo producer with bounded retries when the local queue is full.
///
/// Reloading producers are per topic so each follows its own entry in the shared Kafka config.
pub struct KafkaProducer {
    reloading: bool,
    producers: HashMap<String, Arc<dyn AsyncProducer<KafkaPayload>>>,
    queue_timeout: Duration,
}

impl KafkaProducer {
    pub fn new(
        config: ClientConfig,
        topics: &[&str],
        reloading: bool,
        queue_timeout: Duration,
    ) -> Result<Self, ReloadError> {
        let bootstrap_servers = config
            .get("bootstrap.servers")
            .expect("producer config always sets bootstrap.servers")
            .to_owned();
        let arroyo_config = KafkaConfig::new_producer_config(
            vec![bootstrap_servers],
            Some(config.config_map().clone()),
        );

        let mut producers = HashMap::new();
        if reloading {
            for topic in topics {
                let settings = ReloadConfig {
                    // Taskbroker sends messages without a key, so their order does not
                    // matter. Swap clients right away instead of holding back sends
                    // until the old client has drained.
                    ignore_key_ordering: true,
                    ..ReloadConfig::default()
                };
                let producer: Arc<dyn AsyncProducer<KafkaPayload>> =
                    Arc::new(ReloadingKafkaProducer::from_config(
                        arroyo_config.clone(),
                        ProducerSelector::new(*topic, "taskbroker"),
                        settings,
                    )?);
                producers.insert(topic.to_string(), producer);
            }
        } else {
            let shared: Arc<dyn AsyncProducer<KafkaPayload>> =
                Arc::new(AsyncKafkaProducer::new(arroyo_config)?);
            for topic in topics {
                producers.insert(topic.to_string(), Arc::clone(&shared));
            }
        }

        Ok(Self {
            reloading,
            producers,
            queue_timeout,
        })
    }

    pub fn is_reloading(&self) -> bool {
        self.reloading
    }

    pub fn has_topic(&self, topic: &str) -> bool {
        self.producers.contains_key(topic)
    }

    pub async fn send(&self, topic: &str, payload: &[u8]) -> Result<(), ProducerError> {
        let producer = self
            .producers
            .get(topic)
            .ok_or_else(|| ProducerError::ProducerFailure {
                error: format!("no producer for topic {topic}"),
            })?;
        let destination = TopicOrPartition::Topic(Topic::new(topic));
        let payload = KafkaPayload::new(None, None, Some(payload.to_vec()));
        let deadline = Instant::now() + self.queue_timeout;

        loop {
            match producer.produce(&destination, payload.clone()).await {
                Err(
                    error @ ProducerError::Kafka(KafkaError::MessageProduction(
                        RDKafkaErrorCode::QueueFull,
                    )),
                ) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(error);
                    }
                    // Keep rdkafka's retry interval without sleeping past the deadline.
                    tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
                }
                result => return result,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use rdkafka::ClientConfig;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use rstest::rstest;

    use sentry_arroyo::backends::ProducerError;
    use sentry_arroyo::backends::kafka::types::KafkaPayload;
    use sentry_arroyo::types::{Topic, TopicOrPartition};

    use super::KafkaProducer;

    #[tokio::test]
    #[rstest]
    async fn arroyo_retries_queue_full_until_timeout(#[values(false, true)] reloading: bool) {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("queue.buffering.max.messages", "1")
            .set("message.timeout.ms", "5000");
        let producer =
            KafkaProducer::new(config, &["test"], reloading, Duration::from_millis(20)).unwrap();

        // Fill the queue without polling the delivery future.
        let destination = TopicOrPartition::Topic(Topic::new("test"));
        let _first = producer.producers["test"].produce(
            &destination,
            KafkaPayload::new(None, None, Some(b"first".to_vec())),
        );

        let started = Instant::now();
        let result = producer.send("test", b"second").await;
        assert!(matches!(
            result,
            Err(ProducerError::Kafka(KafkaError::MessageProduction(
                RDKafkaErrorCode::QueueFull
            )))
        ));
        assert!(started.elapsed() >= Duration::from_millis(20));
    }
}
