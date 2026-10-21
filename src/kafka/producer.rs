use std::time::{Duration, Instant};

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::AsyncKafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::{AsyncProducer, ProducerError};
use sentry_arroyo::types::{Topic, TopicOrPartition};

use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

/// An Arroyo producer with bounded retries when the local queue is full.
pub struct KafkaProducer {
    producer: AsyncKafkaProducer,
    queue_timeout: Duration,
}

impl KafkaProducer {
    pub fn new(config: ClientConfig, queue_timeout: Duration) -> Result<Self, KafkaError> {
        let bootstrap_servers = config
            .get("bootstrap.servers")
            .expect("producer config always sets bootstrap.servers")
            .to_owned();
        let arroyo_config = KafkaConfig::new_producer_config(
            vec![bootstrap_servers],
            Some(config.config_map().clone()),
        );
        Ok(Self {
            producer: AsyncKafkaProducer::new(arroyo_config)?,
            queue_timeout,
        })
    }

    pub async fn send(&self, topic: &str, payload: &[u8]) -> Result<(), ProducerError> {
        let destination = TopicOrPartition::Topic(Topic::new(topic));
        let payload = KafkaPayload::new(None, None, Some(payload.to_vec()));
        let deadline = Instant::now() + self.queue_timeout;

        loop {
            match self.producer.produce(&destination, payload.clone()).await {
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

    use sentry_arroyo::backends::kafka::types::KafkaPayload;
    use sentry_arroyo::backends::{AsyncProducer, ProducerError};
    use sentry_arroyo::types::{Topic, TopicOrPartition};

    use super::KafkaProducer;

    #[tokio::test]
    async fn arroyo_retries_queue_full_until_timeout() {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("queue.buffering.max.messages", "1")
            .set("message.timeout.ms", "5000");
        let producer = KafkaProducer::new(config, Duration::from_millis(20)).unwrap();

        // Fill the queue without polling the delivery future.
        let destination = TopicOrPartition::Topic(Topic::new("test"));
        let _first = producer.producer.produce(
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
