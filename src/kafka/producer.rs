use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::AsyncKafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::{AsyncProducer, ProducerError};
use sentry_arroyo::types::{Topic, TopicOrPartition};

/// A producer for one backend. Callers replace it when the runtime flag changes.
pub enum ProducerBackend {
    Rdkafka {
        producer: FutureProducer,
        queue_timeout: Duration,
    },
    Arroyo {
        producer: AsyncKafkaProducer,
        queue_timeout: Duration,
    },
}

impl ProducerBackend {
    pub fn is_arroyo(&self) -> bool {
        matches!(self, Self::Arroyo { .. })
    }

    pub fn new(
        config: ClientConfig,
        use_arroyo: bool,
        queue_timeout: Duration,
    ) -> Result<Self, KafkaError> {
        if use_arroyo {
            let bootstrap_servers = config
                .get("bootstrap.servers")
                .expect("producer config always sets bootstrap.servers")
                .to_owned();
            let arroyo_config = KafkaConfig::new_producer_config(
                vec![bootstrap_servers],
                Some(config.config_map().clone()),
            );
            Ok(Self::Arroyo {
                producer: AsyncKafkaProducer::new(arroyo_config)?,
                queue_timeout,
            })
        } else {
            Ok(Self::Rdkafka {
                producer: config.create()?,
                queue_timeout,
            })
        }
    }

    pub async fn send(&self, topic: &str, payload: &[u8]) -> Result<(), ProducerError> {
        match self {
            Self::Rdkafka {
                producer,
                queue_timeout,
            } => producer
                .send(
                    FutureRecord::<(), [u8]>::to(topic).payload(payload),
                    Timeout::After(*queue_timeout),
                )
                .await
                .map(|_| ())
                .map_err(|(error, _)| error.into()),
            Self::Arroyo {
                producer,
                queue_timeout,
            } => {
                let destination = TopicOrPartition::Topic(Topic::new(topic));
                let payload = KafkaPayload::new(None, None, Some(payload.to_vec()));
                let deadline = Instant::now() + *queue_timeout;

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

    use super::ProducerBackend;

    #[test]
    fn flag_selects_arroyo_producer() {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", "127.0.0.1:1");
        let producer = ProducerBackend::new(config, true, Duration::from_millis(500)).unwrap();

        assert!(producer.is_arroyo());
    }

    #[tokio::test]
    async fn arroyo_retries_queue_full_until_timeout() {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("queue.buffering.max.messages", "1")
            .set("message.timeout.ms", "5000");
        let backend = ProducerBackend::new(config, true, Duration::from_millis(20)).unwrap();
        let ProducerBackend::Arroyo { producer, .. } = &backend else {
            unreachable!();
        };

        // Fill the queue without polling the delivery future.
        let destination = TopicOrPartition::Topic(Topic::new("test"));
        let _first = producer.produce(
            &destination,
            KafkaPayload::new(None, None, Some(b"first".to_vec())),
        );

        let started = Instant::now();
        let result = backend.send("test", b"second").await;
        assert!(matches!(
            result,
            Err(ProducerError::Kafka(KafkaError::MessageProduction(
                RDKafkaErrorCode::QueueFull
            )))
        ));
        assert!(started.elapsed() >= Duration::from_millis(20));
    }
}
