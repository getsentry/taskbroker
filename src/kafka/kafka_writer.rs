use super::consumer::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};
use crate::{config::Config, store::inflight_activation::InflightActivation};
use core::panic;
use futures::{StreamExt, stream::FuturesUnordered};
use prost::Message as _;
use rdkafka::{
    ClientConfig, Message,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use std::{mem::replace, time::Duration};
use tracing::error;

#[derive(Clone)]
pub struct KafkaWriterConfig {
    pub max_batch_len: usize,
    pub max_retry_attempts: u8,
    pub flush_interval_ms: u64,
    pub kafka_client_config: ClientConfig,
    pub kafka_topic: String,
    pub kafka_send_timeout_ms: u64,
}

impl KafkaWriterConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_batch_len: 1024,
            max_retry_attempts: 3,
            flush_interval_ms: 1000,
            kafka_client_config: config.kafka_consumer_config(),
            kafka_topic: config.kafka_topic.clone(),
            kafka_send_timeout_ms: 5000,
        }
    }
}

pub struct KafkaWriter {
    config: KafkaWriterConfig,
    batch: Vec<Vec<u8>>,
    producer: FutureProducer,
}

impl KafkaWriter {
    pub fn new(config: KafkaWriterConfig) -> Self {
        Self {
            batch: Vec::with_capacity(config.max_batch_len),
            producer: config
                .kafka_client_config
                .create()
                .expect("Could not create kafka producer in consumer"),
            config,
        }
    }
}

impl Reducer for KafkaWriter {
    type Input = Vec<InflightActivation>;

    type Output = ();

    async fn reduce(&mut self, batch: Self::Input) -> Result<(), anyhow::Error> {
        self.batch.extend(
            batch
                .into_iter()
                .map(|inflight| inflight.activation.encode_to_vec()),
        );
        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<Self::Output>, anyhow::Error> {
        if self.batch.is_empty() {
            return Ok(Some(()));
        }
        let mut payloads = replace(
            &mut self.batch,
            Vec::with_capacity(self.config.max_batch_len),
        );

        for _ in 0..self.config.max_retry_attempts {
            let deliveries = payloads
                .into_iter()
                .map(|payload| {
                    let producer = self.producer.clone();
                    let config = self.config.clone();
                    async move {
                        producer
                            .send(
                                FutureRecord::<(), Vec<u8>>::to(&config.kafka_topic)
                                    .payload(&payload),
                                Timeout::After(Duration::from_millis(config.kafka_send_timeout_ms)),
                            )
                            .await
                    }
                })
                .collect::<FuturesUnordered<_>>();

            let results = deliveries.collect::<Vec<_>>().await;

            payloads = results
                .into_iter()
                .filter_map(|result| {
                    let Err((reason, msg)) = result else {
                        return None;
                    };
                    error!("consumer.publish.failure {}", reason);
                    Some(
                        msg.payload()
                            .expect("Produced message always have payload")
                            .to_vec(),
                    )
                })
                .collect();

            if payloads.is_empty() {
                break;
            }
        }

        if !payloads.is_empty() {
            panic!("Consumer is unable to write to kafka");
        }

        Ok(Some(()))
    }

    fn reset(&mut self) {
        todo!()
    }

    async fn is_full(&self) -> bool {
        self.batch.len() >= self.config.max_batch_len
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        ReduceConfig {
            shutdown_condition: ReduceShutdownCondition::Drain,
            shutdown_behaviour: ReduceShutdownBehaviour::Flush,
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            flush_interval: Some(Duration::from_millis(self.config.flush_interval_ms)),
        }
    }
}
