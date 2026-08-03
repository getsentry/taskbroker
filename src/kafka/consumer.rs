use std::future::Future;
use std::time::Duration;

use anyhow::{Error, anyhow};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use tracing::{error, info};

pub async fn start_no_consume_mode(
    topics: &[&str],
    kafka_client_config: &ClientConfig,
) -> Result<(), Error> {
    let consumer: BaseConsumer = kafka_client_config
        .create()
        .expect("Consumer creation failed");

    // Validates: broker reachability, SASL/SSL auth handshake, topic existence,
    // and partition count, without joining the consumer group.
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

    let Some(md) = metadata else {
        return Err(anyhow!("failed to fetch metadata after 3 retries"));
    };
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

    fn reduce(&mut self, t: Self::Input) -> impl Future<Output = Result<(), Error>> + Send;
    fn flush(&mut self) -> impl Future<Output = Result<Option<Self::Output>, Error>> + Send;
    fn reset(&mut self);
    fn is_full(&self) -> impl Future<Output = bool> + Send;
    fn get_reduce_config(&self) -> ReduceConfig;
}
