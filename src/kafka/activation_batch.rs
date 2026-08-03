use rdkafka::config::ClientConfig;

use crate::config::Config;

#[derive(Clone)]
pub struct ActivationBatchConfig {
    pub producer_config: ClientConfig,
    pub kafka_topic: String,
    pub kafka_long_topic: String,
    pub send_timeout_ms: u64,
    pub max_batch_time_ms: u64,
    pub max_batch_len: usize,
    pub max_batch_size: usize,
}

impl ActivationBatchConfig {
    pub fn from_topic(config: &Config, topic_name: &str) -> Self {
        Self {
            producer_config: config.kafka_producer_config(),
            kafka_topic: topic_name.to_owned(),
            kafka_long_topic: config.kafka_long_topic.clone(),
            send_timeout_ms: config.kafka_send_timeout_ms,
            max_batch_time_ms: config.store.insert_batch_max_time_ms,
            max_batch_len: config.store.insert_batch_max_length,
            max_batch_size: config.store.insert_batch_max_bytes,
        }
    }
}
