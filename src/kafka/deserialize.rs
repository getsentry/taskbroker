use std::sync::Arc;

use anyhow::Error;
use rdkafka::Message;
use rdkafka::message::OwnedMessage;

use crate::config::Config;
use crate::store::activation::InflightActivation;

use super::deserialize_activation::{self, DeserializeActivationConfig};
use super::deserialize_raw::{self, RawConfig};

pub struct DeserializeConfig {
    activation_config: DeserializeActivationConfig,
    raw_config: Option<RawConfig>,
    /// Retry topic always contains activations, even in raw_mode.
    retry_topic: Option<String>,
}

impl DeserializeConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            activation_config: DeserializeActivationConfig::from_config(config),
            raw_config: RawConfig::from_config(config),
            retry_topic: config.kafka_retry_topic.clone(),
        }
    }
}

/// Create a unified deserializer that handles both normal and raw modes.
/// In raw mode, raw Kafka bytes are wrapped into a TaskActivation.
/// In normal mode, Kafka messages are expected to contain encoded TaskActivation protos.
/// Messages from the retry topic are always deserialized as activations.
pub fn new(
    config: DeserializeConfig,
) -> impl Fn(Arc<OwnedMessage>) -> Result<InflightActivation, Error> {
    let raw_deserializer = config.raw_config.map(deserialize_raw::new);
    let activation_deserializer = deserialize_activation::new(config.activation_config);
    let retry_topic = config.retry_topic;

    move |msg: Arc<OwnedMessage>| {
        // Messages from the retry topic are always activations
        if let Some(ref retry_topic) = retry_topic {
            if msg.topic() == retry_topic {
                return activation_deserializer(msg);
            }
        }

        // For main topic: use raw deserializer in raw_mode, else activation deserializer
        if let Some(ref raw_deserializer) = raw_deserializer {
            raw_deserializer(msg)
        } else {
            activation_deserializer(msg)
        }
    }
}
