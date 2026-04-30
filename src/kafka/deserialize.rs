use std::sync::Arc;

use anyhow::Error;
use rdkafka::message::OwnedMessage;

use crate::config::Config;
use crate::store::activation::InflightActivation;

use super::deserialize_activation::{self, DeserializeActivationConfig};
use super::deserialize_passthrough::{self, PassthroughConfig};

pub struct DeserializeConfig {
    activation_config: DeserializeActivationConfig,
    passthrough_config: Option<PassthroughConfig>,
}

impl DeserializeConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            activation_config: DeserializeActivationConfig::from_config(config),
            passthrough_config: PassthroughConfig::from_config(config),
        }
    }
}

/// Create a unified deserializer that handles both normal and passthrough modes.
/// In passthrough mode, raw Kafka bytes are wrapped into a TaskActivation.
/// In normal mode, Kafka messages are expected to contain encoded TaskActivation protos.
pub fn new(
    config: DeserializeConfig,
) -> impl Fn(Arc<OwnedMessage>) -> Result<InflightActivation, Error> {
    let passthrough_deserializer = config.passthrough_config.map(deserialize_passthrough::new);
    let activation_deserializer = deserialize_activation::new(config.activation_config);

    move |msg: Arc<OwnedMessage>| {
        if let Some(ref pt_deserializer) = passthrough_deserializer {
            pt_deserializer(msg)
        } else {
            activation_deserializer(msg)
        }
    }
}
