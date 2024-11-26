use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Error};
use chrono::Utc;
use prost::Message as _;
use rdkafka::{message::OwnedMessage, Message};
use sentry_protos::sentry::v1::TaskActivation;

use crate::{
    config::Config,
    inflight_activation_store::{InflightActivation, InflightActivationStatus},
};

pub struct DeserializeConfig {
    pub deadletter_deadline: Option<Duration>,
}

impl DeserializeConfig {
    /// Convert from application into service configuration
    pub fn from_config(config: &Config) -> Self {
        Self {
            deadletter_deadline: Some(Duration::from_secs(config.deadletter_deadline as u64)),
        }
    }
}

pub fn new(
    config: DeserializeConfig,
) -> impl Fn(Arc<OwnedMessage>) -> Result<InflightActivation, Error> {
    move |msg: Arc<OwnedMessage>| {
        let Some(payload) = msg.payload() else {
            return Err(anyhow!("Message has no payload"));
        };
        let activation = TaskActivation::decode(payload)?;
        Ok(InflightActivation {
            activation,
            status: InflightActivationStatus::Pending,
            partition: msg.partition(),
            offset: msg.offset(),
            added_at: Utc::now(),
            deadletter_at: config
                .deadletter_deadline
                .map(|duration| Utc::now() + duration),
            processing_deadline: None,
        })
    }
}
