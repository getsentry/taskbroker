use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Error};
use chrono::Utc;
use prost::Message as _;
use rdkafka::{message::OwnedMessage, Message};
use sentry_protos::sentry::v1::TaskActivation;

use crate::inflight_activation_store::{InflightActivation, TaskActivationStatus};

pub struct DeserializerConfig {
    pub deadletter_duration: Option<Duration>,
}

pub fn new(
    config: DeserializerConfig,
) -> impl Fn(Arc<OwnedMessage>) -> Result<InflightActivation, Error> {
    move |msg: Arc<OwnedMessage>| {
        let Some(payload) = msg.payload() else {
            return Err(anyhow!("Message has no payload"));
        };
        let activation = TaskActivation::decode(payload)?;
        Ok(InflightActivation {
            activation,
            status: TaskActivationStatus::Pending,
            partition: msg.partition(),
            offset: msg.offset(),
            added_at: Utc::now(),
            deadletter_at: config
                .deadletter_duration
                .map(|duration| Utc::now() + duration),
            processing_deadline: None,
        })
    }
}
