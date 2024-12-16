use std::ops::Add;
use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Error};
use chrono::{MappedLocalTime, TimeZone, Utc};
use prost::Message as _;
use rdkafka::{message::OwnedMessage, Message};
use sentry_protos::sentry::v1::TaskActivation;

use crate::{
    config::Config,
    inflight_activation_store::{InflightActivation, InflightActivationStatus},
};

pub struct DeserializeConfig {
    pub deadletter_deadline: Duration,
}

impl DeserializeConfig {
    /// Convert from application into service configuration
    pub fn from_config(config: &Config) -> Self {
        Self {
            deadletter_deadline: Duration::from_secs(config.deadletter_deadline as u64),
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
        let namespace = activation.namespace.clone();

        let mut at_most_once = false;
        if let Some(ref retry_state) = activation.retry_state {
            at_most_once = retry_state
                .at_most_once
                .or(Some(false))
                .expect("could not access at_most_once");
        }
        let now = Utc::now();

        // Determine the deadletter_at time using config and activation expires time.
        let mut deadletter_at = now.add(config.deadletter_deadline);
        if let Some(expires) = activation.expires {
            let expires_duration = Duration::from_secs(expires);
            if expires_duration < config.deadletter_deadline {
                // Expiry times are based on the time the task was received
                // not the time it was dequeued from Kafka.
                let activation_received = activation.received_at.map_or(now, |ts| {
                    match Utc.timestamp_opt(ts.seconds, ts.nanos as u32) {
                        MappedLocalTime::Single(ts) => ts,
                        _ => now,
                    }
                });
                deadletter_at = activation_received + expires_duration;
            }
        }

        Ok(InflightActivation {
            activation,
            status: InflightActivationStatus::Pending,
            partition: msg.partition(),
            offset: msg.offset(),
            added_at: Utc::now(),
            deadletter_at: Some(deadletter_at),
            processing_deadline: None,
            at_most_once,
            namespace,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use chrono::Utc;
    use prost::Message as _;
    use rdkafka::{message::OwnedMessage, Timestamp};
    use sentry_protos::sentry::v1::TaskActivation;

    use super::{new, DeserializeConfig};

    #[test]
    fn test_deadletter_from_config() {
        let config = DeserializeConfig {
            deadletter_deadline: Duration::from_secs(900),
        };
        let deserializer = new(config);
        let now = Utc::now();
        let the_past = now - Duration::from_secs(60 * 10);

        #[allow(deprecated)]
        let activation = TaskActivation {
            id: "id_0".into(),
            namespace: "namespace".into(),
            taskname: "taskname".into(),
            parameters: "{}".into(),
            headers: HashMap::new(),
            // not used when the activation doesn't have expires.
            received_at: Some(prost_types::Timestamp {
                seconds: the_past.timestamp(),
                nanos: 0,
            }),
            deadline: None,
            retry_state: None,
            processing_deadline_duration: 10,
            expires: None,
        };
        let message = OwnedMessage::new(
            Some(activation.encode_to_vec()),
            None,
            "taskworker".into(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let arc_message = Arc::new(message);
        let inflight_opt = deserializer(arc_message);

        assert!(inflight_opt.is_ok());
        let inflight = inflight_opt.unwrap();
        let delta = inflight.deadletter_at.unwrap() - now;
        assert!(
            delta.num_seconds() >= 900,
            "Should have at least 900 seconds of delay from now"
        );
    }

    #[test]
    fn test_expires_deadletter() {
        let config = DeserializeConfig {
            deadletter_deadline: Duration::from_secs(900),
        };
        let deserializer = new(config);
        let now = Utc::now();
        let the_past = now - Duration::from_secs(60 * 10);

        #[allow(deprecated)]
        let activation = TaskActivation {
            id: "id_0".into(),
            namespace: "namespace".into(),
            taskname: "taskname".into(),
            parameters: "{}".into(),
            headers: HashMap::new(),
            // used because the activation has expires
            received_at: Some(prost_types::Timestamp {
                seconds: the_past.timestamp(),
                nanos: 0,
            }),
            deadline: None,
            retry_state: None,
            processing_deadline_duration: 10,
            expires: Some(100),
        };
        let message = OwnedMessage::new(
            Some(activation.encode_to_vec()),
            None,
            "taskworker".into(),
            Timestamp::now(),
            0,
            0,
            None,
        );
        let arc_message = Arc::new(message);
        let inflight_opt = deserializer(arc_message);

        assert!(inflight_opt.is_ok());
        let inflight = inflight_opt.unwrap();
        let delta = inflight.deadletter_at.unwrap() - the_past;
        assert!(
            delta.num_seconds() >= 99,
            "Should have ~100 seconds of delay from received_at"
        );
    }
}
