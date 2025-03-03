use std::{sync::Arc, time::Duration};

use anyhow::{Error, anyhow};
use chrono::{MappedLocalTime, TimeZone, Utc};
use prost::Message as _;
use rdkafka::{Message, message::OwnedMessage};
use sentry_protos::taskbroker::v1::TaskActivation;

use crate::store::inflight_activation::{InflightActivation, InflightActivationStatus};

pub fn new() -> impl Fn(Arc<OwnedMessage>) -> Result<InflightActivation, Error> {
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
        let processing_attempts = 0;
        let now = Utc::now();

        let expires_at = if let Some(expires) = activation.expires {
            let expires_duration = Duration::from_secs(expires);
            // Expiry times are based on the time the task was received
            // not the time it was dequeued from Kafka.
            let activation_received = activation.received_at.map_or(now, |ts| {
                match Utc.timestamp_opt(ts.seconds, ts.nanos as u32) {
                    MappedLocalTime::Single(ts) => ts,
                    _ => now,
                }
            });
            Some(activation_received + expires_duration)
        } else {
            None
        };

        Ok(InflightActivation {
            activation,
            status: InflightActivationStatus::Pending,
            partition: msg.partition(),
            offset: msg.offset(),
            added_at: Utc::now(),
            processing_deadline: None,
            processing_attempts,
            expires_at,
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
    use rdkafka::{Timestamp, message::OwnedMessage};
    use sentry_protos::taskbroker::v1::TaskActivation;

    use super::new;

    #[test]
    fn test_processing_attempts_set() {
        let deserializer = new();
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
        assert!(
            inflight.processing_attempts == 0,
            "Should have 0 processing attempts"
        );
    }

    #[test]
    fn test_expires() {
        let deserializer = new();
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
        let delta = inflight.expires_at.unwrap() - the_past;
        assert!(
            delta.num_seconds() >= 99,
            "Should have ~100 seconds of delay from received_at"
        );
    }
}
