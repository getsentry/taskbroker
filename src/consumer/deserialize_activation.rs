use std::sync::Arc;

use anyhow::{anyhow, Error};
use chrono::Utc;
use prost::Message as _;
use rdkafka::{message::OwnedMessage, Message};
use sentry_protos::taskbroker::v1::TaskActivation;

use crate::inflight_activation_store::{InflightActivation, InflightActivationStatus};

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

        Ok(InflightActivation {
            activation,
            status: InflightActivationStatus::Pending,
            partition: msg.partition(),
            offset: msg.offset(),
            added_at: Utc::now(),
            processing_deadline: None,
            processing_attempts,
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
        // let delta = inflight.expires_at - the_past;
        // assert!(
        //     delta.num_seconds() >= 99,
        //     "Should have ~100 seconds of delay from received_at"
        // );
    }
}
