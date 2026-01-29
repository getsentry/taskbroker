use std::{sync::Arc, time::Duration};

use crate::config::Config;
use crate::store::inflight_activation::{InflightActivation, InflightActivationStatus};
use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use prost::Message as _;
use rdkafka::{Message, message::OwnedMessage};
use sentry_protos::taskbroker::v1::OnAttemptsExceeded;
use sentry_protos::taskbroker::v1::TaskActivation;

pub struct DeserializeActivationConfig {
    pub max_delayed_allowed: u64,
}

impl DeserializeActivationConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_delayed_allowed: config.max_delayed_task_allowed_sec,
        }
    }
}

pub fn new(
    config: DeserializeActivationConfig,
) -> impl Fn(Arc<OwnedMessage>) -> Result<InflightActivation, Error> {
    move |msg: Arc<OwnedMessage>| {
        let Some(payload) = msg.payload() else {
            return Err(anyhow!("Message has no payload"));
        };

        let activation = TaskActivation::decode(payload)?;
        let application = activation.application.unwrap_or("".into());
        let namespace = activation.namespace.clone();
        let taskname = activation.taskname.clone();

        println!("Task activation ingested - application: {}", application);

        metrics::histogram!(
            "consumer.message.payload_size_bytes",
            "namespace" => namespace.clone(),
            "taskname" => taskname.clone()
        )
        .record(payload.len() as f64);

        let at_most_once = activation
            .retry_state
            .is_some_and(|retry_state| retry_state.at_most_once.unwrap_or(false));

        let activation_time = activation
            .received_at
            .and_then(|ts| DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
            .unwrap_or(Utc::now());

        let expires_at = activation.expires.map(|secs| {
            let expires = Duration::from_secs(secs);
            activation_time + expires
        });

        let delay_until = activation.delay.map(|secs| {
            let mut delay = Duration::from_secs(secs);
            if secs > config.max_delayed_allowed {
                delay = Duration::from_secs(config.max_delayed_allowed)
            }
            activation_time + delay
        });

        let status = delay_until.map_or(InflightActivationStatus::Pending, |delay_until| {
            if Utc::now() > delay_until {
                InflightActivationStatus::Pending
            } else {
                InflightActivationStatus::Delay
            }
        });

        let on_attempts_exceeded: OnAttemptsExceeded = activation
            .retry_state
            .unwrap_or_default()
            .on_attempts_exceeded
            .try_into()
            .unwrap();

        Ok(InflightActivation {
            id: activation.id.clone(),
            activation: payload.to_vec(),
            status,
            partition: msg.partition(),
            offset: msg.offset(),
            added_at: Utc::now(),
            received_at: activation_time,
            processing_deadline: None,
            processing_deadline_duration: activation.processing_deadline_duration as i32,
            processing_attempts: 0,
            expires_at,
            delay_until,
            at_most_once,
            application,
            namespace,
            taskname,
            on_attempts_exceeded,
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

    use crate::{
        store::inflight_activation::InflightActivationStatus, test_utils::generate_unique_namespace,
    };

    use super::{Config, DeserializeActivationConfig, new};

    #[test]
    fn test_processing_attempts_set() {
        let config = Arc::new(Config::default());
        let deserializer = new(DeserializeActivationConfig::from_config(&config));
        let now = Utc::now();
        let the_past = now - Duration::from_secs(60 * 10);

        #[allow(deprecated)]
        let activation = TaskActivation {
            id: "id_0".into(),
            application: Some("sentry".to_string()),
            namespace: generate_unique_namespace(),
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
            delay: None,
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
        let config = Arc::new(Config::default());
        let deserializer = new(DeserializeActivationConfig::from_config(&config));
        let now = Utc::now();
        let the_past = now - Duration::from_secs(60 * 10);

        #[allow(deprecated)]
        let activation = TaskActivation {
            id: "id_0".into(),
            application: Some("sentry".to_string()),
            namespace: generate_unique_namespace(),
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
            delay: None,
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

    #[test]
    fn test_delay_past() {
        let config = Arc::new(Config::default());
        let deserializer = new(DeserializeActivationConfig::from_config(&config));
        let now = Utc::now();
        let the_past = now - Duration::from_secs(60 * 10);

        #[allow(deprecated)]
        let activation = TaskActivation {
            id: "id_0".into(),
            application: Some("sentry".to_string()),
            namespace: generate_unique_namespace(),
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
            expires: None,
            delay: Some(100),
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
        let delta = inflight.delay_until.unwrap() - the_past;
        assert!(
            delta.num_seconds() >= 99,
            "Should have ~100 seconds of delay from received_at"
        );
        assert_eq!(inflight.status, InflightActivationStatus::Pending)
    }

    #[test]
    fn test_delay_future() {
        let config = Arc::new(Config::default());
        let deserializer = new(DeserializeActivationConfig::from_config(&config));
        let now = Utc::now();

        #[allow(deprecated)]
        let activation = TaskActivation {
            id: "id_0".into(),
            application: Some("sentry".to_string()),
            namespace: generate_unique_namespace(),
            taskname: "taskname".into(),
            parameters: "{}".into(),
            headers: HashMap::new(),
            // used because the activation has delay
            received_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: 0,
            }),
            retry_state: None,
            processing_deadline_duration: 10,
            expires: None,
            delay: Some(100),
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
        let delta = inflight.delay_until.unwrap() - now;
        assert!(
            delta.num_seconds() >= 99,
            "Should have ~100 seconds of delay from received_at"
        );
        assert_eq!(inflight.status, InflightActivationStatus::Delay)
    }

    #[test]
    fn test_delay_max_allowed() {
        let config = Arc::new(Config::default());
        let deserializer = new(DeserializeActivationConfig::from_config(&config));
        let now = Utc::now();
        let delay_sec = config.max_delayed_task_allowed_sec * 2;

        #[allow(deprecated)]
        let activation = TaskActivation {
            id: "id_0".into(),
            application: Some("sentry".to_string()),
            namespace: generate_unique_namespace(),
            taskname: "taskname".into(),
            parameters: "{}".into(),
            headers: HashMap::new(),
            // used because the activation has delay
            received_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: 0,
            }),
            retry_state: None,
            processing_deadline_duration: 10,
            expires: None,
            delay: Some(delay_sec),
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
        let delta = inflight.delay_until.unwrap() - now;
        assert!(
            delta.num_seconds() <= (config.max_delayed_task_allowed_sec as f64 * 1.1) as i64,
            "Should have approxmiately max_delayed_task_allowed_sec of delay from received_at"
        );
        assert_eq!(inflight.status, InflightActivationStatus::Delay)
    }
}
