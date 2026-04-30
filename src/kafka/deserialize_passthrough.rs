use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Error, anyhow};
use chrono::Utc;
use prost::Message as _;
use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivation};
use uuid::Uuid;

use crate::config::Config;
use crate::store::activation::{InflightActivation, InflightActivationStatus};

use super::deserialize_activation::bucket_from_id;

pub struct PassthroughConfig {
    pub namespace: String,
    pub application: String,
    pub taskname: String,
    pub processing_deadline_duration: u64,
}

impl PassthroughConfig {
    pub fn from_config(config: &Config) -> Option<Self> {
        if !config.passthrough_mode {
            return None;
        }
        Some(Self {
            namespace: config
                .passthrough_namespace
                .clone()
                .expect("passthrough_namespace required when passthrough_mode is enabled"),
            application: config
                .passthrough_application
                .clone()
                .expect("passthrough_application required when passthrough_mode is enabled"),
            taskname: config
                .passthrough_taskname
                .clone()
                .expect("passthrough_taskname required when passthrough_mode is enabled"),
            processing_deadline_duration: config.passthrough_processing_deadline_duration,
        })
    }
}

/// Encode raw bytes into msgpack format: {"args": [raw_bytes], "kwargs": {}}
fn encode_passthrough_params(raw_bytes: &[u8]) -> Result<Vec<u8>, Error> {
    use serde::Serialize;

    #[derive(Serialize)]
    struct Params<'a> {
        args: (&'a [u8],),
        kwargs: HashMap<(), ()>,
    }

    let params = Params {
        args: (raw_bytes,),
        kwargs: HashMap::new(),
    };

    rmp_serde::to_vec_named(&params).map_err(|e| anyhow!("Failed to encode msgpack: {}", e))
}

/// Create a deserializer closure for passthrough mode.
/// Wraps raw Kafka message bytes into a TaskActivation with msgpack-encoded parameters_bytes.
pub fn new(
    config: PassthroughConfig,
) -> impl Fn(Arc<OwnedMessage>) -> Result<InflightActivation, Error> {
    move |msg: Arc<OwnedMessage>| {
        let Some(payload) = msg.payload() else {
            return Err(anyhow!("Message has no payload"));
        };

        let id = Uuid::new_v4().to_string();
        let parameters_bytes = encode_passthrough_params(payload)?;
        let now = Utc::now();
        let received_at = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: 0,
        };

        let activation = TaskActivation {
            id: id.clone(),
            application: Some(config.application.clone()),
            namespace: config.namespace.clone(),
            taskname: config.taskname.clone(),
            #[allow(deprecated)]
            parameters: String::new(),
            parameters_bytes,
            headers: HashMap::new(),
            received_at: Some(received_at),
            retry_state: None,
            processing_deadline_duration: config.processing_deadline_duration,
            expires: None,
            delay: None,
        };

        let activation_bytes = activation.encode_to_vec();
        let bucket = bucket_from_id(&id);

        metrics::histogram!(
            "consumer.passthrough.payload_size_bytes",
            "namespace" => config.namespace.clone(),
            "taskname" => config.taskname.clone()
        )
        .record(payload.len() as f64);

        Ok(InflightActivation {
            id,
            activation: activation_bytes,
            status: InflightActivationStatus::Pending,
            partition: msg.partition(),
            offset: msg.offset(),
            added_at: now,
            received_at: now,
            processing_deadline: None,
            claim_expires_at: None,
            processing_deadline_duration: config.processing_deadline_duration as i32,
            processing_attempts: 0,
            expires_at: None,
            delay_until: None,
            at_most_once: false,
            application: config.application.clone(),
            namespace: config.namespace.clone(),
            taskname: config.taskname.clone(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
            bucket,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rdkafka::Timestamp;
    use rdkafka::message::OwnedMessage;

    use super::*;

    #[test]
    fn test_encode_passthrough_params() {
        use serde::Deserialize;

        #[derive(Deserialize, Debug)]
        struct Params {
            args: (Vec<u8>,),
            kwargs: HashMap<(), ()>,
        }

        let raw_bytes = b"hello world";
        let encoded = encode_passthrough_params(raw_bytes).unwrap();

        // Decode and verify
        let decoded: Params = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.args.0, raw_bytes);
        assert!(decoded.kwargs.is_empty());
    }

    #[test]
    fn test_passthrough_deserializer() {
        let config = PassthroughConfig {
            namespace: "test-namespace".to_string(),
            application: "test-app".to_string(),
            taskname: "test-task".to_string(),
            processing_deadline_duration: 60,
        };

        let deserializer = new(config);

        let raw_payload = b"raw kafka message bytes";
        let message = OwnedMessage::new(
            Some(raw_payload.to_vec()),
            None,
            "legacy-topic".into(),
            Timestamp::now(),
            0,
            42,
            None,
        );

        let result = deserializer(Arc::new(message));
        assert!(result.is_ok());

        let inflight = result.unwrap();
        assert_eq!(inflight.namespace, "test-namespace");
        assert_eq!(inflight.application, "test-app");
        assert_eq!(inflight.taskname, "test-task");
        assert_eq!(inflight.processing_deadline_duration, 60);
        assert_eq!(inflight.offset, 42);
        assert_eq!(inflight.status, InflightActivationStatus::Pending);

        // Verify the activation can be decoded
        let activation = TaskActivation::decode(inflight.activation.as_slice()).unwrap();
        assert_eq!(activation.namespace, "test-namespace");
        assert_eq!(activation.application, Some("test-app".to_string()));
        assert_eq!(activation.taskname, "test-task");
        assert!(!activation.parameters_bytes.is_empty());
    }

    #[test]
    fn test_passthrough_deserializer_empty_payload() {
        let config = PassthroughConfig {
            namespace: "test-namespace".to_string(),
            application: "test-app".to_string(),
            taskname: "test-task".to_string(),
            processing_deadline_duration: 60,
        };

        let deserializer = new(config);

        let message = OwnedMessage::new(
            None, // No payload
            None,
            "legacy-topic".into(),
            Timestamp::now(),
            0,
            0,
            None,
        );

        let result = deserializer(Arc::new(message));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no payload"));
    }
}
