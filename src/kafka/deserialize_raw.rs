use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Error;
use chrono::{DateTime, Utc};
use prost::Message as _;
use rdkafka::Message;
use rdkafka::message::Headers;
use rdkafka::message::OwnedMessage;
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivation};
use uuid::Uuid;

use crate::config::Config;
use crate::store::activation::{InflightActivation, InflightActivationStatus};

use super::deserialize_activation::bucket_from_id;

#[derive(serde::Serialize)]
struct RawParams<'a> {
    args: (&'a serde_bytes::Bytes,),
    kwargs: HashMap<(), ()>,
}

pub struct RawConfig {
    pub namespace: String,
    pub application: String,
    pub taskname: String,
    pub processing_deadline_duration: u16,
}

impl RawConfig {
    pub fn from_config(config: &Config) -> Option<Self> {
        if !config.raw_mode {
            return None;
        }
        let application = config
            .raw_application
            .clone()
            .expect("raw_application required when raw_mode is enabled");

        assert!(
            config.worker_map.contains_key(&application),
            "raw_application '{}' must exist in worker_map",
            application
        );

        Some(Self {
            namespace: config
                .raw_namespace
                .clone()
                .expect("raw_namespace required when raw_mode is enabled"),
            application,
            taskname: config
                .raw_taskname
                .clone()
                .expect("raw_taskname required when raw_mode is enabled"),
            processing_deadline_duration: config.raw_processing_deadline_duration,
        })
    }
}

fn extract_headers(msg: &OwnedMessage) -> HashMap<String, String> {
    let Some(headers) = msg.headers() else {
        return HashMap::new();
    };

    let mut result = HashMap::new();
    for i in 0..headers.count() {
        let header = headers.get(i);
        if let Some(value) = header.value
            && let Ok(value_str) = std::str::from_utf8(value)
        {
            result.insert(header.key.to_string(), value_str.to_string());
        }
    }
    result
}

/// Encode raw bytes into msgpack format: {"args": [raw_bytes], "kwargs": {}}
fn encode_raw_params(raw_bytes: &[u8]) -> Vec<u8> {
    let params = RawParams {
        args: (serde_bytes::Bytes::new(raw_bytes),),
        kwargs: HashMap::new(),
    };

    // The only condition where this would fail should be a bug in msgpack or taskbroker. In this
    // case it's better to stop the world instead of trying to recover externally.
    rmp_serde::to_vec_named(&params).expect("Failed to encode msgpack")
}

/// Create a deserializer closure for raw mode.
/// Wraps raw Kafka message bytes into a TaskActivation with msgpack-encoded parameters_bytes.
pub fn new(config: RawConfig) -> impl Fn(Arc<OwnedMessage>) -> Result<InflightActivation, Error> {
    move |msg: Arc<OwnedMessage>| {
        // Whether a message without payload is valid is technically not up to taskbroker, and we
        // can't DLQ messages here. It's easier to convert it to an empty bytestring and let the
        // task fail. Failed tasks can be DLQed in upkeep.rs
        let payload = msg.payload().unwrap_or(b"");

        let id = Uuid::new_v4().to_string();
        let parameters_bytes = encode_raw_params(payload);
        let now = Utc::now();
        let received_at_time = msg
            .timestamp()
            .to_millis()
            .and_then(DateTime::from_timestamp_millis)
            .unwrap_or(now);
        let received_at = prost_types::Timestamp {
            seconds: received_at_time.timestamp(),
            nanos: received_at_time.timestamp_subsec_nanos() as i32,
        };

        let activation = TaskActivation {
            id: id.clone(),
            application: Some(config.application.clone()),
            namespace: config.namespace.clone(),
            taskname: config.taskname.clone(),
            #[allow(deprecated)]
            parameters: String::new(),
            parameters_bytes,
            headers: extract_headers(&msg),
            received_at: Some(received_at),
            retry_state: None,
            processing_deadline_duration: config.processing_deadline_duration.into(),
            expires: None,
            delay: None,
        };

        let activation_bytes = activation.encode_to_vec();
        let bucket = bucket_from_id(&id);

        metrics::histogram!(
            "consumer.raw.payload_size_bytes",
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
            received_at: received_at_time,
            processing_deadline: None,
            claim_expires_at: None,
            processing_deadline_duration: config.processing_deadline_duration.into(),
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
    use rdkafka::message::{Header, OwnedHeaders, OwnedMessage};

    use super::*;

    #[test]
    fn test_encode_raw_params() {
        use serde::Deserialize;

        #[derive(Deserialize, Debug)]
        struct Params {
            args: (Vec<u8>,),
            kwargs: HashMap<(), ()>,
        }

        let raw_bytes = b"hello world";
        let encoded = encode_raw_params(raw_bytes);

        // Verify msgpack uses binary format (0xc4 = bin8), not array format (0x9x). Array format
        // is easy to accidentally serialize to with Vec<u8>.
        // Python's msgpack decodes binary to `bytes` but array to `list[int]`.
        // The encoded format should contain 0xc4 (bin8) followed by length 0x0b (11).
        assert!(
            encoded.windows(2).any(|w| w == [0xc4, 0x0b]),
            "Expected binary format (c4 0b), got: {:02x?}",
            encoded
        );

        // Decode and verify
        let decoded: Params = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.args.0, raw_bytes);
        assert!(decoded.kwargs.is_empty());
    }

    #[test]
    fn test_raw_deserializer() {
        let config = RawConfig {
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
    fn test_raw_deserializer_null_payload() {
        let config = RawConfig {
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
        assert!(result.is_ok());

        let inflight = result.unwrap();
        let activation = TaskActivation::decode(inflight.activation.as_slice()).unwrap();

        // Verify empty payload is encoded as empty bytes
        use serde::Deserialize;
        #[derive(Deserialize)]
        struct Params {
            args: (Vec<u8>,),
        }
        let params: Params = rmp_serde::from_slice(&activation.parameters_bytes).unwrap();
        assert!(params.args.0.is_empty());
    }

    #[test]
    fn test_raw_deserializer_with_headers() {
        let config = RawConfig {
            namespace: "test-namespace".to_string(),
            application: "test-app".to_string(),
            taskname: "test-task".to_string(),
            processing_deadline_duration: 60,
        };

        let deserializer = new(config);

        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "trace-id",
                value: Some("abc123"),
            })
            .insert(Header {
                key: "request-id",
                value: Some("req-456"),
            });

        let raw_payload = b"raw kafka message bytes";
        let message = OwnedMessage::new(
            Some(raw_payload.to_vec()),
            None,
            "legacy-topic".into(),
            Timestamp::now(),
            0,
            42,
            Some(headers),
        );

        let result = deserializer(Arc::new(message));
        assert!(result.is_ok());

        let inflight = result.unwrap();
        let activation = TaskActivation::decode(inflight.activation.as_slice()).unwrap();

        assert_eq!(
            activation.headers.get("trace-id"),
            Some(&"abc123".to_string())
        );
        assert_eq!(
            activation.headers.get("request-id"),
            Some(&"req-456".to_string())
        );
    }
}
