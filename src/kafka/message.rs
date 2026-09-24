use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::types::{BrokerMessage, Partition as ArroyoPartition};

#[derive(Debug, Clone)]
pub struct ConsumerHeader<'a> {
    pub key: &'a str,
    pub value: Option<&'a [u8]>,
}

#[derive(Debug, Clone)]
pub struct ConsumerMessage {
    inner: BrokerMessage<KafkaPayload>,
    headers: HashMap<String, Vec<u8>>,
}

impl ConsumerMessage {
    pub fn new(inner: BrokerMessage<KafkaPayload>) -> Self {
        let mut headers = HashMap::new();
        if let Some(kafka_headers) = inner.payload.headers() {
            // Arroyo's public Header API supports keyed lookup but not full
            // iteration, so preserve Taskbroker headers we explicitly know
            // about through that API.
            if let Some(value) = kafka_headers.get("compression-type") {
                headers.insert("compression-type".to_owned(), value.to_vec());
            }
        }

        Self { inner, headers }
    }

    pub fn partition_key(&self) -> ArroyoPartition {
        self.inner.partition
    }

    pub fn payload(&self) -> Option<&[u8]> {
        self.inner.payload.payload().map(Vec::as_slice)
    }

    pub fn topic(&self) -> &str {
        self.inner.partition.topic.as_str()
    }

    pub fn partition(&self) -> i32 {
        self.inner.partition.index.into()
    }

    pub fn offset(&self) -> i64 {
        self.inner.offset as i64
    }

    pub fn timestamp_millis(&self) -> Option<i64> {
        Some(self.inner.timestamp.timestamp_millis())
    }

    pub fn headers(&self) -> Vec<ConsumerHeader<'_>> {
        self.headers
            .iter()
            .map(|(key, value)| ConsumerHeader {
                key,
                value: Some(value.as_slice()),
            })
            .collect()
    }
}

pub fn timestamp_from_millis(timestamp_millis: Option<i64>) -> DateTime<Utc> {
    timestamp_millis
        .and_then(DateTime::from_timestamp_millis)
        .unwrap_or_else(Utc::now)
}
