use chrono::{DateTime, Utc};
use rdkafka::Message as _;
use rdkafka::message::Headers as _;
use rdkafka::message::OwnedMessage;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::types::BrokerMessage;

#[derive(Clone, Debug)]
pub enum MessageBackend {
    Rdkafka(OwnedMessage),
    Arroyo(BrokerMessage<KafkaPayload>),
}

impl MessageBackend {
    pub fn topic(&self) -> &str {
        match self {
            Self::Rdkafka(message) => message.topic(),
            Self::Arroyo(message) => message.partition.topic.as_str(),
        }
    }

    pub fn partition(&self) -> i32 {
        match self {
            Self::Rdkafka(message) => message.partition(),
            Self::Arroyo(message) => i32::from(message.partition.index),
        }
    }

    pub fn offset(&self) -> i64 {
        match self {
            Self::Rdkafka(message) => message.offset(),
            Self::Arroyo(message) => message.offset as i64,
        }
    }

    pub fn payload(&self) -> Option<&[u8]> {
        match self {
            Self::Rdkafka(message) => message.payload(),
            Self::Arroyo(message) => message.payload.payload().map(Vec::as_slice),
        }
    }

    pub fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Rdkafka(message) => message.key(),
            Self::Arroyo(message) => message.payload.key().map(Vec::as_slice),
        }
    }

    pub fn headers(&self) -> impl Iterator<Item = (&str, Option<&[u8]>)> {
        let rdkafka_headers = match self {
            Self::Rdkafka(message) => message.headers(),
            Self::Arroyo(_) => None,
        };
        let arroyo_headers = match self {
            Self::Rdkafka(_) => None,
            Self::Arroyo(message) => message.payload.headers(),
        };
        rdkafka_headers
            .into_iter()
            .flat_map(|headers| headers.iter())
            .map(|header| (header.key, header.value))
            .chain(
                arroyo_headers
                    .into_iter()
                    .flat_map(|headers| headers.iter())
                    .map(|header| (header.key, header.value)),
            )
    }

    /// Missing and invalid timestamps are left for the caller to default to now.
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Rdkafka(message) => message
                .timestamp()
                .to_millis()
                .and_then(DateTime::from_timestamp_millis),
            Self::Arroyo(message) => {
                // Arroyo uses MIN_UTC for invalid timestamps and UNIX_EPOCH for missing ones.
                // Return None for these values so the caller falls back to now.
                let timestamp = message.timestamp;
                (timestamp != DateTime::<Utc>::MIN_UTC && timestamp != DateTime::<Utc>::UNIX_EPOCH)
                    .then_some(timestamp)
            }
        }
    }
}

impl From<OwnedMessage> for MessageBackend {
    fn from(message: OwnedMessage) -> Self {
        Self::Rdkafka(message)
    }
}

impl From<BrokerMessage<KafkaPayload>> for MessageBackend {
    fn from(message: BrokerMessage<KafkaPayload>) -> Self {
        Self::Arroyo(message)
    }
}
