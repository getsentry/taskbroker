use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct SharedKafkaConfig {
    /// Comma separated list of Kafka brokers to connect to.
    pub brokers: String,

    /// The Kafka consumer group name.
    pub consumer_group: String,

    /// The Kafka topic to fetch task messages from.
    pub topic: String,

    /// The Kafka topic to produce demoted "long" namespace tasks to.
    pub long_topic: String,

    /// The security method used for authentication, like `sasl_plaintext`.
    pub security_protocol: Option<String>,

    /// The hashing algorithm used for authenticatiom, like `scram-sha-256`.
    pub sasl_mechanism: Option<String>,

    /// The SASL username for ingesting messages.
    pub sasl_username: Option<String>,

    /// The SASL password for ingesting messages.
    pub sasl_password: Option<String>,

    /// The location to the CA certificate file.
    pub ssl_ca_location: Option<String>,

    /// The location to the certificate file.
    pub ssl_certificate_location: Option<String>,

    /// The location to the private key file.
    pub ssl_key_location: Option<String>,
}

impl Default for SharedKafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "127.0.0.1:9092".to_owned(),
            consumer_group: "taskworker".to_owned(),
            topic: "taskworker".to_owned(),
            long_topic: "taskworker-long".to_owned(),
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
        }
    }
}

impl SharedKafkaConfig {
    fn deadletter() -> Self {
        Self {
            brokers: "".to_owned(),
            consumer_group: "".to_owned(),
            topic: "taskworker-dlq".to_owned(),
            long_topic: "".to_owned(),
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
        }
    }
}

/// Configuration for Kafka used by every kind of taskbroker, regardless of delivery mode or message format.
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct KafkaConfig {
    /// Configuration for "default" work.
    pub default: SharedKafkaConfig,

    /// Configuration for "deadletter" work.
    pub deadletter: SharedKafkaConfig,

    /// Whether to create missing Kafka topics if they don't exist.
    pub create_missing_topics: bool,

    /// The default number of partitions for a Kafka topic.
    pub default_topic_partitions: i32,

    /// The Kafka session timeout in milliseconds.
    pub session_timeout_ms: usize,

    /// How often, in milliseconds, the consumer should commit.
    pub auto_commit_interval_ms: usize,

    /// Where to start consuming when there is no committed offset (`earliest` or `latest`).
    pub auto_offset_reset: String,

    /// How many milliseconds to wait before timing out when publishing to Kafka.
    pub send_timeout_ms: u64,

    /// The maximum number of bytes allowed for a message on the Kafka producer.
    /// If a message is bigger than this then the produce will fail.
    pub max_message_size: u64,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            default: SharedKafkaConfig::default(),
            deadletter: SharedKafkaConfig::deadletter(),
            create_missing_topics: false,
            default_topic_partitions: 1,
            session_timeout_ms: 6000,
            auto_commit_interval_ms: 5000,
            auto_offset_reset: "latest".to_owned(),
            send_timeout_ms: 500,
            max_message_size: 5000000,
        }
    }
}

impl KafkaConfig {
    /// Derive a Kafka `ClientConfig` instance from this `Config` instance.
    pub fn kafka_consumer_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();

        let config = config
            .set("bootstrap.servers", self.default.brokers.clone())
            .set("group.id", self.default.consumer_group.clone())
            .set("session.timeout.ms", self.session_timeout_ms.to_string())
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "true")
            .set(
                "auto.commit.interval.ms",
                self.auto_commit_interval_ms.to_string(),
            )
            .set("auto.offset.reset", self.auto_offset_reset.to_string())
            .set("enable.auto.offset.store", "false");

        if let Some(sasl_mechanism) = &self.default.sasl_mechanism {
            config.set("sasl.mechanism", sasl_mechanism);
        }

        if let Some(sasl_username) = &self.default.sasl_username {
            config.set("sasl.username", sasl_username);
        }

        if let Some(sasl_password) = &self.default.sasl_password {
            config.set("sasl.password", sasl_password);
        }

        if let Some(security_protocol) = &self.default.security_protocol {
            config.set("security.protocol", security_protocol);
        }

        if let Some(ssl_ca_location) = &self.default.ssl_ca_location {
            config.set("ssl.ca.location", ssl_ca_location);
        }

        if let Some(ssl_certificate_location) = &self.default.ssl_certificate_location {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }

        if let Some(ssl_private_key_location) = &self.default.ssl_key_location {
            config.set("ssl.key.location", ssl_private_key_location);
        }

        config.clone()
    }

    /// Convert the application Config into rdkafka::ClientConfig
    pub fn kafka_producer_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();

        let brokers = if self.deadletter.brokers.is_empty() {
            &self.default.brokers
        } else {
            &self.deadletter.brokers
        };

        let config = config
            .set("bootstrap.servers", brokers)
            .set("message.max.bytes", format!("{}", self.max_message_size));

        if let Some(sasl_mechanism) = &self.deadletter.sasl_mechanism {
            config.set("sasl.mechanism", sasl_mechanism);
        }

        if let Some(sasl_username) = &self.deadletter.sasl_username {
            config.set("sasl.username", sasl_username);
        }

        if let Some(sasl_password) = &self.deadletter.sasl_password {
            config.set("sasl.password", sasl_password);
        }

        if let Some(security_protocol) = &self.deadletter.security_protocol {
            config.set("security.protocol", security_protocol);
        }

        if let Some(ssl_ca_location) = &self.deadletter.ssl_ca_location {
            config.set("ssl.ca.location", ssl_ca_location);
        }

        if let Some(ssl_certificate_location) = &self.deadletter.ssl_certificate_location {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }

        if let Some(ssl_private_key_location) = &self.deadletter.ssl_key_location {
            config.set("ssl.key.location", ssl_private_key_location);
        }

        config.clone()
    }
}

#[cfg(test)]
mod tests {
    use figment::Jail;

    use crate::Args;
    use crate::config::Config;

    #[test]
    fn test_kafka_consumer_config() {
        let args = Args { config: None };
        let config = Config::from_args(&args).unwrap();
        let consumer_config = config.kafka.kafka_consumer_config();

        assert_eq!(
            consumer_config.get("bootstrap.servers").unwrap(),
            "127.0.0.1:9092"
        );
        assert_eq!(consumer_config.get("group.id").unwrap(), "taskworker");
        assert!(consumer_config.get("session.timeout.ms").is_some());
    }

    #[test]
    fn test_kafka_consumer_config_auth() {
        Jail::expect_with(|jail| {
            jail.set_env(
                "TASKBROKER_KAFKA__DEFAULT__SECURITY_PROTOCOL",
                "sasl_plaintext",
            );
            jail.set_env("TASKBROKER_KAFKA__DEFAULT__SASL_MECHANISM", "SCRAM-SHA-256");
            jail.set_env("TASKBROKER_KAFKA__DEFAULT__SASL_USERNAME", "taskbroker");
            jail.set_env("TASKBROKER_KAFKA__DEFAULT__SASL_PASSWORD", "secret-tech");

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            let consumer_config = config.kafka.kafka_consumer_config();

            assert_eq!(
                consumer_config.get("security.protocol").unwrap(),
                "sasl_plaintext",
            );
            assert_eq!(
                consumer_config.get("sasl.mechanism").unwrap(),
                "SCRAM-SHA-256"
            );
            assert_eq!(consumer_config.get("sasl.username").unwrap(), "taskbroker");
            assert_eq!(consumer_config.get("sasl.password").unwrap(), "secret-tech");

            Ok(())
        });
    }

    #[test]
    fn test_kafka_consumer_config_ssl() {
        Jail::expect_with(|jail| {
            jail.set_env(
                "TASKBROKER_KAFKA__DEFAULT__SSL_CA_LOCATION",
                "/etc/ssl/ca-certificate.pem",
            );
            jail.set_env(
                "TASKBROKER_KAFKA__DEFAULT__SSL_CERTIFICATE_LOCATION",
                "/etc/ssl/taskbroker/public.crt",
            );
            jail.set_env(
                "TASKBROKER_KAFKA__DEFAULT__SSL_KEY_LOCATION",
                "/etc/ssl/taskbroker/private.key",
            );

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            let consumer_config = config.kafka.kafka_consumer_config();

            assert_eq!(
                consumer_config.get("ssl.ca.location").unwrap(),
                "/etc/ssl/ca-certificate.pem",
            );
            assert_eq!(
                consumer_config.get("ssl.certificate.location").unwrap(),
                "/etc/ssl/taskbroker/public.crt"
            );
            assert_eq!(
                consumer_config.get("ssl.key.location").unwrap(),
                "/etc/ssl/taskbroker/private.key"
            );

            Ok(())
        });
    }

    #[test]
    fn test_kafka_producer_config() {
        let args = Args { config: None };
        let config = Config::from_args(&args).unwrap();
        let producer_config = config.kafka.kafka_producer_config();

        assert_eq!(
            producer_config.get("bootstrap.servers").unwrap(),
            "127.0.0.1:9092"
        );
        assert!(producer_config.get("group.id").is_none());
        assert!(producer_config.get("session.timeout.ms").is_none());
    }

    #[test]
    fn test_kafka_producer_config_auth() {
        Jail::expect_with(|jail| {
            jail.set_env(
                "TASKBROKER_KAFKA__DEADLETTER__SECURITY_PROTOCOL",
                "sasl_plaintext",
            );
            jail.set_env(
                "TASKBROKER_KAFKA__DEADLETTER__SASL_MECHANISM",
                "SCRAM-SHA-256",
            );
            jail.set_env("TASKBROKER_KAFKA__DEADLETTER__SASL_USERNAME", "taskbroker");
            jail.set_env("TASKBROKER_KAFKA__DEADLETTER__SASL_PASSWORD", "secret-tech");

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            let producer_config = config.kafka.kafka_producer_config();

            assert_eq!(
                producer_config.get("security.protocol").unwrap(),
                "sasl_plaintext"
            );
            assert_eq!(
                producer_config.get("sasl.mechanism").unwrap(),
                "SCRAM-SHA-256"
            );
            assert_eq!(producer_config.get("sasl.username").unwrap(), "taskbroker");
            assert_eq!(producer_config.get("sasl.password").unwrap(), "secret-tech");

            Ok(())
        });
    }

    #[test]
    fn test_kafka_producer_config_ssl() {
        Jail::expect_with(|jail| {
            jail.set_env(
                "TASKBROKER_KAFKA__DEADLETTER__SSL_CA_LOCATION",
                "/etc/ssl/ca-certificate.pem",
            );
            jail.set_env(
                "TASKBROKER_KAFKA__DEADLETTER__SSL_CERTIFICATE_LOCATION",
                "/etc/ssl/taskbroker/public.crt",
            );
            jail.set_env(
                "TASKBROKER_KAFKA__DEADLETTER__SSL_KEY_LOCATION",
                "/etc/ssl/taskbroker/private.key",
            );

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            let producer_config = config.kafka.kafka_producer_config();

            assert_eq!(
                producer_config.get("ssl.ca.location").unwrap(),
                "/etc/ssl/ca-certificate.pem",
            );
            assert_eq!(
                producer_config.get("ssl.certificate.location").unwrap(),
                "/etc/ssl/taskbroker/public.crt"
            );
            assert_eq!(
                producer_config.get("ssl.key.location").unwrap(),
                "/etc/ssl/taskbroker/private.key"
            );

            Ok(())
        });
    }
}
