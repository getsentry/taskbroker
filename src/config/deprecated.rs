use anyhow::Result;
use figment::providers::{Env, Format, Yaml};
use figment::value::{Dict, Map};
use figment::{Figment, Metadata, Profile, Provider};
use serde::{Deserialize, Serialize};

use crate::Args;

#[derive(PartialEq, Debug, Deserialize, Serialize, Default)]
pub struct DeprecatedConfig {
    /// The topic to fetch task messages from.
    /// Deprecated: use kafka_topics instead. Mutually exclusive with the new
    /// format; defaults to None (the historical "taskworker" default applies).
    pub kafka_topic: Option<String>,

    /// Comma separated list of kafka brokers to connect to.
    /// Deprecated: use kafka_clusters instead. Mutually exclusive with the new
    /// format; defaults to None (the historical "127.0.0.1:9092" default is
    /// applied during normalization when no kafka config is provided at all).
    pub kafka_cluster: Option<String>,

    /// The kafka consumer group name.
    /// Deprecated: use kafka_topics instead. Mutually exclusive with the new
    /// format; defaults to None (the historical "taskworker" default applies).
    pub kafka_consumer_group: Option<String>,

    /// The security method used for authentication eg. sasl_plaintext.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_security_protocol: Option<String>,

    /// The hashing algorithm used for authentication eg. scram-sha-256.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_sasl_mechanism: Option<String>,

    /// The sasl username for ingesting messages.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_sasl_username: Option<String>,

    /// The sasl password for ingesting messages.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_sasl_password: Option<String>,

    /// The location to the CA certificate file.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_ssl_ca_location: Option<String>,

    /// The location to the certificate file.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_ssl_certificate_location: Option<String>,

    /// The location to the private key file.
    /// Deprecated: use kafka_clusters instead.
    pub kafka_ssl_key_location: Option<String>,

    /// Comma separated list of kafka brokers to publish dead letter messages on.
    /// Deprecated: declare the deadletter topic in kafka_topics (produce_only)
    /// with a cluster reference instead.
    pub kafka_deadletter_cluster: Option<String>,

    /// The security method used for authentication to the DLQ eg. sasl_plaintext.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_security_protocol: Option<String>,

    /// The hashing algorithm used for authentication to the DLQ eg. scram-sha-256.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_sasl_mechanism: Option<String>,

    /// The sasl username for DLQ publishing.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_sasl_username: Option<String>,

    /// The sasl password for DLQ publishing.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_sasl_password: Option<String>,

    /// The location to the DLQ CA certificate file.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_ssl_ca_location: Option<String>,

    /// The location to the DLQ certificate file.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_ssl_certificate_location: Option<String>,

    /// The location to the DLQ private key file.
    /// Deprecated: configure auth on the referenced cluster in kafka_clusters.
    pub kafka_deadletter_ssl_key_location: Option<String>,

    /// Enable raw mode for consuming unstructured Kafka messages.
    /// In raw mode, Kafka message bytes are wrapped into TaskActivation.
    pub raw_mode: Option<bool>,
}

impl DeprecatedConfig {
    pub fn from_args(args: &Args) -> Result<Self> {
        let mut builder = Figment::from(DeprecatedConfig::default());

        if let Some(path) = &args.config {
            builder = builder.merge(Yaml::file(path));
        }

        builder = builder.merge(Env::prefixed("TASKBROKER_"));
        let config: DeprecatedConfig = builder.extract()?;

        Ok(config)
    }
}

impl Provider for DeprecatedConfig {
    fn metadata(&self) -> Metadata {
        Metadata::named("Taskbroker config")
    }

    fn data(&self) -> Result<Map<Profile, Dict>, figment::Error> {
        figment::providers::Serialized::defaults(DeprecatedConfig::default()).data()
    }
}
