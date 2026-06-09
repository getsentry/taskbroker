use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};

use crate::config::raw::RawModeConfig;

/// Configuration for a single Kafka topic in multi-topic mode.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct TopicConfig {
    /// Which cluster this topic is on (key into kafka_clusters)
    pub cluster: String,
    /// Consumer group for this topic
    pub consumer_group: String,
    /// If true, this topic is produce-only (e.g. retry topics).
    /// Defaults to false, meaning the topic is consumed.
    #[serde(default)]
    pub produce_only: bool,
    /// Raw mode settings. If set, this topic uses raw mode.
    #[serde(default)]
    pub raw: Option<RawModeConfig>,
    /// The kafka session timeout in ms for this topic's consumer.
    /// Falls back to the global `kafka_session_timeout_ms` when unset.
    #[serde(default)]
    pub session_timeout_ms: Option<usize>,
    /// The interval in ms at which this topic's consumer auto-commits.
    /// Falls back to the global `kafka_auto_commit_interval_ms` when unset.
    #[serde(default)]
    pub auto_commit_interval_ms: Option<usize>,
    /// The auto offset reset policy for this topic's consumer.
    /// Falls back to the global `kafka_auto_offset_reset` when unset.
    #[serde(default)]
    pub auto_offset_reset: Option<String>,
}

/// Configuration for a Kafka cluster.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ClusterConfig {
    /// Comma-separated list of broker addresses
    pub address: String,
    /// The security method used for authentication eg. sasl_plaintext
    pub security_protocol: Option<String>,
    /// The hashing algorithm used for authentication eg. scram-sha-256
    pub sasl_mechanism: Option<String>,
    /// The sasl username for authentication
    pub sasl_username: Option<String>,
    /// The sasl password for authentication
    pub sasl_password: Option<String>,
    /// The location to the CA certificate file
    pub ssl_ca_location: Option<String>,
    /// The location to the certificate file
    pub ssl_certificate_location: Option<String>,
    /// The location to the private key file
    pub ssl_key_location: Option<String>,
}

impl ClusterConfig {
    /// Whether any authentication / TLS settings are configured for this
    /// cluster. Used to detect when a producer would carry credentials that
    /// only apply to this specific cluster.
    pub fn has_auth(&self) -> bool {
        self.security_protocol.is_some()
            || self.sasl_mechanism.is_some()
            || self.sasl_username.is_some()
            || self.sasl_password.is_some()
            || self.ssl_ca_location.is_some()
            || self.ssl_certificate_location.is_some()
            || self.ssl_key_location.is_some()
    }

    /// Apply this cluster's `bootstrap.servers` and any configured sasl/ssl
    /// auth onto an rdkafka `ClientConfig`. Shared by the consumer, producer and
    /// admin config builders so they all authenticate identically.
    pub(super) fn apply_to(&self, config: &mut ClientConfig) {
        config.set("bootstrap.servers", self.address.clone());

        if let Some(ref sasl_mechanism) = self.sasl_mechanism {
            config.set("sasl.mechanism", sasl_mechanism);
        }
        if let Some(ref sasl_username) = self.sasl_username {
            config.set("sasl.username", sasl_username);
        }
        if let Some(ref sasl_password) = self.sasl_password {
            config.set("sasl.password", sasl_password);
        }
        if let Some(ref security_protocol) = self.security_protocol {
            config.set("security.protocol", security_protocol);
        }
        if let Some(ref ssl_ca_location) = self.ssl_ca_location {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if let Some(ref ssl_certificate_location) = self.ssl_certificate_location {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }
        if let Some(ref ssl_private_key_location) = self.ssl_key_location {
            config.set("ssl.key.location", ssl_private_key_location);
        }
    }
}
