use figment::{
    Figment, Metadata, Profile, Provider,
    providers::{Env, Format, Yaml},
};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeMap};

use crate::{Args, logging::LogFormat};

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct Config {
    /// The sentry DSN to use for error reporting.
    pub sentry_dsn: Option<String>,

    /// The environment to report to sentry errors to.
    pub sentry_env: Option<Cow<'static, str>>,

    /// The sampling rate for tracing data.
    pub traces_sample_rate: Option<f32>,

    /// The log filter to apply application logging to.
    /// See https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives
    pub log_filter: String,

    /// The log format to use
    pub log_format: LogFormat,

    /// The statsd address to report metrics to.
    pub statsd_addr: String,

    /// Default tags to add to all metrics.
    pub default_metrics_tags: BTreeMap<String, String>,

    /// The hostname and port of the gRPC server.
    pub grpc_addr: String,

    /// The port to bind the grpc service to
    pub grpc_port: u32,

    /// A list of shared secrets that clients use to authenticate.
    /// We support a list of secrets to allow for key rotation.
    pub grpc_shared_secret: Vec<String>,

    /// Comma separated list of kafka brokers to connect to
    pub kafka_cluster: String,

    /// The kafka consumer group name
    pub kafka_consumer_group: String,

    /// The topic to fetch task messages from.
    pub kafka_topic: String,

    /// The security method used for authentication eg. sasl_plaintext
    pub kafka_security_protocol: Option<String>,

    /// The hashing algorithm used for authentication eg. scram-sha-256
    pub kafka_sasl_mechanism: Option<String>,

    /// The sasl username for ingesting messages
    pub kafka_sasl_username: Option<String>,

    /// The sasl password for ingesting messages
    pub kafka_sasl_password: Option<String>,

    /// The location to the CA certificate file
    pub kafka_ssl_ca_location: Option<String>,

    /// The location to the certificate file
    pub kafka_ssl_certificate_location: Option<String>,

    /// The location to the private key file
    pub kafka_ssl_key_location: Option<String>,

    /// Whether to create missing topics if they don't exist.
    pub create_missing_topics: bool,

    /// Comma separated list of kafka brokers to
    /// publish dead letter messages on
    pub kafka_deadletter_cluster: Option<String>,

    /// The kafka topic to publish dead letter messages on
    pub kafka_deadletter_topic: String,

    /// The security method used for authentication to the DLQ eg. sasl_plaintext
    pub kafka_deadletter_security_protocol: Option<String>,

    /// The hashing algorithm used for authentication to the DLQ eg. scram-sha-256
    pub kafka_deadletter_sasl_mechanism: Option<String>,

    /// The sasl username for DLQ publishing
    pub kafka_deadletter_sasl_username: Option<String>,

    /// The sasl password for DLQ publishing
    pub kafka_deadletter_sasl_password: Option<String>,

    /// The location to the DLQ CA certificate file
    pub kafka_deadletter_ssl_ca_location: Option<String>,

    /// The location to the DLQ certificate file
    pub kafka_deadletter_ssl_certificate_location: Option<String>,

    /// The location to the DLQ private key file
    pub kafka_deadletter_ssl_key_location: Option<String>,

    /// The default number of partitions for a topic
    pub default_topic_partitions: i32,

    /// The kafka session timeout in ms
    pub kafka_session_timeout_ms: usize,

    /// The amount of ms that the consumer will commit at.
    pub kafka_auto_commit_interval_ms: usize,

    /// The amount of ms that the consumer will commit at.
    pub kafka_auto_offset_reset: String,

    /// The number of ms for timeouts when publishing messages to kafka.
    pub kafka_send_timeout_ms: u64,

    /// The path to the sqlite database
    pub db_path: String,

    /// The amount of time to wait before retrying writes to db when write fails.
    pub db_write_failure_backoff_ms: u64,

    /// The maximum number of tasks that are buffered
    /// before being written to InflightTaskStore (sqlite).
    pub db_insert_batch_max_len: usize,

    /// The maximum number of bytes that are buffered
    /// before being written to InflightTaskStore (sqlite).
    pub db_insert_batch_max_size: usize,

    /// The time in milliseconds to buffer tasks
    /// before being written to InflightTaskStore (sqlite).
    pub db_insert_batch_max_time_ms: u64,

    /// The maximum size of the sqlite database in bytes.
    /// If the database reaches or exceeds this size, ingestion will
    /// pause until the database size is reduced.
    pub db_max_size: Option<u64>,

    /// The path to the runtime config file
    pub runtime_config_path: Option<String>,

    /// The maximum number of pending records that can be
    /// in the InflightTaskStore (sqlite)
    pub max_pending_count: usize,

    /// The maximum number of delay records that can be
    /// in the InflightTaskStore (sqlite)
    pub max_delay_count: usize,

    /// The maximum number of processing records that can be
    /// in the InflightTaskStore (sqlite)
    pub max_processing_count: usize,

    /// The maximum number of times a task can be reset from
    /// processing back to pending. When this limit is reached,
    /// the activation will be discarded/deadlettered.
    pub max_processing_attempts: usize,

    /// The number of additional seconds that processing deadlines
    /// are extended by. This helps reduce broker deadline resets when
    /// brokers are under load, or there are small networking delays.
    pub processing_deadline_grace_sec: u64,

    /// The frequency at which upkeep tasks
    /// (discarding, retrying activations, etc.) are executed.
    pub upkeep_task_interval_ms: u64,

    /// The number of milliseconds between upkeep runs that indicates unhealthy
    /// performance that should trigger a restart.
    pub upkeep_unhealthy_interval_ms: u64,

    /// Whether to skip the health check if the pods are in a bad state.
    pub health_check_killswitched: bool,

    /// The number of seconds that deadline resets
    /// are skipped after startup. This delay allows workers
    /// time to publish results after a broker restart.
    pub upkeep_deadline_reset_skip_after_startup_sec: u64,

    /// The frequency at which db maintenance tasks
    /// (reclaiming free pages) are executed
    pub maintenance_task_interval_ms: u64,

    /// The maximum number of seconds a task can be delayed until.
    /// Tasks delayed greater than this duration are capped.
    pub max_delayed_task_allowed_sec: u64,

    /// The maximum number of bytes allowed for a message on the Kafka producer.
    /// If a message is bigger than this then the produce will fail.
    pub max_message_size: u64,

    /// The number of pages to vacuum from sqlite when vacuum is run.
    /// If None, all pages will be vacuumed.
    pub vacuum_page_count: Option<usize>,

    /// Enable to have the application perform `VACUUM` on the database
    /// when it starts up, but before the GRPC server, consumer and upkeep begin.
    pub full_vacuum_on_start: bool,

    /// Enable the upkeep thread to perforam a full `VACUUM` on the database
    /// periodically.
    pub full_vacuum_on_upkeep: bool,

    /// The interval in milliseconds between full `VACUUM`s on the database by the upkeep thread.
    pub vacuum_interval_ms: u64,

    /// Enable additional metrics for the sqlite.
    pub enable_sqlite_status_metrics: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sentry_dsn: None,
            sentry_env: None,
            traces_sample_rate: Some(0.0),
            log_filter: "info,librdkafka=warn,h2=off".to_owned(),
            log_format: LogFormat::Text,
            grpc_addr: "0.0.0.0".to_owned(),
            grpc_port: 50051,
            grpc_shared_secret: vec![],
            statsd_addr: "127.0.0.1:8126".parse().unwrap(),
            default_metrics_tags: Default::default(),
            kafka_cluster: "127.0.0.1:9092".to_owned(),
            kafka_consumer_group: "taskworker".to_owned(),
            kafka_sasl_mechanism: None,
            kafka_sasl_username: None,
            kafka_sasl_password: None,
            kafka_ssl_ca_location: None,
            kafka_ssl_certificate_location: None,
            kafka_ssl_key_location: None,
            kafka_security_protocol: None,
            kafka_topic: "taskworker".to_owned(),
            create_missing_topics: false,
            kafka_deadletter_cluster: None,
            kafka_deadletter_topic: "taskworker-dlq".to_owned(),
            kafka_deadletter_sasl_mechanism: None,
            kafka_deadletter_sasl_username: None,
            kafka_deadletter_sasl_password: None,
            kafka_deadletter_security_protocol: None,
            kafka_deadletter_ssl_ca_location: None,
            kafka_deadletter_ssl_certificate_location: None,
            kafka_deadletter_ssl_key_location: None,
            default_topic_partitions: 1,
            kafka_session_timeout_ms: 6000,
            kafka_auto_commit_interval_ms: 5000,
            kafka_auto_offset_reset: "latest".to_owned(),
            kafka_send_timeout_ms: 500,
            db_path: "./taskbroker-inflight.sqlite".to_owned(),
            db_write_failure_backoff_ms: 4000,
            db_insert_batch_max_len: 256,
            db_insert_batch_max_size: 16_000_000,
            db_insert_batch_max_time_ms: 1000,
            db_max_size: Some(3000000000),
            runtime_config_path: None,
            max_pending_count: 2048,
            max_delay_count: 8192,
            max_processing_count: 2048,
            max_processing_attempts: 5,
            processing_deadline_grace_sec: 3,
            upkeep_task_interval_ms: 1000,
            upkeep_unhealthy_interval_ms: 3000,
            health_check_killswitched: false,
            upkeep_deadline_reset_skip_after_startup_sec: 60,
            maintenance_task_interval_ms: 6000,
            max_delayed_task_allowed_sec: 3600,
            max_message_size: 5000000,
            vacuum_page_count: None,
            full_vacuum_on_start: true,
            full_vacuum_on_upkeep: true,
            vacuum_interval_ms: 30000,
            enable_sqlite_status_metrics: true,
        }
    }
}

impl Config {
    /// Build a config instance from defaults, env vars, file + CLI options
    pub fn from_args(args: &Args) -> Result<Self, Box<figment::Error>> {
        let mut builder = Figment::from(Config::default());
        if let Some(path) = &args.config {
            builder = builder.merge(Yaml::file(path));
        }
        builder = builder.merge(Env::prefixed("TASKBROKER_"));
        let config = builder.extract()?;
        Ok(config)
    }

    /// Convert the application Config into rdkafka::ClientConfig
    pub fn kafka_consumer_config(&self) -> ClientConfig {
        let mut new_config = ClientConfig::new();
        let config = new_config
            .set("bootstrap.servers", self.kafka_cluster.clone())
            .set("group.id", self.kafka_consumer_group.clone())
            .set(
                "session.timeout.ms",
                self.kafka_session_timeout_ms.to_string(),
            )
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "true")
            .set(
                "auto.commit.interval.ms",
                self.kafka_auto_commit_interval_ms.to_string(),
            )
            .set(
                "auto.offset.reset",
                self.kafka_auto_offset_reset.to_string(),
            )
            .set("enable.auto.offset.store", "false");

        if let Some(sasl_mechanism) = &self.kafka_sasl_mechanism {
            config.set("sasl.mechanism", sasl_mechanism);
        }
        if let Some(sasl_username) = &self.kafka_sasl_username {
            config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = &self.kafka_sasl_password {
            config.set("sasl.password", sasl_password);
        }
        if let Some(security_protocol) = &self.kafka_security_protocol {
            config.set("security.protocol", security_protocol);
        }
        if let Some(ssl_ca_location) = &self.kafka_ssl_ca_location {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if let Some(ssl_certificate_location) = &self.kafka_ssl_certificate_location {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }
        if let Some(ssl_private_key_location) = &self.kafka_ssl_key_location {
            config.set("ssl.key.location", ssl_private_key_location);
        }

        config.clone()
    }

    /// Convert the application Config into rdkafka::ClientConfig
    pub fn kafka_producer_config(&self) -> ClientConfig {
        let mut new_config = ClientConfig::new();
        let config = new_config
            .set(
                "bootstrap.servers",
                self.kafka_deadletter_cluster
                    .as_ref()
                    .unwrap_or(&self.kafka_cluster),
            )
            .set("message.max.bytes", format!("{}", self.max_message_size));
        if let Some(sasl_mechanism) = &self.kafka_deadletter_sasl_mechanism {
            config.set("sasl.mechanism", sasl_mechanism);
        }
        if let Some(sasl_username) = &self.kafka_deadletter_sasl_username {
            config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = &self.kafka_deadletter_sasl_password {
            config.set("sasl.password", sasl_password);
        }
        if let Some(security_protocol) = &self.kafka_deadletter_security_protocol {
            config.set("security.protocol", security_protocol);
        }
        if let Some(ssl_ca_location) = &self.kafka_deadletter_ssl_ca_location {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if let Some(ssl_certificate_location) = &self.kafka_deadletter_ssl_certificate_location {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }
        if let Some(ssl_private_key_location) = &self.kafka_deadletter_ssl_key_location {
            config.set("ssl.key.location", ssl_private_key_location);
        }

        config.clone()
    }
}

impl Provider for Config {
    fn metadata(&self) -> Metadata {
        Metadata::named("Taskbroker config")
    }

    fn data(&self) -> Result<figment::value::Map<Profile, figment::value::Dict>, figment::Error> {
        figment::providers::Serialized::defaults(Config::default()).data()
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, collections::BTreeMap};

    use super::Config;
    use crate::{Args, logging::LogFormat};
    use figment::Jail;

    #[test]
    fn test_default() {
        let config = Config {
            ..Default::default()
        };
        assert_eq!(config.sentry_dsn, None);
        assert_eq!(config.sentry_env, None);
        assert_eq!(config.log_filter, "info,librdkafka=warn,h2=off");
        assert_eq!(config.log_format, LogFormat::Text);
        assert_eq!(config.grpc_port, 50051);
        assert_eq!(config.kafka_topic, "taskworker");
        assert_eq!(config.db_path, "./taskbroker-inflight.sqlite");
        assert_eq!(config.max_pending_count, 2048);
        assert_eq!(config.max_processing_count, 2048);
        assert_eq!(config.vacuum_page_count, None);
    }

    #[test]
    fn test_from_args_config_file() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
                sentry_dsn: fake_dsn
                sentry_env: prod
                log_filter: debug,rdkafka=off
                log_format: json
                statsd_addr: 127.0.0.1:8126
                default_metrics_tags:
                    key_1: value_1
                kafka_cluster: 10.0.0.1:9092,10.0.0.2:9092
                kafka_topic: error-tasks
                kafka_deadletter_topic: error-tasks-dlq
                kafka_auto_offset_reset: earliest
                db_path: ./taskbroker-error.sqlite
                db_max_size: 3000000000
                max_pending_count: 512
                max_processing_count: 512
                max_processing_attempts: 5
                vacuum_page_count: 1000
                full_vacuum_on_start: true
            "#,
            )?;
            // Env vars always override config file
            jail.set_env("TASKBROKER_LOG_FILTER", "error");

            let args = Args {
                config: Some("config.yaml".to_owned()),
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.sentry_dsn, Some("fake_dsn".to_owned()));
            assert_eq!(config.sentry_env, Some(Cow::Borrowed("prod")));
            assert_eq!(config.log_filter, "error");
            assert_eq!(config.log_format, LogFormat::Json);
            assert_eq!(
                config.kafka_cluster,
                "10.0.0.1:9092,10.0.0.2:9092".to_owned()
            );
            assert_eq!(
                config.default_metrics_tags,
                BTreeMap::from([("key_1".to_owned(), "value_1".to_owned())])
            );
            assert_eq!(config.kafka_consumer_group, "taskworker".to_owned());
            assert_eq!(config.kafka_auto_offset_reset, "earliest".to_owned());
            assert_eq!(config.kafka_session_timeout_ms, 6000.to_owned());
            assert_eq!(config.kafka_topic, "error-tasks".to_owned());
            assert_eq!(config.kafka_deadletter_topic, "error-tasks-dlq".to_owned());
            assert_eq!(config.db_path, "./taskbroker-error.sqlite".to_owned());
            assert_eq!(config.max_pending_count, 512);
            assert_eq!(config.max_processing_count, 512);
            assert_eq!(config.max_processing_attempts, 5);
            assert_eq!(config.vacuum_page_count, Some(1000));
            assert_eq!(config.db_max_size, Some(3_000_000_000));
            assert!(config.full_vacuum_on_start);

            Ok(())
        });
    }

    #[test]
    fn test_from_args_env_and_args() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_LOG_FILTER", "error");
            jail.set_env("TASKBROKER_MAX_PROCESSING_ATTEMPTS", "5");

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.log_filter, "error");
            assert_eq!(config.max_processing_attempts, 5);

            Ok(())
        });
    }

    #[test]
    fn test_from_args_env_test() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_LOG_FILTER", "error");
            jail.set_env("TASKBROKER_MAX_PROCESSING_ATTEMPTS", "5");
            jail.set_env("TASKBROKER_DEFAULT_METRICS_TAGS", "{key=value}");

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.sentry_dsn, None);
            assert_eq!(config.sentry_env, None);
            assert_eq!(config.log_filter, "error");
            assert_eq!(config.kafka_topic, "taskworker".to_owned());
            assert_eq!(config.kafka_deadletter_topic, "taskworker-dlq".to_owned());
            assert_eq!(config.db_path, "./taskbroker-inflight.sqlite".to_owned());
            assert_eq!(config.max_pending_count, 2048);
            assert_eq!(config.max_processing_count, 2048);
            assert_eq!(config.max_processing_attempts, 5);
            assert_eq!(
                config.default_metrics_tags,
                BTreeMap::from([("key".to_owned(), "value".to_owned())])
            );

            Ok(())
        });
    }

    #[test]
    fn test_kafka_consumer_config() {
        let args = Args { config: None };
        let config = Config::from_args(&args).unwrap();
        let consumer_config = config.kafka_consumer_config();

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
            jail.set_env("TASKBROKER_KAFKA_SECURITY_PROTOCOL", "sasl_plaintext");
            jail.set_env("TASKBROKER_KAFKA_SASL_MECHANISM", "SCRAM-SHA-256");
            jail.set_env("TASKBROKER_KAFKA_SASL_USERNAME", "taskbroker");
            jail.set_env("TASKBROKER_KAFKA_SASL_PASSWORD", "secret-tech");

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            let consumer_config = config.kafka_consumer_config();

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
                "TASKBROKER_KAFKA_SSL_CA_LOCATION",
                "/etc/ssl/ca-certificate.pem",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_SSL_CERTIFICATE_LOCATION",
                "/etc/ssl/taskbroker/public.crt",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_SSL_KEY_LOCATION",
                "/etc/ssl/taskbroker/private.key",
            );

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            let consumer_config = config.kafka_consumer_config();

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
        let producer_config = config.kafka_producer_config();

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
                "TASKBROKER_KAFKA_DEADLETTER_SECURITY_PROTOCOL",
                "sasl_plaintext",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_DEADLETTER_SASL_MECHANISM",
                "SCRAM-SHA-256",
            );
            jail.set_env("TASKBROKER_KAFKA_DEADLETTER_SASL_USERNAME", "taskbroker");
            jail.set_env("TASKBROKER_KAFKA_DEADLETTER_SASL_PASSWORD", "secret-tech");

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            let producer_config = config.kafka_producer_config();

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
                "TASKBROKER_KAFKA_DEADLETTER_SSL_CA_LOCATION",
                "/etc/ssl/ca-certificate.pem",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_DEADLETTER_SSL_CERTIFICATE_LOCATION",
                "/etc/ssl/taskbroker/public.crt",
            );
            jail.set_env(
                "TASKBROKER_KAFKA_DEADLETTER_SSL_KEY_LOCATION",
                "/etc/ssl/taskbroker/private.key",
            );

            let args = Args { config: None };
            let config = Config::from_args(&args).unwrap();
            let producer_config = config.kafka_producer_config();

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
