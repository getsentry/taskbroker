use figment::{
    providers::{Env, Format, Serialized, Yaml},
    Figment, Metadata, Profile, Provider,
};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, net::SocketAddr};

use crate::{
    logging::{LogFormat, LogLevel},
    Args,
};

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct Config {
    /// The sentry DSN to use for error reporting.
    pub sentry_dsn: Option<String>,

    /// The environment to report to sentry errors to.
    pub sentry_env: Option<Cow<'static, str>>,

    /// The sampling rate for tracing data.
    pub traces_sample_rate: Option<f32>,

    /// The log level to filter logging to.
    pub log_level: LogLevel,

    /// The log format to use
    pub log_format: LogFormat,

    /// The statsd address to report metrics to.
    pub statsd_addr: SocketAddr,

    /// The hostname and port of the gRPC server.
    pub grpc_addr: String,

    /// The port to bind the grpc service to
    pub grpc_port: u32,

    /// comma separated list of kafka brokers to connect to
    pub kafka_cluster: String,

    /// The kafka consumer group name
    pub kafka_consumer_group: String,

    /// The topic to fetch task messages from.
    pub kafka_topic: String,

    /// The kafka topic to publish dead letter messages on
    pub kafka_deadletter_topic: String,

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

    /// The maximum number of pending records that can be
    /// in the InflightTaskStore (sqlite)
    pub max_pending_count: usize,

    /// The maximum number of tasks that are buffered
    /// before being written to InflightTaskStore (sqlite).
    pub max_pending_buffer_count: usize,

    /// The maximum value for processing deadlines. If a task
    /// does not have a processing deadline set, this will be used.
    pub max_processing_deadline: usize,

    /// The maximum duration a task can be in InflightTaskStore
    /// After this time, messages will be deadlettered if they
    /// are not complete. This should be a multiple of max_processing_deadline
    /// to allow temporary worker deaths to be resolved.
    pub deadletter_deadline: usize,

    /// The frequency at which upkeep tasks are spawned.
    pub upkeep_task_interval_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sentry_dsn: None,
            sentry_env: None,
            traces_sample_rate: Some(0.0),
            log_level: LogLevel::Debug,
            log_format: LogFormat::Text,
            grpc_addr: "0.0.0.0".to_owned(),
            grpc_port: 50051,
            statsd_addr: "127.0.0.1:8126".parse().unwrap(),
            kafka_cluster: "127.0.0.1:9092".to_owned(),
            kafka_consumer_group: "task-worker".to_owned(),
            kafka_topic: "task-worker".to_owned(),
            kafka_deadletter_topic: "task-worker-dlq".to_owned(),
            kafka_session_timeout_ms: 6000,
            kafka_auto_commit_interval_ms: 5000,
            kafka_auto_offset_reset: "latest".to_owned(),
            kafka_send_timeout_ms: 500,
            db_path: "./taskbroker-inflight.sqlite".to_owned(),
            max_pending_count: 2048,
            max_pending_buffer_count: 1,
            max_processing_deadline: 300,
            deadletter_deadline: 900,
            upkeep_task_interval_ms: 200,
        }
    }
}

impl Config {
    /// Build a config instance from defaults, env vars, file + CLI options
    pub fn from_args(args: &Args) -> Result<Self, figment::Error> {
        let mut builder = Figment::from(Config::default());
        if let Some(path) = &args.config {
            builder = builder.merge(Yaml::file(path));
        }
        if let Some(log_level) = &args.log_level {
            builder = builder.merge(Serialized::default("log_level", log_level));
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
            .set("enable.auto.offset.store", "false")
            .set_log_level(self.log_level.kafka_level());
        config.clone()
    }

    /// Convert the application Config into rdkafka::ClientConfig
    pub fn kafka_producer_config(&self) -> ClientConfig {
        let mut new_config = ClientConfig::new();
        let config = new_config
            .set("bootstrap.servers", self.kafka_cluster.clone())
            .set_log_level(self.log_level.kafka_level());
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
    use std::borrow::Cow;

    use super::Config;
    use crate::{
        logging::{LogFormat, LogLevel},
        Args,
    };
    use figment::Jail;

    #[test]
    fn test_default() {
        let config = Config {
            ..Default::default()
        };
        assert_eq!(config.sentry_dsn, None);
        assert_eq!(config.sentry_env, None);
        assert_eq!(config.log_level, LogLevel::Debug);
        assert_eq!(config.log_format, LogFormat::Text);
        assert_eq!(config.grpc_port, 50051);
        assert_eq!(config.kafka_topic, "task-worker");
        assert_eq!(config.db_path, "./taskbroker-inflight.sqlite");
        assert_eq!(config.max_pending_count, 2048);
    }

    #[test]
    fn test_from_args_config_file() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
                sentry_dsn: fake_dsn
                sentry_env: prod
                log_level: info
                log_format: json
                statsd_addr: 127.0.0.1:8126
                kafka_cluster: 10.0.0.1:9092,10.0.0.2:9092
                kafka_topic: error-tasks
                kafka_deadletter_topic: error-tasks-dlq
                kafka_auto_offset_reset: earliest
                db_path: ./taskbroker-error.sqlite
                max_pending_count: 512
                max_processing_deadline: 1000
                deadletter_deadline: 2000
            "#,
            )?;
            // Env vars always override config file
            jail.set_env("TASKBROKER_LOG_LEVEL", "error");

            let args = Args {
                config: Some("config.yaml".to_owned()),
                log_level: None,
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.sentry_dsn, Some("fake_dsn".to_owned()));
            assert_eq!(config.sentry_env, Some(Cow::Borrowed("prod")));
            assert_eq!(config.log_level, LogLevel::Error);
            assert_eq!(config.log_format, LogFormat::Json);
            assert_eq!(
                config.kafka_cluster,
                "10.0.0.1:9092,10.0.0.2:9092".to_owned()
            );
            assert_eq!(config.kafka_consumer_group, "task-worker".to_owned());
            assert_eq!(config.kafka_auto_offset_reset, "earliest".to_owned());
            assert_eq!(config.kafka_session_timeout_ms, 6000.to_owned());
            assert_eq!(config.kafka_topic, "error-tasks".to_owned());
            assert_eq!(config.kafka_deadletter_topic, "error-tasks-dlq".to_owned());
            assert_eq!(config.db_path, "./taskbroker-error.sqlite".to_owned());
            assert_eq!(config.max_pending_count, 512);
            assert_eq!(config.max_processing_deadline, 1000);
            assert_eq!(config.deadletter_deadline, 2000);

            Ok(())
        });
    }

    #[test]
    fn test_from_args_env_and_args() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_LOG_LEVEL", "error");
            jail.set_env("TASKBROKER_DEADLETTER_DEADLINE", "2000");

            let args = Args {
                config: None,
                log_level: Some("debug".to_owned()),
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.log_level, LogLevel::Error);
            assert_eq!(config.deadletter_deadline, 2000);

            Ok(())
        });
    }

    #[test]
    fn test_from_args_env_test() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKBROKER_LOG_LEVEL", "error");
            jail.set_env("TASKBROKER_DEADLETTER_DEADLINE", "2000");

            let args = Args {
                config: None,
                log_level: None,
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.sentry_dsn, None);
            assert_eq!(config.sentry_env, None);
            assert_eq!(config.log_level, LogLevel::Error);
            assert_eq!(config.kafka_topic, "task-worker".to_owned());
            assert_eq!(config.kafka_deadletter_topic, "task-worker-dlq".to_owned());
            assert_eq!(config.db_path, "./taskbroker-inflight.sqlite".to_owned());
            assert_eq!(config.max_pending_count, 2048);
            assert_eq!(config.max_processing_deadline, 300);
            assert_eq!(config.deadletter_deadline, 2000);

            Ok(())
        });
    }

    #[test]
    fn test_kafka_consumer_config() {
        let args = Args {
            config: None,
            log_level: None,
        };
        let config = Config::from_args(&args).unwrap();
        let consumer_config = config.kafka_consumer_config();

        assert_eq!(
            consumer_config.get("bootstrap.servers").unwrap(),
            "127.0.0.1:9092"
        );
        assert_eq!(consumer_config.get("group.id").unwrap(), "task-worker");
        assert!(consumer_config.get("session.timeout.ms").is_some());
    }

    #[test]
    fn test_kafka_producer_config() {
        let args = Args {
            config: None,
            log_level: None,
        };
        let config = Config::from_args(&args).unwrap();
        let producer_config = config.kafka_producer_config();

        assert_eq!(
            producer_config.get("bootstrap.servers").unwrap(),
            "127.0.0.1:9092"
        );
        assert!(producer_config.get("group.id").is_none());
        assert!(producer_config.get("session.timeout.ms").is_none());
    }
}
