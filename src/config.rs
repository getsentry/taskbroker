use figment::{
    providers::{Env, Format, Serialized, Yaml},
    Figment, Metadata, Profile, Provider,
};
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

    /// The log level to filter logging to.
    pub log_level: LogLevel,

    /// The log format to use
    pub log_format: LogFormat,

    /// The statsd address to report metrics to.
    pub statsd_addr: SocketAddr,

    /// The port to bind the grpc service to
    pub grpc_port: u32,

    /// comma separated list of kafka brokers to connect to
    pub kafka_cluster: Vec<String>,

    /// The topic to fetch task messages from.
    pub kafka_topic: String,

    /// The kafka topic to publish dead letter messages on
    pub kafka_deadletter_topic: String,

    /// The path to the sqlite database
    pub db_path: String,

    /// The maximum number of pending records that can be
    /// in the InflightTaskStore
    pub max_pending_count: u32,

    /// The maximum value for processing deadlines. If a task
    /// does not have a processing deadline set, this will be used.
    pub max_processing_deadline: u32,

    /// The maximum duration a task can be in InflightTaskStore
    /// After this time, messages will be deadlettered if they
    /// are not complete. This should be a multiple of max_processing_deadline
    /// to allow temporary worker deaths to be resolved.
    pub deadletter_deadline: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sentry_dsn: None,
            sentry_env: None,
            log_level: LogLevel::Info,
            log_format: LogFormat::Text,
            grpc_port: 50051,
            statsd_addr: "127.0.0.1:8126".parse().unwrap(),
            kafka_cluster: vec!["127.0.0.1:9092".to_owned()],
            kafka_topic: "task-worker".to_owned(),
            kafka_deadletter_topic: "task-worker-dlq".to_owned(),
            db_path: "./taskworker-inflight.sqlite".to_owned(),
            max_pending_count: 200,
            max_processing_deadline: 300,
            deadletter_deadline: 900,
        }
    }
}

impl Config {
    /// Build a config instance from defaults, env vars, file + CLI options
    pub fn from_args(args: &Args) -> Result<Self, figment::Error> {
        let mut builder = Figment::from(Config::default()).merge(Env::prefixed("TASKWORKER_"));

        if let Some(path) = &args.config {
            builder = builder.merge(Yaml::file(path));
        }
        if let Some(log_level) = &args.log_level {
            builder = builder.merge(Serialized::default("log_level", log_level));
        }
        let config = builder.extract()?;
        Ok(config)
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
        assert_eq!(config.log_level, LogLevel::Info);
        assert_eq!(config.log_format, LogFormat::Text);
        assert_eq!(config.grpc_port, 50051);
        assert_eq!(config.kafka_topic, "task-worker");
        assert_eq!(config.db_path, "./taskworker-inflight.sqlite");
        assert_eq!(config.max_pending_count, 200);
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
                kafka_cluster: [10.0.0.1:9092, 10.0.0.2:9092]
                kafka_topic: error-tasks
                kafka_deadletter_topic: error-tasks-dlq
                db_path: ./taskworker-error.sqlite
                max_pending_count: 512
                max_processing_deadline: 1000
                deadletter_deadline: 2000
            "#,
            )?;
            // Env vars are not used if config file sets same key
            jail.set_env("TASKWORKER_LOG_LEVEL", "error");

            let args = Args {
                config: Some("config.yaml".to_owned()),
                log_level: None,
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.sentry_dsn, Some("fake_dsn".to_owned()));
            assert_eq!(config.sentry_env, Some(Cow::Borrowed("prod")));
            assert_eq!(config.log_level, LogLevel::Info);
            assert_eq!(config.log_format, LogFormat::Json);
            assert_eq!(config.kafka_topic, "error-tasks".to_owned());
            assert_eq!(config.kafka_deadletter_topic, "error-tasks-dlq".to_owned());
            assert_eq!(config.db_path, "./taskworker-error.sqlite".to_owned());
            assert_eq!(config.max_pending_count, 512);
            assert_eq!(config.max_processing_deadline, 1000);
            assert_eq!(config.deadletter_deadline, 2000);

            Ok(())
        });
    }

    #[test]
    fn test_from_from_args_env_and_args() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKWORKER_LOG_LEVEL", "error");
            jail.set_env("TASKWORKER_DEADLETTER_DEADLINE", "2000");

            let args = Args {
                config: None,
                log_level: Some("debug".to_owned()),
            };
            let config = Config::from_args(&args).unwrap();
            assert_eq!(config.log_level, LogLevel::Debug);
            assert_eq!(config.deadletter_deadline, 2000);

            Ok(())
        });
    }

    #[test]
    fn test_from_from_args_env() {
        Jail::expect_with(|jail| {
            jail.set_env("TASKWORKER_LOG_LEVEL", "error");
            jail.set_env("TASKWORKER_DEADLETTER_DEADLINE", "2000");

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
            assert_eq!(config.db_path, "./taskworker-inflight.sqlite".to_owned());
            assert_eq!(config.max_pending_count, 200);
            assert_eq!(config.max_processing_deadline, 300);
            assert_eq!(config.deadletter_deadline, 2000);

            Ok(())
        });
    }
}
