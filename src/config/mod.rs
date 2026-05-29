#![allow(clippy::result_large_err)]

pub mod fetch;
pub mod kafka;
pub mod push;
pub mod queue;
pub mod raw;
pub mod store;
pub mod upkeep;

use std::borrow::Cow;
use std::collections::BTreeMap;

use figment::providers::{Env, Format, Yaml};
use figment::{Figment, Metadata, Profile, Provider};
use serde::{Deserialize, Serialize};

use crate::Args;
use crate::config::fetch::FetchConfig;
use crate::config::kafka::KafkaConfig;
use crate::config::push::PushConfig;
use crate::config::raw::RawConfig;
use crate::config::store::StoreConfig;
use crate::config::upkeep::UpkeepConfig;
use crate::logging::LogFormat;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryMode {
    /// Workers pull tasks from the broker.
    Pull,

    /// Broker pushes tasks to workers.
    Push,
}

/// Configuration shared by every kind of taskbroker, regardless of delivery mode or message format.
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct Config {
    /// Sentry DSN to use for error reporting.
    pub sentry_dsn: Option<String>,

    /// Environment for reporting Sentry errors.
    pub sentry_env: Option<Cow<'static, str>>,

    /// Sampling rate for tracing data.
    pub traces_sample_rate: Option<f32>,

    /// Filter for application logs. See `tracing-subscriber` documentation for the format.
    pub log_filter: String,

    /// Log format to use.
    pub log_format: LogFormat,

    /// The `statsd` address to report metrics to.
    pub statsd_addr: String,

    /// Default tags to add to all metrics.
    pub default_metrics_tags: BTreeMap<String, String>,

    /// When true, the upkeep loop emits the current `async_backtrace::taskdump_tree`
    /// snapshot at `debug!` every 30 seconds. Useful for diagnosing hangs in the
    /// store / fetch / push pipelines. Off by default because the tree can be
    /// large and noisy.
    pub log_async_backtrace: bool,

    /// The path to the runtime configuration file.
    pub runtime_config_path: Option<String>,

    /// Hostname for the gRPC server.
    pub grpc_addr: String,

    /// Port for the gRPC server.
    pub grpc_port: u32,

    /// A list of shared secrets that clients use to authenticate.
    /// We support a list of secrets to allow for key rotation.
    pub secrets: Vec<String>,

    /// The maximum number of seconds a task can be delayed.
    /// Tasks delayed greater than this duration are capped.
    pub max_delayed_task_allowed_sec: u64,

    /// How tasks are delivered to workers.
    pub delivery_mode: DeliveryMode,

    /// The Kafka configuration.
    pub kafka: KafkaConfig,

    /// The store config.
    pub store: StoreConfig,

    /// Upkeep configuration.
    pub upkeep: UpkeepConfig,

    /// Push pool / thread configuration.
    pub push: PushConfig,

    /// Fetch pool / thread configuration.
    pub fetch: FetchConfig,

    /// Raw mode configuration.
    pub raw: Option<RawConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sentry_dsn: None,
            sentry_env: None,
            traces_sample_rate: Some(0.0),
            log_filter: "info,librdkafka=warn,h2=off".to_owned(),
            log_format: LogFormat::Text,
            statsd_addr: "127.0.0.1:8126".parse().unwrap(),
            default_metrics_tags: Default::default(),
            log_async_backtrace: false,
            runtime_config_path: None,
            grpc_addr: "0.0.0.0".to_owned(),
            grpc_port: 50051,
            secrets: vec![],
            max_delayed_task_allowed_sec: 3600,
            delivery_mode: DeliveryMode::Pull,
            kafka: KafkaConfig::default(),
            store: StoreConfig::default(),
            upkeep: UpkeepConfig::default(),
            push: PushConfig::default(),
            fetch: FetchConfig::default(),
            raw: None,
        }
    }
}

impl Config {
    /// Build a `Config` instance using (1) defaults, (2) an optional YAML file, and
    /// (3) environment variables prefixed prefixed with `TASKBROKER_` in that order.
    pub fn from_args(args: &Args) -> Result<Self, Box<figment::Error>> {
        let mut builder = Figment::from(Config::default());

        if let Some(path) = &args.config {
            builder = builder.merge(Yaml::file(path));
        }

        builder = builder.merge(Env::prefixed("TASKBROKER_").split("__"));
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
    use std::collections::BTreeMap;

    use figment::Jail;

    use crate::Args;
    use crate::config::fetch::FetchConfig;
    use crate::config::kafka::{KafkaConfig, SharedKafkaConfig};
    use crate::config::push::PushConfig;
    use crate::config::queue::QueueConfig;
    use crate::config::raw::RawConfig;
    use crate::config::store::{DatabaseAdapter, PostgresConfig, SqliteConfig, StoreConfig};
    use crate::config::upkeep::UpkeepConfig;
    use crate::logging::LogFormat;

    use super::{Config, DeliveryMode};

    fn config_from_yaml(yaml: &str) -> Config {
        let mut config = None;

        Jail::expect_with(|jail| {
            jail.create_file("config.yaml", yaml)?;

            let args = Args {
                config: Some("config.yaml".to_owned()),
            };

            config = Some(Config::from_args(&args).unwrap());

            Ok(())
        });

        config.unwrap()
    }

    fn config_from_env(vars: &[(&str, &str)]) -> Config {
        let mut config = None;

        Jail::expect_with(|jail| {
            for (key, value) in vars {
                jail.set_env(key, value);
            }

            config = Some(Config::from_args(&Args { config: None }).unwrap());

            Ok(())
        });

        config.unwrap()
    }

    fn config_from_yaml_and_env(yaml: &str, vars: &[(&str, &str)]) -> Config {
        let mut config = None;

        Jail::expect_with(|jail| {
            jail.create_file("config.yaml", yaml)?;

            for (key, value) in vars {
                jail.set_env(key, value);
            }

            let args = Args {
                config: Some("config.yaml".to_owned()),
            };

            config = Some(Config::from_args(&args).unwrap());

            Ok(())
        });

        config.unwrap()
    }

    fn expected_shared_config() -> Config {
        Config {
            sentry_dsn: Some("fake_dsn".to_owned()),
            sentry_env: Some(Cow::Borrowed("prod")),
            traces_sample_rate: Some(0.25),
            log_filter: "debug,rdkafka=off".to_owned(),
            log_format: LogFormat::Json,
            statsd_addr: "10.0.0.1:8126".to_owned(),
            default_metrics_tags: BTreeMap::from([("key_1".to_owned(), "value_1".to_owned())]),
            log_async_backtrace: true,
            runtime_config_path: Some("/etc/runtime.yaml".to_owned()),
            grpc_addr: "127.0.0.1".to_owned(),
            grpc_port: 6000,
            secrets: vec!["secret-one".to_owned()],
            max_delayed_task_allowed_sec: 7200,
            delivery_mode: DeliveryMode::Push,
            ..Config::default()
        }
    }

    fn expected_kafka_config() -> KafkaConfig {
        KafkaConfig {
            default: SharedKafkaConfig {
                brokers: "10.0.0.1:9092,10.0.0.2:9092".to_owned(),
                topic: "error-tasks".to_owned(),
                sasl_mechanism: Some("SCRAM-SHA-256".to_owned()),
                sasl_username: Some("taskbroker".to_owned()),
                ..SharedKafkaConfig::default()
            },
            deadletter: SharedKafkaConfig {
                brokers: "10.0.0.3:9092".to_owned(),
                consumer_group: String::new(),
                topic: "error-tasks-dlq".to_owned(),
                long_topic: String::new(),
                security_protocol: None,
                sasl_mechanism: None,
                sasl_username: None,
                sasl_password: None,
                ssl_ca_location: None,
                ssl_certificate_location: None,
                ssl_key_location: None,
            },
            auto_offset_reset: "earliest".to_owned(),
            session_timeout_ms: 9000,
            send_timeout_ms: 1000,
            ..KafkaConfig::default()
        }
    }

    fn expected_store_config() -> StoreConfig {
        StoreConfig {
            adapter: DatabaseAdapter::Postgres,
            max_pending_count: 512,
            max_processing_count: 256,
            insert_batch_length: 128,
            maintenance_task_interval_ms: 6001,
            full_vacuum_on_start: false,
            ..StoreConfig::default()
        }
    }

    fn expected_postgres_config() -> PostgresConfig {
        PostgresConfig {
            host: "pg.example.com".to_owned(),
            port: 5433,
            username: "taskbroker".to_owned(),
            password: "secret".to_owned(),
            database_name: "activations".to_owned(),
            default_database_name: "template1".to_owned(),
            run_migrations: true,
            options: "sslmode=require".to_owned(),
        }
    }

    fn expected_sqlite_config() -> SqliteConfig {
        SqliteConfig {
            path: "/tmp/test.sqlite".to_owned(),
            enable_status_metrics: false,
            vacuum_page_count: Some(1000),
        }
    }

    fn expected_upkeep_config() -> UpkeepConfig {
        UpkeepConfig {
            interval_ms: 2000,
            upkeep_unhealthy_interval_ms: 8000,
            max_processing_attempts: 10,
            full_vacuum_on_upkeep: false,
            vacuum_interval_ms: 60000,
            ..UpkeepConfig::default()
        }
    }

    fn expected_push_config() -> PushConfig {
        PushConfig {
            threads: 4,
            timeout_ms: 60000,
            queue: QueueConfig::new(8, 1000),
            worker_map: BTreeMap::from([(
                "sentry".to_owned(),
                "http://worker-sentry:50052".to_owned(),
            )]),
        }
    }

    fn expected_fetch_config() -> FetchConfig {
        FetchConfig {
            threads: 2,
            wait_ms: 250,
            batch_length: 10,
            batch_status_updates: true,
            status_update_batch_size: 32,
            status_update_interval_ms: 500,
        }
    }

    fn expected_raw_config() -> RawConfig {
        RawConfig {
            namespace: "events".to_owned(),
            application: "sentry".to_owned(),
            taskname: "process".to_owned(),
            processing_deadline_duration: 60,
        }
    }

    #[test]
    fn test_default() {
        let config = Config::default();

        assert_eq!(config, Config::default());
        assert_eq!(config.kafka, KafkaConfig::default());
        assert_eq!(config.store, StoreConfig::default());
        assert_eq!(config.store.pg, PostgresConfig::default());
        assert_eq!(config.store.sqlite, SqliteConfig::default());
        assert_eq!(config.upkeep, UpkeepConfig::default());
        assert_eq!(config.push, PushConfig::default());
        assert_eq!(config.fetch, FetchConfig::default());
        assert_eq!(config.raw, None);

        Jail::expect_with(|_jail| {
            let config = Config::from_args(&Args { config: None }).unwrap();
            assert_eq!(config, Config::default());
            Ok(())
        });
    }

    #[test]
    fn test_shared_from_file() {
        let config = config_from_yaml(
            r#"
            sentry_dsn: fake_dsn
            sentry_env: prod
            traces_sample_rate: 0.25
            log_filter: debug,rdkafka=off
            log_format: json
            statsd_addr: 10.0.0.1:8126
            default_metrics_tags:
                key_1: value_1
            log_async_backtrace: true
            runtime_config_path: /etc/runtime.yaml
            grpc_addr: 127.0.0.1
            grpc_port: 6000
            secrets:
                - secret-one
            max_delayed_task_allowed_sec: 7200
            delivery_mode: push
        "#,
        );

        let expected = expected_shared_config();

        // Check shared configuration options
        assert_eq!(config.sentry_dsn, expected.sentry_dsn);
        assert_eq!(config.sentry_env, expected.sentry_env);
        assert_eq!(config.traces_sample_rate, expected.traces_sample_rate);
        assert_eq!(config.log_filter, expected.log_filter);
        assert_eq!(config.log_format, expected.log_format);
        assert_eq!(config.statsd_addr, expected.statsd_addr);
        assert_eq!(config.default_metrics_tags, expected.default_metrics_tags);
        assert_eq!(config.log_async_backtrace, expected.log_async_backtrace);
        assert_eq!(config.runtime_config_path, expected.runtime_config_path);
        assert_eq!(config.grpc_addr, expected.grpc_addr);
        assert_eq!(config.grpc_port, expected.grpc_port);
        assert_eq!(config.secrets, expected.secrets);
        assert_eq!(
            config.max_delayed_task_allowed_sec,
            expected.max_delayed_task_allowed_sec
        );
        assert_eq!(config.delivery_mode, expected.delivery_mode);

        // Check that other configuration options are defaults
        assert_eq!(config.kafka, KafkaConfig::default());
        assert_eq!(config.store, StoreConfig::default());
        assert_eq!(config.upkeep, UpkeepConfig::default());
        assert_eq!(config.push, PushConfig::default());
        assert_eq!(config.fetch, FetchConfig::default());
        assert_eq!(config.raw, None);
    }

    #[test]
    fn test_shared_from_env() {
        let config = config_from_env(&[
            ("TASKBROKER_SENTRY_DSN", "fake_dsn"),
            ("TASKBROKER_SENTRY_ENV", "prod"),
            ("TASKBROKER_TRACES_SAMPLE_RATE", "0.25"),
            ("TASKBROKER_LOG_FILTER", "debug,rdkafka=off"),
            ("TASKBROKER_LOG_FORMAT", "json"),
            ("TASKBROKER_STATSD_ADDR", "10.0.0.1:8126"),
            ("TASKBROKER_DEFAULT_METRICS_TAGS", "{key_1=value_1}"),
            ("TASKBROKER_LOG_ASYNC_BACKTRACE", "true"),
            ("TASKBROKER_RUNTIME_CONFIG_PATH", "/etc/runtime.yaml"),
            ("TASKBROKER_GRPC_ADDR", "127.0.0.1"),
            ("TASKBROKER_GRPC_PORT", "6000"),
            ("TASKBROKER_SECRETS", r#"["secret-one"]"#),
            ("TASKBROKER_MAINTENANCE_TASK_INTERVAL_MS", "3000"),
            ("TASKBROKER_MAX_DELAYED_TASK_ALLOWED_SEC", "7200"),
            ("TASKBROKER_DELIVERY_MODE", "push"),
        ]);
        let expected = expected_shared_config();
        assert_eq!(config.sentry_dsn, expected.sentry_dsn);
        assert_eq!(config.sentry_env, expected.sentry_env);
        assert_eq!(config.traces_sample_rate, expected.traces_sample_rate);
        assert_eq!(config.log_filter, expected.log_filter);
        assert_eq!(config.log_format, expected.log_format);
        assert_eq!(config.statsd_addr, expected.statsd_addr);
        assert_eq!(config.default_metrics_tags, expected.default_metrics_tags);
        assert_eq!(config.log_async_backtrace, expected.log_async_backtrace);
        assert_eq!(config.runtime_config_path, expected.runtime_config_path);
        assert_eq!(config.grpc_addr, expected.grpc_addr);
        assert_eq!(config.grpc_port, expected.grpc_port);
        assert_eq!(config.secrets, expected.secrets);
        assert_eq!(
            config.max_delayed_task_allowed_sec,
            expected.max_delayed_task_allowed_sec
        );
        assert_eq!(config.delivery_mode, expected.delivery_mode);
        assert_eq!(config.kafka, KafkaConfig::default());
        assert_eq!(config.store, StoreConfig::default());
        assert_eq!(config.upkeep, UpkeepConfig::default());
        assert_eq!(config.push, PushConfig::default());
        assert_eq!(config.fetch, FetchConfig::default());
        assert_eq!(config.raw, None);
    }

    #[test]
    fn test_kafka_from_file() {
        let config = config_from_yaml(
            r#"
            kafka:
                default:
                    brokers: 10.0.0.1:9092,10.0.0.2:9092
                    topic: error-tasks
                    sasl_mechanism: SCRAM-SHA-256
                    sasl_username: taskbroker
                deadletter:
                    topic: error-tasks-dlq
                    brokers: 10.0.0.3:9092
                auto_offset_reset: earliest
                session_timeout_ms: 9000
                send_timeout_ms: 1000
        "#,
        );
        assert_eq!(config.kafka, expected_kafka_config());
        let mut expected_config = Config::default();
        expected_config.kafka = expected_kafka_config();
        assert_eq!(config, expected_config);
    }

    #[test]
    fn test_kafka_from_env() {
        let config = config_from_env(&[
            (
                "TASKBROKER_KAFKA__DEFAULT__BROKERS",
                "10.0.0.1:9092,10.0.0.2:9092",
            ),
            ("TASKBROKER_KAFKA__DEFAULT__TOPIC", "error-tasks"),
            ("TASKBROKER_KAFKA__DEFAULT__SASL_MECHANISM", "SCRAM-SHA-256"),
            ("TASKBROKER_KAFKA__DEFAULT__SASL_USERNAME", "taskbroker"),
            ("TASKBROKER_KAFKA__DEADLETTER__TOPIC", "error-tasks-dlq"),
            ("TASKBROKER_KAFKA__DEADLETTER__BROKERS", "10.0.0.3:9092"),
            ("TASKBROKER_KAFKA__AUTO_OFFSET_RESET", "earliest"),
            ("TASKBROKER_KAFKA__SESSION_TIMEOUT_MS", "9000"),
            ("TASKBROKER_KAFKA__SEND_TIMEOUT_MS", "1000"),
        ]);
        assert_eq!(config.kafka, expected_kafka_config());
        let mut expected_config = Config::default();
        expected_config.kafka = expected_kafka_config();
        assert_eq!(config, expected_config);
    }

    #[test]
    fn test_store_from_file() {
        let config = config_from_yaml(
            r#"
            store:
                adapter: postgres
                max_pending_count: 512
                max_processing_count: 256
                insert_batch_length: 128
                maintenance_task_interval_ms: 6001
                full_vacuum_on_start: false
        "#,
        );
        assert_eq!(config.store, expected_store_config());
        let mut default = Config::default();
        default.store = expected_store_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_store_from_env() {
        let config = config_from_env(&[
            ("TASKBROKER_STORE__ADAPTER", "postgres"),
            ("TASKBROKER_STORE__MAX_PENDING_COUNT", "512"),
            ("TASKBROKER_STORE__MAX_PROCESSING_COUNT", "256"),
            ("TASKBROKER_STORE__INSERT_BATCH_LENGTH", "128"),
            ("TASKBROKER_STORE__INSERT_BATCH_LENGTH", "128"),
            ("TASKBROKER_STORE__MAINTENANCE_TASK_INTERVAL_MS", "6001"),
            ("TASKBROKER_STORE__FULL_VACUUM_ON_START", "false"),
        ]);
        assert_eq!(config.store, expected_store_config());
        let mut default = Config::default();
        default.store = expected_store_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_postgres_from_file() {
        let config = config_from_yaml(
            r#"
            store:
                pg:
                    host: pg.example.com
                    port: 5433
                    username: taskbroker
                    password: secret
                    database_name: activations
                    default_database_name: template1
                    run_migrations: true
                    options: sslmode=require
        "#,
        );
        assert_eq!(config.store.pg, expected_postgres_config());
        let mut default = Config::default();
        default.store.pg = expected_postgres_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_postgres_from_env() {
        let config = config_from_env(&[
            ("TASKBROKER_STORE__PG__HOST", "pg.example.com"),
            ("TASKBROKER_STORE__PG__PORT", "5433"),
            ("TASKBROKER_STORE__PG__USERNAME", "taskbroker"),
            ("TASKBROKER_STORE__PG__PASSWORD", "secret"),
            ("TASKBROKER_STORE__PG__DATABASE_NAME", "activations"),
            ("TASKBROKER_STORE__PG__DEFAULT_DATABASE_NAME", "template1"),
            ("TASKBROKER_STORE__PG__RUN_MIGRATIONS", "true"),
            ("TASKBROKER_STORE__PG__OPTIONS", "sslmode=require"),
        ]);
        assert_eq!(config.store.pg, expected_postgres_config());
        let mut default = Config::default();
        default.store.pg = expected_postgres_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_sqlite_from_file() {
        let config = config_from_yaml(
            r#"
            store:
                sqlite:
                    path: /tmp/test.sqlite
                    enable_status_metrics: false
                    vacuum_page_count: 1000
        "#,
        );
        assert_eq!(config.store.sqlite, expected_sqlite_config());
        let mut default = Config::default();
        default.store.sqlite = expected_sqlite_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_sqlite_from_env() {
        let config = config_from_env(&[
            ("TASKBROKER_STORE__SQLITE__PATH", "/tmp/test.sqlite"),
            ("TASKBROKER_STORE__SQLITE__ENABLE_STATUS_METRICS", "false"),
            ("TASKBROKER_STORE__SQLITE__VACUUM_PAGE_COUNT", "1000"),
        ]);
        assert_eq!(config.store.sqlite, expected_sqlite_config());
        let mut default = Config::default();
        default.store.sqlite = expected_sqlite_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_upkeep_from_file() {
        let config = config_from_yaml(
            r#"
            upkeep:
                interval_ms: 2000
                upkeep_unhealthy_interval_ms: 8000
                max_processing_attempts: 10
                full_vacuum_on_upkeep: false
                vacuum_interval_ms: 60000
        "#,
        );
        assert_eq!(config.upkeep, expected_upkeep_config());
        let mut default = Config::default();
        default.upkeep = expected_upkeep_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_upkeep_from_env() {
        let config = config_from_env(&[
            ("TASKBROKER_UPKEEP__INTERVAL_MS", "2000"),
            ("TASKBROKER_UPKEEP__UPKEEP_UNHEALTHY_INTERVAL_MS", "8000"),
            ("TASKBROKER_UPKEEP__MAX_PROCESSING_ATTEMPTS", "10"),
            ("TASKBROKER_UPKEEP__FULL_VACUUM_ON_UPKEEP", "false"),
            ("TASKBROKER_UPKEEP__VACUUM_INTERVAL_MS", "60000"),
        ]);
        assert_eq!(config.upkeep, expected_upkeep_config());
        let mut default = Config::default();
        default.upkeep = expected_upkeep_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_push_from_file() {
        let config = config_from_yaml(
            r#"
            push:
                threads: 4
                timeout_ms: 60000
                queue:
                    size: 8
                    timeout_ms: 1000
                worker_map:
                    sentry: http://worker-sentry:50052
        "#,
        );
        assert_eq!(config.push, expected_push_config());
        let mut default = Config::default();
        default.push = expected_push_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_push_from_env() {
        let config = config_from_env(&[
            ("TASKBROKER_PUSH__THREADS", "4"),
            ("TASKBROKER_PUSH__TIMEOUT_MS", "60000"),
            ("TASKBROKER_PUSH__QUEUE__SIZE", "8"),
            ("TASKBROKER_PUSH__QUEUE__TIMEOUT_MS", "1000"),
            (
                "TASKBROKER_PUSH__WORKER_MAP",
                "{sentry=http://worker-sentry:50052}",
            ),
        ]);
        assert_eq!(config.push, expected_push_config());
        let mut default = Config::default();
        default.push = expected_push_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_fetch_from_file() {
        let config = config_from_yaml(
            r#"
            fetch:
                threads: 2
                wait_ms: 250
                batch_length: 10
                batch_status_updates: true
                status_update_batch_size: 32
                status_update_interval_ms: 500
        "#,
        );
        assert_eq!(config.fetch, expected_fetch_config());
        let mut default = Config::default();
        default.fetch = expected_fetch_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_fetch_from_env() {
        let config = config_from_env(&[
            ("TASKBROKER_FETCH__THREADS", "2"),
            ("TASKBROKER_FETCH__WAIT_MS", "250"),
            ("TASKBROKER_FETCH__BATCH_LENGTH", "10"),
            ("TASKBROKER_FETCH__BATCH_STATUS_UPDATES", "true"),
            ("TASKBROKER_FETCH__STATUS_UPDATE_BATCH_SIZE", "32"),
            ("TASKBROKER_FETCH__STATUS_UPDATE_INTERVAL_MS", "500"),
        ]);
        assert_eq!(config.fetch, expected_fetch_config());
        let mut default = Config::default();
        default.fetch = expected_fetch_config();
        assert_eq!(config, default);
    }

    #[test]
    fn test_raw_from_file() {
        let config = config_from_yaml(
            r#"
            raw:
                namespace: events
                application: sentry
                taskname: process
                processing_deadline_duration: 60
        "#,
        );
        assert_eq!(config.raw, Some(expected_raw_config()));
        let mut default = Config::default();
        default.raw = Some(expected_raw_config());
        assert_eq!(config, default);
    }

    #[test]
    fn test_raw_from_env() {
        let config = config_from_env(&[
            ("TASKBROKER_RAW__NAMESPACE", "events"),
            ("TASKBROKER_RAW__APPLICATION", "sentry"),
            ("TASKBROKER_RAW__TASKNAME", "process"),
            ("TASKBROKER_RAW__PROCESSING_DEADLINE_DURATION", "60"),
        ]);
        assert_eq!(config.raw, Some(expected_raw_config()));
        let mut default = Config::default();
        default.raw = Some(expected_raw_config());
        assert_eq!(config, default);
    }

    #[test]
    fn test_from_args_yaml_and_env() {
        let config = config_from_yaml_and_env(
            r#"
            kafka:
                default:
                    topic: error-tasks
            store:
                adapter: postgres
                sqlite:
                    path: ./taskbroker-error.sqlite
            delivery_mode: pull
            push:
                worker_map:
                    sentry: http://worker-sentry:50052
        "#,
            &[
                ("TASKBROKER_LOG_FILTER", "error"),
                ("TASKBROKER_UPKEEP__MAX_PROCESSING_ATTEMPTS", "10"),
                ("TASKBROKER_DELIVERY_MODE", "push"),
            ],
        );

        assert_eq!(config.log_filter, "error");
        assert_eq!(config.delivery_mode, DeliveryMode::Push);
        assert_eq!(config.kafka.default.topic, "error-tasks");
        assert_eq!(
            config.kafka.default.brokers,
            KafkaConfig::default().default.brokers
        );
        assert_eq!(config.store.adapter, DatabaseAdapter::Postgres);
        assert_eq!(config.store.sqlite.path, "./taskbroker-error.sqlite");
        assert_eq!(config.upkeep.max_processing_attempts, 10);
        assert_eq!(
            config.upkeep.interval_ms,
            UpkeepConfig::default().interval_ms
        );
        assert_eq!(
            config.push.worker_map,
            BTreeMap::from([("sentry".to_owned(), "http://worker-sentry:50052".to_owned(),)])
        );
        assert_eq!(config.fetch, FetchConfig::default());
        assert_eq!(config.raw, None);
    }
}
