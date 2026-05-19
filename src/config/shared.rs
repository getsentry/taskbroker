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

    /// How often maintenance tasks (reclaiming free pages) are executed.
    pub maintenance_task_interval_ms: u64,

    /// Enable to have the application perform `VACUUM` on the database
    /// when it starts up, but before the gRPC server, consumer, and upkeep begin.
    pub full_vacuum_on_start: bool,

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
            maintenance_task_interval_ms: 6000,
            full_vacuum_on_start: true,
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

        builder = builder.merge(Env::prefixed("TASKBROKER_"));
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
