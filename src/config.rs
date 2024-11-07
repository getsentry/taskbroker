use std::net::SocketAddr;
use std::path::Path;
use figment::{Figment, providers::{Format, Yaml, Env}};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
    Off,
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct Config {
    /// The sentry DSN to use for error reporting.
    pub sentry_dsn: Option<String>,

    /// The environment to report to sentry errors to.
    pub sentry_env: Option<String>,

    /// The log level to filter logging to.
    pub log_level: Option<LogLevel>,

    /// The statsd address to report metrics to.
    pub statsd_addr: SocketAddr,

    /// comma separated list of kafka brokers to connect to
    pub kafka_cluster: Vec<String>,

    /// The topic to fetch task messages from.
    pub kafka_topic: String,

    /// The path to the sqlite database
    pub db_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sentry_dsn: None,
            sentry_env: None,
            log_level: Some(LogLevel::Info),
            statsd_addr: "127.0.0.1:8126".parse().unwrap(),
            kafka_cluster: vec!["127.0.0.1:9092".to_owned()],
            kafka_topic: "task-worker".to_owned(),
            db_path: "./taskworker-inflight.sqlite".to_owned(),
        }
    }
}

/// Load configuration from a file path
/// Will merge in environment variables prefixed with TASKWORKER_ as well.
pub fn load_config(path: &Path) -> Result<Config, figment::Error> {
    let figment = Figment::new()
        .merge(Yaml::file(path))
        .merge(Env::prefixed("TASKWORKER_"));
    figment.extract()
}


#[cfg(test)]
mod tests {
    use std::path::Path;
    use super::{Config, LogLevel, load_config};
    use figment::Jail;

    #[test]
    fn test_default() {
        let config = Config {..Default::default()};
        assert_eq!(config.sentry_dsn, None);
        assert_eq!(config.sentry_env, None);
        assert_eq!(config.log_level, Some(LogLevel::Info));
        assert_eq!(config.kafka_topic, "task-worker");
        assert_eq!(config.db_path, "./taskworker-inflight.sqlite");
    }

    #[test]
    fn test_parse_yaml_and_env() {
        Jail::expect_with(|jail| {
            jail.create_file("config.yaml", r#"
                sentry_dsn: fake_dsn
                sentry_env: prod
                log_level: info
                statsd_addr: 127.0.0.1:8126
                kafka_cluster: [10.0.0.1:9092, 10.0.0.2:9092]
                kafka_topic: error-tasks
                db_path: ./taskworker-error.sqlite
            "#)?;
            jail.set_env("TASKWORKER_LOG_LEVEL", "error");

            let config = load_config(Path::new("config.yaml"))?;
            assert_eq!(config.sentry_dsn, Some("fake_dsn".to_owned()));
            assert_eq!(config.log_level, Some(LogLevel::Error));
            assert_eq!(config.kafka_topic, "error-tasks".to_owned());
            assert_eq!(config.db_path, "./taskworker-error.sqlite".to_owned());

            Ok(())
        });
    }
}
