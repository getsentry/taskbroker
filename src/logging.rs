use rdkafka::config::RDKafkaLogLevel;
use sentry::types::Dsn;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::str::FromStr;
use tracing_subscriber::{filter::LevelFilter, prelude::*, Layer};

use crate::{config::Config, get_version};

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

impl LogLevel {
    /// Convert application log levels to tracing::LevelFilter
    pub const fn level_filter(&self) -> LevelFilter {
        match self {
            Self::Error => LevelFilter::ERROR,
            Self::Warn => LevelFilter::WARN,
            Self::Info => LevelFilter::INFO,
            Self::Debug => LevelFilter::DEBUG,
            Self::Trace => LevelFilter::TRACE,
            Self::Off => LevelFilter::OFF,
        }
    }
    /// Convert to the log levels used by rdkafka
    pub const fn kafka_level(&self) -> RDKafkaLogLevel {
        match self {
            Self::Error => RDKafkaLogLevel::Error,
            Self::Warn => RDKafkaLogLevel::Warning,
            Self::Info => RDKafkaLogLevel::Info,
            Self::Debug => RDKafkaLogLevel::Debug,
            Self::Trace => RDKafkaLogLevel::Debug,
            // While emerg is not an off state,
            // emerg is effectively off as we don't
            // use emerg level.
            Self::Off => RDKafkaLogLevel::Emerg,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// JSON structured logging for k8s
    Json,

    /// Text formatted logging for local dev
    Text,
}

#[derive(Debug)]
pub struct LoggingConfig {
    /// The sentry DSN to use for error reporting.
    pub sentry_dsn: Option<String>,

    /// The environment to report to sentry errors to.
    pub sentry_env: Option<Cow<'static, str>>,

    /// The tracing sample rate.
    pub traces_sample_rate: f32,

    /// The log level to filter logging to.
    pub log_level: LogLevel,

    /// The log format to use.
    pub log_format: LogFormat,

    /// Enable ANSI encoding for formatted events.
    pub with_ansi: bool,
}

impl LoggingConfig {
    pub fn from_config(config: &Config) -> Self {
        LoggingConfig {
            sentry_dsn: config.sentry_dsn.clone(),
            sentry_env: config.sentry_env.clone(),
            traces_sample_rate: config.traces_sample_rate.unwrap_or(0.0),
            log_level: config.log_level,
            log_format: config.log_format,
            with_ansi: config.log_with_ansi,
        }
    }
}

/// Setup sentry-sdk and logging/tracing
pub fn init(log_config: LoggingConfig) {
    if let Some(dsn) = &log_config.sentry_dsn {
        let dsn = Some(Dsn::from_str(dsn).expect("Invalid Sentry DSN"));
        let guard = sentry::init(sentry::ClientOptions {
            dsn,
            release: Some(Cow::Borrowed(get_version())),
            environment: log_config.sentry_env.clone(),
            traces_sample_rate: log_config.traces_sample_rate,
            ..Default::default()
        });

        // We manually deinitialize sentry later
        std::mem::forget(guard)
    }

    let subscriber = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true);

    let formatter = match log_config.log_format {
        LogFormat::Json => subscriber
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(true)
            .with_file(true)
            .with_line_number(true)
            .with_ansi(log_config.with_ansi)
            .boxed(),
        LogFormat::Text => subscriber.compact().with_ansi(log_config.with_ansi).boxed(),
    };

    let logs_subscriber = tracing_subscriber::registry()
        .with(formatter.with_filter(log_config.log_level.level_filter()))
        .with(sentry::integrations::tracing::layer());

    logs_subscriber.init();
}
