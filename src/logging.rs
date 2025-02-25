use sentry::types::Dsn;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::str::FromStr;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::prelude::*;

use crate::{config::Config, get_version};

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

    /// The tracing sample rate
    pub traces_sample_rate: f32,

    /// The log level to filter logging to.
    pub log_filter: String,

    /// The log format to use
    pub log_format: LogFormat,
}

impl LoggingConfig {
    pub fn from_config(config: &Config) -> Self {
        LoggingConfig {
            sentry_dsn: config.sentry_dsn.clone(),
            sentry_env: config.sentry_env.clone(),
            traces_sample_rate: config.traces_sample_rate.unwrap_or(0.0),
            log_filter: config.log_filter.clone(),
            log_format: config.log_format,
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
            .boxed(),
        LogFormat::Text => subscriber.compact().boxed(),
    };

    let logs_subscriber = tracing_subscriber::registry()
        .with(formatter.with_filter(EnvFilter::new(log_config.log_filter)))
        .with(sentry::integrations::tracing::layer());

    logs_subscriber.init();
}
