use clap::{Parser, ValueEnum};
use std::fs;

pub mod config;
pub mod fetch;
pub mod flusher;
pub mod grpc;
pub mod kafka;
pub mod logging;
pub mod metrics;
pub mod push;
pub mod runtime_config;
pub mod serde;
pub mod store;
pub mod test_utils;
pub mod tokio;
pub mod upkeep;
pub mod worker;

/// Name of the grpc service.
/// Using the service type to get a name wasn't working across modules.
pub const SERVICE_NAME: &str = "sentry_protos.taskbroker.v1.ConsumerService";

pub fn get_version() -> &'static str {
    let release_name = fs::read_to_string("./VERSION").expect("Unable to read version");
    Box::leak(release_name.into_boxed_str())
}

/// What are we running?
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum Run {
    Migrations,
    Broker,
}

#[derive(Parser, Debug)]
pub struct Args {
    /// What are we running?
    #[arg(short, long, default_value = "broker")]
    pub run: Run,

    /// Path to the configuration file.
    #[arg(short, long)]
    pub config: Option<String>,
}

#[macro_export]
macro_rules! timed {
    ($future:expr, $($histogram_args:tt)+) => {{
        let start = ::std::time::Instant::now();
        let result = $future.await;
        ::metrics::histogram!($($histogram_args)+).record(start.elapsed());
        result
    }};
}
