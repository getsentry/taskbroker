use clap::Parser;
use std::fs;

pub mod config;
pub mod grpc;
pub mod kafka;
pub mod logging;
pub mod metrics;
pub mod pool;
pub mod push;
pub mod runtime_config;
pub mod store;
pub mod test_utils;
pub mod upkeep;

/// Name of the grpc service.
/// Using the service type to get a name wasn't working across modules.
pub const SERVICE_NAME: &str = "sentry_protos.taskbroker.v1.ConsumerService";

pub fn get_version() -> &'static str {
    let release_name = fs::read_to_string("./VERSION").expect("Unable to read version");
    Box::leak(release_name.into_boxed_str())
}

#[derive(Parser, Debug)]
pub struct Args {
    /// Path to the configuration file
    #[arg(short, long, help = "The path to a config file")]
    pub config: Option<String>,
}
