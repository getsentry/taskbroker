use clap::Parser;

#[allow(dead_code)]
pub mod config;
#[allow(dead_code)]
pub mod consumer;
#[allow(dead_code)]
pub mod grpc_server;
#[allow(dead_code)]
pub mod inflight_activation_store;
pub mod logging;
pub mod metrics;

pub const VERSION: &str = env!("TASKWORKER_VERSION");

#[derive(Parser, Debug)]
pub struct Args {
    /// Path to the configuration file
    #[arg(short, long, help = "The path to a config file")]
    pub config: Option<String>,

    #[arg(short, long, help = "Set the logging level filter")]
    pub log_level: Option<String>,
}
