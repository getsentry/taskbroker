use clap::Parser;
use std::fs;

pub mod config;
pub mod grpc;
pub mod kafka;
pub mod logging;
pub mod metrics;
pub mod store;
pub mod upkeep;

#[cfg(test)]
pub mod test_utils;

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
