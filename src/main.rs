use std::sync::Arc;

use anyhow::Error;
use clap::Parser;

use taskbroker::config::Config;
use taskbroker::metrics;
use taskbroker::{Args, get_version};
use taskbroker::{Run, logging, run};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let config = Arc::new(Config::from_args(&args)?);

    println!("taskbroker starting");
    println!("version: {}", get_version().trim());

    logging::init(logging::LoggingConfig::from_config(&config));
    metrics::init(metrics::MetricsConfig::from_config(&config));

    match args.run {
        Run::Broker => run::broker(config).await,
        Run::Migrations => run::migrations(config).await,
    }
}
