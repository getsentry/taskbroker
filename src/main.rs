use anyhow::Error;
use clap::Parser;
use config::Config;
use inflight_task_store::InflightTaskStore;

mod config;
#[allow(dead_code)]
mod inflight_task_store;

#[derive(Parser, Debug)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, help = "The path to a config file")]
    config: Option<String>,

    #[arg(short, long, help = "Set the logging level filter")]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Read command line options
    let args = Args::parse();
    let config = Config::from_args(&args)?;

    InflightTaskStore::new(&config.db_path).await?;
    Ok(())
}
