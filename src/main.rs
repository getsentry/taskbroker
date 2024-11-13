use anyhow::Error;
use clap::Parser;
use config::Config;
use consumer::{
    deserialize_activation::{self},
    inflight_activation_writer::{ActivationWriterConfig, InflightActivationWriter},
    kafka::start_consumer,
    os_stream_writer::{OsStream, OsStreamWriter},
};
use inflight_activation_store::InflightActivationStore;
use std::{sync::Arc, time::Duration};
use tokio::{select, signal, time};
use tracing::info;

#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod consumer;
#[allow(dead_code)]
mod inflight_activation_store;
mod logging;
mod metrics;

pub const VERSION: &str = env!("TASKWORKER_VERSION");

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
    let args = Args::parse();
    let config = Config::from_args(&args)?;

    logging::init(logging::LoggingConfig::from_config(&config));
    metrics::init(metrics::MetricsConfig::from_config(&config));

    let store = Arc::new(InflightActivationStore::new(&config.db_path).await?);
    let rpc_store = store.clone();

    tokio::spawn(async move {
        let mut timer = time::interval(Duration::from_millis(200));
        loop {
            select! {
                _ = signal::ctrl_c() => {
                    break;
                }
                _ = timer.tick() => {
                    let _ = rpc_store.get_pending_activation().await;
                    info!(
                        "Pending activation in store: {}",
                        rpc_store.count_pending_activations().await.unwrap()
                    );
                }
            }
        }
    });

    let kafka_config = config.kafka_client_config();

    start_consumer(
        [&config.kafka_topic as &str].as_ref(),
        &kafka_config,
        processing_strategy!({
            map: deserialize_activation::new(deserialize_activation::Config {
                deadletter_duration: None,
            }),

            reduce: InflightActivationWriter::new(
                store.clone(),
                ActivationWriterConfig::from_config(&config)
            ),

            err: OsStreamWriter::new(
                Duration::from_secs(1),
                OsStream::StdErr,
            ),
        }),
    )
    .await
}
