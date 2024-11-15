use tonic::transport::Server;
use anyhow::Error;
use clap::Parser;
use std::{sync::Arc, time::Duration};
use tokio::{select, signal, time};

use sentry_protos::sentry::v1::consumer_service_server::ConsumerServiceServer;

use config::Config;
use taskbroker::grpc_server::MyConsumerService;
use consumer::{
    deserialize_activation::{self, DeserializeConfig},
    inflight_activation_writer::{ActivationWriterConfig, InflightActivationWriter},
    kafka::start_consumer,
    os_stream_writer::{OsStream, OsStreamWriter},
};
use taskbroker::inflight_activation_store::InflightActivationStore;
use tracing::info;

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
    let config = Arc::new(Config::from_args(&args)?);

    logging::init(logging::LoggingConfig::from_config(&config));
    metrics::init(metrics::MetricsConfig::from_config(&config));
    let store = Arc::new(InflightActivationStore::new(&config.db_path).await?);

    // Upkeep thread
    let upkeep_task = tokio::spawn({
        let upkeep_store = store.clone();
        async move {
            let mut timer = time::interval(Duration::from_millis(200));
            loop {
                select! {
                _ = signal::ctrl_c() => {
                    break;
                }
                _ = timer.tick() => {
                    let _ = upkeep_store.get_pending_activation().await;
                    info!(
                        "Pending activation in store: {}",
                        upkeep_store.count_pending_activations().await.unwrap()
                    );
                }
                }
            }
        }
    });

    // Consumer from kafka
    let consumer_task = tokio::spawn({
        let consumer_store = store.clone();
        let consumer_config = config.clone();
        async move {
            let kafka_config = consumer_config.kafka_client_config();

            start_consumer(
                [&consumer_config.kafka_topic as &str].as_ref(),
                &kafka_config,
                processing_strategy!({
                    map: deserialize_activation::new(DeserializeConfig::from_config(&consumer_config)),
        
                    reduce: InflightActivationWriter::new(
                        consumer_store.clone(),
                        ActivationWriterConfig::from_config(&consumer_config)
                    ),

                    err: OsStreamWriter::new(
                        Duration::from_secs(1),
                        OsStream::StdErr,
                    ),
                }),
            ).await
        }
    });

    // GRPC server
    let grpc_server_task = tokio::spawn({
        let grpc_store = store.clone();
        let grpc_config = config.clone();
        async move {
            let addr = format!("[::1]:{}", grpc_config.grpc_port).parse().expect("Failed to parse address");
            let service = MyConsumerService{ store: grpc_store };

            let server = Server::builder()
                .add_service(ConsumerServiceServer::new(service))
                .serve(addr);
            
                select! {
                    _ = signal::ctrl_c() => {
                        return Ok(());
                    }
                    _ = server => {
                        return Err(anyhow::anyhow!("GRPC server task failed"));
                    }
                }
        }
    });

    let results = tokio::join!(consumer_task, grpc_server_task, upkeep_task);
    let _ = results.0.expect("Consumer task failed");
    let _ = results.1.expect("GRPC server task failed");
    let _ = results.2.expect("Upkeep task failed");
    Ok(())
}
