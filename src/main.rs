use anyhow::Error;
use clap::Parser;
use std::{sync::Arc, time::Duration};
use tokio::{select, signal, time};
use tonic::transport::Server;
use tracing::info;

use sentry_protos::sentry::v1::consumer_service_server::ConsumerServiceServer;

use taskbroker::config::Config;
use taskbroker::consumer::{
    deserialize_activation::{self, DeserializeConfig},
    inflight_activation_writer::{ActivationWriterConfig, InflightActivationWriter},
    kafka::start_consumer,
    os_stream_writer::{OsStream, OsStreamWriter},
};
use taskbroker::grpc_server::MyConsumerService;
use taskbroker::inflight_activation_store::InflightActivationStore;
use taskbroker::logging;
use taskbroker::metrics;
use taskbroker::processing_strategy;
use taskbroker::Args;

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
            let addr = format!("[::1]:{}", grpc_config.grpc_port)
                .parse()
                .expect("Failed to parse address");
            let service = MyConsumerService { store: grpc_store };

            let server = Server::builder()
                .add_service(ConsumerServiceServer::new(service))
                .serve(addr);

            select! {
                _ = signal::ctrl_c() => {
                    Ok(())
                }
                _ = server => {
                    Err(anyhow::anyhow!("GRPC server task failed"))
                }
            }
        }
    });

    let (consumer_result, grpc_server_result, upkeep_result) =
        tokio::join!(consumer_task, grpc_server_task, upkeep_task);

    let _ = consumer_result.expect("Consumer task failed");
    let _ = grpc_server_result.expect("GRPC server task failed");
    upkeep_result.expect("Upkeep task failed");
    Ok(())
}
