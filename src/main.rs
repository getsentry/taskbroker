use tonic::transport::Server;
use anyhow::Error;
use clap::Parser;
use rdkafka::{config::RDKafkaLogLevel, ClientConfig};
use std::{sync::Arc, time::Duration};
use tokio::{select, signal, time};

use sentry_protos::sentry::v1::consumer_service_server::ConsumerServiceServer;

use config::Config;
use grpc_server::MyConsumerService;
use consumer::{
    deserialize_activation::{self},
    inflight_activation_writer::{self, InflightActivationWriter},
    kafka::{start_consumer, ReduceShutdownBehaviour, ReducerWhenFullBehaviour},
    os_stream_writer::{OsStream, OsStreamWriter},
};
use inflight_activation_store::InflightActivationStore;
use tracing::info;

#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod consumer;
#[allow(dead_code)]
mod grpc_server;
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
    // let config = Config::from_args(&args)?;

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
        async move {
            let kafka_topic = config.kafka_topic.clone();
            let kafka_topics = [kafka_topic.as_str()];
            let mut kafka_config = ClientConfig::new();
            kafka_config.set("group.id", "test-taskworker-consumer")
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            .set("enable.auto.offset.store", "false")
            .set_log_level(RDKafkaLogLevel::Debug);
            start_consumer(
                kafka_topics.as_ref(),
                &kafka_config,
                processing_strategy!({
                    map: deserialize_activation::new(deserialize_activation::Config {
                        deadletter_duration: None,
                    }),

                    reduce: InflightActivationWriter::new(
                        consumer_store.clone(),
                        inflight_activation_writer::Config {
                            max_buf_len: 128,
                            max_pending_activations: 2048,
                            flush_interval: None,
                            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
                            shutdown_behaviour: ReduceShutdownBehaviour::Drop,
                        }
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
        async move {
            let addr = "[::1]:50051".parse().expect("Failed to parse address");
            let service = MyConsumerService{ store: grpc_store };

            let server =Server::builder()
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
