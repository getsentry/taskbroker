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

    logging::init(logging::LoggingConfig::from_config(&config));
    metrics::init(metrics::MetricsConfig::from_config(&config));

    let store = Arc::new(InflightActivationStore::new(&config.db_path).await?);
    let rpc_store = store.clone();

    // Upkeep thread
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

    // Consumer from kafka
    let kafka_topic = config.kafka_topic.clone();
    let kafka_topics = [kafka_topic.as_str()];
    let mut config = ClientConfig::new();
    config.set("group.id", "test-taskworker-consumer")
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        .set("enable.auto.offset.store", "false")
        .set_log_level(RDKafkaLogLevel::Debug);
    let consumer_task = start_consumer(
        kafka_topics.as_ref(),
        &config,
        processing_strategy!({
            map: deserialize_activation::new(deserialize_activation::Config {
                deadletter_duration: None,
            }),

            reduce: InflightActivationWriter::new(
                store.clone(),
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
    );
    
    // GRPC server
    let addr = "[::1]:50051".parse()?;
    let grpc_store = store.clone();
    let service = MyConsumerService{ store: grpc_store };

    let grpc_task = Server::builder()
        .add_service(ConsumerServiceServer::new(service))
        .serve(addr);

    select! {
        _ = consumer_task => {
            info!("Consumer task finished");
        }
        _ = grpc_task => {
            info!("GRPC task finished");
        }
    };

    Ok(())
}
