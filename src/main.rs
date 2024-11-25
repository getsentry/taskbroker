use anyhow::{anyhow, Error};
use clap::Parser;
use std::{sync::Arc, time::Duration};
use tokio::signal::unix::SignalKind;
use tokio::task::JoinHandle;
use tokio::{select, time};
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{error, info};

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

async fn log_task_completion(name: &str, task: JoinHandle<Result<(), Error>>) {
    match task.await {
        Ok(Ok(())) => {
            info!("Task {} completed", name);
        }
        Ok(Err(e)) => {
            error!("Task {} failed: {:?}", name, e);
        }
        Err(e) => {
            error!("Task {} panicked: {:?}", name, e);
        }
    }
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
        let upkeep_config = config.clone();
        async move {
            let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
            let mut timer = time::interval(Duration::from_millis(upkeep_config.upkeep_interval));
            loop {
                select! {
                    _ = timer.tick() => {
                        let _ = upkeep_store.get_pending_activation().await;
                        info!(
                            "Pending activation in store: {}",
                            upkeep_store.count_pending_activations().await.unwrap()
                        );
                    }
                    _ = guard.wait() => {
                        info!("Cancellation token received, shutting down upkeep");
                        break;
                    }
                }
            }
            Ok(())
        }
    });

    // Consumer from kafka
    let consumer_task = tokio::spawn({
        let consumer_store = store.clone();
        let consumer_config = config.clone();
        async move {
            let kafka_topic = consumer_config.kafka_topic.clone();
            let topic_list = [kafka_topic.as_str()];
            let kafka_config = consumer_config.kafka_consumer_config();
            // The consumer has an internal thread that listens for cancellations, so it doesn't need
            // an outer select here like the other tasks.
            start_consumer(
                &topic_list,
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
            let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
            let (mut health_reporter_fn, health_service) = health_reporter();
            health_reporter_fn
                .set_serving::<ConsumerServiceServer<MyConsumerService>>()
                .await;
            let addr = format!("{}:{}", grpc_config.grpc_addr, grpc_config.grpc_port)
                .parse()
                .expect("Failed to parse address");
            let service = MyConsumerService { store: grpc_store };

            let server = Server::builder()
                .add_service(ConsumerServiceServer::new(service))
                .add_service(health_service)
                .serve(addr);
            info!("GRPC server listening on {}", addr);
            select! {
                biased;

                res = server => {
                    info!("GRPC server task failed, shutting down");
                    health_reporter_fn.set_not_serving::<ConsumerServiceServer<MyConsumerService>>().await;

                    // Wait for any running requests to drain
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    match res {
                        Ok(()) => Ok(()),
                        Err(e) => Err(anyhow!("GRPC server task failed: {:?}", e)),
                    }
                }
                _ = guard.wait() => {
                    info!("Cancellation token received, shutting down GRPC server");
                    health_reporter_fn.set_not_serving::<ConsumerServiceServer<MyConsumerService>>().await;

                    // Wait for any running requests to drain
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok(())
                }
            }
        }
    });

    elegant_departure::tokio::depart()
        .on_termination()
        .on_sigint()
        .on_signal(SignalKind::hangup())
        .on_signal(SignalKind::quit())
        .on_completion(log_task_completion("consumer", consumer_task))
        .on_completion(log_task_completion("grpc_server", grpc_server_task))
        .on_completion(log_task_completion("upkeep_task", upkeep_task))
        .await;

    Ok(())
}
