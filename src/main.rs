use anyhow::{anyhow, Error};
use clap::Parser;
use std::{sync::Arc, time::Duration};
use tokio::signal::unix::{signal, SignalKind};
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

async fn check_for_shutdown() {
    let (mut hangup, mut interrupt, mut terminate, mut quit) = (
        signal(SignalKind::hangup()).expect("error creating signal stream for hangup"),
        signal(SignalKind::interrupt()).expect("error creating signal stream for hangup"),
        signal(SignalKind::terminate()).expect("error creating signal stream for hangup"),
        signal(SignalKind::quit()).expect("error creating signal stream for hangup"),
    );
    select! {
        _ = hangup.recv() => {
            info!("Hangup received, shutting down");
        }
        _ = interrupt.recv() => {
            info!("Interrupt received, shutting down");
        }
        _ = terminate.recv() => {
            info!("Terminate received, shutting down");
        }
        _ = quit.recv() => {
            info!("Quit received, shutting down");
        }
    }
}

async fn log_task_completion(name: &str, task: JoinHandle<Result<(), Error>>) {
    match task.await {
        Ok(Ok(())) => {
            info!("Task {} completed", name);
        }
        Ok(Err(e)) => {
            error!("Task {} failed: {:?}", name, e);
        }
        Err(e) => {
            error!("Task {} failed: {:?}", name, e);
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
        async move {
            let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
            let mut timer = time::interval(Duration::from_millis(200));
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
            let kafka_config = consumer_config.kafka_client_config();
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
            let addr = format!("[::1]:{}", grpc_config.grpc_port)
                .parse()
                .expect("Failed to parse address");
            let service = MyConsumerService { store: grpc_store };

            let server = Server::builder()
                .add_service(ConsumerServiceServer::new(service))
                .add_service(health_service)
                .serve(addr);

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

    /*
    This select is waiting for either:
    - A shutdown signal, in which case it signals the tasks to shutdown
    - A task completion, in which case it logs the completion (including if its an error)

    Each of the tasks is also using an elegant_departure guard, so if one of them shuts down,
    it will automatically trigger the shutdown of the other tasks as well. Then the code awaits
    the shutdown of all the remaining tasks to ensure everything has shut down cleanly.
    */
    select! {
        _ = check_for_shutdown() => {
            info!("Received shutdown signal, shutting down");
            // Wait for the tasks to finish
            elegant_departure::shutdown().await;
        }
        _ = log_task_completion("consumer", consumer_task) => { }
        _ = log_task_completion("grpc_server", grpc_server_task) => { }
        _ = log_task_completion("upkeep_task", upkeep_task) => { }
    }

    elegant_departure::wait_for_shutdown_complete().await;
    Ok(())
}
