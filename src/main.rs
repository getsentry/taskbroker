use anyhow::{Error, anyhow};
use clap::Parser;
use std::{sync::Arc, time::Duration};
use taskbroker::kafka::inflight_activation_batcher::{
    ActivationBatcherConfig, InflightActivationBatcher,
};
use taskbroker::upkeep::upkeep;
use tokio::signal::unix::SignalKind;
use tokio::task::JoinHandle;
use tokio::{select, time};
use tonic::transport::Server;
use tracing::{error, info};

use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerServiceServer;

use taskbroker::Args;
use taskbroker::config::Config;
use taskbroker::grpc::auth_middleware::AuthLayer;
use taskbroker::grpc::metrics_middleware::MetricsLayer;
use taskbroker::grpc::server::TaskbrokerServer;
use taskbroker::kafka::{
    admin::create_missing_topics,
    consumer::start_consumer,
    deserialize_activation::{self},
    inflight_activation_writer::{ActivationWriterConfig, InflightActivationWriter},
    os_stream_writer::{OsStream, OsStreamWriter},
};
use taskbroker::logging;
use taskbroker::metrics;
use taskbroker::processing_strategy;
use taskbroker::store::inflight_activation::{
    InflightActivationStore, InflightActivationStoreConfig,
};

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

fn main() -> Result<(), Error> {
    let args = Args::parse();
    let config = Arc::new(Config::from_args(&args)?);

    let mut basic = tokio::runtime::Builder::new_multi_thread();
    let mut runtime = basic.enable_all();
    if config.worker_threads > 0 {
        runtime = runtime.worker_threads(config.worker_threads);
    }

    runtime
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { runnable().await })
}

async fn runnable() -> Result<(), Error> {
    let args = Args::parse();
    let config = Arc::new(Config::from_args(&args)?);

    logging::init(logging::LoggingConfig::from_config(&config));
    metrics::init(metrics::MetricsConfig::from_config(&config));
    let store = Arc::new(
        InflightActivationStore::new(
            &config.db_path,
            InflightActivationStoreConfig::from_config(&config),
        )
        .await?,
    );

    // If this is an environment where the topics might not exist, check and create them.
    if config.create_missing_topics {
        let kafka_client_config = config.kafka_consumer_config();
        create_missing_topics(
            kafka_client_config,
            &config.kafka_topic,
            config.default_topic_partitions,
        )
        .await?;
    }

    // Upkeep loop
    let upkeep_task = tokio::spawn({
        let upkeep_store = store.clone();
        let upkeep_config = config.clone();
        async move {
            upkeep(upkeep_config, upkeep_store).await;
            Ok(())
        }
    });

    // Maintenance task loop
    let maintenance_task = tokio::spawn({
        let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
        let maintenance_store = store.clone();
        // TODO make this configurable.
        let mut timer = time::interval(Duration::from_secs(60));
        timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        async move {
            loop {
                select! {
                    _ = timer.tick() => {
                        let _ = maintenance_store.vacuum_db().await;
                        info!("ran maintenance vacuum");
                    },
                    _ = guard.wait() => {
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
            // The consumer has an internal thread that listens for cancellations, so it doesn't need
            // an outer select here like the other tasks.
            start_consumer(
                &[&consumer_config.kafka_topic],
                &consumer_config.kafka_consumer_config(),
                processing_strategy!({
                    map:
                        deserialize_activation::new(),

                    reduce:
                        InflightActivationBatcher::new(
                            ActivationBatcherConfig::from_config(&consumer_config)
                        )
                        => InflightActivationWriter::new(
                            consumer_store.clone(),
                            ActivationWriterConfig::from_config(&consumer_config)
                        ),

                    err:
                        OsStreamWriter::new(
                            Duration::from_secs(1),
                            OsStream::StdErr,
                        ),
                }),
            )
            .await
        }
    });

    // GRPC server
    let grpc_server_task = tokio::spawn({
        let grpc_store = store.clone();
        let grpc_config = config.clone();
        async move {
            let addr = format!("{}:{}", grpc_config.grpc_addr, grpc_config.grpc_port)
                .parse()
                .expect("Failed to parse address");

            let layers = tower::ServiceBuilder::new()
                .layer(MetricsLayer::default())
                .layer(AuthLayer::new(&grpc_config))
                .into_inner();

            let server = Server::builder()
                .layer(layers)
                .add_service(ConsumerServiceServer::new(TaskbrokerServer {
                    store: grpc_store,
                }))
                .serve(addr);

            let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
            info!("GRPC server listening on {}", addr);
            select! {
                biased;

                res = server => {
                    info!("GRPC server task failed, shutting down");

                    // Wait for any running requests to drain
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    match res {
                        Ok(()) => Ok(()),
                        Err(e) => Err(anyhow!("GRPC server task failed: {:?}", e)),
                    }
                }
                _ = guard.wait() => {
                    info!("Cancellation token received, shutting down GRPC server");

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
        .on_completion(log_task_completion("maintenance_task", maintenance_task))
        .await;

    Ok(())
}
