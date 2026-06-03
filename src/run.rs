use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, anyhow};
use chrono::Utc;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerServiceServer;
use sqlx::ConnectOptions;
use sqlx::postgres::PgConnectOptions;
use tokio::signal::unix::SignalKind;
use tokio::task::JoinHandle;
use tokio::{select, time};
use tonic::transport::Server;
use tonic_health::ServingStatus;
use tracing::{debug, error, info, warn};

use crate::config::{Config, DatabaseAdapter, DeliveryMode};
use crate::fetch::FetchPool;
use crate::grpc::auth_middleware::AuthLayer;
use crate::grpc::metrics_middleware::MetricsLayer;
use crate::grpc::server::{TaskbrokerServer, flush_updates};
use crate::kafka::activation_batcher::{ActivationBatcher, ActivationBatcherConfig};
use crate::kafka::activation_writer::{ActivationWriter, ActivationWriterConfig};
use crate::kafka::admin::create_missing_topics;
use crate::kafka::consumer::start_consumer;
use crate::kafka::deserialize::{self, DeserializeConfig};
use crate::kafka::os_stream_writer::{OsStream, OsStreamWriter};
use crate::processing_strategy;
use crate::push::PushPool;
use crate::runtime_config::RuntimeConfigManager;
use crate::store::adapters::postgres::{
    PostgresStore, PostgresStoreConfig, create_default_postgres_pool,
};
use crate::store::adapters::sqlite::{SqliteStore, SqliteStoreConfig};
use crate::store::traits::ActivationStore;
use crate::upkeep::upkeep;
use crate::worker::{Worker, WorkerClient, WorkerMap};
use crate::{SERVICE_NAME, flusher};

async fn log_task_completion<T: AsRef<str>>(name: T, task: JoinHandle<Result<(), Error>>) {
    match task.await {
        Ok(Ok(())) => {
            info!("Task {} completed", name.as_ref());
        }
        Ok(Err(e)) => {
            error!("Task {} failed: {:?}", name.as_ref(), e);
        }
        Err(e) => {
            error!("Task {} panicked: {:?}", name.as_ref(), e);
        }
    }
}

/// Run taskbroker.
pub async fn broker(config: Arc<Config>) -> Result<(), Error> {
    let runtime_config_manager =
        Arc::new(RuntimeConfigManager::new(config.runtime_config_path.clone()).await);

    let store: Arc<dyn ActivationStore> = match config.database_adapter {
        DatabaseAdapter::Sqlite => Arc::new(
            SqliteStore::new(&config.db_path, SqliteStoreConfig::from_config(&config)).await?,
        ),
        DatabaseAdapter::Postgres => {
            Arc::new(PostgresStore::new(PostgresStoreConfig::from_config(&config)).await?)
        }
    };

    // If this is an environment where the topics might not exist, check and create them.
    if config.create_missing_topics {
        let kafka_client_config = config.kafka_consumer_config();
        let (main_topic, _) = config
            .consumable_topic()
            .map_err(|e| anyhow!("invalid config: {}", e))?;
        create_missing_topics(
            kafka_client_config.clone(),
            main_topic,
            config.default_topic_partitions,
        )
        .await?;

        // Create retry topic if configured
        if let Some(ref retry_topic) = config.kafka_retry_topic {
            create_missing_topics(
                kafka_client_config,
                retry_topic,
                config.default_topic_partitions,
            )
            .await?;
        }
    }

    if config.full_vacuum_on_start {
        info!("Running full vacuum on database");
        match store.full_vacuum_db().await {
            Ok(_) => info!("Full vacuum completed."),
            Err(err) => error!("Failed to run full vacuum on startup: {:?}", err),
        }
    }
    // Get startup time after migrations and vacuum
    let startup_time = Utc::now();

    // Taskbroker exposes a grpc.v1.health endpoint. We use upkeep to track the health
    // of the application.
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status(SERVICE_NAME, ServingStatus::Serving)
        .await;

    // Upkeep loop
    let upkeep_task = crate::tokio::spawn({
        let upkeep_store = store.clone();
        let upkeep_config = config.clone();
        let runtime_config_manager = runtime_config_manager.clone();
        async move {
            upkeep(
                upkeep_config,
                upkeep_store,
                startup_time,
                runtime_config_manager.clone(),
                health_reporter.clone(),
            )
            .await?;
            Ok(())
        }
    });

    // Maintenance task loop
    let maintenance_task = crate::tokio::spawn({
        let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
        let maintenance_store = store.clone();
        let mut timer = time::interval(Duration::from_millis(config.maintenance_task_interval_ms));
        timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        async move {
            loop {
                select! {
                    _ = timer.tick() => {
                        match maintenance_store.vacuum_db().await {
                            Ok(_) => debug!("ran maintenance vacuum"),
                            Err(err) => warn!("failed to run maintenance vacuum {:?}", err),
                        }
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
    let consumer_task = crate::tokio::spawn({
        let consumer_store = store.clone();
        let consumer_config = config.clone();
        let runtime_config_manager = runtime_config_manager.clone();

        // Build list of topics to consume from
        let (main_topic, _) = consumer_config
            .consumable_topic()
            .expect("invalid config: no consumable topic");
        let topics_to_consume = [main_topic.to_owned()];

        async move {
            // The consumer has an internal thread that listens for cancellations, so it doesn't need
            // an outer select here like the other tasks.
            let topic_refs: Vec<&str> = topics_to_consume.iter().map(|s| s.as_str()).collect();
            start_consumer(
                &topic_refs,
                &consumer_config.kafka_consumer_config(),
                consumer_store.clone(),
                processing_strategy!({
                    err:
                        OsStreamWriter::new(
                            Duration::from_secs(1),
                            OsStream::StdErr,
                        ),

                    map:
                        deserialize::new(DeserializeConfig::from_config(&consumer_config)),

                    reduce:
                        ActivationBatcher::new(
                            ActivationBatcherConfig::from_config(&consumer_config),
                            runtime_config_manager.clone()
                        ),
                        ActivationWriter::new(
                            consumer_store.clone(),
                            ActivationWriterConfig::from_config(&consumer_config)
                        ),

                }),
            )
            .await
        }
    });

    // Status update flush task
    let (status_update_tx, status_update_task) = if config.batch_status_updates {
        let (tx, rx) = tokio::sync::mpsc::channel(config.status_update_batch_size);

        let flusher_store = store.clone();
        let flusher_config = config.clone();

        let handle = crate::tokio::spawn(async move {
            flusher::run_flusher(
                rx,
                flusher_config.status_update_batch_size,
                flusher_config.status_update_interval_ms,
                move |buffer| Box::pin(flush_updates(flusher_store.clone(), buffer)),
            )
            .await
        });

        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    // GRPC server - only start if port is configured (port 0 disables it)
    let grpc_server_task = if config.grpc_port > 0 {
        Some(crate::tokio::spawn({
            let grpc_store = store.clone();
            let grpc_config = config.clone();
            let grpc_status_tx = status_update_tx.clone();

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
                        config: grpc_config,
                        update_tx: grpc_status_tx,
                    }))
                    .add_service(health_service.clone())
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
        }))
    } else {
        info!("GRPC server disabled (grpc_port=0)");
        None
    };

    // Initialize push queue
    let (sender, receiver) = flume::bounded(config.push_queue_size);

    // Initialize push and fetch pools
    let push_pool = PushPool::new(receiver, config.clone(), store.clone());
    let fetch_pool = FetchPool::new(sender, store.clone(), config.clone());

    // Initialize push threads
    let push_task = if config.delivery_mode == DeliveryMode::Push {
        let mut workers: Vec<WorkerMap> = vec![];

        // For every push thread, create a map from applications to worker connections
        for _ in 0..config.push_threads {
            let mut map = HashMap::new();

            for (application, endpoint) in config.worker_map.clone() {
                let worker = match Worker::connect(config.clone(), endpoint).await {
                    Ok(w) => {
                        debug!("Connected to worker!");
                        Box::new(w) as Box<dyn WorkerClient>
                    }

                    Err(e) => {
                        error!(error = ?e, "Failed to connect to worker");
                        return Err(e);
                    }
                };

                map.insert(application, worker);
            }

            workers.push(map);
        }

        Some(crate::tokio::spawn(async move {
            push_pool.start(workers).await
        }))
    } else {
        None
    };

    // Initialize fetch threads
    let fetch_task = if config.delivery_mode == DeliveryMode::Push {
        Some(crate::tokio::spawn(async move { fetch_pool.start().await }))
    } else {
        None
    };

    let mut departure = elegant_departure::tokio::depart()
        .on_termination()
        .on_sigint()
        .on_signal(SignalKind::hangup())
        .on_signal(SignalKind::quit())
        .on_completion(log_task_completion("consumer", consumer_task))
        .on_completion(log_task_completion("upkeep_task", upkeep_task))
        .on_completion(log_task_completion("maintenance_task", maintenance_task));

    if let Some(task) = grpc_server_task {
        departure = departure.on_completion(log_task_completion("grpc_server", task));
    }

    if let Some(task) = push_task {
        departure = departure.on_completion(log_task_completion("push_task", task));
    }

    if let Some(task) = fetch_task {
        departure = departure.on_completion(log_task_completion("fetch_task", task));
    }

    if let Some(task) = status_update_task {
        departure = departure.on_completion(log_task_completion("status_update_task", task));
    }

    departure.await;
    Ok(())
}

/// Run migrations.
pub async fn migrations(config: Arc<Config>) -> Result<(), Error> {
    if config.database_adapter == DatabaseAdapter::Sqlite {
        return Ok(());
    }

    let mut conn_opts = PgConnectOptions::new()
        .username(&config.pg_ddl_username)
        .password(&config.pg_ddl_password)
        .host(&config.pg_host)
        .port(config.pg_port);

    if let Some(extra_query_params) = config.pg_extra_query_params.as_ref() {
        let url = conn_opts.to_url_lossy();
        let new_url =
            url.as_ref().split('?').next().unwrap().to_string() + "?" + extra_query_params;
        conn_opts = PgConnectOptions::from_str(&new_url).unwrap();
    }

    let default_pool =
        create_default_postgres_pool(&conn_opts, &config.pg_default_database_name).await?;

    // Create the database if it doesn't exist
    let row: (bool,) =
        sqlx::query_as("SELECT EXISTS ( SELECT 1 FROM pg_catalog.pg_database WHERE datname = $1 )")
            .bind(&config.pg_database_name)
            .fetch_one(&default_pool)
            .await?;

    if !row.0 {
        println!("Creating database {}", &config.pg_database_name);
        sqlx::query(format!("CREATE DATABASE {}", &config.pg_database_name).as_str())
            .bind(&config.pg_database_name)
            .execute(&default_pool)
            .await?;
    }

    // Close the default pool
    default_pool.close().await;

    println!("Running migrations on database");
    sqlx::migrate!("./migrations/postgres")
        .run(&default_pool)
        .await?;

    Ok(())
}
