use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, anyhow};
use chrono::Utc;
use clap::Parser;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerServiceServer;
use taskbroker::push::updater::{EagerUpdater, LazyUpdater, Updater};
use taskbroker::worker::{Worker, WorkerClient, WorkerMap};
use tokio::signal::unix::SignalKind;
use tokio::task::JoinHandle;
use tokio::{select, time};
use tonic::transport::Server;
use tonic_health::ServingStatus;
use tracing::{debug, error, info, warn};

use taskbroker::config::store::DatabaseAdapter;
use taskbroker::config::{Config, DeliveryMode};
use taskbroker::fetch::FetchPool;
use taskbroker::grpc::auth_middleware::AuthLayer;
use taskbroker::grpc::metrics_middleware::MetricsLayer;
use taskbroker::grpc::server::{TaskbrokerServer, flush_updates};
use taskbroker::kafka::activation_batcher::{ActivationBatcher, ActivationBatcherConfig};
use taskbroker::kafka::activation_writer::{ActivationWriter, ActivationWriterConfig};
use taskbroker::kafka::admin::create_missing_topics;
use taskbroker::kafka::consumer::start_consumer;
use taskbroker::kafka::deserialize::{self, DeserializeConfig};
use taskbroker::kafka::os_stream_writer::{OsStream, OsStreamWriter};
use taskbroker::metrics;
use taskbroker::processing_strategy;
use taskbroker::push::PushPool;
use taskbroker::runtime_config::RuntimeConfigManager;
use taskbroker::store::adapters::postgres::{self, PostgresStore};
use taskbroker::store::adapters::sqlite::SqliteStore;
use taskbroker::store::traits::ActivationStore;
use taskbroker::upkeep::upkeep;
use taskbroker::{Args, get_version};
use taskbroker::{Run, logging};
use taskbroker::{SERVICE_NAME, flusher};

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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let config = Arc::new(Config::from_args(&args)?);
    let runtime_config_manager =
        Arc::new(RuntimeConfigManager::new(config.runtime_config_path.clone()).await);

    println!("taskbroker starting");
    println!("version: {}", get_version().trim());

    logging::init(logging::LoggingConfig::from_config(&config));
    metrics::init(metrics::MetricsConfig::from_config(&config));

    if args.run == Run::Migrations {
        return config.store.database_adapter.migrate(&config).await;
    }

    let store: Arc<dyn ActivationStore> = match config.store.database_adapter {
        DatabaseAdapter::Sqlite => Arc::new(SqliteStore::new(&config).await?),

        DatabaseAdapter::Postgres => {
            if config.store.pg.run_migrations {
                postgres::migrate(&config.store).await?;
            }

            Arc::new(PostgresStore::new(&config).await?)
        }
    };

    // If this is an environment where the topics might not exist, check and create them.
    if config.create_missing_topics {
        // Group every declared topic by its cluster so each is created on the
        // right brokers (main, retry, deadletter and produce-only topics, which
        // may live on different clusters).
        let mut topics_by_cluster: BTreeMap<&str, Vec<(&str, i32)>> = BTreeMap::new();
        for (topic_name, topic_config) in &config.kafka_topics {
            topics_by_cluster
                .entry(topic_config.cluster.as_str())
                .or_default()
                .push((topic_name.as_str(), config.default_topic_partitions));
        }
        for (cluster, topics) in topics_by_cluster {
            create_missing_topics(config.kafka_admin_config(cluster), &topics).await?;
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
    let upkeep_task = taskbroker::tokio::spawn({
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
    let maintenance_task = taskbroker::tokio::spawn({
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

    // Consumer(s) from kafka. Each consumed topic gets its own consumer (own
    // group.id and cluster), so we spawn one consumer task per consumable topic,
    // all sharing the one activation store.
    let consumer_topics: Vec<String> = config
        .consumable_topics()
        .expect("invalid config: no consumable topic")
        .into_iter()
        .map(|(name, _)| name.to_owned())
        .collect();

    let mut consumer_tasks: Vec<(String, JoinHandle<Result<(), Error>>)> = Vec::new();
    for topic in consumer_topics {
        let consumer_store = store.clone();
        let consumer_config = config.clone();
        let runtime_config_manager = runtime_config_manager.clone();
        let task_topic = topic.clone();

        let handle = taskbroker::tokio::spawn(async move {
            // The consumer has an internal thread that listens for cancellations, so it doesn't need
            // an outer select here like the other tasks.
            let topic_refs = [task_topic.as_str()];
            start_consumer(
                &topic_refs,
                &consumer_config.kafka_consumer_config_for(&task_topic),
                consumer_store.clone(),
                processing_strategy!({
                    err:
                        OsStreamWriter::new(
                            Duration::from_secs(1),
                            OsStream::StdErr,
                        ),

                    map:
                        deserialize::new(DeserializeConfig::from_topic(&consumer_config, &task_topic)),

                    reduce:
                        ActivationBatcher::new(
                            ActivationBatcherConfig::from_topic(&consumer_config, &task_topic),
                            runtime_config_manager.clone()
                        ),
                        ActivationWriter::new(
                            consumer_store.clone(),
                            ActivationWriterConfig::from_topic(&consumer_config, &task_topic)
                        ),

                }),
            )
            .await
        });
        consumer_tasks.push((topic, handle));
    }

    // Status update flush task
    let (status_update_tx, status_update_task) = if config.batch_status_updates {
        let (tx, rx) = tokio::sync::mpsc::channel(config.status_update_batch_size);

        let flusher_store = store.clone();
        let flusher_config = config.clone();

        let handle = taskbroker::tokio::spawn(async move {
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
        Some(taskbroker::tokio::spawn({
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
    let push_pool = PushPool::new(receiver, config.clone());
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

        // Create the correct kind of push updater
        let updater = if config.batch_push_updates {
            let lazy = LazyUpdater::new(config.clone(), store.clone());
            Arc::new(lazy) as Arc<dyn Updater>
        } else {
            let eager = EagerUpdater::new(store.clone());
            Arc::new(eager) as Arc<dyn Updater>
        };

        Some(taskbroker::tokio::spawn(async move {
            push_pool.start(workers, updater, store.clone()).await
        }))
    } else {
        None
    };

    // Initialize fetch threads
    let fetch_task = if config.delivery_mode == DeliveryMode::Push {
        Some(taskbroker::tokio::spawn(
            async move { fetch_pool.start().await },
        ))
    } else {
        None
    };

    let mut departure = elegant_departure::tokio::depart()
        .on_termination()
        .on_sigint()
        .on_signal(SignalKind::hangup())
        .on_signal(SignalKind::quit())
        .on_completion(log_task_completion("upkeep_task", upkeep_task))
        .on_completion(log_task_completion("maintenance_task", maintenance_task));

    for (topic, handle) in consumer_tasks {
        departure =
            departure.on_completion(log_task_completion(format!("consumer:{topic}"), handle));
    }

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
