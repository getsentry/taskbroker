use std::{sync::Arc, time::Duration};

use anyhow::Error;
use chrono::Utc;
use consumer::{
    deserialize_activation::{self},
    inflight_activation_writer::{self, InflightActivationWriter},
    kafka::{start_consumer, ReduceShutdownBehaviour, ReducerWhenFullBehaviour},
    os_stream_writer::{OsStream, OsStreamWriter},
};
use inflight_activation_store::InflightActivationStore;
use rdkafka::{config::RDKafkaLogLevel, ClientConfig};
use tokio::{select, signal, time};
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod consumer;
#[allow(dead_code)]
mod inflight_activation_store;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let topic = "hackweek";
    let consumer_group = "test-taskworker-consumer";
    let bootstrap_servers = "127.0.0.1:9092";

    let store = Arc::new(
        InflightActivationStore::new(&format!("taskworker_{:?}.sqlite", Utc::now())).await?,
    );
    let deadletter_duration = Some(Duration::from_secs(1));

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

    start_consumer(
        [topic].as_ref(),
        ClientConfig::new()
            .set("group.id", consumer_group)
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            .set("enable.auto.offset.store", "false")
            .set_log_level(RDKafkaLogLevel::Debug),
        processing_strategy!({
            map: deserialize_activation::new(deserialize_activation::Config {
                deadletter_duration,
            }),

            reduce: InflightActivationWriter::new(
                store.clone(),
                inflight_activation_writer::Config {
                    max_buf_len: 128,
                    max_pending_tasks: 2048,
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
    )
    .await
}
