use std::{sync::Arc, time::Duration};

use anyhow::Error;
use chrono::Utc;
use consumer::{
    deserialize_activation::{self, DeserializerConfig},
    inflight_activation_writer::{InflightTaskWriter, InflightTaskWriterConfig},
    kafka::{start_consumer, ReduceShutdownBehaviour, ReducerWhenFullBehaviour},
    os_stream_writer::{OsStream, OsStreamWriter},
};
use inflight_activation_store::InflightActivationStore;
use rdkafka::{config::RDKafkaLogLevel, ClientConfig};
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
            map: deserialize_activation::new(DeserializerConfig {
                deadletter_duration,
            }),

            reduce: InflightTaskWriter::new(
                store.clone(),
                InflightTaskWriterConfig {
                    max_buf_len: 2048,
                    flush_interval: Some(Duration::from_secs(1)),
                    when_full_behaviour: ReducerWhenFullBehaviour::Backpressure,
                    shutdown_behaviour: ReduceShutdownBehaviour::Flush,
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
