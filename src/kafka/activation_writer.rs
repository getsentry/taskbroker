//! A single, process-wide activation writer shared by every consumer.
//!
//! All consumers feed it raw deserialized activations via a thin
//! [`ActivationWriterClient`] reduce stage. The shared [`ActivationWriter`] task
//! applies the global filters, forwards demoted-namespace tasks through a single
//! shared producer, and coalesces activations across consumers into larger DB
//! writes.
//!
//! ## Durability / commit coupling
//!
//! `ActivationWriterClient::flush()` ships its locally accumulated batch to the
//! writer and blocks until the writer confirms the batch was **durably stored**.
//! Only then does it return `Ok(Some(()))`, which lets the generic `reduce()`
//! wrapper hand the batch's Kafka messages to the per-consumer `commit` actor.
//! Because each client ships sequentially (it blocks until the previous batch is
//! durable), per-partition offsets are confirmed in order and the existing
//! max-offset commit logic stays correct.
//!
//! There is deliberately no "retry, re-ship" signal back to the client: once the
//! activations are handed to the writer the client no longer holds them, so it
//! must not advance to newer offsets until they are durable. Backpressure and
//! write failures are therefore retried **inside** the writer; the client simply
//! waits. If the writer shuts down it drops the ack channel, the client observes
//! a closed channel, returns `Ok(None)` (no commit), and Kafka redelivers — the
//! at-least-once safety net.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Error;
use chrono::Utc;
use futures::future::join_all;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tracing::{debug, error, instrument};

use crate::config::Config;
use crate::runtime_config::RuntimeConfigManager;
use crate::store::activation::{Activation, ActivationStatus};
use crate::store::traits::ActivationStore;
use crate::store::types::DepthCounts;

use super::consumer::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};

/// How long to wait between re-checks while the writer is backpressured by the
/// database depth limits. Mirrors the consumer's repoll cadence.
const BACKPRESSURE_POLL_MS: u64 = 250;

/// Upper bound on how many consumer batches the writer fuses into one DB write,
/// expressed as a multiple of `db_insert_batch_max_len`. Bounds the size of a
/// single INSERT while still allowing several consumers' batches to coalesce.
const COALESCE_FACTOR: usize = 4;

/// A batch shipped from one [`ActivationWriterClient`] to the writer, with a one-shot
/// channel the writer signals once the batch is durably stored.
pub struct ActivationWriteRequest {
    /// The consumed topic the activations came from. Used for the
    /// demoted-namespace self-forward guard and for metric tags.
    pub source_topic: Arc<str>,
    pub activations: Vec<Activation>,
    /// Signalled (with `()`) once the batch has been durably persisted.
    pub ack: oneshot::Sender<()>,
}

/// Activations from one or more requests, classified by disposition and ready
/// to be forwarded/written. The `acks` are fired together once the DB write of
/// `db_batch` succeeds.
struct PendingWrite {
    db_batch: Vec<Activation>,
    forward_payloads: Vec<Vec<u8>>,
    acks: Vec<oneshot::Sender<()>>,
}

pub struct ActivationWriterConfig {
    pub producer_config: ClientConfig,
    pub kafka_long_topic: String,
    pub send_timeout_ms: u64,
    /// Maximum number of activations fused into one DB write.
    pub max_combined_len: usize,
    pub max_pending_activations: usize,
    pub max_processing_activations: usize,
    pub max_delay_activations: usize,
    pub db_max_size: Option<u64>,
    pub write_failure_backoff_ms: u64,
    pub channel_capacity: usize,
}

impl ActivationWriterConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            producer_config: config.kafka_producer_config(),
            kafka_long_topic: config.kafka_long_topic.clone(),
            send_timeout_ms: config.kafka_send_timeout_ms,
            max_combined_len: config.db_insert_batch_max_len * COALESCE_FACTOR,
            max_pending_activations: config.max_pending_count,
            max_processing_activations: config.max_processing_count,
            max_delay_activations: config.max_delay_count,
            db_max_size: config.db_max_size,
            write_failure_backoff_ms: config.db_write_failure_backoff_ms,
            channel_capacity: config.db_insert_batch_max_len,
        }
    }
}

/// Spawn the shared activation writer. Returns the sender used to construct
/// [`ActivationWriterClient`]s and the task handle (so the caller can await its final
/// drain at shutdown). The writer exits, after handling shutdown, once every
/// sender has been dropped.
pub fn spawn(
    store: Arc<dyn ActivationStore>,
    runtime_config_manager: Arc<RuntimeConfigManager>,
    config: ActivationWriterConfig,
) -> (
    mpsc::Sender<ActivationWriteRequest>,
    tokio::task::JoinHandle<Result<(), Error>>,
) {
    let (tx, rx) = mpsc::channel(config.channel_capacity);

    let producer: FutureProducer = config
        .producer_config
        .create()
        .expect("Could not create kafka producer in shared writer");
    let producer_cluster = config
        .producer_config
        .get("bootstrap.servers")
        .expect("producer config always sets bootstrap.servers")
        .to_owned();

    let writer = ActivationWriter {
        store,
        runtime_config_manager,
        producer: Arc::new(producer),
        producer_cluster,
        config,
        rx,
    };

    let handle = crate::tokio::spawn(writer.run());

    (tx, handle)
}

struct ActivationWriter {
    store: Arc<dyn ActivationStore>,
    runtime_config_manager: Arc<RuntimeConfigManager>,
    producer: Arc<FutureProducer>,
    producer_cluster: String,
    config: ActivationWriterConfig,
    rx: mpsc::Receiver<ActivationWriteRequest>,
}

impl ActivationWriter {
    #[instrument(skip_all)]
    async fn run(mut self) -> Result<(), Error> {
        let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();

        loop {
            // Gather phase: block for the first request, then greedily drain
            // whatever else is already queued so concurrently-shipped batches
            // from different consumers fuse into one write. The store latency of
            // the previous cycle acts as the natural gather window (group commit).
            let first = select! {
                biased;
                _ = guard.wait() => break,
                req = self.rx.recv() => req,
            };
            let Some(first) = first else {
                // All clients dropped their senders: orderly drain complete.
                break;
            };

            let mut raw = vec![first];
            let mut estimate = raw[0].activations.len();
            while estimate < self.config.max_combined_len {
                match self.rx.try_recv() {
                    Ok(req) => {
                        estimate += req.activations.len();
                        raw.push(req);
                    }
                    Err(_) => break,
                }
            }

            let coalesced = raw.len();
            metrics::histogram!("consumer.activation_writer.coalesced_requests")
                .record(coalesced as f64);

            let mut pending = self.classify(raw).await;

            // Forwards are best-effort and sent exactly once, before the write
            // loop, so a store retry never re-forwards (matches the old batcher,
            // which forwarded independently of the writer).
            self.forward(&pending.forward_payloads).await;

            if self.write(&mut pending, &guard).await.is_err() {
                break;
            }

            for ack in pending.acks {
                // Receiver gone (client revoked/aborted) just means those offsets
                // won't be committed and the messages get redelivered.
                let _ = ack.send(());
            }
        }

        debug!("Shared writer shutdown complete");
        Ok(())
    }

    /// Apply the global filters and split requests into a DB batch and a set of
    /// forward payloads. Killswitched/expired activations are dropped (their
    /// offsets still commit once the request is acked).
    async fn classify(&self, raw: Vec<ActivationWriteRequest>) -> PendingWrite {
        let runtime_config = self.runtime_config_manager.read().await;
        let forward_topic = runtime_config
            .demoted_topic
            .clone()
            .unwrap_or_else(|| self.config.kafka_long_topic.clone());

        let mut db_batch = Vec::new();
        let mut forward_payloads = Vec::new();
        let mut acks = Vec::with_capacity(raw.len());

        for ActivationWriteRequest {
            source_topic,
            activations,
            ack,
        } in raw
        {
            acks.push(ack);

            for activation in activations {
                if runtime_config
                    .drop_task_killswitch
                    .contains(&activation.taskname)
                {
                    metrics::counter!(
                        "filter.drop_task_killswitch",
                        "topic" => source_topic.to_string(),
                        "taskname" => activation.taskname.clone(),
                    )
                    .increment(1);
                    continue;
                }

                if let Some(expires_at) = activation.expires_at
                    && Utc::now() > expires_at
                {
                    metrics::counter!(
                        "filter.expired_at_consumer",
                        "topic" => source_topic.to_string(),
                    )
                    .increment(1);
                    continue;
                }

                if runtime_config
                    .demoted_namespaces
                    .contains(&activation.namespace)
                {
                    if forward_topic.as_str() == source_topic.as_ref() {
                        // Already on the demoted topic; don't forward to self,
                        // write it normally instead.
                        metrics::counter!(
                            "filter.forward_task_demoted_namespace.skipped",
                            "topic" => source_topic.to_string(),
                            "namespace" => activation.namespace.clone(),
                            "taskname" => activation.taskname.clone(),
                        )
                        .increment(1);
                    } else {
                        metrics::counter!(
                            "filter.forward_task_demoted_namespace",
                            "topic" => source_topic.to_string(),
                            "namespace" => activation.namespace.clone(),
                            "taskname" => activation.taskname.clone(),
                        )
                        .increment(1);
                        forward_payloads.push(activation.activation);
                        continue;
                    }
                }

                db_batch.push(activation);
            }
        }

        PendingWrite {
            db_batch,
            forward_payloads,
            acks,
        }
    }

    /// Forward demoted-namespace payloads to the long/demoted topic. Best-effort:
    /// failures are counted but not retried.
    async fn forward(&mut self, payloads: &[Vec<u8>]) {
        if payloads.is_empty() {
            return;
        }

        let runtime_config = self.runtime_config_manager.read().await;
        // The forwarding producer authenticates against the deadletter cluster,
        // so default demoted forwarding there too when no cluster is configured.
        let forward_cluster = runtime_config
            .demoted_topic_cluster
            .clone()
            .unwrap_or_else(|| {
                self.config
                    .producer_config
                    .get("bootstrap.servers")
                    .expect("producer config always sets bootstrap.servers")
                    .to_string()
            });
        if self.producer_cluster != forward_cluster {
            let mut new_config = self.config.producer_config.clone();
            new_config.set("bootstrap.servers", &forward_cluster);
            self.producer = Arc::new(
                new_config
                    .create()
                    .expect("Could not create kafka producer in shared writer"),
            );
            self.producer_cluster = forward_cluster;
        }
        let forward_topic = runtime_config
            .demoted_topic
            .clone()
            .unwrap_or_else(|| self.config.kafka_long_topic.clone());

        let sends = payloads.iter().map(|payload| {
            self.producer.send(
                FutureRecord::<(), Vec<u8>>::to(&forward_topic).payload(payload),
                Timeout::After(Duration::from_millis(self.config.send_timeout_ms)),
            )
        });
        let results = join_all(sends).await;
        let success_count = results.iter().filter(|r| r.is_ok()).count();

        metrics::histogram!("consumer.forward_attempts").record(results.len() as f64);
        metrics::histogram!("consumer.forward_successes").record(success_count as f64);
        metrics::histogram!("consumer.forward_failures")
            .record((results.len() - success_count) as f64);
    }

    /// Write `pending.db_batch`, waiting out depth-limit backpressure and
    /// retrying write failures. Returns `Err(())` if a shutdown was observed
    /// while waiting (the caller then exits without acking, so the batch is
    /// redelivered).
    async fn write(
        &self,
        pending: &mut PendingWrite,
        guard: &elegant_departure::ShutdownGuard,
    ) -> Result<(), ()> {
        loop {
            if self.is_backpressured(&pending.db_batch).await {
                select! {
                    _ = guard.wait() => return Err(()),
                    _ = sleep(Duration::from_millis(BACKPRESSURE_POLL_MS)) => continue,
                }
            }

            if pending.db_batch.is_empty() {
                return Ok(());
            }

            let write_start = Instant::now();
            match self.store.store(&pending.db_batch).await {
                Ok(entries) => {
                    let lag = Utc::now()
                        - pending
                            .db_batch
                            .iter()
                            .map(|item| item.received_at)
                            .min_by_key(|item| item.timestamp())
                            .unwrap();
                    metrics::histogram!("consumer.inflight_activation_writer.write_to_store")
                        .record(write_start.elapsed());
                    metrics::histogram!("consumer.inflight_activation_writer.insert_lag")
                        .record(lag.num_seconds() as f64);
                    metrics::counter!("consumer.inflight_activation_writer.stored")
                        .increment(entries);
                    return Ok(());
                }
                Err(err) => {
                    error!("Unable to write activations to store: {}", err);
                    metrics::counter!("consumer.inflight_activation_writer.write_failed")
                        .increment(1);
                    select! {
                        _ = guard.wait() => return Err(()),
                        _ = sleep(Duration::from_millis(self.config.write_failure_backoff_ms)) => continue,
                    }
                }
            }
        }
    }

    /// Depth-limit backpressure, evaluated over the combined batch (more correct
    /// than each consumer deciding independently).
    async fn is_backpressured(&self, db_batch: &[Activation]) -> bool {
        if db_batch.is_empty() {
            return false;
        }

        let DepthCounts {
            pending,
            delay,
            claimed,
            processing,
        } = self
            .store
            .count_depths()
            .await
            .expect("Error communicating with activation store");

        let exceeded_pending_limit = pending + db_batch.len() > self.config.max_pending_activations;
        let exceeded_delay_limit = delay + db_batch.len() > self.config.max_delay_activations;
        let exceeded_processing_limit =
            processing + claimed >= self.config.max_processing_activations;
        let exceeded_db_size = if let Some(db_max_size) = self.config.db_max_size {
            self.store
                .db_size()
                .await
                .expect("Error getting database size")
                >= db_max_size
        } else {
            false
        };

        let has_delay = db_batch
            .iter()
            .any(|activation| activation.status == ActivationStatus::Delay);
        let has_pending = db_batch
            .iter()
            .any(|activation| activation.status == ActivationStatus::Pending);

        if exceeded_processing_limit
            || exceeded_db_size
            || exceeded_delay_limit && (has_delay || exceeded_pending_limit)
            || exceeded_pending_limit && has_pending
        {
            let reason = if exceeded_processing_limit {
                "processing_limit"
            } else if exceeded_delay_limit {
                "delay_limit"
            } else if exceeded_db_size {
                "db_size_limit"
            } else {
                "pending_limit"
            };
            metrics::counter!(
                "consumer.inflight_activation_writer.backpressure",
                "reason" => reason,
            )
            .increment(1);
            true
        } else {
            false
        }
    }
}

/// Per-consumer reduce stage. Accumulates deserialized activations and ships
/// them to the shared writer, blocking until they are durably stored.
pub struct ActivationWriterClient {
    tx: mpsc::Sender<ActivationWriteRequest>,
    source_topic: Arc<str>,
    batch: Vec<Activation>,
    batch_size: usize,
    max_batch_len: usize,
    max_batch_size: usize,
    flush_interval: Duration,
}

impl ActivationWriterClient {
    pub fn new(
        tx: mpsc::Sender<ActivationWriteRequest>,
        source_topic: &str,
        config: &Config,
    ) -> Self {
        Self {
            tx,
            source_topic: Arc::from(source_topic),
            batch: Vec::with_capacity(config.db_insert_batch_max_len),
            batch_size: 0,
            max_batch_len: config.db_insert_batch_max_len,
            max_batch_size: config.db_insert_batch_max_size,
            flush_interval: Duration::from_millis(config.db_insert_batch_max_time_ms),
        }
    }
}

impl Reducer for ActivationWriterClient {
    type Input = Activation;
    type Output = ();

    async fn reduce(&mut self, activation: Self::Input) -> Result<(), Error> {
        self.batch_size += activation.activation.len();
        self.batch.push(activation);
        Ok(())
    }

    #[instrument(skip_all)]
    async fn flush(&mut self) -> Result<Option<Self::Output>, Error> {
        if self.batch.is_empty() {
            // Nothing buffered (e.g. a timer tick on an idle consumer): there are
            // still in-flight messages to commit, so signal completion.
            return Ok(Some(()));
        }

        let activations = std::mem::take(&mut self.batch);
        self.batch_size = 0;

        let (ack_tx, ack_rx) = oneshot::channel();
        let request = ActivationWriteRequest {
            source_topic: self.source_topic.clone(),
            activations,
            ack: ack_tx,
        };

        if self.tx.send(request).await.is_err() {
            // Writer is gone (shutting down). Don't commit; messages redeliver.
            return Ok(None);
        }

        match ack_rx.await {
            // Durably stored: commit the offsets for this batch.
            Ok(()) => Ok(Some(())),
            // Writer dropped the ack (shutdown). Don't commit; messages redeliver.
            Err(_) => Ok(None),
        }
    }

    fn reset(&mut self) {
        self.batch.clear();
        self.batch_size = 0;
    }

    async fn is_full(&self) -> bool {
        self.batch.len() >= self.max_batch_len || self.batch_size >= self.max_batch_size
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        ReduceConfig {
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            // Drop the un-shipped buffer on revoke; those offsets aren't
            // committed and the new owner reprocesses them.
            shutdown_behaviour: ReduceShutdownBehaviour::Drop,
            shutdown_condition: ReduceShutdownCondition::Signal,
            flush_interval: Some(self.flush_interval),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

    use chrono::Utc;
    use rstest::rstest;
    use tempfile::NamedTempFile;
    use tokio::sync::{mpsc, oneshot};

    use crate::config::Config;
    use crate::runtime_config::RuntimeConfigManager;
    use crate::store::activation::ActivationBuilder;
    use crate::store::traits::ActivationStore;
    use crate::test_utils::{
        TaskActivationBuilder, create_test_store, generate_unique_namespace, make_activations,
    };

    use super::{
        ActivationWriteRequest, ActivationWriter, ActivationWriterClient, ActivationWriterConfig,
        Reducer,
    };

    /// A validated default config (populates `kafka_topics`, needed to build the
    /// forward producer).
    fn validated_config() -> Config {
        let mut config = Config::default();
        config.normalize_and_validate().unwrap();
        config
    }

    /// Build an writer wired to `store`/`runtime_config` for exercising the
    /// writer's internal methods directly. The receiver is a throwaway.
    fn make_writer(
        store: Arc<dyn ActivationStore>,
        runtime_config_manager: Arc<RuntimeConfigManager>,
        config: ActivationWriterConfig,
    ) -> ActivationWriter {
        let producer = config
            .producer_config
            .create()
            .expect("could not create test producer");
        let producer_cluster = config
            .producer_config
            .get("bootstrap.servers")
            .unwrap()
            .to_owned();
        let (_tx, rx) = mpsc::channel(1);
        ActivationWriter {
            store,
            runtime_config_manager,
            producer: Arc::new(producer),
            producer_cluster,
            config,
            rx,
        }
    }

    fn request(
        source_topic: &str,
        activations: Vec<crate::store::activation::Activation>,
    ) -> ActivationWriteRequest {
        let (ack, _ack_rx) = oneshot::channel();
        ActivationWriteRequest {
            source_topic: Arc::from(source_topic),
            activations,
            ack,
        }
    }

    async fn runtime_config_from_yaml(yaml: &str) -> Arc<RuntimeConfigManager> {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{}", yaml).unwrap();
        file.flush().unwrap();
        Arc::new(RuntimeConfigManager::new(Some(file.path().to_str().unwrap().to_string())).await)
    }

    #[tokio::test]
    #[rstest]
    #[case::sqlite("sqlite")]
    #[case::postgres("postgres")]
    async fn test_classify_drops_killswitch(#[case] adapter: &str) {
        let store = create_test_store(adapter).await;
        let runtime_config = runtime_config_from_yaml(
            r#"
drop_task_killswitch:
  - task_to_be_filtered
demoted_namespaces:
  -"#,
        )
        .await;
        let writer = make_writer(
            store.clone(),
            runtime_config,
            ActivationWriterConfig::from_config(&validated_config()),
        );

        let namespace = generate_unique_namespace();
        let dropped = ActivationBuilder::new()
            .id("0")
            .taskname("task_to_be_filtered")
            .namespace(&namespace)
            .build(TaskActivationBuilder::new());
        let kept = ActivationBuilder::new()
            .id("1")
            .taskname("good_task")
            .namespace(&namespace)
            .build(TaskActivationBuilder::new());

        let pending = writer
            .classify(vec![request("topic", vec![dropped, kept])])
            .await;
        assert_eq!(pending.db_batch.len(), 1);
        assert_eq!(pending.db_batch[0].taskname, "good_task");
        assert!(pending.forward_payloads.is_empty());
        store.remove_db().await.unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case::sqlite("sqlite")]
    #[case::postgres("postgres")]
    async fn test_classify_drops_expired(#[case] adapter: &str) {
        let store = create_test_store(adapter).await;
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let writer = make_writer(
            store.clone(),
            runtime_config,
            ActivationWriterConfig::from_config(&validated_config()),
        );

        let namespace = generate_unique_namespace();
        let expired = ActivationBuilder::new()
            .id("0")
            .taskname("task")
            .namespace(&namespace)
            .expires_at(Utc::now())
            .build(TaskActivationBuilder::new());

        let pending = writer.classify(vec![request("topic", vec![expired])]).await;
        assert!(pending.db_batch.is_empty());
        assert!(pending.forward_payloads.is_empty());
        store.remove_db().await.unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case::sqlite("sqlite")]
    #[case::postgres("postgres")]
    async fn test_classify_forwards_demoted_namespace(#[case] adapter: &str) {
        let store = create_test_store(adapter).await;
        let runtime_config = runtime_config_from_yaml(
            r#"
drop_task_killswitch:
  -
demoted_namespaces:
  - bad_namespace
demoted_topic_cluster: 0.0.0.0:9092
demoted_topic: taskworker-demoted"#,
        )
        .await;
        let writer = make_writer(
            store.clone(),
            runtime_config,
            ActivationWriterConfig::from_config(&validated_config()),
        );

        let demoted = ActivationBuilder::new()
            .id("0")
            .taskname("task")
            .namespace("bad_namespace")
            .build(TaskActivationBuilder::new());
        let normal = ActivationBuilder::new()
            .id("1")
            .taskname("task")
            .namespace("good_namespace")
            .build(TaskActivationBuilder::new());

        let pending = writer
            .classify(vec![request("taskworker", vec![demoted, normal])])
            .await;
        assert_eq!(pending.forward_payloads.len(), 1);
        assert_eq!(pending.db_batch.len(), 1);
        assert_eq!(pending.db_batch[0].namespace, "good_namespace");
        store.remove_db().await.unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case::sqlite("sqlite")]
    #[case::postgres("postgres")]
    async fn test_classify_skips_self_forward(#[case] adapter: &str) {
        let store = create_test_store(adapter).await;
        // Forward topic equals the source topic: demoted tasks are written
        // normally instead of being forwarded to themselves.
        let runtime_config = runtime_config_from_yaml(
            r#"
drop_task_killswitch:
  -
demoted_namespaces:
  - bad_namespace
demoted_topic: taskworker-demoted"#,
        )
        .await;
        let writer = make_writer(
            store.clone(),
            runtime_config,
            ActivationWriterConfig::from_config(&validated_config()),
        );

        let demoted = ActivationBuilder::new()
            .id("0")
            .taskname("task")
            .namespace("bad_namespace")
            .build(TaskActivationBuilder::new());

        let pending = writer
            .classify(vec![request("taskworker-demoted", vec![demoted])])
            .await;
        assert!(pending.forward_payloads.is_empty());
        assert_eq!(pending.db_batch.len(), 1);
        store.remove_db().await.unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case::sqlite("sqlite")]
    #[case::postgres("postgres")]
    async fn test_classify_coalesces_across_topics(#[case] adapter: &str) {
        let store = create_test_store(adapter).await;
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let writer = make_writer(
            store.clone(),
            runtime_config,
            ActivationWriterConfig::from_config(&validated_config()),
        );

        let namespace = generate_unique_namespace();
        let mk = |id: &str| {
            ActivationBuilder::new()
                .id(id)
                .taskname("task")
                .namespace(&namespace)
                .build(TaskActivationBuilder::new())
        };

        // Two requests from different consumers fuse into one DB batch, with one
        // ack per request.
        let pending = writer
            .classify(vec![
                request("topic-a", vec![mk("0"), mk("1")]),
                request("topic-b", vec![mk("2")]),
            ])
            .await;
        assert_eq!(pending.db_batch.len(), 3);
        assert_eq!(pending.acks.len(), 2);
        store.remove_db().await.unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case::sqlite("sqlite")]
    #[case::postgres("postgres")]
    async fn test_is_backpressured_pending_limit(#[case] adapter: &str) {
        let store = create_test_store(adapter).await;
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let mut config = ActivationWriterConfig::from_config(&validated_config());
        config.max_pending_activations = 0;
        config.max_delay_activations = 0;
        let writer = make_writer(store.clone(), runtime_config, config);

        let namespace = generate_unique_namespace();
        let batch = vec![
            ActivationBuilder::new()
                .id("0")
                .taskname("task")
                .namespace(&namespace)
                .build(TaskActivationBuilder::new()),
        ];
        assert!(writer.is_backpressured(&batch).await);
        // An empty batch is never backpressured.
        assert!(!writer.is_backpressured(&[]).await);
        store.remove_db().await.unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case::sqlite("sqlite")]
    #[case::postgres("postgres")]
    async fn test_is_backpressured_db_size_limit(#[case] adapter: &str) {
        let store = create_test_store(adapter).await;
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let mut config = ActivationWriterConfig::from_config(&validated_config());
        // 200 rows is ~50KB.
        config.db_max_size = Some(50_000);
        config.max_pending_activations = 5000;
        config.max_processing_activations = 5000;
        let first_round = make_activations(200);
        store.store(&first_round).await.unwrap();
        assert!(store.db_size().await.unwrap() > 50_000);

        let writer = make_writer(store.clone(), runtime_config, config);
        let batch = make_activations(1);
        assert!(writer.is_backpressured(&batch).await);
        store.remove_db().await.unwrap();
    }

    #[tokio::test]
    async fn test_client_is_full() {
        let config = Config {
            db_insert_batch_max_len: 2,
            db_insert_batch_max_size: 100_000,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(1);
        let mut client = ActivationWriterClient::new(tx, "topic", &config);

        let namespace = generate_unique_namespace();
        let mk = |id: &str| {
            ActivationBuilder::new()
                .id(id)
                .taskname("task")
                .namespace(&namespace)
                .build(TaskActivationBuilder::new())
        };

        assert!(!client.is_full().await);
        client.reduce(mk("0")).await.unwrap();
        assert!(!client.is_full().await);
        client.reduce(mk("1")).await.unwrap();
        assert!(client.is_full().await);
    }

    #[tokio::test]
    async fn test_client_flush_empty_commits() {
        let config = Config::default();
        let (tx, _rx) = mpsc::channel(1);
        let mut client = ActivationWriterClient::new(tx, "topic", &config);
        // An empty buffer still reports success so idle-timer flushes commit any
        // in-flight (dropped/forwarded) offsets.
        assert_eq!(client.flush().await.unwrap(), Some(()));
    }

    #[tokio::test]
    async fn test_client_flush_commits_after_durable_ack() {
        let config = Config::default();
        let (tx, mut rx) = mpsc::channel::<ActivationWriteRequest>(1);
        let mut client = ActivationWriterClient::new(tx, "topic", &config);

        let namespace = generate_unique_namespace();
        client
            .reduce(
                ActivationBuilder::new()
                    .id("0")
                    .taskname("task")
                    .namespace(&namespace)
                    .build(TaskActivationBuilder::new()),
            )
            .await
            .unwrap();

        // Stand in for the writer: receive the request, then ack durability.
        let writer = tokio::spawn(async move {
            let req = rx.recv().await.unwrap();
            assert_eq!(req.activations.len(), 1);
            req.ack.send(()).unwrap();
        });

        assert_eq!(client.flush().await.unwrap(), Some(()));
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn test_client_flush_no_commit_when_writer_drops_ack() {
        let config = Config::default();
        let (tx, mut rx) = mpsc::channel::<ActivationWriteRequest>(1);
        let mut client = ActivationWriterClient::new(tx, "topic", &config);

        let namespace = generate_unique_namespace();
        client
            .reduce(
                ActivationBuilder::new()
                    .id("0")
                    .taskname("task")
                    .namespace(&namespace)
                    .build(TaskActivationBuilder::new()),
            )
            .await
            .unwrap();

        // Writer receives the request but drops the ack (e.g. shutdown).
        let writer = tokio::spawn(async move {
            let _req = rx.recv().await.unwrap();
            // drop _req -> drops the ack sender
        });

        assert_eq!(client.flush().await.unwrap(), None);
        writer.await.unwrap();
    }
}
