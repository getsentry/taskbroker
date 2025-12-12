use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use tokio::time::sleep;
use tracing::{debug, error, instrument};

use crate::{
    config::Config,
    store::inflight_activation::{
        InflightActivation, InflightActivationStatus, InflightActivationStore,
    },
};

use super::consumer::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};

pub struct ActivationWriterConfig {
    pub max_buf_len: usize,
    pub max_pending_activations: usize,
    pub max_processing_activations: usize,
    pub max_delay_activations: usize,
    pub db_max_size: Option<u64>,
    pub write_failure_backoff_ms: u64,
}

impl ActivationWriterConfig {
    /// Convert from application configuration into InflightActivationWriter config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            db_max_size: config.db_max_size,
            max_buf_len: config.db_insert_batch_max_len,
            max_pending_activations: config.max_pending_count,
            max_processing_activations: config.max_processing_count,
            max_delay_activations: config.max_delay_count,
            write_failure_backoff_ms: config.db_write_failure_backoff_ms,
        }
    }
}

pub struct InflightActivationWriter {
    config: ActivationWriterConfig,
    store: Arc<InflightActivationStore>,
    batch: Option<Vec<InflightActivation>>,
}

impl InflightActivationWriter {
    pub fn new(store: Arc<InflightActivationStore>, config: ActivationWriterConfig) -> Self {
        Self {
            config,
            store,
            batch: None,
        }
    }
}

impl Reducer for InflightActivationWriter {
    type Input = Vec<InflightActivation>;

    type Output = ();

    async fn reduce(&mut self, batch: Self::Input) -> Result<(), anyhow::Error> {
        assert!(self.batch.is_none());
        self.batch = Some(batch);
        Ok(())
    }

    #[instrument(skip_all)]
    async fn flush(&mut self) -> Result<Option<Self::Output>, anyhow::Error> {
        let Some(ref batch) = self.batch else {
            return Ok(None);
        };

        // If batch is empty (all tasks were forwarded), just mark as complete
        if batch.is_empty() {
            self.batch.take();
            return Ok(Some(()));
        }

        // Check if writing the batch would exceed the limits
        let exceeded_pending_limit = self
            .store
            .count_pending_activations()
            .await
            .expect("Error communicating with activation store")
            + batch.len()
            > self.config.max_pending_activations;
        let exceeded_delay_limit = self
            .store
            .count_by_status(InflightActivationStatus::Delay)
            .await
            .expect("Error communicating with activation store")
            + batch.len()
            > self.config.max_delay_activations;
        let exceeded_processing_limit = self
            .store
            .count_by_status(InflightActivationStatus::Processing)
            .await
            .expect("Error communicating with activation store")
            >= self.config.max_processing_activations;
        let exceeded_db_size = if let Some(db_max_size) = self.config.db_max_size {
            self.store
                .db_size()
                .await
                .expect("Error getting database size")
                >= db_max_size
        } else {
            false
        };

        // Check if the entire batch is either pending or delay
        let has_delay = batch
            .iter()
            .any(|activation| activation.status == InflightActivationStatus::Delay);
        let has_pending = batch
            .iter()
            .any(|activation| activation.status == InflightActivationStatus::Pending);

        // Backpressure if any of these conditions are met:
        // 1. The processing limit is exceeded
        // 2. The delay limit is exceeded AND either:
        //    a. There are delay activations in the batch, OR
        //    b. The pending limit is also exceeded
        // 3. The pending limit is exceeded AND there are pending activations
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

            return Ok(None);
        }

        let batch = self.batch.clone().unwrap();
        let write_to_store_start = Instant::now();
        let res = self.store.store(batch.clone()).await;
        match res {
            Ok(res) => {
                self.batch.take();
                let lag = Utc::now()
                    - batch
                        .iter()
                        .map(|item| item.received_at)
                        .min_by_key(|item| item.timestamp())
                        .unwrap();

                metrics::histogram!("consumer.inflight_activation_writer.write_to_store")
                    .record(write_to_store_start.elapsed());
                metrics::histogram!("consumer.inflight_activation_writer.insert_lag")
                    .record(lag.num_seconds() as f64);
                metrics::counter!("consumer.inflight_activation_writer.stored")
                    .increment(res.rows_affected);
                debug!(
                    "Inserted {:?} entries with max lag: {:?}s",
                    res.rows_affected,
                    lag.num_seconds()
                );
                Ok(Some(()))
            }
            Err(err) => {
                error!("Unable to write to sqlite: {}", err);
                metrics::counter!("consumer.inflight_activation_writer.write_failed").increment(1);
                sleep(Duration::from_millis(self.config.write_failure_backoff_ms)).await;
                Ok(None)
            }
        }
    }

    fn reset(&mut self) {}

    async fn is_full(&self) -> bool {
        self.batch.is_some()
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        ReduceConfig {
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            shutdown_behaviour: ReduceShutdownBehaviour::Flush,
            shutdown_condition: ReduceShutdownCondition::Signal,
            flush_interval: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ActivationWriterConfig, InflightActivation, InflightActivationWriter, Reducer};
    use chrono::{DateTime, Utc};
    use prost::Message;
    use prost_types::Timestamp;
    use std::collections::HashMap;

    use sentry_protos::taskbroker::v1::OnAttemptsExceeded;
    use sentry_protos::taskbroker::v1::TaskActivation;
    use std::sync::Arc;

    use crate::store::inflight_activation::{
        InflightActivationStatus, InflightActivationStore, InflightActivationStoreConfig,
    };
    use crate::test_utils::make_activations;
    use crate::test_utils::{create_integration_config, generate_temp_filename};

    #[tokio::test]
    async fn test_writer_flush_batch() {
        let writer_config = ActivationWriterConfig {
            db_max_size: None,
            max_buf_len: 100,
            max_pending_activations: 10,
            max_processing_activations: 10,
            max_delay_activations: 10,
            write_failure_backoff_ms: 4000,
        };
        let mut writer = InflightActivationWriter::new(
            Arc::new(
                InflightActivationStore::new(
                    &generate_temp_filename(),
                    InflightActivationStoreConfig::from_config(&create_integration_config()),
                )
                .await
                .unwrap(),
            ),
            writer_config,
        );
        let received_at = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let batch = vec![
            InflightActivation {
                id: "0".to_string(),
                activation: TaskActivation {
                    id: "0".to_string(),
                    application: Some("sentry".to_string()),
                    namespace: "namespace".to_string(),
                    taskname: "pending_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(received_at),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                }
                .encode_to_vec(),
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                received_at: DateTime::from_timestamp(
                    received_at.seconds,
                    received_at.nanos as u32,
                )
                .unwrap(),
                processing_attempts: 0,
                processing_deadline_duration: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
                taskname: "pending_task".to_string(),
                on_attempts_exceeded: OnAttemptsExceeded::Discard,
            },
            InflightActivation {
                id: "1".to_string(),
                activation: TaskActivation {
                    id: "1".to_string(),
                    application: Some("sentry".to_string()),
                    namespace: "namespace".to_string(),
                    taskname: "delay_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(received_at),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                }
                .encode_to_vec(),
                status: InflightActivationStatus::Delay,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                received_at: DateTime::from_timestamp(
                    received_at.seconds,
                    received_at.nanos as u32,
                )
                .unwrap(),
                processing_attempts: 0,
                processing_deadline_duration: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
                taskname: "delay_task".to_string(),
                on_attempts_exceeded: OnAttemptsExceeded::Discard,
            },
        ];

        writer.reduce(batch).await.unwrap();
        writer.flush().await.unwrap();
        let count_pending = writer.store.count_pending_activations().await.unwrap();
        let count_delay = writer
            .store
            .count_by_status(InflightActivationStatus::Delay)
            .await
            .unwrap();
        assert_eq!(count_pending + count_delay, 2);
    }

    #[tokio::test]
    async fn test_writer_flush_only_pending() {
        let writer_config = ActivationWriterConfig {
            db_max_size: None,
            max_buf_len: 100,
            max_pending_activations: 10,
            max_processing_activations: 10,
            max_delay_activations: 0,
            write_failure_backoff_ms: 4000,
        };
        let mut writer = InflightActivationWriter::new(
            Arc::new(
                InflightActivationStore::new(
                    &generate_temp_filename(),
                    InflightActivationStoreConfig::from_config(&create_integration_config()),
                )
                .await
                .unwrap(),
            ),
            writer_config,
        );
        let received_at = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let batch = vec![InflightActivation {
            id: "0".to_string(),
            activation: TaskActivation {
                id: "0".to_string(),
                application: Some("sentry".to_string()),
                namespace: "namespace".to_string(),
                taskname: "pending_task".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: Some(received_at),
                retry_state: None,
                processing_deadline_duration: 0,
                expires: None,
                delay: None,
            }
            .encode_to_vec(),
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            received_at: DateTime::from_timestamp(received_at.seconds, received_at.nanos as u32)
                .unwrap(),
            processing_attempts: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            processing_deadline_duration: 0,
            at_most_once: false,
            namespace: "namespace".to_string(),
            taskname: "pending_task".to_string(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
        }];

        writer.reduce(batch).await.unwrap();
        writer.flush().await.unwrap();
        let count_pending = writer.store.count_pending_activations().await.unwrap();
        assert_eq!(count_pending, 1);
    }

    #[tokio::test]
    async fn test_writer_flush_only_delay() {
        let writer_config = ActivationWriterConfig {
            db_max_size: None,
            max_buf_len: 100,
            max_pending_activations: 0,
            max_processing_activations: 10,
            max_delay_activations: 10,
            write_failure_backoff_ms: 4000,
        };
        let mut writer = InflightActivationWriter::new(
            Arc::new(
                InflightActivationStore::new(
                    &generate_temp_filename(),
                    InflightActivationStoreConfig::from_config(&create_integration_config()),
                )
                .await
                .unwrap(),
            ),
            writer_config,
        );

        let received_at = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let batch = vec![InflightActivation {
            id: "0".to_string(),
            activation: TaskActivation {
                id: "0".to_string(),
                application: Some("sentry".to_string()),
                namespace: "namespace".to_string(),
                taskname: "pending_task".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: Some(received_at),
                retry_state: None,
                processing_deadline_duration: 0,
                expires: None,
                delay: None,
            }
            .encode_to_vec(),
            status: InflightActivationStatus::Delay,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            received_at: DateTime::from_timestamp(received_at.seconds, received_at.nanos as u32)
                .unwrap(),
            processing_attempts: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            processing_deadline_duration: 0,
            at_most_once: false,
            namespace: "namespace".to_string(),
            taskname: "pending_task".to_string(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
        }];

        writer.reduce(batch).await.unwrap();
        writer.flush().await.unwrap();
        let count_delay = writer
            .store
            .count_by_status(InflightActivationStatus::Delay)
            .await
            .unwrap();
        assert_eq!(count_delay, 1);
    }

    #[tokio::test]
    async fn test_writer_backpressure_pending_limit_reached() {
        let writer_config = ActivationWriterConfig {
            db_max_size: None,
            max_buf_len: 100,
            max_pending_activations: 0,
            max_processing_activations: 10,
            max_delay_activations: 0,
            write_failure_backoff_ms: 4000,
        };
        let mut writer = InflightActivationWriter::new(
            Arc::new(
                InflightActivationStore::new(
                    &generate_temp_filename(),
                    InflightActivationStoreConfig::from_config(&create_integration_config()),
                )
                .await
                .unwrap(),
            ),
            writer_config,
        );
        let received_at = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let batch = vec![
            InflightActivation {
                id: "0".to_string(),
                activation: TaskActivation {
                    id: "0".to_string(),
                    application: Some("sentry".to_string()),
                    namespace: "namespace".to_string(),
                    taskname: "pending_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(received_at),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                }
                .encode_to_vec(),
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                received_at: DateTime::from_timestamp(
                    received_at.seconds,
                    received_at.nanos as u32,
                )
                .unwrap(),
                processing_attempts: 0,
                processing_deadline_duration: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
                taskname: "pending_task".to_string(),
                on_attempts_exceeded: OnAttemptsExceeded::Discard,
            },
            InflightActivation {
                id: "1".to_string(),
                activation: TaskActivation {
                    id: "1".to_string(),
                    application: Some("sentry".to_string()),
                    namespace: "namespace".to_string(),
                    taskname: "delay_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(received_at),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                }
                .encode_to_vec(),
                status: InflightActivationStatus::Delay,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                received_at: DateTime::from_timestamp(
                    received_at.seconds,
                    received_at.nanos as u32,
                )
                .unwrap(),
                processing_attempts: 0,
                processing_deadline_duration: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
                taskname: "delay_task".to_string(),
                on_attempts_exceeded: OnAttemptsExceeded::Discard,
            },
        ];

        writer.reduce(batch).await.unwrap();
        writer.flush().await.unwrap();
        let count_pending = writer.store.count_pending_activations().await.unwrap();
        assert_eq!(count_pending, 0);
        let count_delay = writer
            .store
            .count_by_status(InflightActivationStatus::Delay)
            .await
            .unwrap();
        assert_eq!(count_delay, 0);
    }

    #[tokio::test]
    async fn test_writer_backpressure_only_delay_limit_reached_and_entire_batch_is_pending() {
        let writer_config = ActivationWriterConfig {
            db_max_size: None,
            max_buf_len: 100,
            max_pending_activations: 10,
            max_processing_activations: 10,
            max_delay_activations: 0,
            write_failure_backoff_ms: 4000,
        };
        let mut writer = InflightActivationWriter::new(
            Arc::new(
                InflightActivationStore::new(
                    &generate_temp_filename(),
                    InflightActivationStoreConfig::from_config(&create_integration_config()),
                )
                .await
                .unwrap(),
            ),
            writer_config,
        );

        let received_at = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let batch = vec![
            InflightActivation {
                id: "0".to_string(),
                activation: TaskActivation {
                    id: "0".to_string(),
                    application: Some("sentry".to_string()),
                    namespace: "namespace".to_string(),
                    taskname: "pending_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(received_at),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                }
                .encode_to_vec(),
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                received_at: DateTime::from_timestamp(
                    received_at.seconds,
                    received_at.nanos as u32,
                )
                .unwrap(),
                processing_attempts: 0,
                processing_deadline_duration: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
                taskname: "pending_task".to_string(),
                on_attempts_exceeded: OnAttemptsExceeded::Discard,
            },
            InflightActivation {
                id: "1".to_string(),
                activation: TaskActivation {
                    id: "1".to_string(),
                    application: Some("sentry".to_string()),
                    namespace: "namespace".to_string(),
                    taskname: "pending_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(received_at),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                }
                .encode_to_vec(),
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                received_at: DateTime::from_timestamp(
                    received_at.seconds,
                    received_at.nanos as u32,
                )
                .unwrap(),
                processing_attempts: 0,
                processing_deadline_duration: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
                taskname: "pending_task".to_string(),
                on_attempts_exceeded: OnAttemptsExceeded::Discard,
            },
        ];

        writer.reduce(batch).await.unwrap();
        writer.flush().await.unwrap();
        let count_pending = writer.store.count_pending_activations().await.unwrap();
        assert_eq!(count_pending, 2);
        let count_delay = writer
            .store
            .count_by_status(InflightActivationStatus::Delay)
            .await
            .unwrap();
        assert_eq!(count_delay, 0);
    }

    #[tokio::test]
    async fn test_writer_backpressure_processing_limit_reached() {
        let writer_config = ActivationWriterConfig {
            db_max_size: None,
            max_buf_len: 100,
            max_pending_activations: 10,
            max_processing_activations: 1,
            max_delay_activations: 0,
            write_failure_backoff_ms: 4000,
        };
        let store = Arc::new(
            InflightActivationStore::new(
                &generate_temp_filename(),
                InflightActivationStoreConfig::from_config(&create_integration_config()),
            )
            .await
            .unwrap(),
        );

        let received_at = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let existing_activation = InflightActivation {
            id: "existing".to_string(),
            activation: TaskActivation {
                id: "existing".to_string(),
                application: Some("sentry".to_string()),
                namespace: "namespace".to_string(),
                taskname: "existing_task".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: Some(received_at),
                retry_state: None,
                processing_deadline_duration: 0,
                expires: None,
                delay: None,
            }
            .encode_to_vec(),
            status: InflightActivationStatus::Processing,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            received_at: DateTime::from_timestamp(received_at.seconds, received_at.nanos as u32)
                .unwrap(),
            processing_attempts: 0,
            processing_deadline_duration: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".to_string(),
            taskname: "existing_task".to_string(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
        };
        store.store(vec![existing_activation]).await.unwrap();

        let mut writer = InflightActivationWriter::new(store.clone(), writer_config);
        let batch = vec![
            InflightActivation {
                id: "0".to_string(),
                activation: TaskActivation {
                    id: "0".to_string(),
                    application: Some("sentry".to_string()),
                    namespace: "namespace".to_string(),
                    taskname: "pending_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(received_at),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                }
                .encode_to_vec(),
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                received_at: DateTime::from_timestamp(
                    received_at.seconds,
                    received_at.nanos as u32,
                )
                .unwrap(),
                processing_attempts: 0,
                processing_deadline_duration: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
                taskname: "pending_task".to_string(),
                on_attempts_exceeded: OnAttemptsExceeded::Discard,
            },
            InflightActivation {
                id: "1".to_string(),
                activation: TaskActivation {
                    id: "1".to_string(),
                    application: Some("sentry".to_string()),
                    namespace: "namespace".to_string(),
                    taskname: "delay_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(received_at),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                }
                .encode_to_vec(),
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                received_at: DateTime::from_timestamp(
                    received_at.seconds,
                    received_at.nanos as u32,
                )
                .unwrap(),
                processing_attempts: 0,
                processing_deadline_duration: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
                taskname: "delay_task".to_string(),
                on_attempts_exceeded: OnAttemptsExceeded::Discard,
            },
        ];

        writer.reduce(batch).await.unwrap();
        let flush_result = writer.flush().await.unwrap();

        assert!(flush_result.is_none());

        let count_pending = writer.store.count_pending_activations().await.unwrap();
        assert_eq!(count_pending, 0);
        let count_delay = writer
            .store
            .count_by_status(InflightActivationStatus::Delay)
            .await
            .unwrap();
        assert_eq!(count_delay, 0);
        let count_processing = writer
            .store
            .count_by_status(InflightActivationStatus::Processing)
            .await
            .unwrap();
        // Only the existing processing activation should remain, new ones should be blocked
        assert_eq!(count_processing, 1);
    }

    #[tokio::test]
    async fn test_writer_backpressure_db_size_limit_reached() {
        let writer_config = ActivationWriterConfig {
            // 200 rows is ~50KB
            db_max_size: Some(50_000),
            max_buf_len: 100,
            max_pending_activations: 5000,
            max_processing_activations: 5000,
            max_delay_activations: 0,
            write_failure_backoff_ms: 4000,
        };
        let store = Arc::new(
            InflightActivationStore::new(
                &generate_temp_filename(),
                InflightActivationStoreConfig::from_config(&create_integration_config()),
            )
            .await
            .unwrap(),
        );
        let first_round = make_activations(200);
        store.store(first_round).await.unwrap();
        assert!(store.db_size().await.unwrap() > 50_000);

        // Make more activations that won't be stored.
        let second_round = make_activations(10);

        let mut writer = InflightActivationWriter::new(store.clone(), writer_config);
        writer.reduce(second_round).await.unwrap();
        let flush_result = writer.flush().await.unwrap();
        assert!(flush_result.is_none());

        let count_pending = writer.store.count_pending_activations().await.unwrap();
        assert_eq!(count_pending, 200);
    }

    #[tokio::test]
    async fn test_writer_flush_empty_batch() {
        let writer_config = ActivationWriterConfig {
            db_max_size: None,
            max_buf_len: 100,
            max_pending_activations: 10,
            max_processing_activations: 10,
            max_delay_activations: 10,
            write_failure_backoff_ms: 4000,
        };
        let store = Arc::new(
            InflightActivationStore::new(
                &generate_temp_filename(),
                InflightActivationStoreConfig::from_config(&create_integration_config()),
            )
            .await
            .unwrap(),
        );
        let mut writer = InflightActivationWriter::new(store.clone(), writer_config);
        writer.reduce(vec![]).await.unwrap();
        let flush_result = writer.flush().await.unwrap();
        assert!(flush_result.is_some());
    }
}
