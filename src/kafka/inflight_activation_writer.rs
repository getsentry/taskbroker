use std::{mem::take, sync::Arc, time::Instant};

use anyhow::Ok;
use chrono::{DateTime, Utc};
use tracing::{debug, instrument};

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
    pub max_delay_activations: usize,
}

impl ActivationWriterConfig {
    /// Convert from application configuration into InflightActivationWriter config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_buf_len: config.db_insert_batch_size,
            max_pending_activations: config.max_pending_count,
            max_delay_activations: config.max_delay_count,
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

        // Check if the entire batch is either pending or delay
        let has_delay = batch
            .iter()
            .any(|activation| activation.status == InflightActivationStatus::Delay);
        let has_pending = batch
            .iter()
            .any(|activation| activation.status == InflightActivationStatus::Pending);

        // Backpressure if any of these conditions are met:
        // 1. The delay limit is exceeded AND either:
        //    - there are delay activations in the batch, OR
        //    - the pending limit is also exceeded
        // 2. The pending limit is exceeded AND there are pending activations
        if (has_delay || exceeded_pending_limit) && exceeded_delay_limit
            || exceeded_pending_limit && has_pending
        {
            return Ok(None);
        }

        let lag = Utc::now()
            - batch
                .iter()
                .map(|item| {
                    let ts = item
                        .activation
                        .received_at
                        .expect("All activations should have received_at");

                    DateTime::from_timestamp(ts.seconds, ts.nanos as u32).unwrap()
                })
                .min_by_key(|item| item.timestamp())
                .unwrap();

        let write_to_store_start = Instant::now();
        let res = self.store.store(take(&mut self.batch).unwrap()).await?;

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
    use chrono::Utc;
    use std::collections::HashMap;

    use sentry_protos::taskbroker::v1::TaskActivation;
    use std::sync::Arc;

    use crate::store::inflight_activation::{
        InflightActivationStatus, InflightActivationStore, InflightActivationStoreConfig,
    };
    use crate::test_utils::{create_integration_config, generate_temp_filename};

    #[tokio::test]
    async fn test_writer_flush_batch() {
        let writer_config = ActivationWriterConfig {
            max_buf_len: 100,
            max_pending_activations: 10,
            max_delay_activations: 10,
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

        let batch = vec![
            InflightActivation {
                activation: TaskActivation {
                    id: "0".to_string(),
                    namespace: "namespace".to_string(),
                    taskname: "pending_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(prost_types::Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                },
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                processing_attempts: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
            },
            InflightActivation {
                activation: TaskActivation {
                    id: "1".to_string(),
                    namespace: "namespace".to_string(),
                    taskname: "delay_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(prost_types::Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                },
                status: InflightActivationStatus::Delay,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                processing_attempts: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
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
            max_buf_len: 100,
            max_pending_activations: 10,
            max_delay_activations: 0,
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

        let batch = vec![InflightActivation {
            activation: TaskActivation {
                id: "0".to_string(),
                namespace: "namespace".to_string(),
                taskname: "pending_task".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                retry_state: None,
                processing_deadline_duration: 0,
                expires: None,
                delay: None,
            },
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            processing_attempts: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".to_string(),
        }];

        writer.reduce(batch).await.unwrap();
        writer.flush().await.unwrap();
        let count_pending = writer.store.count_pending_activations().await.unwrap();
        assert_eq!(count_pending, 1);
    }

    #[tokio::test]
    async fn test_writer_flush_only_delay() {
        let writer_config = ActivationWriterConfig {
            max_buf_len: 100,
            max_pending_activations: 0,
            max_delay_activations: 10,
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

        let batch = vec![InflightActivation {
            activation: TaskActivation {
                id: "0".to_string(),
                namespace: "namespace".to_string(),
                taskname: "pending_task".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                retry_state: None,
                processing_deadline_duration: 0,
                expires: None,
                delay: None,
            },
            status: InflightActivationStatus::Delay,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            processing_attempts: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".to_string(),
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
    async fn test_writer_backpressure() {
        let writer_config = ActivationWriterConfig {
            max_buf_len: 100,
            max_pending_activations: 0,
            max_delay_activations: 0,
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

        let batch = vec![
            InflightActivation {
                activation: TaskActivation {
                    id: "0".to_string(),
                    namespace: "namespace".to_string(),
                    taskname: "pending_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(prost_types::Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                },
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                processing_attempts: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
            },
            InflightActivation {
                activation: TaskActivation {
                    id: "1".to_string(),
                    namespace: "namespace".to_string(),
                    taskname: "delay_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(prost_types::Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                },
                status: InflightActivationStatus::Delay,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                processing_attempts: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
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
            max_buf_len: 100,
            max_pending_activations: 10,
            max_delay_activations: 0,
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

        let batch = vec![
            InflightActivation {
                activation: TaskActivation {
                    id: "0".to_string(),
                    namespace: "namespace".to_string(),
                    taskname: "pending_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(prost_types::Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                },
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                processing_attempts: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
            },
            InflightActivation {
                activation: TaskActivation {
                    id: "1".to_string(),
                    namespace: "namespace".to_string(),
                    taskname: "pending_task".to_string(),
                    parameters: "{}".to_string(),
                    headers: HashMap::new(),
                    received_at: Some(prost_types::Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    retry_state: None,
                    processing_deadline_duration: 0,
                    expires: None,
                    delay: None,
                },
                status: InflightActivationStatus::Pending,
                partition: 0,
                offset: 0,
                added_at: Utc::now(),
                processing_attempts: 0,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                at_most_once: false,
                namespace: "namespace".to_string(),
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
}
