use std::{mem::take, sync::Arc};

use anyhow::Ok;
use chrono::{DateTime, Utc};
use tracing::{debug, instrument};

use crate::{
    config::Config,
    inflight_activation_store::{InflightActivation, InflightActivationStore},
};

use super::kafka::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};

pub struct ActivationWriterConfig {
    pub max_buf_len: usize,
    pub max_pending_activations: usize,
}

impl ActivationWriterConfig {
    /// Convert from application configuration into InflightActivationWriter config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_buf_len: config.max_pending_buffer_count,
            max_pending_activations: config.max_pending_count,
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
    async fn flush(&mut self) -> Result<Self::Output, anyhow::Error> {
        let Some(batch) = take(&mut self.batch) else {
            return Ok(());
        };
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

        let res = self.store.store(batch).await?;
        metrics::histogram!("consumer.inflight_activation_writer.insert_lag")
            .record(lag.num_seconds() as f64);
        metrics::counter!("consumer.inflight_activation_writer.stored")
            .increment(res.rows_affected);
        debug!(
            "Inserted {:?} entries with max lag: {:?}s",
            res.rows_affected,
            lag.num_seconds()
        );

        Ok(())
    }

    fn reset(&mut self) {}

    async fn is_full(&self) -> bool {
        self.batch.is_some()
            || self
                .store
                .count_pending_activations()
                .await
                .expect("Error communicating with activation store")
                + self.config.max_buf_len
                > self.config.max_pending_activations
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
