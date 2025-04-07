use super::consumer::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};
use crate::{
    config::Config,
    store::inflight_activation::{InflightActivation, InflightActivationStore},
};
use anyhow::Ok;
use chrono::{DateTime, Utc};
use std::{mem::take, sync::Arc};
use tracing::{debug, instrument};

pub struct ActivationStoreWriterConfig {
    pub max_buf_len: usize,
    pub max_pending_activations: usize,
}

impl ActivationStoreWriterConfig {
    /// Convert from application configuration into InflightActivationWriter config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_buf_len: config.db_insert_batch_size,
            max_pending_activations: config.max_pending_count,
        }
    }
}

pub struct ActivationStoreWriter {
    config: ActivationStoreWriterConfig,
    store: Arc<InflightActivationStore>,
    batch: Option<Vec<InflightActivation>>,
}

impl ActivationStoreWriter {
    pub fn new(store: Arc<InflightActivationStore>, config: ActivationStoreWriterConfig) -> Self {
        Self {
            config,
            store,
            batch: None,
        }
    }
}

impl Reducer for ActivationStoreWriter {
    type Input = Vec<InflightActivation>;

    type Output = Vec<InflightActivation>;

    async fn reduce(&mut self, batch: Self::Input) -> Result<(), anyhow::Error> {
        assert!(self.batch.is_none());
        self.batch = Some(batch);
        Ok(())
    }

    #[instrument(skip_all)]
    async fn flush(&mut self) -> Result<Option<Self::Output>, anyhow::Error> {
        let Some(ref mut batch) = self.batch else {
            return Ok(None);
        };

        if self
            .store
            .count_pending_activations()
            .await
            .expect("Error communicating with activation store")
            + batch.len()
            > self.config.max_pending_activations
        {
            return Ok(None);
        }

        let (ready_activations, delayed_activations): (Vec<_>, Vec<_>) = take(&mut self.batch)
            .unwrap()
            .into_iter()
            .partition(|_| true);

        let lag = Utc::now()
            - ready_activations
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

        let res = self.store.store(ready_activations).await?;
        metrics::histogram!("consumer.inflight_activation_writer.insert_lag")
            .record(lag.num_seconds() as f64);
        metrics::counter!("consumer.inflight_activation_writer.stored")
            .increment(res.rows_affected);
        debug!(
            "Inserted {:?} entries with max lag: {:?}s",
            res.rows_affected,
            lag.num_seconds()
        );

        Ok(Some(delayed_activations))
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
