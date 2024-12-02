use std::{mem::replace, sync::Arc, time::Duration};

use chrono::DateTime;
use tracing::debug;

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
    pub flush_interval: Option<Duration>,
    pub when_full_behaviour: ReducerWhenFullBehaviour,
    pub shutdown_behaviour: ReduceShutdownBehaviour,
}

impl ActivationWriterConfig {
    /// Convert from application configuration into InflightActivationWriter config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_buf_len: config.max_pending_buffer_count,
            max_pending_activations: config.max_pending_count,
            flush_interval: Some(Duration::from_secs(4)),
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            shutdown_behaviour: ReduceShutdownBehaviour::Drop,
        }
    }
}

pub struct InflightActivationWriter {
    store: Arc<InflightActivationStore>,
    buffer: Vec<InflightActivation>,
    config: ActivationWriterConfig,
}

impl InflightActivationWriter {
    pub fn new(store: Arc<InflightActivationStore>, config: ActivationWriterConfig) -> Self {
        Self {
            store,
            buffer: Vec::with_capacity(config.max_buf_len),
            config,
        }
    }
}

impl Reducer for InflightActivationWriter {
    type Input = InflightActivation;

    type Output = ();

    async fn reduce(&mut self, t: Self::Input) -> Result<(), anyhow::Error> {
        self.buffer.push(t);
        Ok(())
    }

    async fn flush(&mut self) -> Result<Self::Output, anyhow::Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let records = replace(
            &mut self.buffer,
            Vec::with_capacity(self.config.max_buf_len),
        );

        let oldest = records
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

        let res = self.store.store(records).await?;

        metrics::histogram!("consumer.inflight_activation_writer.insert_lag")
            .record(oldest.timestamp() as f64);
        metrics::counter!("consumer.inflight_activation_writer.stored")
            .increment(res.rows_affected);
        debug!("Inserted {:?} entries {:?} lag", res.rows_affected, oldest);

        Ok(())
    }

    fn reset(&mut self) {
        self.buffer.clear();
    }

    async fn is_full(&self) -> bool {
        self.buffer.len() >= self.config.max_buf_len
            || self
                .store
                .count_pending_activations()
                .await
                .expect("Error communicating with activation store")
                + self.buffer.len()
                >= self.config.max_pending_activations
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        ReduceConfig {
            shutdown_condition: ReduceShutdownCondition::Signal,
            shutdown_behaviour: ReduceShutdownBehaviour::Flush,
            when_full_behaviour: self.config.when_full_behaviour,
            flush_interval: self.config.flush_interval,
        }
    }
}
