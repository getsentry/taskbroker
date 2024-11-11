use std::{mem::replace, sync::Arc, time::Duration};

use tracing::info;

use crate::inflight_activation_store::{InflightActivation, InflightActivationStore};

use super::kafka::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};

pub struct Config {
    pub max_buf_len: usize,
    pub max_pending_tasks: usize,
    pub flush_interval: Option<Duration>,
    pub when_full_behaviour: ReducerWhenFullBehaviour,
    pub shutdown_behaviour: ReduceShutdownBehaviour,
}

pub struct InflightActivationWriter {
    store: Arc<InflightActivationStore>,
    buffer: Vec<InflightActivation>,
    config: Config,
}

impl InflightActivationWriter {
    pub fn new(store: Arc<InflightActivationStore>, config: Config) -> Self {
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
        let res = self
            .store
            .store(replace(
                &mut self.buffer,
                Vec::with_capacity(self.config.max_buf_len),
            ))
            .await?;
        info!("Inserted {:?} entries", res.rows_affected);
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
                >= self.config.max_pending_tasks
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