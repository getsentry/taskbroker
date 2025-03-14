use std::{mem::replace, time::Duration};

use crate::{config::Config, store::inflight_activation::InflightActivation};

use super::consumer::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};

pub struct ActivationBatcherConfig {
    pub max_buf_len: usize,
}

impl ActivationBatcherConfig {
    /// Convert from application configuration into ActivationBatcher config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_buf_len: config.max_pending_buffer_count,
        }
    }
}

pub struct InflightActivationBatcher {
    buffer: Vec<InflightActivation>,
    config: ActivationBatcherConfig,
}

impl InflightActivationBatcher {
    pub fn new(config: ActivationBatcherConfig) -> Self {
        Self {
            buffer: Vec::with_capacity(config.max_buf_len),
            config,
        }
    }
}

impl Reducer for InflightActivationBatcher {
    type Input = InflightActivation;

    type Output = Vec<InflightActivation>;

    async fn reduce(&mut self, t: Self::Input) -> Result<(), anyhow::Error> {
        self.buffer.push(t);
        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<Self::Output>, anyhow::Error> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        Ok(Some(replace(
            &mut self.buffer,
            Vec::with_capacity(self.config.max_buf_len),
        )))
    }

    fn reset(&mut self) {
        self.buffer.clear();
    }

    async fn is_full(&self) -> bool {
        self.buffer.len() >= self.config.max_buf_len
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        ReduceConfig {
            shutdown_condition: ReduceShutdownCondition::Signal,
            shutdown_behaviour: ReduceShutdownBehaviour::Drop,
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            flush_interval: Some(Duration::from_secs(1)),
        }
    }
}
