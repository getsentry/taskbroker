use std::{mem::replace, sync::Arc, time::Duration};

use crate::{
    config::Config, runtime_config::RuntimeConfigManager,
    store::inflight_activation::InflightActivation,
};

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
            max_buf_len: config.db_insert_batch_size,
        }
    }
}

pub struct InflightActivationBatcher {
    buffer: Vec<InflightActivation>,
    config: ActivationBatcherConfig,
    runtime_config_manager: Arc<RuntimeConfigManager>,
}

impl InflightActivationBatcher {
    pub fn new(
        config: ActivationBatcherConfig,
        runtime_config_manager: Arc<RuntimeConfigManager>,
    ) -> Self {
        Self {
            buffer: Vec::with_capacity(config.max_buf_len),
            config,
            runtime_config_manager,
        }
    }
}

impl Reducer for InflightActivationBatcher {
    type Input = InflightActivation;

    type Output = Vec<InflightActivation>;

    async fn reduce(&mut self, t: Self::Input) -> Result<(), anyhow::Error> {
        let runtime_config = self.runtime_config_manager.read().await;
        let task_name = &t.activation.taskname;
        if !runtime_config.drop_task_killswitch.contains(task_name) {
            self.buffer.push(t);
        } else {
            metrics::counter!("filter.drop_task_killswitch", "taskname" => task_name.clone())
                .increment(1);
        }

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

#[cfg(test)]
mod tests {
    use super::{
        ActivationBatcherConfig, Config, InflightActivation, InflightActivationBatcher, Reducer,
        RuntimeConfigManager,
    };
    use chrono::Utc;
    use std::collections::HashMap;
    use tokio::fs;

    use sentry_protos::taskbroker::v1::TaskActivation;
    use std::sync::Arc;

    use crate::store::inflight_activation::InflightActivationStatus;

    #[tokio::test]
    async fn test_drop_task_due_to_killswitch() {
        let test_yaml = r#"
drop_task_killswitch:
  - task_to_be_filtered"#;

        let test_path = "test_drop_task_due_to_killswitch.yaml";
        fs::write(test_path, test_yaml).await.unwrap();

        let runtime_config = Arc::new(RuntimeConfigManager::new(Some(test_path.to_string())).await);
        let config = Arc::new(Config::default());
        let mut batcher = InflightActivationBatcher::new(
            ActivationBatcherConfig::from_config(&config),
            runtime_config,
        );

        let inflight_activation_0 = InflightActivation {
            activation: TaskActivation {
                id: "0".to_string(),
                namespace: "namespace".to_string(),
                taskname: "task_to_be_filtered".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: None,
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
        };

        batcher.reduce(inflight_activation_0).await.unwrap();
        assert_eq!(batcher.buffer.len(), 0);

        fs::remove_file(test_path).await.unwrap();
    }
}
