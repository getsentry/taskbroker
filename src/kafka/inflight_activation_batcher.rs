use crate::{
    config::Config, runtime_config::RuntimeConfigManager,
    store::inflight_activation::InflightActivation,
};
use chrono::Utc;
use std::{mem::replace, sync::Arc, time::Duration};

use super::consumer::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};

pub struct ActivationBatcherConfig {
    pub max_batch_time_ms: u64,
    pub max_batch_len: usize,
    pub max_batch_size: usize,
}

impl ActivationBatcherConfig {
    /// Convert from application configuration into ActivationBatcher config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_batch_time_ms: config.db_insert_batch_max_time_ms,
            max_batch_len: config.db_insert_batch_max_len,
            max_batch_size: config.db_insert_batch_max_size,
        }
    }
}

pub struct InflightActivationBatcher {
    batch: Vec<InflightActivation>,
    batch_size: usize,
    config: ActivationBatcherConfig,
    runtime_config_manager: Arc<RuntimeConfigManager>,
}

impl InflightActivationBatcher {
    pub fn new(
        config: ActivationBatcherConfig,
        runtime_config_manager: Arc<RuntimeConfigManager>,
    ) -> Self {
        Self {
            batch: Vec::with_capacity(config.max_batch_len),
            batch_size: 0,
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
        let task_name = &t.taskname;

        if runtime_config.drop_task_killswitch.contains(task_name) {
            metrics::counter!("filter.drop_task_killswitch", "taskname" => task_name.clone())
                .increment(1);
            return Ok(());
        }

        if let Some(expires_at) = t.expires_at
            && Utc::now() > expires_at
        {
            metrics::counter!("filter.expired_at_consumer").increment(1);
            return Ok(());
        }

        self.batch_size += t.activation.len();
        self.batch.push(t);

        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<Self::Output>, anyhow::Error> {
        if self.batch.is_empty() {
            return Ok(None);
        }

        metrics::histogram!("consumer.batch_rows").record(self.batch.len() as f64);
        metrics::histogram!("consumer.batch_bytes").record(self.batch_size as f64);

        self.batch_size = 0;

        Ok(Some(replace(
            &mut self.batch,
            Vec::with_capacity(self.config.max_batch_len),
        )))
    }

    fn reset(&mut self) {
        self.batch_size = 0;
        self.batch.clear();
    }

    async fn is_full(&self) -> bool {
        self.batch.len() >= self.config.max_batch_len
            || self.batch_size >= self.config.max_batch_size
    }

    fn get_reduce_config(&self) -> ReduceConfig {
        ReduceConfig {
            shutdown_condition: ReduceShutdownCondition::Signal,
            shutdown_behaviour: ReduceShutdownBehaviour::Drop,
            when_full_behaviour: ReducerWhenFullBehaviour::Flush,
            flush_interval: Some(Duration::from_millis(self.config.max_batch_time_ms)),
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

    use prost::Message;
    use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivation};
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
            id: "0".to_string(),
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
            }
            .encode_to_vec(),
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            received_at: Utc::now(),
            processing_attempts: 0,
            processing_deadline_duration: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".to_string(),
            taskname: "task_to_be_filtered".to_string(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
        };

        batcher.reduce(inflight_activation_0).await.unwrap();
        assert_eq!(batcher.batch.len(), 0);

        fs::remove_file(test_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_task_due_to_expiry() {
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let config = Arc::new(Config::default());
        let mut batcher = InflightActivationBatcher::new(
            ActivationBatcherConfig::from_config(&config),
            runtime_config,
        );

        let inflight_activation_0 = InflightActivation {
            id: "0".to_string(),
            activation: TaskActivation {
                id: "0".to_string(),
                namespace: "namespace".to_string(),
                taskname: "task_to_be_filtered".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: None,
                retry_state: None,
                processing_deadline_duration: 0,
                expires: Some(0),
                delay: None,
            }
            .encode_to_vec(),
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            received_at: Utc::now(),
            processing_attempts: 0,
            processing_deadline_duration: 0,
            expires_at: Some(Utc::now()),
            delay_until: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".to_string(),
            taskname: "task_to_be_filtered".to_string(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
        };

        batcher.reduce(inflight_activation_0).await.unwrap();
        assert_eq!(batcher.batch.len(), 0);
    }

    #[tokio::test]
    async fn test_close_by_bytes_limit() {
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let config = Arc::new(Config {
            db_insert_batch_max_size: 1,
            db_insert_batch_max_len: 2,
            ..Default::default()
        });

        let mut batcher = InflightActivationBatcher::new(
            ActivationBatcherConfig::from_config(&config),
            runtime_config,
        );

        let inflight_activation_0 = InflightActivation {
            id: "0".to_string(),
            activation: TaskActivation {
                id: "0".to_string(),
                namespace: "namespace".to_string(),
                taskname: "taskname".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: None,
                retry_state: None,
                processing_deadline_duration: 0,
                expires: Some(0),
                delay: None,
            }
            .encode_to_vec(),
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            received_at: Utc::now(),
            processing_attempts: 0,
            processing_deadline_duration: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".to_string(),
            taskname: "taskname".to_string(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
        };

        batcher.reduce(inflight_activation_0).await.unwrap();
        assert!(batcher.is_full().await);
        batcher.flush().await.unwrap();
        assert!(!batcher.is_full().await)
    }

    #[tokio::test]
    async fn test_close_by_rows_limit() {
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let config = Arc::new(Config {
            db_insert_batch_max_size: 100000,
            db_insert_batch_max_len: 2,
            ..Default::default()
        });

        let mut batcher = InflightActivationBatcher::new(
            ActivationBatcherConfig::from_config(&config),
            runtime_config,
        );

        let inflight_activation_0 = InflightActivation {
            id: "0".to_string(),
            activation: TaskActivation {
                id: "0".to_string(),
                namespace: "namespace".to_string(),
                taskname: "taskname".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: None,
                retry_state: None,
                processing_deadline_duration: 0,
                expires: Some(0),
                delay: None,
            }
            .encode_to_vec(),
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            received_at: Utc::now(),
            processing_attempts: 0,
            processing_deadline_duration: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".to_string(),
            taskname: "taskname".to_string(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
        };

        let inflight_activation_1 = InflightActivation {
            id: "1".to_string(),
            activation: TaskActivation {
                id: "1".to_string(),
                namespace: "namespace".to_string(),
                taskname: "taskname".to_string(),
                parameters: "{}".to_string(),
                headers: HashMap::new(),
                received_at: None,
                retry_state: None,
                processing_deadline_duration: 0,
                expires: Some(0),
                delay: None,
            }
            .encode_to_vec(),
            status: InflightActivationStatus::Pending,
            partition: 0,
            offset: 0,
            added_at: Utc::now(),
            received_at: Utc::now(),
            processing_attempts: 0,
            processing_deadline_duration: 0,
            expires_at: None,
            delay_until: None,
            processing_deadline: None,
            at_most_once: false,
            namespace: "namespace".to_string(),
            taskname: "taskname".to_string(),
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
        };

        batcher.reduce(inflight_activation_0).await.unwrap();
        batcher.reduce(inflight_activation_1).await.unwrap();
        assert!(batcher.is_full().await);
        batcher.flush().await.unwrap();
        assert!(!batcher.is_full().await)
    }
}
