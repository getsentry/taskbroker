use crate::{
    config::Config, runtime_config::RuntimeConfigManager,
    store::inflight_activation::InflightActivation,
};
use chrono::Utc;
use futures::future::join_all;
use rdkafka::config::ClientConfig;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use std::{mem::replace, sync::Arc, time::Duration};

use super::consumer::{
    ReduceConfig, ReduceShutdownBehaviour, ReduceShutdownCondition, Reducer,
    ReducerWhenFullBehaviour,
};

pub struct ActivationBatcherConfig {
    pub producer_config: ClientConfig,
    pub kafka_cluster: String,
    pub kafka_topic: String,
    pub kafka_long_topic: String,
    pub send_timeout_ms: u64,
    pub max_batch_time_ms: u64,
    pub max_batch_len: usize,
    pub max_batch_size: usize,
}

impl ActivationBatcherConfig {
    /// Convert from application configuration into ActivationBatcher config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            producer_config: config.kafka_producer_config(),
            kafka_cluster: config.kafka_cluster.clone(),
            kafka_topic: config.kafka_topic.clone(),
            kafka_long_topic: config.kafka_long_topic.clone(),
            send_timeout_ms: config.kafka_send_timeout_ms,
            max_batch_time_ms: config.db_insert_batch_max_time_ms,
            max_batch_len: config.db_insert_batch_max_len,
            max_batch_size: config.db_insert_batch_max_size,
        }
    }
}

pub struct InflightActivationBatcher {
    batch: Vec<InflightActivation>,
    batch_size: usize,
    forward_batch: Vec<Vec<u8>>, // payload
    config: ActivationBatcherConfig,
    runtime_config_manager: Arc<RuntimeConfigManager>,
    producer: Arc<FutureProducer>,
    producer_cluster: String,
}

impl InflightActivationBatcher {
    pub fn new(
        config: ActivationBatcherConfig,
        runtime_config_manager: Arc<RuntimeConfigManager>,
    ) -> Self {
        let producer: Arc<FutureProducer> = Arc::new(
            config
                .producer_config
                .create()
                .expect("Could not create kafka producer in inflight activation batcher"),
        );
        let producer_cluster = config
            .producer_config
            .get("bootstrap.servers")
            .unwrap()
            .to_owned();
        Self {
            batch: Vec::with_capacity(config.max_batch_len),
            batch_size: 0,
            forward_batch: Vec::with_capacity(config.max_batch_len),
            config,
            runtime_config_manager,
            producer,
            producer_cluster,
        }
    }
}

impl Reducer for InflightActivationBatcher {
    type Input = InflightActivation;

    type Output = Vec<InflightActivation>;

    async fn reduce(&mut self, t: Self::Input) -> Result<(), anyhow::Error> {
        let runtime_config = self.runtime_config_manager.read().await;
        let forward_topic = runtime_config
            .demoted_topic
            .clone()
            .unwrap_or(self.config.kafka_long_topic.clone());
        let task_name = &t.taskname;
        let namespace = &t.namespace;

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

        if runtime_config.demoted_namespaces.contains(namespace) {
            if forward_topic == self.config.kafka_topic {
                metrics::counter!(
                    "filter.forward_task_demoted_namespace.skipped",
                    "namespace" => namespace.clone(),
                    "taskname" => task_name.clone(),
                )
                .increment(1);
            } else {
                metrics::counter!(
                    "filter.forward_task_demoted_namespace",
                    "namespace" => namespace.clone(),
                    "taskname" => task_name.clone(),
                )
                .increment(1);
                self.forward_batch.push(t.activation.clone());
                return Ok(());
            }
        }

        self.batch_size += t.activation.len();
        self.batch.push(t);

        Ok(())
    }

    async fn flush(&mut self) -> Result<Option<Self::Output>, anyhow::Error> {
        if self.batch.is_empty() && self.forward_batch.is_empty() {
            return Ok(None);
        }

        metrics::histogram!("consumer.batch_rows").record(self.batch.len() as f64);
        metrics::histogram!("consumer.batch_bytes").record(self.batch_size as f64);

        // Send all forward batch in parallel
        if !self.forward_batch.is_empty() {
            let runtime_config = self.runtime_config_manager.read().await;
            let forward_cluster = runtime_config
                .demoted_topic_cluster
                .clone()
                .unwrap_or(self.config.kafka_cluster.clone());
            if self.producer_cluster != forward_cluster {
                let mut new_config = self.config.producer_config.clone();
                new_config.set("bootstrap.servers", &forward_cluster);
                self.producer = Arc::new(
                    new_config
                        .create()
                        .expect("Could not create kafka producer in inflight activation batcher"),
                );
                self.producer_cluster = forward_cluster;
            }
            let forward_topic = runtime_config
                .demoted_topic
                .clone()
                .unwrap_or(self.config.kafka_long_topic.clone());
            let sends = self.forward_batch.iter().map(|payload| {
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

            self.forward_batch.clear();
        }

        self.batch_size = 0;

        Ok(Some(replace(
            &mut self.batch,
            Vec::with_capacity(self.config.max_batch_len),
        )))
    }

    fn reset(&mut self) {
        self.batch_size = 0;
        self.forward_batch.clear();
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
        ActivationBatcherConfig, Config, InflightActivationBatcher, Reducer, RuntimeConfigManager,
    };
    use chrono::Utc;
    use tokio::fs;

    use std::sync::Arc;

    use crate::{
        store::{
            inflight_activation::InflightActivationBuilder, task_activation::TaskActivationBuilder,
        },
        test_utils::generate_unique_namespace,
    };

    #[tokio::test]
    async fn test_drop_task_due_to_killswitch() {
        let test_yaml = r#"
drop_task_killswitch:
  - task_to_be_filtered
demoted_namespaces:
  -"#;

        let test_path = "test_drop_task_due_to_killswitch.yaml";
        fs::write(test_path, test_yaml).await.unwrap();

        let runtime_config = Arc::new(RuntimeConfigManager::new(Some(test_path.to_string())).await);
        let config = Arc::new(Config::default());
        let mut batcher = InflightActivationBatcher::new(
            ActivationBatcherConfig::from_config(&config),
            runtime_config,
        );

        let namespace = generate_unique_namespace();

        let inflight_activation_0 = InflightActivationBuilder::default()
            .id("0")
            .taskname("task_to_be_filtered")
            .namespace(&namespace)
            .activation(
                TaskActivationBuilder::default()
                    .id("0")
                    .taskname("task_to_be_filtered")
                    .namespace(namespace)
                    .build(),
            )
            .build();

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

        let namespace = generate_unique_namespace();

        let inflight_activation_0 = InflightActivationBuilder::default()
            .id("0")
            .taskname("task_to_be_filtered")
            .namespace(&namespace)
            .expires_at(Utc::now())
            .activation(
                TaskActivationBuilder::default()
                    .id("0")
                    .taskname("task_to_be_filtered")
                    .namespace(namespace)
                    .expires(0)
                    .build(),
            )
            .build();

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

        let namespace = generate_unique_namespace();

        let inflight_activation_0 = InflightActivationBuilder::default()
            .id("0")
            .taskname("taskname")
            .namespace(&namespace)
            .activation(
                TaskActivationBuilder::default()
                    .id("0")
                    .taskname("taskname")
                    .namespace(&namespace)
                    .expires(0)
                    .build(),
            )
            .build();

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

        let namespace = generate_unique_namespace();

        let inflight_activation_0 = InflightActivationBuilder::default()
            .id("0")
            .taskname("taskname")
            .namespace(&namespace)
            .activation(
                TaskActivationBuilder::default()
                    .id("0")
                    .taskname("taskname")
                    .namespace(&namespace)
                    .expires(0)
                    .build(),
            )
            .build();

        let inflight_activation_1 = InflightActivationBuilder::default()
            .id("1")
            .taskname("taskname")
            .namespace(&namespace)
            .activation(
                TaskActivationBuilder::default()
                    .id("1")
                    .taskname("taskname")
                    .namespace(&namespace)
                    .expires(0)
                    .build(),
            )
            .build();

        batcher.reduce(inflight_activation_0).await.unwrap();
        batcher.reduce(inflight_activation_1).await.unwrap();
        assert!(batcher.is_full().await);
        batcher.flush().await.unwrap();
        assert!(!batcher.is_full().await)
    }

    #[tokio::test]
    async fn test_forward_task_due_to_demoted_namespace() {
        let test_yaml = r#"
drop_task_killswitch:
  -
demoted_namespaces:
  - bad_namespace
demoted_topic_cluster: 0.0.0.0:9092
demoted_topic: taskworker-demoted"#;

        let test_path = "test_forward_task_due_to_demoted_namespace.yaml";
        fs::write(test_path, test_yaml).await.unwrap();

        let runtime_config = Arc::new(RuntimeConfigManager::new(Some(test_path.to_string())).await);
        let config = Arc::new(Config::default());
        let mut batcher = InflightActivationBatcher::new(
            ActivationBatcherConfig::from_config(&config),
            runtime_config,
        );

        assert_eq!(batcher.producer_cluster, config.kafka_cluster.clone());

        let inflight_activation_0 = InflightActivationBuilder::default()
            .id("0")
            .taskname("task_to_be_filtered")
            .namespace("bad_namespace")
            .activation(
                TaskActivationBuilder::default()
                    .id("0")
                    .taskname("task_to_be_filtered")
                    .namespace("bad_namespace")
                    .build(),
            )
            .build();

        let inflight_activation_1 = InflightActivationBuilder::default()
            .id("1")
            .taskname("good_task")
            .namespace("good_namespace")
            .activation(
                TaskActivationBuilder::default()
                    .id("1")
                    .taskname("good_task")
                    .namespace("good_namespace")
                    .expires(0)
                    .build(),
            )
            .build();

        batcher.reduce(inflight_activation_0).await.unwrap();
        batcher.reduce(inflight_activation_1).await.unwrap();

        assert_eq!(batcher.batch.len(), 1);
        assert_eq!(batcher.forward_batch.len(), 1);

        let flush_result = batcher.flush().await.unwrap();
        assert!(flush_result.is_some());
        assert_eq!(flush_result.as_ref().unwrap().len(), 1);
        assert_eq!(
            flush_result.as_ref().unwrap()[0].namespace,
            "good_namespace"
        );
        assert_eq!(flush_result.as_ref().unwrap()[0].taskname, "good_task");
        assert_eq!(batcher.batch.len(), 0);
        assert_eq!(batcher.forward_batch.len(), 0);
        assert_eq!(batcher.producer_cluster, "0.0.0.0:9092");

        fs::remove_file(test_path).await.unwrap();
    }
}
