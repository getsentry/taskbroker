use std::mem::replace;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::future::join_all;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::config::Config;
use crate::runtime_config::RuntimeConfigManager;
use crate::store::activation::Activation;

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
    /// Convert from application configuration into ActivationBatcher config for a
    /// single consumed topic. Each consumer has its own batcher, so the topic is
    /// passed explicitly rather than derived from "the" consumable topic.
    pub fn from_topic(config: &Config, topic_name: &str) -> Self {
        let topic_config = config
            .kafka_topics
            .get(topic_name)
            .unwrap_or_else(|| panic!("unknown topic '{topic_name}'"));
        let cluster = config
            .cluster(&topic_config.cluster)
            .expect("cluster not found");

        Self {
            producer_config: config.kafka_producer_config(),
            kafka_cluster: cluster.address.clone(),
            kafka_topic: topic_name.to_owned(),
            kafka_long_topic: config.kafka_long_topic.clone(),
            send_timeout_ms: config.kafka_send_timeout_ms,
            max_batch_time_ms: config.db_insert_batch_max_time_ms,
            max_batch_len: config.db_insert_batch_max_len,
            max_batch_size: config.db_insert_batch_max_size,
        }
    }
}

pub struct ActivationBatcher {
    batch: Vec<Activation>,
    batch_size: usize,
    forward_batch: Vec<Vec<u8>>, // payload
    config: ActivationBatcherConfig,
    runtime_config_manager: Arc<RuntimeConfigManager>,
    producer: Arc<FutureProducer>,
    producer_cluster: String,
}

impl ActivationBatcher {
    pub fn new(
        config: ActivationBatcherConfig,
        runtime_config_manager: Arc<RuntimeConfigManager>,
    ) -> Self {
        let producer: Arc<FutureProducer> = Arc::new(
            config
                .producer_config
                .create()
                .expect("Could not create kafka producer in activation batcher"),
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

impl Reducer for ActivationBatcher {
    type Input = Activation;

    type Output = Vec<Activation>;

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
                        .expect("Could not create kafka producer in activation batcher"),
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
    use std::io::Write;
    use std::sync::Arc;

    use chrono::Utc;
    use tempfile::NamedTempFile;

    use crate::store::activation::ActivationBuilder;
    use crate::test_utils::{TaskActivationBuilder, generate_unique_namespace};

    use super::{
        ActivationBatcher, ActivationBatcherConfig, Config, Reducer, RuntimeConfigManager,
    };

    #[tokio::test]
    async fn test_drop_task_due_to_killswitch() {
        let test_yaml = r#"
drop_task_killswitch:
  - task_to_be_filtered
demoted_namespaces:
  -"#;

        let mut config_file = NamedTempFile::new().unwrap();
        writeln!(config_file, "{}", test_yaml).unwrap();
        config_file.flush().unwrap();

        let runtime_config = Arc::new(
            RuntimeConfigManager::new(Some(config_file.path().to_str().unwrap().to_string())).await,
        );
        let mut config = Config::default();
        config.normalize_and_validate().unwrap();
        let config = Arc::new(config);
        let mut batcher = ActivationBatcher::new(
            ActivationBatcherConfig::from_topic(&config, config.consumable_topics().unwrap()[0].0),
            runtime_config,
        );

        let namespace = generate_unique_namespace();

        let activation_0 = ActivationBuilder::new()
            .id("0")
            .taskname("task_to_be_filtered")
            .namespace(&namespace)
            .build(TaskActivationBuilder::new());

        batcher.reduce(activation_0).await.unwrap();
        assert_eq!(batcher.batch.len(), 0);
    }

    #[tokio::test]
    async fn test_drop_task_due_to_expiry() {
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let mut config = Config::default();
        config.normalize_and_validate().unwrap();
        let config = Arc::new(config);
        let mut batcher = ActivationBatcher::new(
            ActivationBatcherConfig::from_topic(&config, config.consumable_topics().unwrap()[0].0),
            runtime_config,
        );

        let namespace = generate_unique_namespace();

        let activation_0 = ActivationBuilder::new()
            .id("0")
            .taskname("task_to_be_filtered")
            .namespace(&namespace)
            .expires_at(Utc::now())
            .build(TaskActivationBuilder::new());

        batcher.reduce(activation_0).await.unwrap();
        assert_eq!(batcher.batch.len(), 0);
    }

    #[tokio::test]
    async fn test_close_by_bytes_limit() {
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let mut config = Config {
            db_insert_batch_max_size: 1,
            db_insert_batch_max_len: 2,
            ..Default::default()
        };
        config.normalize_and_validate().unwrap();
        let config = Arc::new(config);

        let mut batcher = ActivationBatcher::new(
            ActivationBatcherConfig::from_topic(&config, config.consumable_topics().unwrap()[0].0),
            runtime_config,
        );

        let namespace = generate_unique_namespace();

        let activation_0 = ActivationBuilder::new()
            .id("0")
            .taskname("taskname")
            .namespace(&namespace)
            .build(TaskActivationBuilder::new());

        batcher.reduce(activation_0).await.unwrap();
        assert!(batcher.is_full().await);
        batcher.flush().await.unwrap();
        assert!(!batcher.is_full().await)
    }

    #[tokio::test]
    async fn test_close_by_rows_limit() {
        let runtime_config = Arc::new(RuntimeConfigManager::new(None).await);
        let mut config = Config {
            db_insert_batch_max_size: 100000,
            db_insert_batch_max_len: 2,
            ..Default::default()
        };
        config.normalize_and_validate().unwrap();
        let config = Arc::new(config);

        let mut batcher = ActivationBatcher::new(
            ActivationBatcherConfig::from_topic(&config, config.consumable_topics().unwrap()[0].0),
            runtime_config,
        );

        let namespace = generate_unique_namespace();

        let activation_0 = ActivationBuilder::new()
            .id("0")
            .taskname("taskname")
            .namespace(&namespace)
            .build(TaskActivationBuilder::new());

        let activation_1 = ActivationBuilder::new()
            .id("1")
            .taskname("taskname")
            .namespace(&namespace)
            .build(TaskActivationBuilder::new());

        batcher.reduce(activation_0).await.unwrap();
        batcher.reduce(activation_1).await.unwrap();
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

        let mut config_file = NamedTempFile::new().unwrap();
        writeln!(config_file, "{}", test_yaml).unwrap();
        config_file.flush().unwrap();

        let runtime_config = Arc::new(
            RuntimeConfigManager::new(Some(config_file.path().to_str().unwrap().to_string())).await,
        );
        let mut config = Config::default();
        config.normalize_and_validate().unwrap();
        let config = Arc::new(config);
        let mut batcher = ActivationBatcher::new(
            ActivationBatcherConfig::from_topic(&config, config.consumable_topics().unwrap()[0].0),
            runtime_config,
        );

        let (_, topic_config) = config.consumable_topics().unwrap()[0];
        let cluster_address = config
            .cluster(&topic_config.cluster)
            .unwrap()
            .address
            .clone();
        assert_eq!(batcher.producer_cluster, cluster_address);

        let activation_0 = ActivationBuilder::new()
            .id("0")
            .taskname("task_to_be_filtered")
            .namespace("bad_namespace")
            .build(TaskActivationBuilder::new());

        let activation_1 = ActivationBuilder::new()
            .id("1")
            .taskname("good_task")
            .namespace("good_namespace")
            .build(TaskActivationBuilder::new());

        batcher.reduce(activation_0).await.unwrap();
        batcher.reduce(activation_1).await.unwrap();

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
    }
}
