use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Debug, Deserialize, Clone, PartialEq, Default)]
pub struct RuntimeConfig {
    /// A list of tasks to drop before inserting into sqlite.
    pub drop_task_killswitch: Vec<String>,
}

pub struct RuntimeConfigManager {
    pub config: Arc<RwLock<RuntimeConfig>>,
    pub handler: JoinHandle<()>,
}

impl RuntimeConfigManager {
    pub async fn new(path: Option<String>) -> Self {
        let runtime_config = Arc::new(RwLock::new(Default::default()));
        let _ = Self::reload_config(&path, &runtime_config).await;
        Self {
            config: runtime_config.clone(),
            handler: tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(60));
                loop {
                    Self::reload_config(&path, &runtime_config).await;
                    interval.tick().await;
                }
            }),
        }
    }

    async fn reload_config(path: &Option<String>, config: &Arc<RwLock<RuntimeConfig>>) {
        if path.is_none() {
            return;
        }
        let yaml_path = path.as_ref().unwrap();
        let contents = fs::read_to_string(yaml_path).await;
        let new_config: RuntimeConfig = match contents {
            Ok(contents) => {
                let runtime_config = serde_yaml::from_str::<RuntimeConfig>(&contents);
                match runtime_config {
                    Ok(runtime_config) => runtime_config,
                    Err(e) => {
                        error!(
                            "Failed to parse runtime config file: {}. Continue to use existing runtime configs.",
                            e
                        );
                        return;
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to read yaml file: {}. Continue to use existing runtime configs.",
                    e
                );
                return;
            }
        };
        if new_config != *config.read().await {
            *config.write().await = new_config;
            info!("Reloaded new runtime config from {}", yaml_path);
        }
    }

    pub async fn read(&self) -> RuntimeConfig {
        let m = self.config.read().await;
        m.clone()
    }
}

impl Drop for RuntimeConfigManager {
    fn drop(&mut self) {
        self.handler.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeConfigManager;
    use tokio::fs;

    #[tokio::test]
    async fn test_runtime_config_manager() {
        let test_yaml = r#"
drop_task_killswitch:
  - test:do_nothing"#;

        let test_path = "test_runtime_config_manager.yaml";
        fs::write(test_path, test_yaml).await.unwrap();

        let runtime_config = RuntimeConfigManager::new(Some(test_path.to_string())).await;
        let config = runtime_config.read().await;
        assert_eq!(config.drop_task_killswitch.len(), 1);
        assert_eq!(config.drop_task_killswitch[0], "test:do_nothing");

        fs::remove_file(test_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_missing_runtime_config_file() {
        let test_path = "runtime_test_config.yaml";

        let runtime_config = RuntimeConfigManager::new(Some(test_path.to_string())).await;
        let config = runtime_config.read().await;
        assert_eq!(config.drop_task_killswitch.len(), 0);
    }

    #[tokio::test]
    async fn test_invalid_runtime_config_file() {
        let test_yaml = r#"
droop_task_killswitch:
  - test:do_nothing"#;

        let test_path = "test_invalid_runtime_config_file.yaml";
        fs::write(test_path, test_yaml).await.unwrap();

        let runtime_config = RuntimeConfigManager::new(Some(test_path.to_string())).await;
        let config = runtime_config.read().await;
        println!("config: {config:?}");
        assert_eq!(config.drop_task_killswitch.len(), 0);

        fs::remove_file(test_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_preserve_runtime_config_file() {
        let test_yaml = r#"
drop_task_killswitch:
  - test:do_nothing"#;

        let test_path = "test_preserve_runtime_config_file.yaml";
        fs::write(test_path, test_yaml).await.unwrap();

        let runtime_config = RuntimeConfigManager::new(Some(test_path.to_string())).await;
        let config = runtime_config.read().await;
        assert_eq!(config.drop_task_killswitch.len(), 1);
        assert_eq!(config.drop_task_killswitch[0], "test:do_nothing");

        let invalid_yaml = r#"
droop_task_killswitch:
  - test:do_nothing"#;

        fs::write(test_path, invalid_yaml).await.unwrap();
        RuntimeConfigManager::reload_config(&Some(test_path.to_string()), &runtime_config.config)
            .await;
        let config = runtime_config.read().await;
        assert_eq!(config.drop_task_killswitch.len(), 1);
        assert_eq!(config.drop_task_killswitch[0], "test:do_nothing");

        fs::remove_file(test_path).await.unwrap();
    }
}
