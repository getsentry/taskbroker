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
    pub async fn new(path: String) -> Self {
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

    pub async fn reload_config(path: &str, config: &Arc<RwLock<RuntimeConfig>>) {
        let contents = fs::read_to_string(path).await;
        let new_config: RuntimeConfig = match contents {
            Ok(contents) => {
                let runtime_config = serde_yaml::from_str::<RuntimeConfig>(&contents);
                match runtime_config {
                    Ok(runtime_config) => runtime_config,
                    Err(e) => {
                        error!("Using default runtime configs. Failed to parse file: {}", e);
                        RuntimeConfig::default()
                    }
                }
            }
            Err(e) => {
                error!(
                    "Using default runtime configs. Failed to read yaml file: {}",
                    e
                );
                RuntimeConfig::default()
            }
        };
        if new_config != *config.read().await {
            *config.write().await = new_config;
            info!("Reloaded new runtime config from {}", path);
        }
    }

    pub async fn read(&self) -> RuntimeConfig {
        let m = self.config.read().await;
        m.clone()
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

        let test_path = "runtime_test_config.yaml";
        fs::write(test_path, test_yaml).await.unwrap();

        let runtime_config = RuntimeConfigManager::new(test_path.to_string()).await;
        let config = runtime_config.read().await;
        assert_eq!(config.drop_task_killswitch.len(), 1);
        assert_eq!(config.drop_task_killswitch[0], "test:do_nothing");

        fs::remove_file(test_path).await.unwrap();
    }
}
