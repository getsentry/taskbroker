use serde::Deserialize;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{error, info};

#[derive(Debug, Deserialize, Clone, PartialEq, Default)]
pub struct RuntimeConfig {
    /// A list of tasks to drop before inserting into sqlite.
    pub drop_task_killswitch: Vec<String>,
}

pub struct RuntimeConfigManager {
    pub config: RwLock<RuntimeConfig>,
    pub path: String,
}

impl RuntimeConfigManager {
    pub async fn new(path: String) -> Self {
        let runtime_config = Self::read_yaml_file(&path).await;
        Self {
            config: RwLock::new(runtime_config),
            path,
        }
    }

    async fn read_yaml_file(path: &str) -> RuntimeConfig {
        let contents = fs::read_to_string(path).await;
        match contents {
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
        }
    }

    pub async fn reload_config(&self) -> Result<bool, anyhow::Error> {
        let new_config = Self::read_yaml_file(&self.path).await;
        if new_config != *self.config.read().await {
            *self.config.write().await = new_config;
            info!("Reloaded new runtime config from {}", self.path);
            Ok(true)
        } else {
            Ok(false)
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

        std::fs::write(
            test_path,
            r#"
drop_task_killswitch:
  - test:do_nothing
  - test:also_do_nothing"#,
        )
        .unwrap();

        let _ = runtime_config.reload_config().await;
        let config = runtime_config.read().await;
        assert_eq!(config.drop_task_killswitch.len(), 2);
        assert_eq!(config.drop_task_killswitch[0], "test:do_nothing");
        assert_eq!(config.drop_task_killswitch[1], "test:also_do_nothing");

        fs::remove_file(test_path).await.unwrap();
    }
}
