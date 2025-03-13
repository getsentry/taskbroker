use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Debug, Deserialize, Clone)]
pub struct RuntimeConfig {
    /// A list of tasks to drop before inserting into sqlite.
    pub drop_task_killswitch: Vec<String>,
}

pub struct RuntimeConfigManager {
    pub config: RwLock<RuntimeConfig>,
    pub path: String,
}

impl RuntimeConfigManager {
    pub fn new(path: String) -> Self {
        let runtime_config = Self::read_yaml_file(&path);
        Self {
            config: RwLock::new(runtime_config),
            path,
        }
    }

    fn read_yaml_file(path: &str) -> RuntimeConfig {
        let contents = std::fs::read_to_string(path)
            .unwrap_or_else(|_| panic!("Failed to read config file from {}", path));
        serde_yaml::from_str(&contents)
            .unwrap_or_else(|_| panic!("Failed to parse YAML from {}", path))
    }

    pub async fn reload_config(&self) {
        let new_config = Self::read_yaml_file(&self.path);
        *self.config.write().await = new_config;
        info!("Reloaded runtime config from {}", self.path);
    }

    pub async fn read(&self) -> RuntimeConfig {
        let m = self.config.read().await;
        m.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeConfigManager;

    #[tokio::test]
    async fn test_runtime_config_manager() {
        let test_yaml = r#"
drop_task_killswitch:
  - test:do_nothing"#;

        let test_path = "runtime_test_config.yaml";
        std::fs::write(test_path, test_yaml).unwrap();

        let runtime_config = RuntimeConfigManager::new(test_path.to_string());
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

        runtime_config.reload_config().await;
        let config = runtime_config.read().await;
        assert_eq!(config.drop_task_killswitch.len(), 2);
        assert_eq!(config.drop_task_killswitch[0], "test:do_nothing");
        assert_eq!(config.drop_task_killswitch[1], "test:also_do_nothing");

        std::fs::remove_file(test_path).unwrap();
    }
}
