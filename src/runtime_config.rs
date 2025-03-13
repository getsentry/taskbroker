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
