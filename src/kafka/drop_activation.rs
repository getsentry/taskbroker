use crate::kafka::consumer::Filter;
use crate::runtime_config::RuntimeConfigManager;
use crate::store::inflight_activation::InflightActivation;
use anyhow::Error;
use std::sync::Arc;

pub struct DropActivationKillswitch {
    runtime_config_manager: Arc<RuntimeConfigManager>,
}

impl DropActivationKillswitch {
    pub fn new(runtime_config_manager: Arc<RuntimeConfigManager>) -> Self {
        Self {
            runtime_config_manager,
        }
    }
}

impl Filter for DropActivationKillswitch {
    type Input = InflightActivation;

    async fn filter(&self, activation: &Self::Input) -> Result<bool, Error> {
        let runtime_config = self.runtime_config_manager.read().await;
        let task_name = &activation.activation.taskname;
        if runtime_config.drop_task_killswitch.contains(task_name) {
            metrics::counter!("filter.drop_task_killswitch", "taskname" => task_name.clone())
                .increment(1);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
