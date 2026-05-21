use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::config::queue::QueueConfig;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct PushConfig {
    /// The number of concurrent pushers each dispatcher should run.
    pub threads: usize,

    /// Maximum time in milliseconds for a single push RPC to the worker service. This should be greater than the worker's internal timeout.
    pub timeout_ms: u64,

    /// The push queue configuration.
    pub queue: QueueConfig,

    /// Maps every application to its worker endpoint, both represented as strings.
    pub worker_map: BTreeMap<String, String>,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            timeout_ms: 30000,
            worker_map: [("sentry".into(), "http://127.0.0.1:50052".into())].into(),
            queue: QueueConfig::new(1, 5000),
        }
    }
}
