use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::config::queue::QueueConfig;

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct PushConfig {
    /// The number of concurrent pushers each dispatcher should run.
    pub threads: usize,

    /// Maximum time in milliseconds for a single push RPC to the worker service. This should be greater than the worker's internal timeout.
    pub timeout_ms: u64,

    /// The push queue configuration.
    pub queue: QueueConfig,

    /// Update statuses from the gRPC server in batches?
    pub batch_status_updates: bool,

    /// The size of a batch of status updates.
    pub status_update_batch_size: usize,

    /// Maximum milliseconds to wait before flushing a batch of status updates.
    pub status_update_interval_ms: u64,

    /// Maps every application to its worker endpoint, both represented as strings.
    pub worker_map: BTreeMap<String, String>,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            timeout_ms: 30000,
            batch_status_updates: false,
            status_update_batch_size: 1,
            status_update_interval_ms: 100,
            worker_map: [("sentry".into(), "http://127.0.0.1:50052".into())].into(),
            queue: QueueConfig::new(1, 5000),
        }
    }
}
