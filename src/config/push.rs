use std::time::Duration;

use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::config::batch::BatchConfig;
use crate::config::validate;

#[derive(PartialEq, Debug, Deserialize, Serialize, Validate)]
pub struct PushQueueConfig {
    /// The size of the push queue.
    #[validate(range(min = 1))]
    pub size: usize,

    /// Maximum time to wait when submitting an activation to the push pool.
    #[serde(with = "crate::serde::duration")]
    #[validate(custom(function = "validate::nonzero_duration"))]
    pub timeout: Duration,
}

impl Default for PushQueueConfig {
    fn default() -> Self {
        Self {
            size: 1,
            timeout: Duration::from_millis(5000),
        }
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize, Validate)]
pub struct PushUpdateConfig {
    // Update claimed → processing updates in batches?
    pub batched: bool,

    /// Describes update batching behavior if enabled by `batched`.
    #[validate(nested)]
    pub batch: BatchConfig,
}

impl Default for PushUpdateConfig {
    fn default() -> Self {
        Self {
            batched: false,
            batch: BatchConfig {
                length: 1,
                interval: Duration::from_millis(100),
                size: 1, // We don't use size here
            },
        }
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize, Validate)]
pub struct PushConfig {
    /// The number of concurrent push threads to run.
    #[validate(range(min = 1))]
    pub threads: usize,

    /// Maximum time for a single push RPC to the worker service. This should be greater than the worker's internal timeout.
    #[serde(with = "crate::serde::duration")]
    #[validate(custom(function = "validate::nonzero_duration"))]
    pub timeout: Duration,

    /// The push queue configuration.
    #[validate(nested)]
    pub queue: PushQueueConfig,

    // Configure how claimed → processing updates are performed.
    #[validate(nested)]
    pub update: PushUpdateConfig,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            timeout: Duration::from_millis(30000),
            queue: PushQueueConfig::default(),
            update: PushUpdateConfig::default(),
        }
    }
}
