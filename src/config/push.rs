use std::time::Duration;

use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError};

// (TODO) Create a `validate` module to keep all of our custom validators (there are now at least two).
fn validate_nonzero_duration(duration: &Duration) -> Result<(), ValidationError> {
    if duration.is_zero() {
        Err(ValidationError::new("nonzero_duration"))
    } else {
        Ok(())
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize, Validate)]
pub struct PushQueueConfig {
    /// The size of the push queue.
    #[validate(range(min = 1))]
    pub size: usize,

    /// Maximum time to wait when submitting an activation to the push pool.
    #[serde(with = "crate::serde::duration")]
    #[validate(custom(function = "validate_nonzero_duration"))]
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
pub struct PushConfig {
    /// The number of concurrent push threads to run.
    #[validate(range(min = 1))]
    pub threads: usize,

    /// Maximum time for a single push RPC to the worker service. This should be greater than the worker's internal timeout.
    #[serde(with = "crate::serde::duration")]
    #[validate(custom(function = "validate_nonzero_duration"))]
    pub timeout: Duration,

    /// The push queue configuration.
    #[validate(nested)]
    pub queue: PushQueueConfig,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            timeout: Duration::from_millis(30000),
            queue: PushQueueConfig::default(),
        }
    }
}
