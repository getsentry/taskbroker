use std::time::Duration;

use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError};

#[derive(PartialEq, Debug, Deserialize, Serialize, Validate)]
pub struct PushConfig {
    /// The number of concurrent push threads to run.
    #[validate(range(min = 1))]
    pub threads: usize,

    /// Maximum time for a single push RPC to the worker service. This should be greater than the worker's internal timeout.
    #[serde(with = "crate::serde::duration")]
    #[validate(custom(function = "validate_nonzero_duration"))]
    pub timeout: Duration,
}

fn validate_nonzero_duration(duration: &Duration) -> Result<(), ValidationError> {
    if duration.is_zero() {
        Err(ValidationError::new("nonzero_duration"))
    } else {
        Ok(())
    }
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            timeout: Duration::from_millis(30000),
        }
    }
}
