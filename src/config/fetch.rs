use std::time::Duration;

use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError};

use crate::fetch::MAX_FETCH_THREADS;

// (TODO) Create a `validate` module to keep all of our custom validators (there are now at least two).
fn validate_power_of_two(n: usize) -> Result<(), ValidationError> {
    if n.is_power_of_two() {
        Ok(())
    } else {
        Err(ValidationError::new("not_power_of_two"))
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize, Validate)]
pub struct FetchConfig {
    /// The number of concurrent fetch loops in push mode, which should be ≤ `MAX_FETCH_THREADS` and a power of two.
    #[validate(range(min = 1, max = MAX_FETCH_THREADS), custom(function = "validate_power_of_two"))]
    pub threads: usize,

    /// Time in milliseconds to wait between fetch attempts when no pending activation is found.
    #[serde(with = "crate::serde::duration")]
    pub backoff: Duration,

    /// The number of activations to claim with a single fetch query.
    #[validate(range(min = 1))]
    pub batch_length: i32,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            backoff: Duration::from_millis(100),
            batch_length: 1,
        }
    }
}
