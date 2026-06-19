use std::time::Duration;

use serde::{Deserialize, Serialize};
use validator::Validate;

/// Configures batching behavior. Currently used only by `PushUpdateConfig`, but can be used
/// anywhere batching happens. Fields can be ignored.
#[derive(PartialEq, Debug, Deserialize, Serialize, Validate)]
pub struct BatchConfig {
    /// The maximum or desired length of a batch, depending on context.
    #[validate(range(min = 1))]
    pub length: usize,

    /// The maximum or desired size in bytes of a batch, depending on context.
    #[validate(range(min = 1))]
    pub size: usize,

    /// How often the batch should be flushed. If set to zero, it will be flushed repeatedly without waiting.
    #[serde(with = "crate::serde::duration")]
    pub interval: Duration,
}
