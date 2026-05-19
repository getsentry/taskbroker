use serde::{Deserialize, Serialize};

/// Configuration for a general queue.
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct QueueConfig {
    /// The size of the queue.
    pub size: usize,

    /// Maximum time in milliseconds to wait when appending to the queue.
    pub timeout_ms: u64,
}

impl QueueConfig {
    pub fn new(size: usize, timeout_ms: u64) -> Self {
        Self { size, timeout_ms }
    }
}
