use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FetchConfig {
    /// The number of concurrent fetch loops in push mode, which should be ≤ `MAX_FETCH_THREADS` and a power of two.
    /// If it's not a power of two or it's too large, it will be rounded to a valid nearby value.
    pub threads: usize,

    /// Time in milliseconds to wait between fetch attempts when no pending activation is found.
    pub wait_ms: u64,

    /// The number of activations to claim with a single fetch query.
    pub batch_length: i32,

    /// Update statuses from the gRPC server in batches?
    pub batch_status_updates: bool,

    /// The size of a batch of status updates.
    pub status_update_batch_size: usize,

    /// Maximum milliseconds to wait before flushing a batch of status updates.
    pub status_update_interval_ms: u64,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            wait_ms: 100,
            batch_length: 1,
            batch_status_updates: false,
            status_update_batch_size: 1,
            status_update_interval_ms: 100,
        }
    }
}
