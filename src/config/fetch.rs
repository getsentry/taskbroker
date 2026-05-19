use serde::{Deserialize, Serialize};

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct FetchConfig {
    /// The number of concurrent fetch loops in push mode, which should be ≤ `MAX_FETCH_THREADS` and a power of two.
    /// If it's not a power of two or it's too large, it will be rounded to a valid nearby value.
    pub threads: usize,

    /// Time in milliseconds to wait between fetch attempts when no pending activation is found.
    pub wait_ms: u64,

    /// The number of activations to claim with a single fetch query.
    pub batch_length: i32,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            wait_ms: 100,
            batch_length: 1,
        }
    }
}
