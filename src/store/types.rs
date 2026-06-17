use crate::store::activation::ActivationStatus;

pub type BucketRange = (i16, i16);

/// A status update for a single activation.
///
/// When `max_attempts` or `delay_on_retry` is set (for Retry status), the
/// activation's `retry_state` is also updated alongside the status column.
#[derive(Clone, Debug)]
pub struct StatusUpdate {
    pub id: String,
    pub status: ActivationStatus,
    pub max_attempts: Option<u32>,
    pub delay_on_retry: Option<u64>,
}

pub struct FailedTasksForwarder {
    pub to_discard: Vec<(String, Vec<u8>)>,
    pub to_deadletter: Vec<(String, Vec<u8>)>,
}

/// Counts pending, delayed, and processing tasks for backpressure and upkeep.
pub struct DepthCounts {
    /// The number of pending tasks in the store.
    pub pending: usize,

    /// Number of delayed tasks in the store.
    pub delay: usize,

    /// Activations claimed for push delivery but not yet marked processing.
    pub claimed: usize,

    /// The number of processing tasks in the store.
    pub processing: usize,
}
