pub type BucketRange = (i16, i16);

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
