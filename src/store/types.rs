pub type BucketRange = (i16, i16);

/// A Kafka topic paired with one of its partition indices. Partition indices
/// overlap across topics, so contention filtering and per-partition gauges must
/// be keyed by the (topic, partition) pair rather than the partition alone.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl std::fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

impl From<&(String, i32)> for TopicPartition {
    fn from((topic, partition): &(String, i32)) -> Self {
        Self::new(topic.clone(), *partition)
    }
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
