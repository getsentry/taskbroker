use std::cmp::Ordering;

use crate::store::inflight_activation::{InflightActivation, InflightActivationStatus};
use chrono::{DateTime, Utc};
use sentry_protos::taskbroker::v1::OnAttemptsExceeded;
use sqlx::FromRow;

/// sqlx Row mapping for activation_blobs table
#[derive(Clone, Debug, PartialEq, FromRow)]
pub struct ActivationBlob {
    /// The task id
    pub id: String,
    /// The protobuf activation that was received from kafka
    pub activation: Vec<u8>,
}

impl TryFrom<InflightActivation> for ActivationBlob {
    type Error = anyhow::Error;

    fn try_from(value: InflightActivation) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            activation: value.activation,
        })
    }
}

/// sqlx Row mapping for activation_metadata table
#[derive(Clone, Debug, PartialEq, FromRow)]
pub struct ActivationMetadata {
    /// The task id
    pub id: String,

    /// Details about the task
    // TODO do these need to be here? Or should they be columns
    // on the activation_blobs
    pub namespace: String,
    pub taskname: String,

    /// The current status of the activation
    pub status: InflightActivationStatus,

    /// The timestamp a task was stored in Kafka
    pub received_at: DateTime<Utc>,

    /// The timestamp when the activation was stored in activation store.
    pub added_at: DateTime<Utc>,

    /// The number of times the activation has been attempted to be processed. This counter is
    /// incremented everytime a task is reset from processing back to pending. When this
    /// exceeds max_processing_attempts, the task is discarded/deadlettered.
    pub processing_attempts: i32,

    /// The duration in seconds that a worker has to complete task execution.
    /// When an activation is moved from pending -> processing a result is expected
    /// in this many seconds.
    pub processing_deadline_duration: u32,

    /// The timestamp for when processing should be complete
    pub processing_deadline: Option<DateTime<Utc>>,

    /// Whether or not the activation uses at_most_once.
    /// When enabled activations are not retried when processing_deadlines
    /// are exceeded.
    pub at_most_once: bool,

    /// What to do when the maximum number of attempts to complete a task is exceeded
    #[sqlx(try_from = "i32")]
    pub on_attempts_exceeded: OnAttemptsExceeded,

    /// If the task has specified an expiry, this is the timestamp after which the task should be removed from inflight store
    pub expires_at: Option<DateTime<Utc>>,

    /// If the task has specified a delay, this is the timestamp after which the task can be sent to workers
    pub delay_until: Option<DateTime<Utc>>,
}

impl ActivationMetadata {
    /// The number of milliseconds between an activation's received timestamp
    /// and the provided datetime
    pub fn received_latency(&self, now: DateTime<Utc>) -> i64 {
        now.signed_duration_since(self.received_at)
            .num_milliseconds()
            - self.delay_until.map_or(0, |delay_until| {
                delay_until
                    .signed_duration_since(self.received_at)
                    .num_milliseconds()
            })
    }

    pub fn pending_entry(&self) -> TimestampEntry {
        // TODO could use ref-str here. clone is simpler to write though.
        TimestampEntry {
            id: self.id.clone(),
            timestamp: Some(self.added_at),
        }
    }

    pub fn processing_entry(&self) -> TimestampEntry {
        TimestampEntry {
            id: self.id.clone(),
            timestamp: self.processing_deadline,
        }
    }

    pub fn delayed_entry(&self) -> TimestampEntry {
        TimestampEntry {
            id: self.id.clone(),
            timestamp: self.delay_until,
        }
    }
}

impl TryFrom<InflightActivation> for ActivationMetadata {
    type Error = anyhow::Error;

    fn try_from(value: InflightActivation) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            namespace: value.namespace,
            taskname: value.taskname,
            status: value.status,
            received_at: value.received_at,
            added_at: value.added_at,
            processing_attempts: value.processing_attempts,
            processing_deadline_duration: value.processing_deadline_duration,
            processing_deadline: value.processing_deadline,
            at_most_once: value.at_most_once,
            on_attempts_exceeded: value.on_attempts_exceeded,
            expires_at: value.expires_at,
            delay_until: value.delay_until,
        })
    }
}

/// We can safely clone from a reference as all fields are cloneable.
impl TryFrom<&InflightActivation> for ActivationMetadata {
    type Error = anyhow::Error;

    fn try_from(value: &InflightActivation) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.clone(),
            namespace: value.namespace.clone(),
            taskname: value.taskname.clone(),
            status: value.status,
            received_at: value.received_at,
            added_at: value.added_at,
            processing_attempts: value.processing_attempts,
            processing_deadline_duration: value.processing_deadline_duration,
            processing_deadline: value.processing_deadline,
            at_most_once: value.at_most_once,
            on_attempts_exceeded: value.on_attempts_exceeded,
            expires_at: value.expires_at,
            delay_until: value.delay_until,
        })
    }
}

/// A struct representing a metadata entry in a binary heap.
/// Typically used with ActivationMetadata
#[derive(Clone, Debug)]
pub struct TimestampEntry {
    pub id: String,
    pub timestamp: Option<DateTime<Utc>>,
}

impl Ord for TimestampEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for TimestampEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TimestampEntry {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

impl Eq for TimestampEntry {}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use chrono::{Duration, Utc};
    use sentry_protos::taskbroker::v1::OnAttemptsExceeded;

    use crate::store::inflight_activation::InflightActivationStatus;

    use super::ActivationMetadata;

    #[test]
    fn heap_entry_methods() {
        let record = ActivationMetadata {
            id: "id_0".into(),
            namespace: "default".into(),
            taskname: "do_stuff".into(),
            received_at: Utc::now(),
            added_at: Utc::now() + Duration::seconds(2),
            processing_attempts: 0,
            processing_deadline_duration: 60,
            processing_deadline: Some(Utc::now() + Duration::seconds(30)),
            at_most_once: false,
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
            expires_at: None,
            delay_until: None,
            status: InflightActivationStatus::Pending,
        };
        let entry = record.pending_entry();
        assert_eq!(record.id, entry.id);
        assert_eq!(record.added_at, entry.timestamp.unwrap());

        let entry = record.processing_entry();
        assert_eq!(record.id, entry.id);
        assert_eq!(record.processing_deadline, entry.timestamp);

        let entry = record.delayed_entry();
        assert_eq!(record.id, entry.id);
        assert_eq!(record.delay_until, entry.timestamp);
    }

    #[test]
    fn entry_cmp() {
        let first = ActivationMetadata {
            id: "id_0".into(),
            namespace: "default".into(),
            taskname: "do_stuff".into(),
            received_at: Utc::now(),
            added_at: Utc::now() + Duration::seconds(2),
            processing_attempts: 0,
            processing_deadline_duration: 60,
            processing_deadline: Some(Utc::now() + Duration::seconds(30)),
            at_most_once: false,
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
            expires_at: None,
            delay_until: None,
            status: InflightActivationStatus::Pending,
        };
        let second = ActivationMetadata {
            id: "id_1".into(),
            namespace: "default".into(),
            taskname: "do_stuff".into(),
            received_at: Utc::now(),
            added_at: Utc::now() + Duration::seconds(3),
            processing_attempts: 0,
            processing_deadline_duration: 60,
            processing_deadline: Some(Utc::now() + Duration::seconds(30)),
            at_most_once: false,
            on_attempts_exceeded: OnAttemptsExceeded::Discard,
            expires_at: None,
            delay_until: None,
            status: InflightActivationStatus::Pending,
        };
        let first_entry = first.pending_entry();
        let second_entry = second.pending_entry();
        assert!(first_entry < second_entry);

        assert_eq!(Ordering::Less, first_entry.cmp(&second_entry));
    }
}
