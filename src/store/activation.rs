use std::fmt::{Display, Formatter, Result as FmtResult};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivationStatus};
use sqlx::Type;

/// The members of this enum should be a superset of the members
/// of `InflightActivationStatus` in `sentry_protos`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Type)]
pub enum InflightActivationStatus {
    /// Unused but necessary to align with sentry-protos
    Unspecified,
    Pending,
    Claimed,
    Processing,
    Failure,
    Retry,
    Complete,
    Delay,
}

impl Display for InflightActivationStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{:?}", self)
    }
}

impl FromStr for InflightActivationStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "Unspecified" {
            Ok(InflightActivationStatus::Unspecified)
        } else if s == "Pending" {
            Ok(InflightActivationStatus::Pending)
        } else if s == "Claimed" {
            Ok(InflightActivationStatus::Claimed)
        } else if s == "Processing" {
            Ok(InflightActivationStatus::Processing)
        } else if s == "Failure" {
            Ok(InflightActivationStatus::Failure)
        } else if s == "Retry" {
            Ok(InflightActivationStatus::Retry)
        } else if s == "Complete" {
            Ok(InflightActivationStatus::Complete)
        } else if s == "Delay" {
            Ok(InflightActivationStatus::Delay)
        } else {
            Err(format!("Unknown inflight activation status string: {}", s))
        }
    }
}

impl InflightActivationStatus {
    /// Is the current value a 'conclusion' status that can be supplied over GRPC.
    pub fn is_conclusion(&self) -> bool {
        matches!(
            self,
            InflightActivationStatus::Complete
                | InflightActivationStatus::Retry
                | InflightActivationStatus::Failure
        )
    }
}

impl From<TaskActivationStatus> for InflightActivationStatus {
    fn from(item: TaskActivationStatus) -> Self {
        match item {
            TaskActivationStatus::Unspecified => InflightActivationStatus::Unspecified,
            TaskActivationStatus::Pending => InflightActivationStatus::Pending,
            TaskActivationStatus::Processing => InflightActivationStatus::Processing,
            TaskActivationStatus::Failure => InflightActivationStatus::Failure,
            TaskActivationStatus::Retry => InflightActivationStatus::Retry,
            TaskActivationStatus::Complete => InflightActivationStatus::Complete,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Builder)]
#[builder(pattern = "owned")]
#[builder(build_fn(name = "_build"))]
#[builder(field(public))]
pub struct InflightActivation {
    #[builder(setter(into))]
    pub id: String,

    // The task application
    #[builder(setter(into), default = "sentry".into())]
    pub application: String,

    /// The task namespace.
    #[builder(setter(into))]
    pub namespace: String,

    /// The task name.
    #[builder(setter(into))]
    pub taskname: String,

    /// The Protobuf activation that was received from Kafka.
    #[builder(setter(custom))]
    pub activation: Vec<u8>,

    /// The current status of the activation
    #[builder(default = InflightActivationStatus::Pending)]
    pub status: InflightActivationStatus,

    /// The partition the activation was received from
    #[builder(default = 0)]
    pub partition: i32,

    /// The offset the activation had
    #[builder(default = 0)]
    pub offset: i64,

    /// The timestamp when the activation was stored in activation store.
    #[builder(default = Utc::now())]
    pub added_at: DateTime<Utc>,

    /// The timestamp a task was stored in Kafka
    #[builder(default = Utc::now())]
    pub received_at: DateTime<Utc>,

    /// The number of times the activation has been attempted to be processed. This counter is
    /// incremented everytime a task is reset from processing back to pending. When this
    /// exceeds max_processing_attempts, the task is discarded/deadlettered.
    #[builder(default = 0)]
    pub processing_attempts: i32,

    /// The duration in seconds that a worker has to complete task execution.
    /// When an activation is moved from pending -> processing a result is expected
    /// in this many seconds.
    #[builder(default = 0)]
    pub processing_deadline_duration: i32,

    /// If the task has specified an expiry, this is the timestamp after which the task should be removed from inflight store
    #[builder(default = None, setter(strip_option))]
    pub expires_at: Option<DateTime<Utc>>,

    /// If the task has specified a delay, this is the timestamp after which the task can be sent to workers
    #[builder(default = None, setter(strip_option))]
    pub delay_until: Option<DateTime<Utc>>,

    /// The timestamp for when processing should be complete
    #[builder(default = None, setter(strip_option))]
    pub processing_deadline: Option<DateTime<Utc>>,

    /// If a task is still claimed after this time, upkeep may release the claim.
    #[builder(default = None, setter(strip_option))]
    pub claim_expires_at: Option<DateTime<Utc>>,

    /// What to do when the maximum number of attempts to complete a task is exceeded
    #[builder(default = OnAttemptsExceeded::Discard)]
    pub on_attempts_exceeded: OnAttemptsExceeded,

    /// Whether or not the activation uses at_most_once.
    /// When enabled activations are not retried when processing_deadlines
    /// are exceeded.
    #[builder(default = false)]
    pub at_most_once: bool,

    /// Bucket derived from activation ID (UUID as number % 256). Set once on ingestion.
    #[builder(setter(skip), default = "0")]
    pub bucket: i16,
}

impl InflightActivation {
    /// The number of milliseconds between an activation's received timestamp and the provided datetime.
    pub fn received_latency(&self, now: DateTime<Utc>) -> i64 {
        now.signed_duration_since(self.received_at)
            .num_milliseconds()
            - self.delay_until.map_or(0, |delay_until| {
                delay_until
                    .signed_duration_since(self.received_at)
                    .num_milliseconds()
            })
    }
}
