use std::collections::HashMap;

use prost_types::Timestamp;
use sentry_protos::taskbroker::v1::{self, RetryState};

/// Build `TaskActivation`s by only providing values you care about.
///
/// ### Required Fields
/// - `id`
/// - `namespace`
/// - `taskname`
///
/// ### Usage
///
/// ```rs
/// TaskActivationBuilder::new()
///     .id("task-123")
///     .namespace("my-namespace")
///     .taskname("my-task")
///     .build()
/// ```
///
/// The code above is equivalent to the snippet below.
///
/// ```rs
/// TaskActivation {
///     id: "task-123".to_string(),
///     namespace: "my-namespace".to_string(),
///     taskname: "my-task".to_string(),
///     parameters: "{}".to_string(),
///     headers: HashMap::new(),
///     processing_deadline_duration: 0,
///     received_at: None,
///     retry_state: None,
///     expires: None,
///     delay: None,
/// }
/// ```
///
pub struct TaskActivationBuilder {
    id: Option<String>,
    namespace: Option<String>,
    taskname: Option<String>,
    parameters: Option<String>,
    headers: Option<HashMap<String, String>>,
    received_at: Option<Timestamp>,
    retry_state: Option<RetryState>,
    processing_deadline_duration: Option<u64>,
    expires: Option<u64>,
    delay: Option<u64>,
}

impl TaskActivationBuilder {
    pub fn new() -> Self {
        Self {
            id: None,
            namespace: None,
            taskname: None,
            parameters: None,
            headers: None,
            received_at: None,
            retry_state: None,
            processing_deadline_duration: None,
            expires: None,
            delay: None,
        }
    }

    pub fn id<T: Into<String>>(mut self, id: T) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn namespace<T: Into<String>>(mut self, namespace: T) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn taskname<T: Into<String>>(mut self, taskname: T) -> Self {
        self.taskname = Some(taskname.into());
        self
    }

    pub fn parameters<T: Into<String>>(mut self, parameters: T) -> Self {
        self.parameters = Some(parameters.into());
        self
    }

    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn received_at(mut self, received_at: Timestamp) -> Self {
        self.received_at = Some(received_at);
        self
    }

    pub fn retry_state(mut self, retry_state: RetryState) -> Self {
        self.retry_state = Some(retry_state);
        self
    }

    pub fn processing_deadline_duration(mut self, duration: u64) -> Self {
        self.processing_deadline_duration = Some(duration);
        self
    }

    pub fn expires(mut self, expires: u64) -> Self {
        self.expires = Some(expires);
        self
    }

    pub fn delay(mut self, delay: u64) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn build(self) -> v1::TaskActivation {
        v1::TaskActivation {
            id: self.id.expect("id is required"),
            namespace: self.namespace.expect("namespace is required"),
            taskname: self.taskname.expect("taskname is required"),
            parameters: self.parameters.unwrap_or_else(|| "{}".to_string()),
            headers: self.headers.unwrap_or_else(|| HashMap::new()),
            processing_deadline_duration: self.processing_deadline_duration.unwrap_or(0),
            received_at: self.received_at,
            retry_state: self.retry_state,
            expires: self.expires,
            delay: self.delay,
        }
    }
}

impl Default for TaskActivationBuilder {
    fn default() -> Self {
        Self::new()
    }
}
