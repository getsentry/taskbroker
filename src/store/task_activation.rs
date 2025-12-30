use prost_types::Timestamp;
use sentry_protos::taskbroker::v1;
use std::collections::HashMap;

macro_rules! builder_setter {
    // For types that should accept `impl Into<T>`
    ($field:ident: impl Into<$ty:ty>) => {
        pub fn $field<T: Into<$ty>>(mut self, $field: T) -> Self {
            self.$field = Some($field.into());
            self
        }
    };

    // For types that should be used directly
    ($field:ident: $ty:ty) => {
        pub fn $field(mut self, $field: $ty) -> Self {
            self.$field = Some($field);
            self
        }
    };
}

pub struct TaskActivationBuilder {
    id: Option<String>,
    namespace: Option<String>,
    taskname: Option<String>,
    parameters: Option<String>,
    headers: Option<HashMap<String, String>>,
    received_at: Option<Timestamp>,
    retry_state: Option<v1::RetryState>,
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

    // String fields that accept `impl Into<String>`
    builder_setter!(id: impl Into<String>);
    builder_setter!(namespace: impl Into<String>);
    builder_setter!(taskname: impl Into<String>);
    builder_setter!(parameters: impl Into<String>);

    // Other fields
    builder_setter!(headers: HashMap<String, String>);
    builder_setter!(received_at: Timestamp);
    builder_setter!(retry_state: v1::RetryState);
    builder_setter!(processing_deadline_duration: u64);
    builder_setter!(expires: u64);
    builder_setter!(delay: u64);

    pub fn build(self) -> v1::TaskActivation {
        v1::TaskActivation {
            id: self.id.expect("id is required"),
            namespace: self.namespace.expect("namespace is required"),
            taskname: self.taskname.expect("taskname is required"),
            parameters: self.parameters.unwrap_or_else(|| "{}".to_string()),
            headers: self.headers.unwrap_or_default(),
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
