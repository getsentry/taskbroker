use rand::Rng;
use std::collections::HashMap;

use crate::inflight_activation_store::{InflightActivation, InflightActivationStore, TaskActivationStatus};
use chrono::Utc;
use sentry_protos::sentry::v1::TaskActivation;

/// Generate a unique filename for isolated SQLite databases.
pub fn generate_temp_filename() -> String {
    let mut rng = rand::thread_rng();
    format!("/var/tmp/{}-{}.sqlite", Utc::now(), rng.gen::<u64>())
}

/// Create a collection of pending unsaved activations.
pub fn make_activations(count: u32) -> Vec<InflightActivation> {
    let mut records: Vec<InflightActivation> = vec![];
    for i in 0..count {
        #[allow(deprecated)]
        let item = InflightActivation {
            activation: TaskActivation {
                id: format!("id_{}", i),
                namespace: "namespace".into(),
                taskname: "taskname".into(),
                parameters: "{}".into(),
                headers: HashMap::new(),
                received_at: Some(prost_types::Timestamp {
                    seconds: Utc::now().timestamp(),
                    nanos: 0,
                }),
                deadline: None,
                retry_state: None,
                processing_deadline_duration: 10,
                expires: None,
            },
            status: TaskActivationStatus::Pending,
            partition: 0,
            offset: i as i64,
            added_at: Utc::now(),
            deadletter_at: None,
            processing_deadline: None,
        };
        records.push(item);
    }
    records
}

pub async fn assert_count_by_status(store: &InflightActivationStore, status: TaskActivationStatus, expected: usize) {
    let count = store.count_by_status(status).await.unwrap();
    assert_eq!(count, expected);
}
