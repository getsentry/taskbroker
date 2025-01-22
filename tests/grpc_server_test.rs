use chrono::Utc;
use rand::Rng;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{GetTaskRequest, SetTaskStatusRequest};
use std::sync::Arc;
use tonic::{Code, Request};

use taskbroker::grpc_server::MyConsumerService;
use taskbroker::inflight_activation_store::InflightActivationStore;

fn generate_temp_filename() -> String {
    let mut rng = rand::thread_rng();
    format!("/var/tmp/{}-{}.sqlite", Utc::now(), rng.gen::<u64>())
}

#[tokio::test]
async fn test_get_task() {
    let url = generate_temp_filename();
    let store = Arc::new(InflightActivationStore::new(&url).await.unwrap());
    let service = MyConsumerService { store };
    let request = GetTaskRequest { namespace: None };
    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    assert_eq!(e.message(), "No pending activation");
}

#[tokio::test]
#[allow(deprecated)]
async fn test_set_task_status() {
    let url = generate_temp_filename();
    let store = Arc::new(InflightActivationStore::new(&url).await.unwrap());
    let service = MyConsumerService { store };
    let request = SetTaskStatusRequest {
        id: "test_task".to_string(),
        status: 5, // Complete
        fetch_next_task: None,
    };
    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_none());
}

#[tokio::test]
#[allow(deprecated)]
async fn test_set_task_status_invalid() {
    let url = generate_temp_filename();
    let store = Arc::new(InflightActivationStore::new(&url).await.unwrap());
    let service = MyConsumerService { store };
    let request = SetTaskStatusRequest {
        id: "test_task".to_string(),
        status: 1, // Invalid
        fetch_next_task: None,
    };
    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
    assert_eq!(
        e.message(),
        "Invalid status, expects 3 (Failure), 4 (Retry), or 5 (Complete)"
    );
}
