use std::sync::Arc;
use tonic::{Request, Code};
use sentry_protos::sentry::v1::{GetTaskRequest, SetTaskStatusRequest};
use sentry_protos::sentry::v1::consumer_service_server::ConsumerService;


use taskbroker::grpc_server::MyConsumerService;
use taskbroker::inflight_activation_store::InflightActivationStore;


#[tokio::test]
async fn test_get_task() {
    let store = Arc::new(InflightActivationStore::new("test_db.sqlite").await.unwrap());
    // TODO: clear the db?
    let service = MyConsumerService{ store };
    let request = GetTaskRequest { };
    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    assert_eq!(e.message(), "No pending activation");
}

#[tokio::test]
async fn test_set_task_status() {
    let store = Arc::new(InflightActivationStore::new("test_db.sqlite").await.unwrap());
    let service = MyConsumerService{ store };
    let request = SetTaskStatusRequest {
        id: "test_task".to_string(),
        status: 5, // Complete
        fetch_next: Some(false),
    };
    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert_eq!(resp.get_ref().task.is_none(), true);
    assert_eq!(resp.get_ref().error.is_none(), true);
}

#[tokio::test]
async fn test_set_task_status_invalid() {
    let store = Arc::new(InflightActivationStore::new("test_db.sqlite").await.unwrap());
    let service = MyConsumerService{ store };
    let request = SetTaskStatusRequest {
        id: "test_task".to_string(),
        status: 1, // Invalid
        fetch_next: Some(false),
    };
    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
    assert_eq!(e.message(), "Invalid status, expects 3 (Failure), 4 (Retry), or 5 (Complete)");
}
