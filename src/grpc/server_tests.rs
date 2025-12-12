use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{FetchNextTask, GetTaskRequest, SetTaskStatusRequest};
use tonic::{Code, Request};

use crate::grpc::server::TaskbrokerServer;

use crate::test_utils::{create_test_store, make_activations};

#[tokio::test]
async fn test_get_task() {
    let store = create_test_store().await;
    let service = TaskbrokerServer { store };
    let request = GetTaskRequest { namespace: None, application: None };
    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    assert_eq!(e.message(), "No pending activation");
}

#[tokio::test]
#[allow(deprecated)]
async fn test_set_task_status() {
    let store = create_test_store().await;
    let service = TaskbrokerServer { store };
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
    let store = create_test_store().await;
    let service = TaskbrokerServer { store };
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
        "Invalid status, expects 3 (Failure), 4 (Retry), or 5 (Complete), but got: Pending"
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn test_get_task_success() {
    let store = create_test_store().await;
    let activations = make_activations(1);
    store.store(activations).await.unwrap();

    let service = TaskbrokerServer { store };
    let request = GetTaskRequest { namespace: None, application: None };
    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert!(task.id == "id_0");
}

#[tokio::test]
#[allow(deprecated)]
async fn test_set_task_status_success() {
    let store = create_test_store().await;
    let activations = make_activations(2);
    store.store(activations).await.unwrap();

    let service = TaskbrokerServer { store };

    let request = GetTaskRequest { namespace: None, application: None};
    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert!(task.id == "id_0");

    let request = SetTaskStatusRequest {
        id: "id_0".to_string(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask { namespace: None, application: None}),
    };
    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert_eq!(task.id, "id_1");
}
