use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{FetchNextTask, GetTaskRequest, SetTaskStatusRequest};
use tonic::{Code, Request};

use crate::grpc::server::TaskbrokerServer;

use crate::test_utils::{create_redis_test_store, make_activations};

#[tokio::test]
async fn test_get_task() {
    let store = create_redis_test_store().await;
    store.delete_all_keys().await.unwrap();
    let service = TaskbrokerServer { store };
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
    let store = create_redis_test_store().await;
    store.delete_all_keys().await.unwrap();
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
    let store = create_redis_test_store().await;
    store.delete_all_keys().await.unwrap();
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
    let store = create_redis_test_store().await;
    store.delete_all_keys().await.unwrap();
    let activations = make_activations(1);
    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store: store.clone(),
    };
    let request = GetTaskRequest { namespace: None };
    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert!(task.id == "id_0");
    assert!(store.count_pending_activations().await.unwrap() == 0);
    assert!(store.count_processing_activations().await.unwrap() == 1);
}

#[tokio::test]
#[allow(deprecated)]
async fn test_set_task_status_success() {
    let store = create_redis_test_store().await;
    store.delete_all_keys().await.unwrap();
    let activations = make_activations(2);
    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store: store.clone(),
    };

    let request = GetTaskRequest { namespace: None };
    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert!(task.id == "id_0" || task.id == "id_1");
    let first_task_id = task.id.clone();
    let request = SetTaskStatusRequest {
        id: first_task_id.clone(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask { namespace: None }),
    };
    let response = service.set_task_status(Request::new(request)).await;
    println!("response: {:?}", response);
    assert!(response.is_ok(), "response: {:?}", response);
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    let second_task_id = if first_task_id == "id_0" {
        "id_1"
    } else {
        "id_0"
    };
    assert_eq!(task.id, second_task_id);
    let pending_count = store.count_pending_activations().await.unwrap();
    let processing_count = store.count_processing_activations().await.unwrap();
    assert!(pending_count == 0, "pending_count: {:?}", pending_count);
    assert!(
        processing_count == 1,
        "processing_count: {:?}",
        processing_count
    );
}
