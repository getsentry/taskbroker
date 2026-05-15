use std::sync::Arc;

use prost::Message;
use rstest::rstest;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{
    FetchNextTask, GetTaskRequest, SetTaskStatusRequest, TaskActivation,
};
use tokio::sync::mpsc;
use tonic::{Code, Request};

use crate::config::{Config, DeliveryMode};
use crate::grpc::server::{StatusUpdate, TaskbrokerServer};
use crate::store::activation::InflightActivationStatus;
use crate::test_utils::{create_config, create_test_store, make_activations};

#[tokio::test]
async fn test_get_task_push_mode_returns_permission_denied() {
    let store = create_test_store("sqlite").await;
    let config = Arc::new(Config {
        delivery_mode: DeliveryMode::Push,
        ..Config::default()
    });

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    let request = GetTaskRequest {
        namespace: None,
        application: None,
    };

    let response = service.get_task(Request::new(request)).await;

    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
    assert_eq!(e.message(), "Cannot call while broker is in PUSH mode");
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_task(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    let request = GetTaskRequest {
        namespace: None,
        application: None,
    };

    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    assert_eq!(e.message(), "No pending activations");
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_task_status(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

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
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_task_status_invalid(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

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
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_get_task_success(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let activations = make_activations(1);
    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store: store.clone(),
        config,
        update_tx: None,
    };

    let request = GetTaskRequest {
        namespace: None,
        application: None,
    };

    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert!(task.id == "id_0");

    let row = store.get_by_id("id_0").await.unwrap().expect("claimed row");
    assert_eq!(row.status, InflightActivationStatus::Processing);
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_get_task_with_application_success(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let mut activations = make_activations(2);

    let mut payload = TaskActivation::decode(&activations[1].activation as &[u8]).unwrap();
    payload.application = Some("hammers".into());
    activations[1].activation = payload.encode_to_vec();
    activations[1].application = "hammers".into();

    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    let request = GetTaskRequest {
        namespace: None,
        application: Some("hammers".into()),
    };

    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert_eq!(task.id, "id_1");
    assert_eq!(task.application, Some("hammers".into()));
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_get_task_with_namespace_requires_application(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let activations = make_activations(2);
    let namespace = activations[0].namespace.clone();

    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    let request = GetTaskRequest {
        namespace: Some(namespace),
        application: None,
    };

    let response = service.get_task(Request::new(request)).await;

    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_task_status_success(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let activations = make_activations(2);
    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    let request = GetTaskRequest {
        namespace: None,
        application: None,
    };

    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert!(task.id == "id_0");

    let request = SetTaskStatusRequest {
        id: "id_0".to_string(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask {
            namespace: None,
            application: None,
        }),
    };
    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert_eq!(task.id, "id_1");
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_task_status_with_application(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let mut activations = make_activations(2);

    let mut payload = TaskActivation::decode(&activations[1].activation as &[u8]).unwrap();
    payload.application = Some("hammers".into());
    activations[1].activation = payload.encode_to_vec();
    activations[1].application = "hammers".into();

    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    let request = SetTaskStatusRequest {
        id: "id_0".to_string(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask {
            application: Some("hammers".into()),
            namespace: None,
        }),
    };

    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_ok());

    let resp = response.unwrap();
    let task_opt = &resp.get_ref().task;
    assert!(task_opt.is_some());

    let task = task_opt.as_ref().unwrap();
    assert_eq!(task.id, "id_1");
    assert_eq!(task.application, Some("hammers".into()));
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_task_status_with_application_no_match(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let mut activations = make_activations(2);

    let mut payload = TaskActivation::decode(&activations[1].activation as &[u8]).unwrap();
    payload.application = Some("hammers".into());
    activations[1].activation = payload.encode_to_vec();
    activations[1].application = "hammers".into();

    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    // Request a task from an application without any activations.
    let request = SetTaskStatusRequest {
        id: "id_0".to_string(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask {
            application: Some("no-matches".into()),
            namespace: None,
        }),
    };

    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_ok());
    assert!(response.unwrap().get_ref().task.is_none());
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_task_status_with_namespace_requires_application(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let activations = make_activations(2);
    let namespace = activations[0].namespace.clone();

    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    let request = SetTaskStatusRequest {
        id: "id_0".to_string(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask {
            application: None,
            namespace: Some(namespace),
        }),
    };

    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_ok());
    assert!(
        response.unwrap().get_ref().task.is_none(),
        "namespace without application yields no next task in response"
    );
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_task_status_forwards_to_update_channel(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let (update_tx, mut update_rx) = mpsc::channel::<StatusUpdate>(8);

    let activations = make_activations(2);
    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store: store.clone(),
        config,
        update_tx: Some(update_tx),
    };

    let response = service
        .get_task(Request::new(GetTaskRequest {
            namespace: None,
            application: None,
        }))
        .await
        .unwrap();
    assert_eq!(response.get_ref().task.as_ref().unwrap().id, "id_0");

    let response = service
        .set_task_status(Request::new(SetTaskStatusRequest {
            id: "id_0".to_string(),
            status: 5, // Complete
            fetch_next_task: Some(FetchNextTask {
                namespace: None,
                application: None,
            }),
        }))
        .await
        .unwrap();

    assert!(
        response.get_ref().task.is_none(),
        "push path returns no next task from the store"
    );

    let (id, status) = update_rx.recv().await.expect("status update on channel");
    assert_eq!(id, "id_0");
    assert_eq!(status, InflightActivationStatus::Complete);

    let row = store.get_by_id("id_0").await.unwrap().expect("row exists");
    assert_eq!(
        row.status,
        InflightActivationStatus::Processing,
        "handler does not write status; flush_updates applies channel batches"
    );
}

#[tokio::test]
async fn test_set_task_status_update_channel_closed_returns_internal() {
    let store = create_test_store("sqlite").await;
    let config = create_config();

    let (update_tx, update_rx) = mpsc::channel::<StatusUpdate>(8);
    drop(update_rx);

    let activations = make_activations(1);
    store.store(activations).await.unwrap();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: Some(update_tx),
    };

    let response = service
        .set_task_status(Request::new(SetTaskStatusRequest {
            id: "id_0".to_string(),
            status: 5,
            fetch_next_task: None,
        }))
        .await;

    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::Internal);
    assert_eq!(e.message(), "Status update channel closed");
}
