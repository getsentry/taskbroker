use std::sync::Arc;

use prost::Message;
use rstest::rstest;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{
    FetchNextTask, GetTaskRequest, SetBatchActivationStatusRequest, SetTaskStatusRequest,
    TaskActivation,
};
use tokio::sync::mpsc;
use tonic::{Code, Request};

use crate::config::{Config, DeliveryMode};
use crate::grpc::server::{StatusUpdate, TaskbrokerServer, flush_updates};
use crate::store::activation::ActivationStatus;
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
        max_attempts: None,
        delay_on_retry: None,
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
        max_attempts: None,
        delay_on_retry: None,
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
    store.store(&activations).await.unwrap();

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
    assert_eq!(row.status, ActivationStatus::Processing);
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

    store.store(&activations).await.unwrap();

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

    store.store(&activations).await.unwrap();

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
    store.store(&activations).await.unwrap();

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
        max_attempts: None,
        delay_on_retry: None,
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

    store.store(&activations).await.unwrap();

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
        max_attempts: None,
        delay_on_retry: None,
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

    store.store(&activations).await.unwrap();

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
        max_attempts: None,
        delay_on_retry: None,
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

    store.store(&activations).await.unwrap();

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
        max_attempts: None,
        delay_on_retry: None,
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
    store.store(&activations).await.unwrap();

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
            max_attempts: None,
            delay_on_retry: None,
        }))
        .await
        .unwrap();

    assert!(
        response.get_ref().task.is_none(),
        "push path returns no next task from the store"
    );

    let update = update_rx.recv().await.expect("status update on channel");
    assert_eq!(update.id, "id_0");
    assert_eq!(update.status, ActivationStatus::Complete);

    let row = store.get_by_id("id_0").await.unwrap().expect("row exists");
    assert_eq!(
        row.status,
        ActivationStatus::Processing,
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
    store.store(&activations).await.unwrap();

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
            max_attempts: None,
            delay_on_retry: None,
        }))
        .await;

    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::Internal);
    assert_eq!(e.message(), "Status update channel closed");
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_batch_activation_status_success(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let activations = make_activations(3);
    store.store(&activations).await.unwrap();

    let service = TaskbrokerServer {
        store: store.clone(),
        config,
        update_tx: None,
    };

    // All updates share a conclusion status and have no retry state, so they
    // are grouped into set_status_batch calls keyed by status.
    let request = SetBatchActivationStatusRequest {
        updates: vec![
            SetTaskStatusRequest {
                id: "id_0".to_string(),
                status: 5, // Complete
                fetch_next_task: None,
                max_attempts: None,
                delay_on_retry: None,
            },
            SetTaskStatusRequest {
                id: "id_1".to_string(),
                status: 5, // Complete
                fetch_next_task: None,
                max_attempts: None,
                delay_on_retry: None,
            },
            SetTaskStatusRequest {
                id: "id_2".to_string(),
                status: 3, // Failure
                fetch_next_task: None,
                max_attempts: None,
                delay_on_retry: None,
            },
        ],
    };

    let response = service
        .set_batch_activation_status(Request::new(request))
        .await;
    assert!(response.is_ok());

    let row0 = store.get_by_id("id_0").await.unwrap().expect("row exists");
    assert_eq!(row0.status, ActivationStatus::Complete);
    let row1 = store.get_by_id("id_1").await.unwrap().expect("row exists");
    assert_eq!(row1.status, ActivationStatus::Complete);
    let row2 = store.get_by_id("id_2").await.unwrap().expect("row exists");
    assert_eq!(row2.status, ActivationStatus::Failure);
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_set_batch_activation_status_empty(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    let request = SetBatchActivationStatusRequest { updates: vec![] };

    let response = service
        .set_batch_activation_status(Request::new(request))
        .await;
    assert!(response.is_ok());
}

#[tokio::test]
#[allow(deprecated)]
async fn test_set_batch_activation_status_invalid_status() {
    let store = create_test_store("sqlite").await;
    let config = create_config();

    let service = TaskbrokerServer {
        store,
        config,
        update_tx: None,
    };

    // 99 is not a valid TaskActivationStatus value, so deserialization fails.
    let request = SetBatchActivationStatusRequest {
        updates: vec![SetTaskStatusRequest {
            id: "test_task".to_string(),
            status: 99,
            fetch_next_task: None,
            max_attempts: None,
            delay_on_retry: None,
        }],
    };

    let response = service
        .set_batch_activation_status(Request::new(request))
        .await;
    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_batch_activation_status_retry(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let activations = make_activations(1);
    store.store(&activations).await.unwrap();

    let service = TaskbrokerServer {
        store: store.clone(),
        config,
        update_tx: None,
    };

    // max_attempts / delay_on_retry are set, so set_status_batch also updates the
    // activation's retry_state in the same transaction as the status flip.
    let request = SetBatchActivationStatusRequest {
        updates: vec![SetTaskStatusRequest {
            id: "id_0".to_string(),
            status: 4, // Retry
            fetch_next_task: None,
            max_attempts: Some(5),
            delay_on_retry: Some(60),
        }],
    };

    let response = service
        .set_batch_activation_status(Request::new(request))
        .await;
    assert!(response.is_ok());

    let row = store.get_by_id("id_0").await.unwrap().expect("row exists");
    assert_eq!(row.status, ActivationStatus::Retry);
    let activation = TaskActivation::decode(&row.activation as &[u8]).unwrap();
    let retry_state = activation.retry_state.expect("retry_state persisted");
    assert_eq!(retry_state.max_attempts, 5);
    assert_eq!(retry_state.delay_on_retry, Some(60));
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
#[allow(deprecated)]
async fn test_set_batch_activation_status_mixed(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let config = create_config();

    let activations = make_activations(3);
    store.store(&activations).await.unwrap();

    let service = TaskbrokerServer {
        store: store.clone(),
        config,
        update_tx: None,
    };

    // Two conclusion updates plus one retry update that carries retry state, all
    // handled by a single set_status_batch call that groups by status and updates
    // the retry update's blob.
    let request = SetBatchActivationStatusRequest {
        updates: vec![
            SetTaskStatusRequest {
                id: "id_0".to_string(),
                status: 5, // Complete
                fetch_next_task: None,
                max_attempts: None,
                delay_on_retry: None,
            },
            SetTaskStatusRequest {
                id: "id_1".to_string(),
                status: 5, // Complete
                fetch_next_task: None,
                max_attempts: None,
                delay_on_retry: None,
            },
            SetTaskStatusRequest {
                id: "id_2".to_string(),
                status: 4, // Retry
                fetch_next_task: None,
                max_attempts: Some(3),
                delay_on_retry: Some(60),
            },
        ],
    };

    let response = service
        .set_batch_activation_status(Request::new(request))
        .await;
    assert!(response.is_ok());

    let row0 = store.get_by_id("id_0").await.unwrap().expect("row exists");
    assert_eq!(row0.status, ActivationStatus::Complete);
    let row1 = store.get_by_id("id_1").await.unwrap().expect("row exists");
    assert_eq!(row1.status, ActivationStatus::Complete);
    let row2 = store.get_by_id("id_2").await.unwrap().expect("row exists");
    assert_eq!(row2.status, ActivationStatus::Retry);
    let activation2 = TaskActivation::decode(&row2.activation as &[u8]).unwrap();
    let retry_state = activation2.retry_state.expect("retry_state persisted");
    assert_eq!(retry_state.max_attempts, 3);
    assert_eq!(retry_state.delay_on_retry, Some(60));
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_flush_updates_mixed_statuses_and_retry(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let activations = make_activations(3);
    store.store(&activations).await.unwrap();

    // A single flush carrying multiple statuses, including a retry update that also
    // mutates retry_state. The flusher groups by status and applies the retry blob.
    let mut buffer = vec![
        StatusUpdate {
            id: "id_0".to_string(),
            status: ActivationStatus::Complete,
            max_attempts: None,
            delay_on_retry: None,
        },
        StatusUpdate {
            id: "id_1".to_string(),
            status: ActivationStatus::Failure,
            max_attempts: None,
            delay_on_retry: None,
        },
        StatusUpdate {
            id: "id_2".to_string(),
            status: ActivationStatus::Retry,
            max_attempts: Some(7),
            delay_on_retry: Some(90),
        },
    ];

    flush_updates(store.clone(), &mut buffer).await;
    assert!(buffer.is_empty(), "buffer is drained on a successful flush");

    let row0 = store.get_by_id("id_0").await.unwrap().expect("row exists");
    assert_eq!(row0.status, ActivationStatus::Complete);
    let row1 = store.get_by_id("id_1").await.unwrap().expect("row exists");
    assert_eq!(row1.status, ActivationStatus::Failure);
    let row2 = store.get_by_id("id_2").await.unwrap().expect("row exists");
    assert_eq!(row2.status, ActivationStatus::Retry);
    let activation2 = TaskActivation::decode(&row2.activation as &[u8]).unwrap();
    let retry_state = activation2.retry_state.expect("retry_state persisted");
    assert_eq!(retry_state.max_attempts, 7);
    assert_eq!(retry_state.delay_on_retry, Some(90));
}
