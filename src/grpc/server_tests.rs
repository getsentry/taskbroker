use std::sync::Arc;

use prost::Message;
use rstest::rstest;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{
    FetchNextTask, GetTaskRequest, SetTaskStatusRequest, TaskActivation, TaskActivationStatus,
    TaskError,
};
use tonic::{Code, Request};

use crate::config::{Config, DeliveryMode};
use crate::grpc::server::TaskbrokerServer;
use crate::store::activation::InflightActivationStatus;
use crate::test_utils::{
    create_config, create_test_store, make_activations, make_failing_store, seed_inflight,
};

#[tokio::test]
async fn test_get_task_push_mode_returns_permission_denied() {
    let store = create_test_store("sqlite").await;
    let config = Arc::new(Config {
        delivery_mode: DeliveryMode::Push,
        ..Config::default()
    });

    let service = TaskbrokerServer { store, config };
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

    let service = TaskbrokerServer { store, config };
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

    let service = TaskbrokerServer { store, config };
    let request = SetTaskStatusRequest {
        id: "test_task".to_string(),
        status: 5, // Complete
        fetch_next_task: None,
        error: None,
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

    let service = TaskbrokerServer { store, config };
    let request = SetTaskStatusRequest {
        id: "test_task".to_string(),
        status: 1, // Invalid
        fetch_next_task: None,
        error: None,
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
    assert_eq!(task.id, "id_0");

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

    let service = TaskbrokerServer { store, config };
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

    let service = TaskbrokerServer { store, config };
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

    let service = TaskbrokerServer { store, config };

    let request = GetTaskRequest {
        namespace: None,
        application: None,
    };
    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_some());
    let task = resp.get_ref().task.as_ref().unwrap();
    assert_eq!(task.id, "id_0");

    let request = SetTaskStatusRequest {
        id: "id_0".to_string(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask {
            namespace: None,
            application: None,
        }),
        error: None,
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

    let service = TaskbrokerServer { store, config };
    let request = SetTaskStatusRequest {
        id: "id_0".to_string(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask {
            application: Some("hammers".into()),
            namespace: None,
        }),
        error: None,
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

    let service = TaskbrokerServer { store, config };
    // Request a task from an application without any activations.
    let request = SetTaskStatusRequest {
        id: "id_0".to_string(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask {
            application: Some("no-matches".into()),
            namespace: None,
        }),
        error: None,
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

    let service = TaskbrokerServer { store, config };
    let request = SetTaskStatusRequest {
        id: "id_0".to_string(),
        status: 5, // Complete
        fetch_next_task: Some(FetchNextTask {
            application: None,
            namespace: Some(namespace),
        }),
        error: None,
    };
    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_ok());
    assert!(
        response.unwrap().get_ref().task.is_none(),
        "namespace without application yields no next task in response"
    );
}

#[tokio::test]
async fn set_task_status_failure_with_error_logs_after_persist() {
    use tracing_test::traced_test;

    #[traced_test]
    async fn inner() {
        let store = create_test_store("sqlite").await;
        let config = create_config();
        let id = seed_inflight(&store, "sentry.tasks.store.save_event", "ingest.errors").await;

        let server = TaskbrokerServer {
            store: store.clone(),
            config,
        };

        server
            .set_task_status(Request::new(SetTaskStatusRequest {
                id: id.clone(),
                status: TaskActivationStatus::Failure as i32,
                fetch_next_task: None,
                error: Some(TaskError {
                    exception_type: "django.db.utils.OperationalError".into(),
                    exception_message: r#"could not access file "$libdir/btree_gist""#.into(),
                    traceback: Some("Traceback ...".into()),
                }),
            }))
            .await
            .unwrap();

        assert!(logs_contain("task reported failure"));
        assert!(logs_contain("sentry.tasks.store.save_event"));
        assert!(logs_contain("django.db.utils.OperationalError"));
        assert!(logs_contain("btree_gist"));
        assert_eq!(
            store.get_by_id(&id).await.unwrap().unwrap().status,
            InflightActivationStatus::Failure,
        );
    }

    inner().await;
}

#[tokio::test]
async fn set_task_status_retry_with_error_logs() {
    use tracing_test::traced_test;

    #[traced_test]
    async fn inner() {
        let store = create_test_store("sqlite").await;
        let config = create_config();
        let id = seed_inflight(&store, "x", "y").await;
        let server = TaskbrokerServer {
            store: store.clone(),
            config,
        };

        server
            .set_task_status(Request::new(SetTaskStatusRequest {
                id,
                status: TaskActivationStatus::Retry as i32,
                fetch_next_task: None,
                error: Some(TaskError {
                    exception_type: "requests.exceptions.ConnectionError".into(),
                    exception_message: "connection reset".into(),
                    traceback: None,
                }),
            }))
            .await
            .unwrap();

        assert!(logs_contain("task reported failure"));
        assert!(logs_contain("requests.exceptions.ConnectionError"));
    }

    inner().await;
}

#[tokio::test]
async fn set_task_status_retry_without_error_is_silent() {
    use tracing_test::traced_test;

    #[traced_test]
    async fn inner() {
        let store = create_test_store("sqlite").await;
        let config = create_config();
        let id = seed_inflight(&store, "x", "y").await;
        let server = TaskbrokerServer { store, config };

        server
            .set_task_status(Request::new(SetTaskStatusRequest {
                id,
                status: TaskActivationStatus::Retry as i32,
                fetch_next_task: None,
                error: None,
            }))
            .await
            .unwrap();

        assert!(!logs_contain("task reported failure"));
    }

    inner().await;
}

#[tokio::test]
async fn set_task_status_failure_without_error_still_logs_task_context() {
    use tracing_test::traced_test;

    #[traced_test]
    async fn inner() {
        let store = create_test_store("sqlite").await;
        let config = create_config();
        let id = seed_inflight(&store, "some.task", "some.namespace").await;
        let server = TaskbrokerServer { store, config };

        server
            .set_task_status(Request::new(SetTaskStatusRequest {
                id: id.clone(),
                status: TaskActivationStatus::Failure as i32,
                fetch_next_task: None,
                error: None,
            }))
            .await
            .unwrap();

        assert!(logs_contain("task reported failure"));
        assert!(logs_contain("some.task"));
        assert!(logs_contain("some.namespace"));
    }

    inner().await;
}

#[tokio::test]
async fn set_task_status_does_not_log_when_store_update_fails() {
    use tracing_test::traced_test;

    #[traced_test]
    async fn inner() {
        let store = make_failing_store().await;
        let config = create_config();
        let server = TaskbrokerServer { store, config };

        let _ = server
            .set_task_status(Request::new(SetTaskStatusRequest {
                id: "abc".into(),
                status: TaskActivationStatus::Failure as i32,
                fetch_next_task: None,
                error: Some(TaskError {
                    exception_type: "X".into(),
                    exception_message: "Y".into(),
                    traceback: None,
                }),
            }))
            .await;

        assert!(!logs_contain("task reported failure"));
    }

    inner().await;
}

#[tokio::test]
async fn set_task_status_complete_does_not_log_failure() {
    use tracing_test::traced_test;

    #[traced_test]
    async fn inner() {
        let store = create_test_store("sqlite").await;
        let config = create_config();
        let id = seed_inflight(&store, "x", "y").await;
        let server = TaskbrokerServer { store, config };

        server
            .set_task_status(Request::new(SetTaskStatusRequest {
                id,
                status: TaskActivationStatus::Complete as i32,
                fetch_next_task: None,
                error: None,
            }))
            .await
            .unwrap();

        assert!(!logs_contain("task reported failure"));
    }

    inner().await;
}
