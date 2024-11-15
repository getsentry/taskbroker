use sentry_protos::sentry::v1::consumer_service_server::ConsumerService;
use sentry_protos::sentry::v1::{GetTaskRequest, SetTaskStatusRequest};
use std::sync::Arc;
use std::{future::Future, pin::Pin, task};
use tonic::{Code, Request};

use async_once_cell::Lazy;

use taskbroker::grpc_server::MyConsumerService;
use taskbroker::inflight_activation_store::InflightActivationStore;

struct F(Option<Pin<Box<dyn Future<Output = Arc<InflightActivationStore>> + Send>>>);
impl Future for F {
    type Output = Arc<InflightActivationStore>;
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> task::Poll<Arc<InflightActivationStore>> {
        Pin::new(self.0.get_or_insert_with(|| {
            Box::pin(async {
                let store = Arc::new(
                    InflightActivationStore::new("test_db.sqlite")
                        .await
                        .unwrap(),
                );
                store.clear().await.unwrap();
                store
            })
        }))
        .poll(cx)
    }
}

static STORE: Lazy<Arc<InflightActivationStore>, F> = Lazy::new(F(None));

#[tokio::test]
async fn test_get_task() {
    let store = STORE.get_unpin().await.clone();
    let service = MyConsumerService { store };
    let request = GetTaskRequest {};
    let response = service.get_task(Request::new(request)).await;
    assert!(response.is_err());
    let e = response.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    assert_eq!(e.message(), "No pending activation");
}

#[tokio::test]
async fn test_set_task_status() {
    let store = STORE.get_unpin().await.clone();
    let service = MyConsumerService { store };
    let request = SetTaskStatusRequest {
        id: "test_task".to_string(),
        status: 5, // Complete
        fetch_next: Some(false),
    };
    let response = service.set_task_status(Request::new(request)).await;
    assert!(response.is_ok());
    let resp = response.unwrap();
    assert!(resp.get_ref().task.is_none());
    assert!(resp.get_ref().error.is_none());
}

#[tokio::test]
async fn test_set_task_status_invalid() {
    let store = STORE.get_unpin().await.clone();
    let service = MyConsumerService { store };
    let request = SetTaskStatusRequest {
        id: "test_task".to_string(),
        status: 1, // Invalid
        fetch_next: Some(false),
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
