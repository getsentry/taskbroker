use std::{sync::Arc, time::Duration};
use tonic::{Request, Response, Status};
use sentry_protos::sentry::v1::{GetTaskRequest, GetTaskResponse, SetTaskStatusRequest, SetTaskStatusResponse};


use taskbroker::grpc_server::MyConsumerService;
use taskbroker::inflight_activation_store::InflightActivationStore;


#[tokio::test]
async fn test_get_task() {
    let store = Arc::new(InflightActivationStore::new("test_db").await.unwrap());
    let service = MyConsumerService{ store };
    let request = GetTaskRequest { };
    let response = service.get_task(Request::new(request)).await;
    match response {
        Ok(response) => {
            assert_eq!(response.task.is_none(), true);
        },
        Err(e) => {
            panic!("Error: {}", e);
        }
    }
}