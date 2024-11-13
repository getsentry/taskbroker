
use tonic::{Request, Response, Status};
use std::sync::Arc;


use sentry_protos::sentry::v1::{GetTaskRequest, GetTaskResponse, SetTaskStatusRequest, Error, SetTaskStatusResponse};
use sentry_protos::sentry::v1::consumer_service_server::ConsumerService;

use super::inflight_activation_store::InflightActivationStore;

pub struct MyConsumerService {
    pub store: Arc<InflightActivationStore>,
}

#[tonic::async_trait]
impl ConsumerService for MyConsumerService {
    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskResponse>, Status> {
        println!("Got a request: {:?}", request);
        let inflight = self.store.get_pending_activation().await;
        match inflight {
            Ok(Some(inflight)) => {
                let resp = GetTaskResponse {
                    task: Some(inflight.activation),
                    error: None,
                };
                Ok(Response::new(resp))
            },
            Ok(None) => {
                let resp = GetTaskResponse {
                    task: None,
                    error: Some(Error { code: 404, message: "No pending activation".to_string(), details: vec![] }),
                };
                Ok(Response::new(resp))
            },
            Err(e) => {
                Err(Status::not_found(e.to_string()))
            }
        }
    }

    async fn set_task_status(
        &self,
        request: Request<SetTaskStatusRequest>,
    ) -> Result<Response<SetTaskStatusResponse>, Status> {
        println!("Got a request: {:?}", request);

        let resp = SetTaskStatusResponse {
            task: None,
            error: None,
        };
        Ok(Response::new(resp))
    }
}