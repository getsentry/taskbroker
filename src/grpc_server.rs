
use tonic::{Request, Response, Status};

use sentry_protos::sentry::v1::{GetTaskRequest, GetTaskResponse, SetTaskStatusRequest, SetTaskStatusResponse};
use sentry_protos::sentry::v1::consumer_service_server::ConsumerService;

#[derive(Debug, Default)]
pub struct MyConsumerService {}

#[tonic::async_trait]
impl ConsumerService for MyConsumerService {
    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskResponse>, Status> {
        println!("Got a request: {:?}", request);

        let resp = GetTaskResponse {
            task: None,
            error: None,
        };
        Ok(Response::new(resp))
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