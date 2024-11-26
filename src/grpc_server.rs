use std::sync::Arc;
use tonic::{Request, Response, Status};

use sentry_protos::sentry::v1::consumer_service_server::ConsumerService;
use sentry_protos::sentry::v1::{
    GetTaskRequest, GetTaskResponse, SetTaskStatusRequest, SetTaskStatusResponse,
    TaskActivationStatus,
};

use super::inflight_activation_store::{InflightActivationStatus, InflightActivationStore};
use tracing::{info, instrument};

pub struct MyConsumerService {
    pub store: Arc<InflightActivationStore>,
}

#[tonic::async_trait]
impl ConsumerService for MyConsumerService {
    #[instrument(skip(self))]
    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskResponse>, Status> {
        info!("Got a get_taskrequest: {:?}", request);
        let inflight = self.store.get_pending_activation().await;
        match inflight {
            Ok(Some(inflight)) => {
                let resp = GetTaskResponse {
                    task: Some(inflight.activation),
                    error: None,
                };
                Ok(Response::new(resp))
            }
            Ok(None) => return Err(Status::not_found("No pending activation")),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    #[instrument(skip(self))]
    async fn set_task_status(
        &self,
        request: Request<SetTaskStatusRequest>,
    ) -> Result<Response<SetTaskStatusResponse>, Status> {
        info!("Got a set_task_status request: {:?}", request);

        let id = request.get_ref().id.clone();

        let proto_status = TaskActivationStatus::try_from(request.get_ref().status);
        let status: InflightActivationStatus = match proto_status {
            Ok(value) => value.into(),
            Err(_) => return Err(Status::invalid_argument("Invalid status")),
        };
        if !status.is_conclusion() {
            return Err(Status::invalid_argument(
                "Invalid status, expects 3 (Failure), 4 (Retry), or 5 (Complete)",
            ));
        }

        let inflight = self.store.set_status(&id, status).await;
        match inflight {
            Ok(()) => {}
            Err(e) => return Err(Status::internal(e.to_string())),
        }

        let mut response = SetTaskStatusResponse {
            task: None,
            error: None,
        };

        if let Some(fetch_next) = request.get_ref().fetch_next {
            if fetch_next {
                let inflight = self.store.get_pending_activation().await;
                match inflight {
                    Ok(Some(inflight)) => {
                        response.task = Some(inflight.activation);
                    }
                    Ok(None) => return Err(Status::not_found("No pending activation")),
                    Err(e) => return Err(Status::internal(e.to_string())),
                }
            }
        }

        Ok(Response::new(response))
    }
}
