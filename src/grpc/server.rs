use chrono::Utc;
use prost::Message;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{
    FetchNextTask, GetTaskRequest, GetTaskResponse, SetTaskStatusRequest, SetTaskStatusResponse,
    TaskActivation, TaskActivationStatus,
};
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};

use crate::store::inflight_activation::{InflightActivationStatus, InflightActivationStore};
use crate::store::inflight_redis_activation::RedisActivationStore;
use tracing::{error, instrument};

pub struct TaskbrokerServer {
    pub store: Arc<RedisActivationStore>,
}

#[tonic::async_trait]
impl ConsumerService for TaskbrokerServer {
    #[instrument(skip_all)]
    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskResponse>, Status> {
        let start_time = Instant::now();
        let namespace = &request.get_ref().namespace;
        let inflight = self
            .store
            .get_pending_activation(namespace.as_deref())
            .await;

        match inflight {
            Ok(Some(inflight)) => {
                let now = Utc::now();
                if inflight.processing_attempts < 1 {
                    let received_to_gettask_latency = inflight.received_latency(now);
                    if received_to_gettask_latency > 0 {
                        metrics::histogram!(
                            "grpc_server.received_to_gettask.latency",
                            "namespace" => inflight.namespace,
                            "taskname" => inflight.taskname,
                        )
                        .record(received_to_gettask_latency as f64);
                    }
                }
                let resp = GetTaskResponse {
                    task: Some(TaskActivation::decode(&inflight.activation as &[u8]).unwrap()),
                };
                metrics::histogram!("grpc_server.get_task.duration").record(start_time.elapsed());

                Ok(Response::new(resp))
            }
            Ok(None) => Err(Status::not_found("No pending activation")),
            Err(e) => {
                error!("Unable to retrieve pending activation: {:?}", e);
                Err(Status::internal("Unable to retrieve pending activation"))
            }
        }
    }

    #[instrument(skip_all)]
    async fn set_task_status(
        &self,
        request: Request<SetTaskStatusRequest>,
    ) -> Result<Response<SetTaskStatusResponse>, Status> {
        let start_time = Instant::now();
        let id = request.get_ref().id.clone();

        let status: InflightActivationStatus =
            TaskActivationStatus::try_from(request.get_ref().status)
                .map_err(|e| {
                    Status::invalid_argument(format!("Unable to deserialize status: {e:?}"))
                })?
                .into();

        if !status.is_conclusion() {
            return Err(Status::invalid_argument(format!(
                "Invalid status, expects 3 (Failure), 4 (Retry), or 5 (Complete), but got: {status:?}"
            )));
        }
        if status == InflightActivationStatus::Failure {
            metrics::counter!("grpc_server.set_status.failure").increment(1);
        }

        let update_result = self.store.set_status(&id, status).await;
        if let Err(e) = update_result {
            error!(
                ?id,
                ?status,
                "Unable to update status of activation: {:?}",
                e,
            );
            return Err(Status::internal(format!(
                "Unable to update status of {id:?} to {status:?}"
            )));
        }
        metrics::histogram!("grpc_server.set_status.duration").record(start_time.elapsed());

        let Some(FetchNextTask { ref namespace }) = request.get_ref().fetch_next_task else {
            return Ok(Response::new(SetTaskStatusResponse { task: None }));
        };

        let start_time = Instant::now();
        let res = match self
            .store
            .get_pending_activation(namespace.as_deref())
            .await
        {
            Err(e) => {
                error!("Unable to fetch next task: {:?}", e);
                Err(Status::internal("Unable to fetch next task"))
            }
            Ok(None) => Err(Status::not_found("No pending activation")),
            Ok(Some(inflight)) => {
                if inflight.processing_attempts < 1 {
                    let now = Utc::now();
                    let received_to_gettask_latency = inflight.received_latency(now);
                    if received_to_gettask_latency > 0 {
                        metrics::histogram!(
                            "grpc_server.received_to_gettask.latency",
                            "namespace" => inflight.namespace,
                            "taskname" => inflight.taskname,
                        )
                        .record(received_to_gettask_latency as f64);
                    }
                }
                Ok(Response::new(SetTaskStatusResponse {
                    task: Some(TaskActivation::decode(&inflight.activation as &[u8]).unwrap()),
                }))
            }
        };
        metrics::histogram!("grpc_server.fetch_next.duration").record(start_time.elapsed());
        res
    }
}
