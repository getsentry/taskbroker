use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use prost::Message;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{
    FetchNextTask, GetTaskRequest, GetTaskResponse, SetTaskStatusRequest, SetTaskStatusResponse,
    TaskActivation, TaskActivationStatus,
};
use tonic::{Request, Response, Status};
use tracing::{error, instrument, warn};

use crate::config::{Config, DeliveryMode};
use crate::store::activation::InflightActivationStatus;
use crate::store::traits::InflightActivationStore;

pub struct TaskbrokerServer {
    pub store: Arc<dyn InflightActivationStore>,
    pub config: Arc<Config>,
}

#[tonic::async_trait]
impl ConsumerService for TaskbrokerServer {
    #[instrument(skip_all)]
    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskResponse>, Status> {
        if self.config.delivery_mode == DeliveryMode::Push {
            return Err(Status::permission_denied(
                "Cannot call while broker is in PUSH mode",
            ));
        }

        let start_time = Instant::now();

        let application = &request.get_ref().application;
        let namespace = &request.get_ref().namespace;

        let inflight = self
            .store
            .claim_activation_for_pull(application.as_deref(), namespace.as_deref())
            .await;

        match inflight {
            Ok(None) => Err(Status::not_found("No pending activations")),

            Ok(Some(inflight)) => {
                let now = Utc::now();

                if inflight.processing_attempts < 1 {
                    let received_to_gettask_latency = inflight.received_latency(now);
                    if received_to_gettask_latency > 0 {
                        metrics::histogram!(
                            "grpc_server.received_to_gettask.latency",
                            "namespace" => inflight.namespace.clone(),
                            "taskname" => inflight.taskname.clone(),
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

        match self.store.set_status(&id, status).await {
            Ok(Some(_)) => metrics::counter!(
                "grpc_server.set_status",
                "result" => "ok",
                "status" => status.to_string()
            )
            .increment(1),

            Ok(None) => metrics::counter!(
                "grpc_server.set_status",
                "result" => "not_found",
                "status" => status.to_string()
            )
            .increment(1),

            Err(e) => {
                metrics::counter!(
                    "grpc_server.set_status",
                    "result" => "error",
                    "status" => status.to_string()
                )
                .increment(1);

                error!(
                    ?id,
                    ?status,
                    "Unable to update status of activation: {:?}",
                    e,
                );

                metrics::histogram!("grpc_server.set_status.duration").record(start_time.elapsed());
                return Err(Status::internal(format!(
                    "Unable to update status of {id:?} to {status:?}"
                )));
            }
        }

        metrics::histogram!("grpc_server.set_status.duration").record(start_time.elapsed());

        if self.config.delivery_mode == DeliveryMode::Push {
            return Ok(Response::new(SetTaskStatusResponse { task: None }));
        }

        let Some(FetchNextTask {
            ref namespace,
            ref application,
        }) = request.get_ref().fetch_next_task
        else {
            return Ok(Response::new(SetTaskStatusResponse { task: None }));
        };

        let start_time = Instant::now();

        let res = match self
            .store
            .claim_activation_for_pull(application.as_deref(), namespace.as_deref())
            .await
        {
            Err(e) => {
                error!("Unable to fetch next task: {:?}", e);
                Err(Status::internal("Unable to fetch next task"))
            }

            Ok(None) => {
                warn!("No pending activations");

                // If we return an error, the worker will place the result back in its internal queue and send the update again in the future, which is not desired
                Ok(Response::new(SetTaskStatusResponse { task: None }))
            }

            Ok(Some(inflight)) => {
                if inflight.processing_attempts < 1 {
                    let now = Utc::now();
                    let received_to_gettask_latency = inflight.received_latency(now);
                    if received_to_gettask_latency > 0 {
                        metrics::histogram!(
                            "grpc_server.received_to_gettask.latency",
                            "namespace" => inflight.namespace.clone(),
                            "taskname" => inflight.taskname.clone(),
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
