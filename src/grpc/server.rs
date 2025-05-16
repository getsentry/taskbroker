use chrono::Utc;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{
    FetchNextTask, GetTaskRequest, GetTaskResponse, SetTaskStatusRequest, SetTaskStatusResponse,
    TaskActivationStatus,
};
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};

use crate::store::inflight_activation::{
    InflightActivationStatus, InflightActivationStore, received_latency,
};
use tracing::{error, instrument};

pub struct TaskbrokerServer {
    pub store: Arc<InflightActivationStore>,
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
                let result = self.store.get_activation_blob(&inflight.id).await;
                let activation = match result {
                    Ok(activation) => activation,
                    Err(e) => {
                        error!("Unable to retrieve pending activation: {:?}", e);
                        return Err(Status::internal("Unable to retrieve pending activation"));
                    }
                };
                let now = Utc::now();
                let received_to_gettask_latency =
                    received_latency(&activation, inflight.delay_until, now);
                if received_to_gettask_latency > 0 {
                    metrics::histogram!(
                        "grpc_server.received_to_gettask.latency",
                        "namespace" => inflight.namespace,
                        "taskname" => activation.taskname.clone(),
                    )
                    .record(received_to_gettask_latency as f64);
                }

                let resp = GetTaskResponse {
                    task: Some(activation),
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
                    Status::invalid_argument(format!("Unable to deserialize status: {:?}", e))
                })?
                .into();

        if !status.is_conclusion() {
            return Err(Status::invalid_argument(format!(
                "Invalid status, expects 3 (Failure), 4 (Retry), or 5 (Complete), but got: {:?}",
                status
            )));
        }
        if status == InflightActivationStatus::Failure {
            metrics::counter!("grpc_server.set_status.failure").increment(1);
        }

        let update_result = self.store.set_status(&id, status).await;
        if let Err(e) = update_result {
            error!(
                "Unable to update status of {:?} to {:?}: {:?}",
                id, status, e
            );
            return Err(Status::internal(format!(
                "Unable to update status of {:?} to {:?}",
                id, status
            )));
        }
        metrics::histogram!("grpc_server.set_status.duration").record(start_time.elapsed());

        if let Ok(Some(inflight_activation)) = update_result {
            let result = self
                .store
                .get_activation_blob(&inflight_activation.id)
                .await;
            let activation = match result {
                Ok(activation) => activation,
                Err(e) => {
                    error!("Unable to retrieve activation body: {:?}", e);
                    return Err(Status::internal("Unable to retrieve activation body"));
                }
            };
            let duration = Utc::now() - inflight_activation.added_at;
            metrics::histogram!(
                "task_execution.conclusion.duration",
                "namespace" => activation.namespace.clone(),
                "taskname" => activation.taskname.clone(),
            )
            .record(duration.num_milliseconds() as f64);

            // Record the time taken from when the task started processing to when it finished
            // Use the processing deadline to calculate the time taken
            if let Some(processing_deadline) = inflight_activation.processing_deadline {
                let execution_remaining =
                    processing_deadline.timestamp_millis() - Utc::now().timestamp_millis();

                // If the task has passed the processing deadline, then execution_remaining will be negative
                // This then gets added to the processing deadline duration to get the execution time
                let execution_time =
                    (activation.processing_deadline_duration as i64 * 1000) - execution_remaining;
                metrics::histogram!(
                    "task_execution.completion_time",
                    "namespace" => activation.namespace,
                    "taskname" => activation.taskname,
                )
                .record(execution_time as f64);
            }
        }

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
                let result = self.store.get_activation_blob(&inflight.id).await;
                let activation = match result {
                    Ok(activation) => activation,
                    Err(e) => {
                        error!("Unable to retrieve pending activation: {:?}", e);
                        return Err(Status::internal("Unable to retrieve pending activation"));
                    }
                };
                let now = Utc::now();
                let received_to_gettask_latency =
                    received_latency(&activation, inflight.delay_until, now);
                if received_to_gettask_latency > 0 {
                    metrics::histogram!(
                        "grpc_server.received_to_gettask.latency",
                        "namespace" => activation.namespace.clone(),
                        "taskname" => activation.taskname.clone(),
                    )
                    .record(received_to_gettask_latency as f64);
                }
                Ok(Response::new(SetTaskStatusResponse {
                    task: Some(activation),
                }))
            }
        };
        metrics::histogram!("grpc_server.fetch_next.duration").record(start_time.elapsed());
        res
    }
}
