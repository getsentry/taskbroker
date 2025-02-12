use chrono::Utc;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{
    FetchNextTask, GetTaskRequest, GetTaskResponse, SetTaskStatusRequest, SetTaskStatusResponse,
    TaskActivationStatus,
};
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};

use super::inflight_activation_store::{
    InflightActivation, InflightActivationStatus, InflightActivationStore,
};
use tracing::{error, instrument};

pub struct MyConsumerService {
    pub store: Arc<InflightActivationStore>,
}

#[tonic::async_trait]
impl ConsumerService for MyConsumerService {
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
                let resp = GetTaskResponse {
                    task: Some(inflight.activation),
                };
                metrics::histogram!("grpc_server.get_task.duration").record(start_time.elapsed());

                Ok(Response::new(resp))
            }
            Ok(None) => return Err(Status::not_found("No pending activation")),
            Err(e) => Err(Status::internal(e.to_string())),
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

        if let Err(e) = self.store.set_status(&id, status).await {
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

        if let Ok(Some(inflight_activation)) = self.store.get_by_id(&id).await {
            let duration = Utc::now() - inflight_activation.added_at;
            metrics::histogram!(
                "task_execution.conclusion.duration",
                "namespace" => inflight_activation.activation.namespace.clone(),
                "taskname" => inflight_activation.activation.taskname.clone(),
            )
            .record(duration.num_milliseconds() as f64);

            // Record the time taken from when the task started processing to when it finished
            // Use the processing deadline to calculate the time taken
            if let Some(processing_deadline) = inflight_activation.processing_deadline {
                let mut execution_remaining =
                    processing_deadline.timestamp_millis() - Utc::now().timestamp_millis();
                if execution_remaining < 0 {
                    execution_remaining = 0;
                }
                let execution_time = inflight_activation.activation.processing_deadline_duration
                    - (execution_remaining * 1000) as u64;
                metrics::histogram!("task_execution.completion_time", "namespace" => inflight_activation.activation.namespace.clone(),
                    "taskname" => inflight_activation.activation.taskname.clone()).record(execution_time as f64);
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
            Ok(Some(InflightActivation { activation, .. })) => {
                Ok(Response::new(SetTaskStatusResponse {
                    task: Some(activation),
                }))
            }
        };
        metrics::histogram!("grpc_server.fetch_next.duration").record(start_time.elapsed());
        res
    }
}
