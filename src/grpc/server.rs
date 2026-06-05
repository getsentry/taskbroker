use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Error, Result};
use chrono::Utc;
use prost::Message;
use sentry_protos::taskbroker::v1::consumer_service_server::ConsumerService;
use sentry_protos::taskbroker::v1::{
    FetchNextTask, GetTaskRequest, GetTaskResponse, SetBatchActivationStatusRequest,
    SetBatchActivationStatusResponse, SetTaskStatusRequest, SetTaskStatusResponse, TaskActivation,
    TaskActivationStatus,
};
use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status};
use tracing::{debug, error, instrument, warn};

use crate::config::{Config, DeliveryMode};
use crate::store::activation::ActivationStatus;
use crate::store::traits::ActivationStore;

pub struct TaskbrokerServer {
    pub store: Arc<dyn ActivationStore>,
    pub config: Arc<Config>,
    pub update_tx: Option<Sender<StatusUpdate>>,
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

        let status: ActivationStatus = TaskActivationStatus::try_from(request.get_ref().status)
            .map_err(|e| Status::invalid_argument(format!("Unable to deserialize status: {e:?}")))?
            .into();

        if !status.is_conclusion() {
            return Err(Status::invalid_argument(format!(
                "Invalid status, expects 3 (Failure), 4 (Retry), or 5 (Complete), but got: {status:?}"
            )));
        }

        if status == ActivationStatus::Failure {
            metrics::counter!("grpc_server.set_status.failure").increment(1);
        }

        let max_attempts = request.get_ref().max_attempts;
        let delay_on_retry = request.get_ref().delay_on_retry;

        // Use batching channel if available and we don't need to update retry state.
        // If max_attempts or delay_on_retry is Some, we can't use batching API to update the
        // activation, and have to fall back to individual set_status.
        if let Some(ref tx) = self.update_tx
            && max_attempts.is_none()
            && delay_on_retry.is_none()
        {
            tx.send((id, status))
                .await
                .map_err(|_| Status::internal("Status update channel closed"))?;

            metrics::histogram!("grpc_server.set_status.duration").record(start_time.elapsed());
            return Ok(Response::new(SetTaskStatusResponse { task: None }));
        }

        match self
            .store
            .set_status(&id, status, max_attempts, delay_on_retry)
            .await
        {
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

    #[instrument(skip_all)]
    async fn set_batch_activation_status(
        &self,
        request: Request<SetBatchActivationStatusRequest>,
    ) -> Result<Response<SetBatchActivationStatusResponse>, Status> {
        let start_time = Instant::now();
        let updates = request.get_ref().updates.clone();

        // Updates can be broken into different batches based on the status and the retry state.
        let mut batches: HashMap<ActivationStatus, Vec<String>> = HashMap::new();
        let mut retry_updates: Vec<SetTaskStatusRequest> = Vec::new();

        for update in updates {
            let id = update.id.clone();
            let status: ActivationStatus = TaskActivationStatus::try_from(update.status)
                .map_err(|e| {
                    Status::invalid_argument(format!("Unable to deserialize status: {e:?}"))
                })?
                .into();
            if !status.is_conclusion() {
                return Err(Status::invalid_argument(format!(
                    "Invalid status, expects 3 (Failure), 4 (Retry), or 5 (Complete), but got: {status:?}"
                )));
            }
            if status == ActivationStatus::Failure {
                metrics::counter!("grpc_server.set_status.failure").increment(1);
            }
            if update.max_attempts.is_some() || update.delay_on_retry.is_some() {
                retry_updates.push(update);
            } else {
                batches.entry(status).or_default().push(id);
            }
        }

        // For each status and batch of IDs, update the status for those IDs. In the case of a failure, return an error to the worker.
        // Track and log a warning if the number of rows updated is less than the number of IDs requested.
        metrics::histogram!("grpc_server.set_batch_activation_status.num_batches")
            .record(batches.len() as f64);
        for (status, ids) in batches {
            let requested = ids.len() as u64;
            metrics::histogram!("grpc_server.set_batch_activation_status.batch_size", "status" => status.to_string()).record(requested as f64);
            match self.store.set_status_batch(&ids, status).await {
                Ok(affected) => {
                    metrics::histogram!("grpc_server.batch_activation_status.affected_diff", "status" => status.to_string())
                        .record((requested - affected) as f64);
                    if affected < requested {
                        metrics::histogram!(
                            "grpc_server.set_batch_activation_status.partial",
                            "status" => status.to_string()
                        )
                        .record((requested - affected) as f64);
                        warn!(
                            ?status,
                            requested, affected, "Updated fewer rows than IDs requested in batch"
                        );
                    }
                }
                Err(e) => {
                    metrics::counter!("grpc_server.set_status", "result" => "error", "status" => status.to_string()).increment(requested);
                    metrics::histogram!("grpc_server.batch_activation_status.duration")
                        .record(start_time.elapsed());
                    error!("Failed to set batch activation status: {:?}", e);
                    return Err(Status::internal("Failed to set batch activation status"));
                }
            }
        }

        metrics::histogram!("grpc_server.set_batch_activation_status.retry_updates")
            .record(retry_updates.len() as f64);
        for update in retry_updates {
            let id = update.id.clone();
            let status: ActivationStatus = TaskActivationStatus::try_from(update.status)
                .map_err(|e| {
                    Status::invalid_argument(format!("Unable to deserialize status: {e:?}"))
                })?
                .into();
            let max_attempts = update.max_attempts;
            let delay_on_retry = update.delay_on_retry;
            match self
                .store
                .set_status(&id, status, max_attempts, delay_on_retry)
                .await
            {
                Ok(Some(_)) => metrics::counter!("grpc_server.set_status", "result" => "ok", "status" => status.to_string()).increment(1),
                Ok(None) => metrics::counter!("grpc_server.set_status", "result" => "not_found", "status" => status.to_string()).increment(1),

                Err(e) => {
                    metrics::counter!("grpc_server.set_status", "result" => "error", "status" => status.to_string()).increment(1);

                    error!("Unable to update status of activation in batch {:?} to {:?}: {:?}", id, status, e);

                    metrics::histogram!("grpc_server.batch_activation_status.duration").record(start_time.elapsed());
                    return Err(Status::internal(format!(
                        "Unable to update status of activation in batch {id:?} to {status:?}"
                    )));
                }
            }
        }
        metrics::histogram!("grpc_server.set_batch_activation_status.duration")
            .record(start_time.elapsed());
        return Ok(Response::new(SetBatchActivationStatusResponse {}));
    }
}

pub type StatusUpdate = (String, ActivationStatus);

pub async fn flush_updates(store: Arc<dyn ActivationStore>, buffer: &mut Vec<StatusUpdate>) {
    if buffer.is_empty() {
        return;
    }

    let mut by_status: HashMap<ActivationStatus, Vec<String>> = HashMap::new();

    for (id, status) in buffer.drain(..) {
        by_status.entry(status).or_default().push(id);
    }

    for (status, ids) in by_status {
        let requested = ids.len() as u64;
        let st = status.to_string();

        metrics::histogram!("grpc_server.flush_updates.requested", "status" => st.clone())
            .record(requested as f64);

        match store.set_status_batch(&ids, status).await {
            Ok(affected) => {
                metrics::histogram!(
                    "grpc_server.flush_updates.affected",
                    "status" => st.clone()
                )
                .record(affected as f64);

                metrics::counter!(
                    "grpc_server.flush_updates.updated",
                    "status" => st.clone()
                )
                .increment(affected);

                metrics::counter!("grpc_server.flush_updates", "result" => "ok").increment(1);

                if affected < requested {
                    metrics::counter!(
                        "grpc_server.flush_updates.partial",
                        "status" => st.clone()
                    )
                    .increment(1);

                    warn!(
                        ?status,
                        requested, affected, "Updated fewer rows than IDs requested from server"
                    );
                }

                debug!(
                    ?status,
                    affected, requested, "Flushed status batch from server"
                );
            }

            Err(e) => {
                metrics::counter!("grpc_server.flush_updates", "result" => "error").increment(1);

                error!(
                    ?status,
                    requested,
                    error = ?e,
                    "Failed to flush status batch from server"
                );

                // Push failed updates back into the buffer so they can be retried on next flush
                for id in ids {
                    buffer.push((id, status));
                }
            }
        }
    }
}
