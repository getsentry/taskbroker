use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
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
pub use crate::store::types::StatusUpdate;

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

        // Use the batching channel if available. Retry-state updates (max_attempts /
        // delay_on_retry) are batched too — the flusher's set_status_batch applies them.
        if let Some(ref tx) = self.update_tx {
            tx.send(StatusUpdate {
                id,
                status,
                max_attempts,
                delay_on_retry,
            })
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
                debug!("No pending activations");

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

        // Validate and convert every update into a StatusUpdate. Retry state (max_attempts /
        // delay_on_retry) rides along and is applied by set_status_batch in the same transaction.
        let mut status_updates: Vec<StatusUpdate> = Vec::with_capacity(updates.len());
        let mut sizes_by_status: HashMap<ActivationStatus, u64> = HashMap::new();
        for update in updates {
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
            *sizes_by_status.entry(status).or_default() += 1;
            status_updates.push(StatusUpdate {
                id: update.id,
                status,
                max_attempts: update.max_attempts,
                delay_on_retry: update.delay_on_retry,
            });
        }

        metrics::histogram!("grpc_server.set_batch_activation_status.num_batches")
            .record(sizes_by_status.len() as f64);
        for (status, size) in &sizes_by_status {
            metrics::histogram!("grpc_server.set_batch_activation_status.batch_size", "status" => status.to_string())
                .record(*size as f64);
        }

        let requested = status_updates.len() as u64;
        match self.store.set_status_batch(&status_updates).await {
            Ok(affected) => {
                metrics::histogram!("grpc_server.set_batch_activation_status.affected_diff")
                    .record((requested - affected) as f64);
                if affected < requested {
                    metrics::histogram!("grpc_server.set_batch_activation_status.partial")
                        .record((requested - affected) as f64);
                    warn!(
                        requested,
                        affected, "Updated fewer rows than IDs requested in batch"
                    );
                }
            }
            Err(e) => {
                metrics::counter!("grpc_server.set_status", "result" => "error")
                    .increment(requested);
                metrics::histogram!("grpc_server.set_batch_activation_status.duration")
                    .record(start_time.elapsed());
                error!("Failed to set batch activation status: {:?}", e);
                return Err(Status::internal("Failed to set batch activation status"));
            }
        }

        metrics::histogram!("grpc_server.set_batch_activation_status.duration")
            .record(start_time.elapsed());
        return Ok(Response::new(SetBatchActivationStatusResponse {}));
    }
}

pub async fn flush_updates(store: Arc<dyn ActivationStore>, buffer: &mut Vec<StatusUpdate>) {
    if buffer.is_empty() {
        return;
    }

    let requested = buffer.len() as u64;
    metrics::histogram!("grpc_server.flush_updates.requested").record(requested as f64);

    match store.set_status_batch(buffer).await {
        Ok(affected) => {
            metrics::histogram!("grpc_server.flush_updates.affected").record(affected as f64);
            metrics::counter!("grpc_server.flush_updates.updated").increment(affected);
            metrics::counter!("grpc_server.flush_updates", "result" => "ok").increment(1);

            if affected < requested {
                metrics::counter!("grpc_server.flush_updates.partial").increment(1);
                warn!(
                    requested,
                    affected, "Updated fewer rows than IDs requested from server"
                );
            }

            debug!(affected, requested, "Flushed status batch from server");

            // Successfully flushed; clear the buffer.
            buffer.clear();
        }

        Err(e) => {
            metrics::counter!("grpc_server.flush_updates", "result" => "error").increment(1);

            error!(
                requested,
                error = ?e,
                "Failed to flush status batch from server"
            );

            // Leave the buffer intact so the updates are retried on the next flush.
        }
    }
}
