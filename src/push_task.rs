use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result};
use prost::Message;
use sentry_protos::taskbroker::v1::TaskActivation;
use sentry_protos::taskworker::v1::AddTaskRequest;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::store::inflight_activation::{InflightActivation, InflightActivationStore};
use crate::worker_router::WorkerRouter;

/// Service that continuously pushes pending task activations to configured workers.
pub struct TaskPusher {
    /// The activation store.
    store: Arc<InflightActivationStore>,

    /// The broker configuration.
    config: Arc<Config>,

    router: Arc<RwLock<WorkerRouter>>,
}

impl TaskPusher {
    /// Create a new `TaskPusher` instance.
    pub fn new(store: Arc<InflightActivationStore>, config: Arc<Config>) -> Self {
        let router = Arc::new(RwLock::new(WorkerRouter::new(&config.taskworker_addresses)));

        Self {
            store,
            config,
            router,
        }
    }

    /// Start the push loop. Runs until the shutdown guard signals termination.
    pub async fn start(self) -> Result<(), Error> {
        info!("Push task loop starting...");

        let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
        let router = self.router.clone();

        // Spawn a separate task to update the candidate worker collection
        tokio::spawn(async move {
            let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
            let mut beep_interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = guard.wait() => {
                        break;
                    }

                    _ = beep_interval.tick() => {
                        router.write().await.update().await;
                    }
                }
            }
        });

        loop {
            tokio::select! {
                _ = guard.wait() => {
                    info!("Push task loop received shutdown signal");
                    break;
                }

                _ = async {
                    self.process_next_task().await;
                } => {}
            }
        }

        info!("Push task loop shutting down...");
        Ok(())
    }
}

impl TaskPusher {
    /// Process the next pending task from the store.
    async fn process_next_task(&self) {
        match self.store.peek_pending_activation().await {
            Ok(Some(inflight)) => {
                if let Err(e) = self.handle_task_push(inflight).await {
                    debug!("Task push resulted in error - {:?}", e);
                }
            }

            Ok(None) => {
                debug!("No pending tasks, sleeping briefly");
                sleep(10).await;
            }

            Err(e) => {
                error!("Failed to fetch pending activation - {:?}", e);
                sleep(100).await;
            }
        }
    }

    /// Handle pushing a single task to a worker.
    async fn handle_task_push(&self, inflight: InflightActivation) -> Result<()> {
        let task_id = inflight.id.clone();

        // Decode the protobuf activation from stored bytes
        let activation = TaskActivation::decode(&inflight.activation as &[u8]).map_err(|e| {
            error!("Failed to decode activation {task_id}: {:?}", e);
            e
        })?;

        // Push task to the best available worker
        self.push_to_worker(activation, &task_id).await
    }

    /// Push a task through the worker router and handle the result.
    async fn push_to_worker(&self, activation: TaskActivation, task_id: &str) -> Result<()> {
        let request = AddTaskRequest {
            task: Some(activation),
            callback_url: self.config.taskworker_callback_url.clone(),
        };

        let result = self.router.write().await.push_task(&request).await;

        match result {
            Ok(()) => {
                info!("Pushed task {task_id}");
                self.mark_task_as_processing(task_id).await;
                Ok(())
            }

            Err(e) => {
                warn!("Could not push task {task_id} - {:?}", e);
                sleep(100).await;
                Err(e)
            }
        }
    }

    /// Mark a task as processing if it's still pending.
    async fn mark_task_as_processing(&self, task_id: &str) {
        match self.store.mark_as_processing_if_pending(task_id).await {
            Ok(true) => {
                info!("Task {} pushed and marked as processing", task_id);
            }

            Ok(false) => {
                warn!("Task {task_id} was already taken by another process (race condition)");
            }

            Err(e) => {
                error!("Failed to mark task {task_id} as processing - {:?}", e);
                sleep(100).await;
            }
        }
    }
}

async fn sleep(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await
}
