use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result};
use futures::future::join_all;
use prost::Message;
use sentry_protos::taskbroker::v1::TaskActivation;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::grpc::taskworker_client::TaskworkerClient;
use crate::store::inflight_activation::{InflightActivation, InflightActivationStore};

/// Service that continuously pushes pending task activations to configured workers.
pub struct TaskPusher {
    /// The activation store.
    store: Arc<InflightActivationStore>,

    /// The broker configuration.
    config: Arc<Config>,

    /// Map from addresses to worker clients.
    clients: HashMap<String, TaskworkerClient>,
}

impl TaskPusher {
    /// Create a new `TaskPusher` instance.
    pub fn new(store: Arc<InflightActivationStore>, config: Arc<Config>) -> Self {
        let clients: HashMap<String, TaskworkerClient> = config
            .taskworker_addresses
            .clone()
            .into_iter()
            .map(|addr| (addr.clone(), TaskworkerClient::new(addr)))
            .collect();

        Self {
            store,
            config,
            clients,
        }
    }

    /// Start the push loop. Runs until the shutdown guard signals termination.
    pub async fn start(self) -> Result<(), Error> {
        info!("Push task loop starting...");

        let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();

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

        // Find the best worker (smallest queue)
        let Some(worker_address) = self.select_best_worker().await else {
            error!("No workers are available!");
            sleep(100).await;
            return Err(anyhow::anyhow!("No workers available"));
        };

        info!("Sending to worker at address {}", worker_address);

        // Push to the selected worker
        self.push_to_worker(&worker_address, activation, &task_id)
            .await
    }

    /// Select the worker with the smallest queue size.
    async fn select_best_worker(&self) -> Option<String> {
        let futures = self.clients.values().map(|client| client.get_queue_size());
        let results = join_all(futures).await;

        results
            .into_iter()
            .filter_map(Result::ok)
            .min_by_key(|res| res.length)
            .map(|res| res.address)
    }

    /// Push a task to a specific worker and handle the result.
    async fn push_to_worker(
        &self,
        address: &str,
        activation: TaskActivation,
        task_id: &str,
    ) -> Result<()> {
        let client = self
            .clients
            .get(address)
            .ok_or_else(|| anyhow::anyhow!("Client not found for address {}", address))?;

        let result = client
            .add_task(activation, self.config.taskworker_callback_url.clone())
            .await;

        match result {
            Ok(true) => {
                info!("Worker {address} accepted task {task_id}");
                self.mark_task_as_processing(task_id).await;
                Ok(())
            }

            Ok(false) => {
                info!("Worker rejected task {task_id} (queue full or other reason)");
                sleep(10).await;
                Err(anyhow::anyhow!("Worker rejected task"))
            }

            Err(e) => {
                warn!("Failed to push task {task_id} to worker - {:?}", e);
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
