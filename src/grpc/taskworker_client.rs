use anyhow::Error;
use sentry_protos::taskbroker::v1::TaskActivation;
use sentry_protos::taskworker::v1::{AddTaskRequest, AddTaskResponse, worker_client::WorkerClient};
use sentry_protos::taskworker::v1::{GetQueueSizeRequest, GetQueueSizeResponse};
use tonic::{Request, Status};
use tracing::{debug, warn};

/// Client for communicating with taskworker processes.
/// Uses lazy connection - connects on each request rather than maintaining a persistent connection.
pub struct TaskworkerClient {
    pub address: String,
}

impl TaskworkerClient {
    /// Create a new TaskworkerClient with the given worker address.
    /// No connection is established until add_task is called.
    pub fn new(address: String) -> Self {
        Self { address }
    }

    pub async fn get_queue_size(&self) -> Result<GetQueueSizeResponse, Error> {
        // Lazily connect to the worker
        let mut client = match WorkerClient::connect(self.address.clone()).await {
            Ok(client) => {
                debug!("Connected to taskworker at {}", self.address);
                client
            }
            Err(e) => {
                warn!(
                    "Failed to connect to taskworker at {}: {:?}",
                    self.address, e
                );
                return Err(e.into());
            }
        };

        let request = Request::new(GetQueueSizeRequest {});

        match client.get_queue_size(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => {
                warn!("RPC call to taskworker failed: {:?}", status);
                Err(status.into())
            }
        }
    }

    /// Push a task to the taskworker.
    /// Returns Ok(true) if the worker accepted the task,
    /// Ok(false) if the worker rejected it (e.g., queue full),
    /// or Err if there was a connection/RPC error.
    pub async fn add_task(
        &self,
        activation: TaskActivation,
        callback_url: String,
    ) -> Result<bool, Error> {
        // Lazily connect to the worker
        let mut client = match WorkerClient::connect(self.address.clone()).await {
            Ok(client) => {
                debug!("Connected to taskworker at {}", self.address);
                client
            }
            Err(e) => {
                warn!(
                    "Failed to connect to taskworker at {}: {:?}",
                    self.address, e
                );
                return Err(e.into());
            }
        };

        // Build the request
        let request = Request::new(AddTaskRequest {
            task: Some(activation),
            callback_url,
        });

        // Call the RPC
        match client.add_task(request).await {
            Ok(response) => {
                let added = response.into_inner().added;
                if added {
                    debug!("Taskworker accepted task");
                } else {
                    debug!("Taskworker rejected task (queue full or other reason)");
                }
                Ok(added)
            }
            Err(status) => {
                warn!("RPC call to taskworker failed: {:?}", status);
                Err(status.into())
            }
        }
    }
}
