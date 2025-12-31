use anyhow::Error;
use sentry_protos::taskbroker::v1::TaskActivation;
use sentry_protos::taskworker::v1::{AddTaskRequest, worker_service_client::WorkerServiceClient};
use sentry_protos::taskworker::v1::{GetQueueSizeRequest, GetQueueSizeResponse};
use tonic::Request;
use tonic::transport::Channel;
use tracing::{debug, warn};

/// Client for communicating with taskworkers. Connects on each request (lazily) rather than maintaining a persistent connection.
pub struct TaskworkerClient {
    pub address: String,
}

impl TaskworkerClient {
    /// Create a new `TaskworkerClient` with the given worker address.
    pub fn new(address: String) -> Self {
        Self { address }
    }

    /// Lazily connect to the worker.
    async fn connect(&self) -> Result<WorkerServiceClient<Channel>, Error> {
        match WorkerServiceClient::connect(self.address.clone()).await {
            Ok(client) => {
                debug!("Connected to taskworker at {}", self.address);
                Ok(client)
            }

            Err(e) => {
                warn!(
                    "Failed to connect to taskworker at {}: {:?}",
                    self.address, e
                );

                Err(e.into())
            }
        }
    }

    /// Get taskworker's current queue size.
    pub async fn get_queue_size(&self) -> Result<GetQueueSizeResponse, Error> {
        // Connect to the worker
        let mut client = self.connect().await?;

        // Build the request
        let request = Request::new(GetQueueSizeRequest {});

        // Perform RPC
        client
            .get_queue_size(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| {
                warn!("taskworker RPC failed - {:?}", status);
                status.into()
            })
    }

    /// Push a task to the taskworker.
    /// - Returns `Ok(true)` if the worker accepted the task
    /// - Returns `Ok(false)` if the worker rejected it (for example, due to a full queue)
    /// - Returns `Err` if there was a connection or RPC error
    pub async fn add_task(
        &self,
        activation: TaskActivation,
        callback_url: String,
    ) -> Result<bool, Error> {
        // Connect to the worker
        let mut client = self.connect().await?;

        // Build the request
        let request = Request::new(AddTaskRequest {
            task: Some(activation),
            callback_url,
        });

        // Perform RPC
        client
            .add_task(request)
            .await
            .map(|response| {
                let added = response.into_inner().added;

                if added {
                    debug!("taskworker accepted task");
                } else {
                    debug!("taskworker rejected task (queue full or other reason)");
                }

                added
            })
            .map_err(|status| {
                warn!("taskworker RPC failed - {:?}", status);
                status.into()
            })
    }
}
