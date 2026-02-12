use anyhow::Result;
use sentry_protos::taskworker::v1::{
    PushTaskRequest, PushTaskResponse, worker_service_client::WorkerServiceClient,
};
use tonic::transport::Channel;
use tracing::{info, warn};

/// Client for communicating with the worker service (through a load balancer).
pub struct WorkerClient {
    /// The RPC client connection.
    connection: WorkerServiceClient<Channel>,
}

impl WorkerClient {
    /// Create a new `LoadBalancerClient` instance and connect to the endpoint.
    pub async fn new<T: Into<String>>(endpoint: T) -> Result<Self> {
        let endpoint = endpoint.into();
        info!("Connecting to worker service at {}...", endpoint);

        let connection = WorkerServiceClient::connect(endpoint.clone()).await?;

        info!("Successfully connected to worker service!");

        Ok(Self { connection })
    }

    /// Push a task to the worker service and wait for its completion.
    pub async fn push_task(&mut self, request: PushTaskRequest) -> Result<PushTaskResponse> {
        match self.connection.push_task(request).await {
            Ok(response) => Ok(response.into_inner()),

            Err(e) => {
                warn!("Failed to push task - {:?}", e);
                Err(e.into())
            }
        }
    }
}
