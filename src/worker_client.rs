use anyhow::Result;
use sentry_protos::taskworker::v1::{PushTaskRequest, worker_service_client::WorkerServiceClient};
use tonic::transport::Channel;
use tracing::{info, warn};

/// Client for communicating with the worker service (through a load balancer).
#[derive(Debug, Clone)]
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
    pub async fn submit(&mut self, request: PushTaskRequest) -> Result<()> {
        match self.connection.push_task(request).await {
            Ok(_) => Ok(()),

            Err(e) => {
                warn!("Failed to push task - {:?}", e);
                Err(e.into())
            }
        }
    }
}
