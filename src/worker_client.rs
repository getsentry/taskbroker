use std::sync::Arc;

use anyhow::Result;
use sentry_protos::{
    taskbroker::v1::TaskActivationStatus,
    taskworker::v1::{RunTaskRequest, worker_service_client::WorkerServiceClient},
};
use tonic::transport::Channel;
use tracing::{info, warn};

use crate::store::inflight_activation::InflightActivationStore;

/// Client for communicating with the worker service (through a load balancer).
pub struct WorkerClient {
    /// The RPC client connection.
    connection: WorkerServiceClient<Channel>,

    store: Arc<InflightActivationStore>,

    /// The worker service address.
    endpoint: String,
}

impl WorkerClient {
    /// Create a new `LoadBalancerClient` instance and connect to the endpoint.
    pub async fn new<T: Into<String>>(
        endpoint: T,
        store: Arc<InflightActivationStore>,
    ) -> Result<Self> {
        let endpoint = endpoint.into();
        info!("Connecting to worker service at {}...", endpoint);

        let connection = WorkerServiceClient::connect(endpoint.clone()).await?;

        info!("Successfully connected to worker service!");

        Ok(Self {
            connection,
            endpoint,
            store,
        })
    }

    /// Push a task to the worker service and wait for its completion.
    pub async fn run(&mut self, request: RunTaskRequest) -> Result<()> {
        let id = request.task.clone().unwrap().id;

        match self.connection.run_task(request).await {
            Ok(response) => {
                let response = response.into_inner();

                let status = TaskActivationStatus::try_from(response.status.clone())
                    .unwrap()
                    .into();

                self.store.set_status(&id, status).await.unwrap();

                Ok(())
            }

            Err(e) => {
                warn!("Failed to push task - {:?}", e);
                Err(e.into())
            }
        }
    }

    /// Attempt to reconnect to the worker service if the connection fails.
    pub async fn reconnect(&mut self) -> Result<()> {
        info!(
            "Attempting to reconnect to worker service at {}...",
            self.endpoint
        );

        self.connection = WorkerServiceClient::connect(self.endpoint.clone()).await?;

        info!("Successfully reconnected to load balancer!");

        Ok(())
    }
}
