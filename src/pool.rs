use std::collections::HashMap;

use anyhow::Result;
use rand::seq::IteratorRandom;
use sentry_protos::taskworker::v1::{PushTaskRequest, worker_service_client::WorkerServiceClient};
use tonic::transport::{Channel, Error};
use tracing::{info, warn};

#[derive(Clone)]
pub struct WorkerPool {
    /// Maps every worker address to its client.
    clients: HashMap<String, WorkerClient>,

    /// All available worker addresses.
    addresses: Vec<String>,
}

#[derive(Clone)]
struct WorkerClient {
    /// The actual RPC client connection.
    connection: WorkerServiceClient<Channel>,

    /// The worker address.
    address: String,

    /// The worker's ESTIMATED queue size.
    queue_size: u32,
}

impl WorkerClient {
    pub fn new(connection: WorkerServiceClient<Channel>, address: String, queue_size: u32) -> Self {
        Self {
            connection,
            address,
            queue_size,
        }
    }
}

impl WorkerPool {
    /// Create a new `WorkerPool` instance.
    pub fn new(addresses: Vec<String>) -> Self {
        Self {
            addresses,
            clients: HashMap::new(),
        }
    }

    /// Call this function over and over again in another thread to keep the pool of active connections updated.
    pub async fn update(&mut self) {
        for address in self.addresses.clone() {
            if !self.clients.contains_key(&address) {
                match connect(&address).await {
                    Ok(connection) => {
                        info!("Connected to {address}");

                        let client = WorkerClient::new(connection, address.clone(), 0);
                        self.clients.insert(address, client);
                    }

                    Err(e) => {
                        warn!("Couldn't connect to {address} - {:?}", e);
                    }
                }
            }
        }
    }

    /// Try pushing a task to the best worker using P2C (Power of Two Choices).
    pub async fn push(&mut self, request: PushTaskRequest) -> Result<()> {
        let candidate = {
            let mut rng = rand::rng();

            self.clients
                .values()
                .choose_multiple(&mut rng, 2)
                .into_iter()
                .min_by_key(|client| client.queue_size)
                .cloned()
        };

        let Some(mut client) = candidate else {
            return Err(anyhow::anyhow!("No connected workers"));
        };

        let address = client.address.clone();

        match client.connection.push_task(request).await {
            Ok(response) => {
                let response = response.into_inner();

                if !response.added {
                    return Err(anyhow::anyhow!("Selected worker was full"));
                }

                // Update this worker's queue size
                client.queue_size = 5;
                self.clients.insert(address, client);
                Ok(())
            }

            Err(e) => {
                // Remove this unhealthy worker from the active connection pool
                self.clients.remove(&address);
                Err(e.into())
            }
        }
    }
}

#[inline]
async fn connect<T: Into<String>>(address: T) -> Result<WorkerServiceClient<Channel>, Error> {
    WorkerServiceClient::connect(address.into()).await
}
