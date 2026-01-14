use std::cmp::Ordering;

use anyhow::Result;
use dashmap::DashMap;
use rand::Rng;
use rand::seq::IteratorRandom;
use sentry_protos::taskworker::v1::{PushTaskRequest, worker_service_client::WorkerServiceClient};
use tonic::transport::{Channel, Error};
use tracing::{info, warn};

pub struct WorkerPool {
    /// Maps every worker address to its client.
    /// Uses DashMap for concurrent access without external locking.
    clients: DashMap<String, WorkerClient>,
}

#[derive(Clone)]
struct WorkerClient {
    /// The actual RPC client connection.
    connection: WorkerServiceClient<Channel>,

    /// The worker address.
    address: String,

    /// The worker's last known queue size.
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
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// Register worker address and attempt to connect immediately.
    /// Only adds the worker to the pool if the connection succeeds.
    pub async fn add_worker<T: Into<String>>(&self, address: T) {
        let address = address.into();
        info!("Adding worker {address}");

        // Only add to the pool if we can connect
        match connect(&address).await {
            Ok(connection) => {
                info!("Connected to {address}");

                let client = WorkerClient::new(connection, address.clone(), 0);

                self.clients.insert(address.clone(), client);
            }

            Err(e) => {
                warn!(
                    "Did not register worker {address} due to connection error - {:?}",
                    e
                );
            }
        }
    }

    /// Unregister worker address during execution.
    pub fn remove_worker(&self, address: &String) {
        info!("Removing worker {address}");
        self.clients.remove(address);
    }

    /// Try pushing a task to the best worker using P2C (Power of Two Choices).
    pub async fn push(&self, request: PushTaskRequest) -> Result<()> {
        let candidate = {
            let mut rng = rand::rng();

            self.clients
                .iter()
                .map(|entry| entry.value().clone())
                .choose_multiple(&mut rng, 2)
                .into_iter()
                .min_by(|a, b| {
                    match a.queue_size.cmp(&b.queue_size) {
                        // When two workers are the same, we pick one randomly to avoid hammering one worker repeatedly
                        Ordering::Equal => {
                            if rng.random::<bool>() {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        other => other,
                    }
                })
        };

        let Some(mut client) = candidate else {
            return Err(anyhow::anyhow!("No connected workers"));
        };

        let address = client.address.clone();

        match client.connection.push_task(request).await {
            Ok(response) => {
                let response = response.into_inner();

                // Update this worker's queue size
                client.queue_size = response.queue_size;
                self.clients.insert(address, client);

                Ok(())
            }

            Err(e) => {
                warn!(
                    "Removing worker {address} from pool due to RPC error - {:?}",
                    e
                );

                // Remove this unhealthy worker
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
