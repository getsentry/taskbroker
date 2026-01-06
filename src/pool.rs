use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, hash_map::Entry};

use anyhow::Result;
use itertools::Itertools;
use rand::Rng;
use rand::seq::IteratorRandom;
use sentry_protos::taskworker::v1::{PushTaskRequest, worker_service_client::WorkerServiceClient};
use tonic::transport::{Channel, Error};
use tracing::{info, warn};

#[derive(Clone)]
pub struct WorkerPool {
    /// Maps every worker address to its client.
    clients: HashMap<String, WorkerClient>,

    /// All available worker addresses.
    addresses: HashSet<String>,
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
    pub fn new<T: IntoIterator<Item = String>>(addresses: T) -> Self {
        Self {
            addresses: addresses.into_iter().collect(),
            clients: HashMap::new(),
        }
    }

    /// Register worker address during execution.
    pub fn add_worker<T: Into<String>>(&mut self, address: T) {
        let address = address.into();
        info!("Adding worker {address}");
        self.addresses.insert(address);
    }

    /// Unregister worker address during execution.
    pub fn remove_worker(&mut self, address: &String) {
        info!("Removing worker {address}");
        self.addresses.remove(address);
        self.clients.remove(address);
    }

    /// Decrement `queue_size` for the worker with address `address`. Called when worker reports task status.
    // pub fn decrement_queue_size(&mut self, address: &String) {
    //     if let Some(client) = self.clients.get_mut(address) {
    //         client.queue_size = client.queue_size.saturating_sub(1);
    //     }
    // }

    /// Call this function over and over again in another thread to keep the pool of active connections updated.
    pub async fn update(&mut self) {
        for address in &self.addresses {
            if let Entry::Vacant(e) = self.clients.entry(address.into()) {
                match connect(address).await {
                    Ok(connection) => {
                        info!("Connected to {address}");

                        let client = WorkerClient::new(connection, address.clone(), 0);
                        e.insert(client);
                    }

                    Err(e) => {
                        warn!("Couldn't connect to {address} - {:?}", e);
                    }
                }
            }
        }

        let pool = self
            .clients
            .iter()
            .map(|(address, client)| format!("{address}:{}", client.queue_size))
            .join(",");

        info!(pool)
    }

    /// Try pushing a task to the best worker using P2C (Power of Two Choices).
    pub async fn push(&mut self, request: PushTaskRequest) -> Result<()> {
        let candidate = {
            let mut rng = rand::rng();

            self.clients
                .values()
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

                // Calculate estimation error before updating
                let estimated = client.queue_size;
                let actual = response.queue_size;
                let error = (estimated as i64) - (actual as i64);

                // Record the absolute error
                metrics::histogram!("worker.queue_size.estimation_error", "worker" => address.clone())
                    .record(error.abs() as f64);

                // Record the signed error to see if we're systematically over/under-estimating
                metrics::histogram!("worker.queue_size.estimation_delta", "worker" => address.clone())
                    .record(error as f64);

                // Record both values for reference
                metrics::gauge!("worker.queue_size.estimated", "worker" => address.clone())
                    .set(estimated as f64);
                metrics::gauge!("worker.queue_size.actual", "worker" => address.clone())
                    .set(actual as f64);

                // Update this worker's queue size
                client.queue_size = actual;
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
