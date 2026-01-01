use anyhow::{Error, Result};
use futures::future::join_all;
use itertools::Itertools;
use sentry_protos::taskworker::v1::{
    AddTaskRequest, GetQueueSizeRequest, worker_service_client::WorkerServiceClient,
};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tonic::{Request, transport::Channel};

type WorkerClient = WorkerServiceClient<Channel>;

pub struct WorkerRouter {
    workers: Arc<RwLock<HashSet<String>>>,
    candidates: Vec<String>,
}

impl WorkerRouter {
    pub fn new<T: Into<String>>(workers: impl IntoIterator<Item = T>) -> Self {
        let workers = Arc::new(RwLock::new(workers.into_iter().map(Into::into).collect()));
        let candidates = vec![];

        Self {
            workers,
            candidates,
        }
    }

    pub async fn start(&mut self) {
        let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = guard.wait() => {
                    break;
                }

                _ = interval.tick() => {
                    self.update().await;
                }
            }
        }
    }

    /// Update candidate workers.
    pub async fn update(&mut self) {
        // (1) Connect to every worker
        let workers = self.workers.read().await;
        let futures = workers.iter().map(|address| connect(address));

        let mut clients: Vec<WorkerClient> = join_all(futures)
            .await
            .into_iter()
            .filter_map(Result::ok)
            .collect();

        // (2) Sort workers by queue size (smallest first)
        let futures = clients
            .iter_mut()
            .map(|client| client.get_queue_size(Request::new(GetQueueSizeRequest {})));

        self.candidates = join_all(futures)
            .await
            .into_iter()
            .filter_map(Result::ok)
            .map(|response| response.into_inner())
            .sorted_by_key(|res| res.length)
            .map(|res| res.address)
            .collect();
    }

    // Try pushing a task to the best worker available.
    pub async fn push_task(&mut self, request: &AddTaskRequest) -> Result<()> {
        let mut full = false;

        for address in &self.candidates {
            // Skip this worker if we can't connect
            let Ok(mut client) = connect(address).await else {
                continue;
            };

            // Skip this worker if pushing the task fails
            let Ok(response) = client.add_task(request.clone()).await else {
                continue;
            };

            // If the task was pushed successfully, we're done
            if response.into_inner().added {
                return Ok(());
            }

            // Indicate that at least one worker is full
            full = true;
        }

        if full {
            Err(anyhow::anyhow!("All workers are full"))
        } else {
            Err(anyhow::anyhow!("All workers are unhealthy"))
        }
    }
}

async fn connect<T: Into<String>>(address: T) -> Result<WorkerServiceClient<Channel>, Error> {
    Ok(WorkerServiceClient::connect(address.into()).await?)
}
