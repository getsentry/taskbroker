use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use tokio::time::sleep;
use tonic::async_trait;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::helpers;
use crate::push::PushPool;
use crate::store::inflight_activation::InflightActivation;
use crate::store::inflight_activation::InflightActivationStore;

/// Thin interface for the push pool. It mostly serves to enable proper unit testing, but it also decouples fetch logic from push logic even further.
#[async_trait]
pub trait TaskPusher {
    /// Push a single task to the worker service.
    async fn push_task(&self, activation: InflightActivation) -> Result<()>;
}

#[async_trait]
impl TaskPusher for PushPool {
    async fn push_task(&self, activation: InflightActivation) -> Result<()> {
        self.submit(activation).await
    }
}

/// Wrapper around `config.fetch_threads` asynchronous tasks, each of which fetches a pending activation from the store, passes is to the push pool, and repeats.
pub struct FetchPool<T: TaskPusher> {
    /// Inflight activation store.
    store: Arc<dyn InflightActivationStore>,

    /// Pool of push threads that push activations to the worker service.
    pusher: Arc<T>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl<T: TaskPusher + Send + Sync + 'static> FetchPool<T> {
    /// Initialize a new fetch pool.
    pub fn new(
        store: Arc<dyn InflightActivationStore>,
        config: Arc<Config>,
        pusher: Arc<T>,
    ) -> Self {
        Self {
            store,
            config,
            pusher,
        }
    }

    /// Spawn `config.fetch_threads` asynchronous tasks, each of which repeatedly moves pending activations from the store to the push pool until the shutdown signal is received.
    pub async fn start(&self) -> Result<()> {
        let mut fetch_pool = helpers::spawn_pool(self.config.fetch_threads, |_| {
            let store = self.store.clone();
            let pusher = self.pusher.clone();

            let guard = get_shutdown_guard().shutdown_on_drop();

            async move {
                loop {
                    tokio::select! {
                        _ = guard.wait() => {
                            info!("Fetch loop received shutdown signal");
                            return;
                        }

                        _ = async {
                            debug!("About to fetch next activation...");

                            //  Instead of returning when `fetch_activation` fails, we just try again
                            if let Ok(false) = fetch_activation(store.clone(), pusher.clone()).await {
                                // Found no pending activations, wait for some to appear
                                sleep(Duration::from_millis(100)).await;
                            }
                        } => {}
                    }
                }
            }
        });

        while let Some(res) = fetch_pool.join_next().await {
            if let Err(e) = res {
                return Err(e.into());
            }
        }

        Ok(())
    }
}

/// Grab the next pending activation from the store, mark it as processing, and send to push channel. Return...
/// - `Ok(true)` if an activation was found
/// - `Ok(false)` if none pending
/// - `Err` if fetching failed.
pub async fn fetch_activation<T: TaskPusher>(
    store: Arc<dyn InflightActivationStore>,
    pusher: Arc<T>,
) -> Result<bool> {
    let start = Instant::now();
    metrics::counter!("fetch.fetch_activation.calls").increment(1);

    debug!("Fetching next pending activation...");

    let found = match store.get_pending_activation(None, None).await {
        Ok(Some(activation)) => {
            let id = activation.id.clone();
            debug!("Atomically fetched and marked task {id} as processing");

            // Times out after `config.push_timeout_ms` milliseconds
            if let Err(e) = pusher.push_task(activation).await {
                // Do not return `Err` because the fetch itself succeeded
                error!("Failed to submit task {id} to push pool - {:?}", e);
            }

            true
        }

        Ok(_) => {
            debug!("No pending activations");
            false
        }

        Err(e) => {
            error!("Failed to fetch pending activation - {:?}", e);
            return Err(e);
        }
    };

    metrics::histogram!("fetch.fetch_activation.duration").record(start.elapsed());
    Ok(found)
}

#[cfg(test)]
mod tests;
