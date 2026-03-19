use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use tokio::time::sleep;
use tonic::async_trait;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::push::PushPool;
use crate::store::inflight_activation::{InflightActivation, InflightActivationStore};

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
        let fetch_wait_ms = self.config.fetch_wait_ms;

        let mut fetch_pool = crate::tokio::spawn_pool(self.config.fetch_threads, |_| {
            let store = self.store.clone();
            let pusher = self.pusher.clone();
            let config = self.config.clone();

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
                            match fetch_activation(store.clone(), pusher.clone(), config.clone()).await {
                                Ok(false) | Err(_) => {
                                    // Found no pending activations OR there is an issue with the store OR submitting to push pool failed
                                    sleep(Duration::from_millis(fetch_wait_ms)).await;
                                }

                                Ok(true) => {
                                    // Fetched pending activation successfully, so nothing else needs to be done
                                }
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
    config: Arc<Config>,
) -> Result<bool> {
    let start = Instant::now();
    metrics::counter!("fetch.fetch_activation.calls").increment(1);

    debug!("Fetching next pending activation...");

    let result = store
        .get_pending_activations_from_namespaces(
            config.application.as_deref(),
            config.namespaces.as_deref(),
            Some(1),
        )
        .await;

    let found = match result {
        Ok(activations) if !activations.is_empty() => {
            let activation = activations.into_iter().next().unwrap();
            let id = activation.id.clone();

            debug!("Atomically fetched and marked task {id} as processing");

            if let Err(e) = pusher.push_task(activation).await {
                // Once processing deadline expires, status will be set back to pending
                error!("Failed to submit task {id} to push pool - {e:?}");
                return Err(e);
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
