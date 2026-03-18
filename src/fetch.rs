use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::push::PushPool;
use crate::store::inflight_activation::InflightActivationStore;

/// Wrapper around `config.fetch_threads` asynchronous tasks, each of which fetches a pending activation from the store, passes is to the push pool, and repeats.
pub struct FetchPool {
    /// Inflight activation store.
    store: Arc<dyn InflightActivationStore>,

    /// Pool of push threads that push activations to the worker service.
    push_pool: Arc<PushPool>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl FetchPool {
    /// Initialize a new fetch pool.
    pub fn new(
        store: Arc<dyn InflightActivationStore>,
        config: Arc<Config>,
        push_pool: Arc<PushPool>,
    ) -> Self {
        Self {
            store,
            push_pool,
            config,
        }
    }

    /// Spawn `config.fetch_threads` asynchronous tasks, each of which repeatedly moves pending activations from the store to the push pool until the shutdown signal is received.
    pub async fn start(&self) -> Result<()> {
        let mut handles = vec![];

        for _ in 0..self.config.fetch_threads.max(1) {
            let guard = get_shutdown_guard().shutdown_on_drop();

            let store = self.store.clone();
            let push_pool = self.push_pool.clone();

            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = guard.wait() => {
                            info!("Fetch loop received shutdown signal");
                            break;
                        }

                        _ = async {
                            debug!("About to fetch next activation...");
                            fetch_activations(store.clone(), push_pool.clone()).await;
                        } => {}
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            if let Err(e) = handle.await {
                return Err(e.into());
            }
        }

        Ok(())
    }
}

/// Grab the next pending activation from the store, mark it as processing, and send to push channel.
pub async fn fetch_activations(store: Arc<dyn InflightActivationStore>, push_pool: Arc<PushPool>) {
    let start = Instant::now();
    metrics::counter!("fetch.fetch_activations.runs").increment(1);

    debug!("Fetching next pending activation...");

    match store.get_pending_activation(None, None).await {
        Ok(Some(activation)) => {
            let id = activation.id.clone();
            debug!("Atomically fetched and marked task {id} as processing");

            if let Err(e) = push_pool.submit(activation).await {
                error!("Failed to submit task {id} to push pool - {:?}", e);
            }

            metrics::histogram!("fetch.fetch_activations.duration").record(start.elapsed());
        }

        Ok(_) => {
            debug!("No pending activations, sleeping briefly...");
            sleep(Duration::from_millis(100)).await;

            metrics::histogram!("fetch.fetch_activations.duration").record(start.elapsed());
        }

        Err(e) => {
            error!("Failed to fetch pending activation - {:?}", e);
            sleep(Duration::from_millis(100)).await;

            metrics::histogram!("fetch.fetch_activations.duration").record(start.elapsed());
        }
    }
}
