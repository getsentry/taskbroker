use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use tokio::time::sleep;
use tonic::async_trait;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::push::{PushError, PushPool};
use crate::store::inflight_activation::{InflightActivation, InflightActivationStore};

/// Thin interface for the push pool. It mostly serves to enable proper unit testing, but it also decouples fetch logic from push logic even further.
#[async_trait]
pub trait TaskPusher {
    /// Push a single task to the worker service.
    async fn push_task(&self, activation: InflightActivation) -> Result<(), PushError>;
}

#[async_trait]
impl TaskPusher for PushPool {
    async fn push_task(&self, activation: InflightActivation) -> Result<(), PushError> {
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
                            debug!("Fetching next pending activation...");
                            metrics::counter!("fetch.loop.count").increment(1);

                            let start = Instant::now();
                            let mut backoff = false;

                            let application = config.application.as_deref();
                            let namespaces = config.namespaces.as_deref();

                            match store.get_pending_activation(application, namespaces).await {
                                Ok(Some(activation)) => {
                                    let id = activation.id.clone();

                                    debug!(
                                        task_id = %id,
                                        "Fetched and marked task as processing"
                                    );

                                    if let Err(e) = pusher.push_task(activation).await {
                                        match e {
                                            PushError::Timeout => warn!(
                                                task_id = %id,
                                                "Submit to push pool timed out after milliseconds",
                                            ),

                                            PushError::Channel(e) => warn!(
                                                task_id = %id,
                                                error = ?e,
                                                "Submit to push pool failed due to channel error",
                                            )
                                        }

                                        backoff = true;
                                    }
                                }

                                Ok(None) => {
                                    debug!("No pending activations");

                                    // Wait for pending activations to appear
                                    backoff = true;
                                }

                                Err(e) => {
                                    warn!(
                                        error = ?e,
                                        "Store failed while fetching task"
                                    );

                                    // Store may be down, wait before trying again
                                    backoff = true;
                                }
                            };

                            metrics::histogram!("fetch.loop.duration")
                                .record(start.elapsed());

                            if backoff {
                                sleep(Duration::from_millis(fetch_wait_ms)).await;
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

#[cfg(test)]
mod tests;
