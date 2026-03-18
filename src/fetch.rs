use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::push::PushPool;
use crate::store::inflight_activation::InflightActivationStore;

type BucketRange = (i16, i16);

const BUCKET_COUNT: usize = 256;

pub struct FetchPool {
    store: Arc<dyn InflightActivationStore>,
    push_pool: Arc<PushPool>,
    config: Arc<Config>,
}

impl FetchPool {
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

    pub async fn start(&self) -> Result<()> {
        let mut handles = vec![];

        let fetch_threads = self.config.fetch_threads.max(1);
        let buckets_per_thread = BUCKET_COUNT / fetch_threads;

        for i in 0..fetch_threads {
            let min = (i * buckets_per_thread) as i16;
            let max = ((i + 1) * buckets_per_thread - 1) as i16;

            let guard = get_shutdown_guard().shutdown_on_drop();

            let store = self.store.clone();
            let push_pool = self.push_pool.clone();
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = guard.wait() => {
                            info!("Fetch loop received shutdown signal");
                            break;
                        }

                        _ = async {
                            debug!("About to fetch next activation...");
                            fetch_activations(store.clone(), push_pool.clone(), config.clone(), (min, max)).await;
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
pub async fn fetch_activations(
    store: Arc<dyn InflightActivationStore>,
    push_pool: Arc<PushPool>,
    config: Arc<Config>,
    buckets: BucketRange,
) {
    let start = Instant::now();
    metrics::counter!("pusher.fetch_activations.runs").increment(1);

    let batch_size = config.push_batch_size.max(1) as i32;

    debug!(
        "Fetching next batch of {} pending activations...",
        batch_size
    );

    match store
        .get_pending_activations_from_namespaces(None, None, Some(batch_size), buckets)
        .await
    {
        Ok(activations) if !activations.is_empty() => {
            let fetched = activations.len();

            debug!("Atomically fetched and marked {fetched} tasks as processing");

            metrics::histogram!("task_pusher.batch.fetched").record(fetched as f64);

            metrics::histogram!("task_pusher.batch.expected").record(batch_size as f64);

            for activation in activations {
                let id = activation.id.clone();

                if let Err(e) = push_pool.submit(activation).await {
                    error!("Failed to submit task {id} to push pool - {:?}", e);
                }
            }

            metrics::histogram!("pusher.fetch_activations.duration").record(start.elapsed());
        }

        Ok(_) => {
            debug!("No pending activations, sleeping briefly...");
            sleep(Duration::from_millis(100)).await;

            metrics::histogram!("pusher.fetch_activations.duration").record(start.elapsed());
        }

        Err(e) => {
            error!("Failed to fetch pending activations - {:?}", e);
            sleep(Duration::from_millis(100)).await;

            metrics::histogram!("pusher.fetch_activations.duration").record(start.elapsed());
        }
    }
}
