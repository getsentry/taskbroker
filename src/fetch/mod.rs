use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use async_backtrace::framed;
use flume::Sender;

use crate::config::Config;
use crate::fetch::thread::FetchThread;
use crate::store::activation::Activation;
use crate::store::traits::ActivationStore;
use crate::store::types::BucketRange;

mod thread;

/// This value should be a power of two. If it decreases, some ranges will no longer be queried.
/// That means the pending activation query will skip tasks within these ranges.
pub const MAX_FETCH_THREADS: usize = 256;

/// Inclusive bucket range for fetch thread `thread_index` when using `fetch_threads` concurrent fetch loops.
/// Requires `fetch_threads` to divide [`MAX_FETCH_THREADS`] (enforced via [`normalize_fetch_threads`]).
pub fn bucket_range_for_fetch_thread(thread_index: usize, fetch_threads: usize) -> BucketRange {
    let maximum = MAX_FETCH_THREADS;
    let buckets_per_range = maximum / fetch_threads;

    let low = (thread_index * buckets_per_range) as i16;
    let high = ((thread_index + 1) * buckets_per_range - 1) as i16;

    (low, high)
}

/// Wrapper around `config.fetch_threads` asynchronous tasks, each of which fetches batches of pending activations from the store, passes them to the push pool, and repeats.
pub struct FetchPool {
    /// The sending end of a channel that accepts task activations.
    sender: Sender<(Activation, Instant)>,

    /// Activation store.
    store: Arc<dyn ActivationStore>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl FetchPool {
    /// Initialize a new fetch pool.
    pub fn new(
        sender: Sender<(Activation, Instant)>,
        store: Arc<dyn ActivationStore>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            sender,
            store,
            config,
        }
    }

    /// Spawns one task per effective fetch thread ([`normalize_fetch_threads`]), each claiming pending work only in its bucket subrange.
    #[framed]
    pub async fn start(&self) -> Result<()> {
        let fetch_threads = self.config.fetch.threads;

        let mut fetch_pool = crate::tokio::spawn_pool(fetch_threads, |thread_index| {
            let config = self.config.clone();

            let bucket = bucket_range_for_fetch_thread(thread_index, fetch_threads);

            let mut thread = FetchThread {
                sender: self.sender.clone(),
                store: self.store.clone(),
                config,
                bucket,
            };

            async move { thread.start().await }
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
