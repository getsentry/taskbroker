use std::sync::Arc;

use anyhow::Result;
use async_backtrace::framed;
use flume::Receiver;
use tokio::task::JoinSet;

use crate::config::Config;
use crate::push::thread::{PushItem, PushThread};
use crate::push::updater::Updater;
use crate::store::traits::ActivationStore;
use crate::tokio::spawn_pool;
use crate::worker::WorkerMap;

mod thread;
pub mod updater;

/// Approximately the longest time it should take to perform a task update query (used by `compute_claim_lease_ms`).
/// Setting this too low will not impact correctness, but in the worst case, it may cause tasks to excute more than once.
const QUERY_MS: u64 = 1000;

/// Computes how long a claim should remain active before upkeep reverts back to pending.
///
/// A claimed activation is not yet processing. It still has to be submitted by
/// the fetch loop to the bounded push queue, wait behind any already queued
/// activations, be pushed to the worker, and then be marked as processing in the
/// store. The lease therefore covers...
///
/// - The longest possible time to submit one claimed fetch batch into a full queue
/// - The longest possible time for the push queue to drain
/// - The (roughly) longest possible time to update every task to "processing" (or "pending")
/// - When push updates are batched, the longest time a successful push can sit in the updater buffer
///
/// This computation could definitely use some refinement, but it's more accurate than
/// what we had before, and it's located in one place rather than being copied between
/// the SQLite and Postgres adapters.
pub fn compute_claim_lease_ms(config: &Config) -> u64 {
    // Imagine we are computing the lease for some task in a batch of N tasks
    let fetch_batch_size = config.fetch_batch_size.max(1) as u64;
    let push_threads = config.push.threads.max(1) as u64;
    let push_queue_size = config.push.queue.size.max(1) as u64;

    // In the worst case, this task is the last in a batch where every queue push times out
    let queue_ms = fetch_batch_size * config.push.queue.timeout.as_millis() as u64;

    // The push queue drains in "rounds" since there are multiple push threads
    // If there are 5 push threads and the queue has 10 slots, it will only take ⌈10 / 5⌉ = 2 rounds to drain it
    // We add 1 to account for the task we are pushing right now
    let rounds = push_queue_size.div_ceil(push_threads) + 1;

    // In the worst case, every task in the push queue will time out when pushing
    let drain_ms = rounds * config.push.timeout.as_millis() as u64;

    // In the worst case, the tasks in the push queue will take `QUERY_MS` time to update from claimed to processing
    // Since query timeouts aren't actually enforced right now, this is merely an approximation
    let update_query_ms = rounds * QUERY_MS;

    // Batched push updates can wait in the updater buffer until either the batch fills or the periodic flush runs
    let batch_delay_ms = if config.batch_push_updates {
        config.push_update_interval_ms as u64
    } else {
        0
    };

    queue_ms + drain_ms + update_query_ms + batch_delay_ms
}

/// Error returned when enqueueing an activation for the push workers fails.
#[derive(Debug)]
pub enum QueueError {
    /// The bounded queue stayed full until `push_queue_timeout_ms` elapsed.
    Timeout,

    /// Channel closed.
    Closed,
}

/// Wrapper around `config.push.threads` asynchronous tasks, each of which receives an activation from the channel, sends it to the worker service, and repeats.
pub struct PushPool {
    /// The receiving end of a channel that accepts task activations.
    receiver: Receiver<PushItem>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl PushPool {
    /// Initialize a new push pool.
    pub fn new(receiver: Receiver<PushItem>, config: Arc<Config>) -> Self {
        Self { receiver, config }
    }

    /// Spawn `config.push.threads` asynchronous tasks, each of which repeatedly moves pending activations from the channel to the worker service until the shutdown signal is received.
    #[framed]
    pub async fn start(
        &self,
        workers: Vec<WorkerMap>,
        updater: Arc<dyn Updater>,
        store: Arc<dyn ActivationStore>,
    ) -> Result<()> {
        let mut workers = workers.into_iter();

        // Start updater daemon
        let upd = tokio::spawn({
            let updater = updater.clone();
            async move { updater.start().await }
        });

        let mut threads: JoinSet<Result<()>> = spawn_pool(self.config.push.threads, |_| {
            let mut thread = PushThread {
                workers: workers.next().unwrap(),
                receiver: self.receiver.clone(),
                updater: updater.clone(),
                store: store.clone(),
            };

            async move { thread.start().await }
        });

        let mut result = Ok(());

        while let Some(r) = threads.join_next().await {
            // Propagate fatal errors
            if let Ok(Err(e)) = r {
                result = Err(e);
                break;
            }

            // Join error
            if let Err(e) = r {
                result = Err(e.into());
                break;
            }
        }

        // We may have exited the `while` loop due to push thread failure, wait until all others are closed
        threads.shutdown().await;

        // Stop the updater daemon
        updater.stop();
        upd.await??;

        result
    }
}

#[cfg(test)]
mod tests;
