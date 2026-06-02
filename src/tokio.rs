use std::future::Future;

use sentry::{Hub, SentryFutureExt};
use tokio::task::{JoinHandle, JoinSet};

/// Spawns a task with the main Sentry hub propagated.
///
/// This ensures that global Sentry tags configured via `configure_scope` on
/// the main thread are available in spawned tasks. Without this, tokio worker
/// threads would have their own hub without those tags.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future.bind_hub(Hub::new_from_top(Hub::main())))
}

/// Spawns `max(n, 1)` tasks, each running the future produced by `f` with the task's index.
/// Returns a [`JoinSet`] containing all spawned tasks.
pub fn spawn_pool<F, Fut>(n: usize, mut f: F) -> JoinSet<Fut::Output>
where
    F: FnMut(usize) -> Fut,
    Fut: Future + Send + 'static,
    Fut::Output: Send,
{
    let mut join_set = JoinSet::new();

    let count = n.max(1);
    for i in 0..count {
        join_set.spawn(f(i).bind_hub(Hub::new_from_top(Hub::main())));
    }

    join_set
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spawn_pool_spawns_one_worker_when_n_is_zero() {
        let mut set = spawn_pool(0, |i| async move { i });
        assert_eq!(set.join_next().await.unwrap().unwrap(), 0);
        assert!(set.join_next().await.is_none());
    }
}
