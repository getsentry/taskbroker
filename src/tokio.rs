use std::future::Future;

use tokio::task::JoinSet;

/// Spawns `max(n, 1)` tasks, each running the future produced by `f` with the task's index.
/// Returns a [`JoinSet`] containing all spawned tasks.
pub fn spawn_pool<F, Fut>(n: usize, f: F) -> JoinSet<Fut::Output>
where
    F: Fn(usize) -> Fut,
    Fut: Future + Send + 'static,
    Fut::Output: Send,
{
    let mut join_set = JoinSet::new();

    let count = n.max(1);
    for i in 0..count {
        join_set.spawn(f(i));
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
