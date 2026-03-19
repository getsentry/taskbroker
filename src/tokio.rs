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
