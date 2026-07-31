use std::time::Duration;

use tokio::runtime::Handle;
use tokio::time::{self, MissedTickBehavior};

/// Computes the mean worker-thread busy ratio in `[0.0, 1.0]`.
///
/// `busy_delta_secs` is the total busy time accrued across all worker threads
/// since the previous sample; dividing by `elapsed_secs * num_workers` (the
/// maximum possible busy time for the whole pool over that window) yields the
/// fraction of the pool that was busy.
#[cfg(tokio_unstable)]
fn utilization_ratio(busy_delta_secs: f64, elapsed_secs: f64, num_workers: usize) -> f64 {
    if elapsed_secs <= 0.0 || num_workers == 0 {
        return 0.0;
    }
    (busy_delta_secs / (elapsed_secs * num_workers as f64)).clamp(0.0, 1.0)
}

/// Samples the current Tokio runtime's metrics every `interval` and emits them
/// to statsd until a shutdown signal is received.
pub async fn run(interval: Duration) -> anyhow::Result<()> {
    let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    let runtime_metrics = Handle::current().metrics();

    let mut timer = time::interval(interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

    #[cfg(not(tokio_unstable))]
    tracing::warn!(
        "Tokio worker utilization metric unavailable: built without `--cfg tokio_unstable`; \
         emitting stable runtime counts only"
    );

    #[cfg(tokio_unstable)]
    let mut sampler = unstable::WorkerBusySampler::new(&runtime_metrics);

    loop {
        tokio::select! {
            _ = timer.tick() => {
                // Stable metrics (available regardless of `tokio_unstable`).
                metrics::gauge!("runtime.num_workers")
                    .set(runtime_metrics.num_workers() as f64);
                metrics::gauge!("runtime.num_alive_tasks")
                    .set(runtime_metrics.num_alive_tasks() as f64);

                // Detailed per-worker metrics (require `tokio_unstable`).
                #[cfg(tokio_unstable)]
                {
                    let sample = sampler.sample(&runtime_metrics);
                    metrics::gauge!("runtime.worker_utilization").set(sample.utilization);
                    metrics::gauge!("runtime.global_queue_depth")
                        .set(sample.global_queue_depth as f64);
                    metrics::gauge!("runtime.worker_local_queue_depth")
                        .set(sample.local_queue_depth as f64);
                }
            }
            _ = guard.wait() => break,
        }
    }

    Ok(())
}

#[cfg(tokio_unstable)]
mod unstable {
    use std::time::Instant;

    use tokio::runtime::RuntimeMetrics;

    use super::utilization_ratio;

    /// A single sampling result derived from the runtime's per-worker metrics.
    pub struct Sample {
        /// Mean worker-thread busy ratio in `[0.0, 1.0]`.
        pub utilization: f64,
        /// Number of tasks queued in the runtime's global (injection) queue.
        pub global_queue_depth: usize,
        /// Total tasks queued across all workers' local run queues.
        pub local_queue_depth: usize,
    }

    /// Tracks the cumulative worker busy duration between samples so that
    /// utilization can be computed from the delta over the sampling window.
    pub struct WorkerBusySampler {
        prev_busy_secs: f64,
        last_sample: Instant,
    }

    impl WorkerBusySampler {
        pub fn new(runtime_metrics: &RuntimeMetrics) -> Self {
            Self {
                prev_busy_secs: total_busy_secs(runtime_metrics),
                last_sample: Instant::now(),
            }
        }

        pub fn sample(&mut self, runtime_metrics: &RuntimeMetrics) -> Sample {
            let now = Instant::now();
            let busy_secs = total_busy_secs(runtime_metrics);
            let elapsed_secs = now.duration_since(self.last_sample).as_secs_f64();
            // `worker_total_busy_duration` is monotonic, but guard against clock
            // effects / worker count changes producing a negative delta.
            let busy_delta_secs = (busy_secs - self.prev_busy_secs).max(0.0);

            self.prev_busy_secs = busy_secs;
            self.last_sample = now;

            let num_workers = runtime_metrics.num_workers();
            let local_queue_depth = (0..num_workers)
                .map(|worker| runtime_metrics.worker_local_queue_depth(worker))
                .sum();

            Sample {
                utilization: utilization_ratio(busy_delta_secs, elapsed_secs, num_workers),
                global_queue_depth: runtime_metrics.global_queue_depth(),
                local_queue_depth,
            }
        }
    }

    /// Sums `worker_total_busy_duration` across every worker thread, in seconds.
    fn total_busy_secs(runtime_metrics: &RuntimeMetrics) -> f64 {
        (0..runtime_metrics.num_workers())
            .map(|worker| {
                runtime_metrics
                    .worker_total_busy_duration(worker)
                    .as_secs_f64()
            })
            .sum()
    }
}

#[cfg(all(test, tokio_unstable))]
mod tests {
    use super::utilization_ratio;

    #[test]
    fn utilization_is_busy_over_capacity() {
        // 4 workers, 2s window => 8s of capacity. 4s busy => 50% utilization.
        assert_eq!(utilization_ratio(4.0, 2.0, 4), 0.5);
    }

    #[test]
    fn utilization_fully_busy() {
        // Every worker busy the whole window => 100%.
        assert_eq!(utilization_ratio(16.0, 2.0, 8), 1.0);
    }

    #[test]
    fn utilization_clamps_above_one() {
        // Rounding/measurement skew can push busy slightly over capacity.
        assert_eq!(utilization_ratio(9.0, 2.0, 4), 1.0);
    }

    #[test]
    fn utilization_handles_zero_elapsed() {
        assert_eq!(utilization_ratio(1.0, 0.0, 4), 0.0);
    }

    #[test]
    fn utilization_handles_zero_workers() {
        assert_eq!(utilization_ratio(1.0, 2.0, 0), 0.0);
    }

    #[test]
    fn utilization_idle() {
        assert_eq!(utilization_ratio(0.0, 2.0, 4), 0.0);
    }
}
