//! Per-broker Tokio runtime metrics.
//!
//! Emits gauges describing how busy the shared Tokio worker pool is, the most
//! important being `runtime.worker_utilization`: the mean fraction of worker
//! threads that were actively executing tasks over the last sampling window.
//!
//! Tokio exposes `worker_total_busy_duration` as a **monotonic cumulative counter** per worker,
//! not an instantaneous rate. A single reading is meaningless on its own; to derive
//! a utilization ratio we remember the previous cumulative total (and when it was taken) and divide
//! the delta over the window by that window's maximum possible busy time
//! (`elapsed * num_workers`). [`WorkerBusySampler`] owns that state.

use std::time::{Duration, Instant};

use tokio::runtime::{Handle, RuntimeMetrics};
use tokio::time::{self, MissedTickBehavior};

#[cfg(not(tokio_unstable))]
compile_error!(
    "taskbroker's runtime metrics require the Tokio unstable API; build with \
     `--cfg tokio_unstable` (see .cargo/config.toml) or set \
     RUSTFLAGS=\"--cfg tokio_unstable\""
);

/// Computes the mean worker-thread busy ratio in `[0.0, 1.0]`.
///
/// `busy_secs` is the total busy time accrued across all worker threads over
/// the window; dividing by `elapsed_secs * num_workers` (the maximum possible
/// busy time for the whole pool over that window) yields the fraction of the
/// pool that was busy.
fn utilization_ratio(busy_secs: f64, elapsed_secs: f64, num_workers: usize) -> f64 {
    if elapsed_secs <= 0.0 || num_workers == 0 {
        return 0.0;
    }
    (busy_secs / (elapsed_secs * num_workers as f64)).clamp(0.0, 1.0)
}

/// Samples the current Tokio runtime's metrics every `interval` and emits them
/// to statsd until a shutdown signal is received.
pub async fn run(interval: Duration) -> anyhow::Result<()> {
    let guard = elegant_departure::get_shutdown_guard().shutdown_on_drop();
    let runtime_metrics = Handle::current().metrics();

    let mut timer = time::interval(interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut sampler = WorkerBusySampler::new(&runtime_metrics);

    loop {
        tokio::select! {
            _ = timer.tick() => {
                metrics::gauge!("runtime.num_workers")
                    .set(runtime_metrics.num_workers() as f64);
                metrics::gauge!("runtime.num_alive_tasks")
                    .set(runtime_metrics.num_alive_tasks() as f64);

                let sample = sampler.sample(&runtime_metrics);
                metrics::gauge!("runtime.worker_utilization").set(sample.utilization);
                metrics::gauge!("runtime.global_queue_depth")
                    .set(sample.global_queue_depth as f64);
                metrics::gauge!("runtime.worker_local_queue_depth")
                    .set(sample.local_queue_depth as f64);
            }
            _ = guard.wait() => break,
        }
    }

    Ok(())
}

/// A single sampling result derived from the runtime's per-worker metrics.
struct Sample {
    /// Mean worker-thread busy ratio in `[0.0, 1.0]`.
    utilization: f64,
    /// Number of tasks queued in the runtime's global (injection) queue.
    global_queue_depth: usize,
    /// Total tasks queued across every worker's local run queue.
    local_queue_depth: usize,
}

/// Holds the state needed to turn Tokio's monotonic `worker_total_busy_duration`
/// counter into a per-window utilization ratio: the previous cumulative busy
/// total and the instant it was read.
struct WorkerBusySampler {
    prev_busy_secs: f64,
    last_sample: Instant,
}

impl WorkerBusySampler {
    fn new(runtime_metrics: &RuntimeMetrics) -> Self {
        Self {
            prev_busy_secs: total_busy_secs(runtime_metrics),
            last_sample: Instant::now(),
        }
    }

    fn sample(&mut self, runtime_metrics: &RuntimeMetrics) -> Sample {
        let now = Instant::now();
        let cumulative_busy_secs = total_busy_secs(runtime_metrics);
        let elapsed_secs = now.duration_since(self.last_sample).as_secs_f64();
        // The counter is monotonic, but clamp to guard against clock effects or
        // a changing worker count producing a negative delta.
        let busy_delta_secs = (cumulative_busy_secs - self.prev_busy_secs).max(0.0);

        self.prev_busy_secs = cumulative_busy_secs;
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

/// Sums each worker's cumulative `worker_total_busy_duration` (a monotonic
/// counter of time spent executing tasks, never idle/parked) into a single
/// pool-wide total, in seconds.
fn total_busy_secs(runtime_metrics: &RuntimeMetrics) -> f64 {
    (0..runtime_metrics.num_workers())
        .map(|worker| {
            runtime_metrics
                .worker_total_busy_duration(worker)
                .as_secs_f64()
        })
        .sum()
}

#[cfg(test)]
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
