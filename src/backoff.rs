use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use crate::config::Config;

/// Shared leaky bucket wait to pace fetching in push mode.
/// Certain events, such as submission failures, can call `increase` to increment `wait_ms` by `increment_ms` milliseconds.
/// Before submitting each task to the push pool, a fetch thread will sleep `wait_ms` milliseconds first.
/// Every `decay_interval` milliseconds, the value of `wait_ms` is decremented by `decay_amount_ms`.
/// The value of `wait_ms` is capped at `max_wait_ms`.
pub struct Backoff {
    wait_ms: AtomicU32,
    increment_ms: u32,
    max_wait_ms: u32,
    decay_interval_ms: u64,
    decay_amount_ms: u32,
}

impl Backoff {
    /// When `backoff_max_wait_ms` is zero, `increase` and `decay_tick` are NOPs.
    pub fn from_config(config: &Config) -> Self {
        Self {
            wait_ms: AtomicU32::new(0),
            increment_ms: config.backoff_increment_ms,
            max_wait_ms: config.backoff_max_wait_ms,
            decay_interval_ms: config.backoff_decay_interval_ms,
            decay_amount_ms: config.backoff_decay_amount_ms,
        }
    }

    /// Whether the decay task should run (push mode only; see `main`).
    pub fn decays(&self) -> bool {
        self.max_wait_ms > 0
    }

    pub fn decay_interval(&self) -> Duration {
        Duration::from_millis(self.decay_interval_ms)
    }

    /// Add `increment_ms`, clamped to `max_wait_ms`. Does nothing if `increment_ms` is zero.
    pub fn increase(&self) {
        if self.increment_ms == 0 {
            return;
        }

        let mut current = self.wait_ms.load(Ordering::Relaxed);
        loop {
            if current >= self.max_wait_ms {
                return;
            }

            let new = (current + self.increment_ms).min(self.max_wait_ms);
            match self.wait_ms.compare_exchange_weak(
                current,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    metrics::gauge!("push.backoff.wait_ms").set(new as f64);
                    return;
                }

                Err(c) => current = c,
            }
        }
    }

    /// Get the current wait time.
    pub fn get(&self) -> u32 {
        self.wait_ms.load(Ordering::Relaxed)
    }

    /// Decrement the current wait time.
    pub fn decay_tick(&self) {
        if self.max_wait_ms == 0 {
            // Global backoff not enabled
            return;
        }

        let _ = self
            .wait_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(self.decay_amount_ms))
            });

        metrics::gauge!("push.backoff.wait_ms").set(self.get() as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(max: u32, increment: u32, decay_amt: u32) -> Config {
        Config {
            backoff_max_wait_ms: max,
            backoff_increment_ms: increment,
            backoff_decay_interval_ms: 10,
            backoff_decay_amount_ms: decay_amt,
            ..Config::default()
        }
    }

    #[test]
    fn increase_respects_cap() {
        let b = Backoff::from_config(&cfg(100, 40, 10));

        b.increase();
        assert_eq!(b.get(), 40);

        b.increase();
        assert_eq!(b.get(), 80);

        b.increase();
        assert_eq!(b.get(), 100);

        b.increase();
        assert_eq!(b.get(), 100);
    }

    #[test]
    fn decay_tick_reduces_wait() {
        let b = Backoff::from_config(&cfg(100, 50, 15));

        b.increase();
        assert_eq!(b.get(), 50);

        b.decay_tick();
        assert_eq!(b.get(), 35);

        b.decay_tick();
        assert_eq!(b.get(), 20);

        b.decay_tick();
        assert_eq!(b.get(), 5);

        b.decay_tick();
        assert_eq!(b.get(), 0);
    }

    #[test]
    fn disabled_is_no_op() {
        let b = Backoff::from_config(&Config::default());
        assert!(!b.decays());

        b.increase();
        assert_eq!(b.get(), 0);

        b.decay_tick();
        assert_eq!(b.get(), 0);
    }

    #[test]
    fn concurrent_increase_and_decay_stays_bounded() {
        let b = std::sync::Arc::new(Backoff::from_config(&cfg(200, 5, 3)));
        let mut handles = vec![];

        for _ in 0..8 {
            let x = b.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    x.increase();
                }
            }));
        }

        for _ in 0..20 {
            b.decay_tick();
        }

        for h in handles {
            h.join().unwrap();
        }

        assert!(b.get() <= 200);
    }
}
