//! Lightweight in-process metrics and slow-command logging.
//!
//! Equivalent to the Java `ClientBalancerStatLog` + `RedisLog.slowLog`: cheap
//! atomic counters plus a `tracing` warning when a command exceeds the slow
//! threshold. No external metrics backend is assumed.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// A set of counters for one logical resource (a `host:port` or an HA group).
#[derive(Debug, Default)]
pub struct Stats {
    /// Total commands attempted.
    total: AtomicU64,
    /// Commands that ultimately failed (after retries).
    errors: AtomicU64,
    /// Commands slower than the slow threshold.
    slow: AtomicU64,
    /// Times the pool refused a borrow because it was unhealthy/paused.
    unavailable: AtomicU64,
    /// Accumulated command latency, in microseconds, for averaging.
    total_micros: AtomicU64,
}

impl Stats {
    /// A fresh, zeroed counter set.
    pub fn new() -> Self {
        Stats::default()
    }

    /// Record a completed command: its latency, whether it errored, and whether
    /// it crossed `slow_threshold` (emitting a slow-log warning if so).
    pub fn record(&self, name: &str, elapsed: Duration, is_error: bool, slow_threshold: Duration) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.total_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        if is_error {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        if elapsed >= slow_threshold {
            self.slow.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                target: "breeze_redis::slowlog",
                command = name,
                elapsed_ms = elapsed.as_millis() as u64,
                "slow redis command"
            );
        }
    }

    /// Record that the pool had no healthy endpoint to serve a request.
    pub fn record_unavailable(&self) {
        self.unavailable.fetch_add(1, Ordering::Relaxed);
    }

    /// A point-in-time snapshot of the counters.
    pub fn snapshot(&self) -> StatsSnapshot {
        let total = self.total.load(Ordering::Relaxed);
        let total_micros = self.total_micros.load(Ordering::Relaxed);
        StatsSnapshot {
            total,
            errors: self.errors.load(Ordering::Relaxed),
            slow: self.slow.load(Ordering::Relaxed),
            unavailable: self.unavailable.load(Ordering::Relaxed),
            avg_micros: total_micros.checked_div(total).unwrap_or(0),
        }
    }
}

/// An immutable copy of a [`Stats`] set for reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total commands attempted.
    pub total: u64,
    /// Commands that ultimately failed.
    pub errors: u64,
    /// Commands slower than the slow threshold.
    pub slow: u64,
    /// Borrow attempts refused by an unhealthy/paused pool.
    pub unavailable: u64,
    /// Mean command latency in microseconds.
    pub avg_micros: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_averages() {
        let stats = Stats::new();
        let slow = Duration::from_millis(50);
        stats.record("get", Duration::from_micros(100), false, slow);
        stats.record("get", Duration::from_micros(300), true, slow);
        stats.record("get", Duration::from_millis(80), false, slow);
        let snap = stats.snapshot();
        assert_eq!(snap.total, 3);
        assert_eq!(snap.errors, 1);
        assert_eq!(snap.slow, 1);
        assert_eq!(snap.avg_micros, (100 + 300 + 80_000) / 3);
    }
}
