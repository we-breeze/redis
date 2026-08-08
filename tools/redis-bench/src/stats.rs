//! Latency and throughput accounting for a benchmark run.
//!
//! Each worker records into its own local HDR histogram and counts its own
//! errors. At the end the per-worker data is merged into one [`Summary`] for
//! percentile reporting. Using per-worker histograms avoids contention on the
//! hot path.
//!
//! [`MemoryWindow`] layers per-run heap accounting on top of the global
//! `brz-mem` counters (enabled under the `memory-stats` feature) so the report
//! can show allocations per request — the headline figure for spotting
//! allocation-driven hot paths.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hdrhistogram::Histogram;

/// A single worker's local histogram and error counter.
pub struct WorkerStats {
    hist: Histogram<u64>,
    errors: u64,
}

impl WorkerStats {
    pub fn new() -> Self {
        // Track from 1us up to ~60s at 3 significant figures.
        let hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        Self { hist, errors: 0 }
    }

    /// Record a successful operation's latency.
    pub fn record(&mut self, elapsed: Duration) {
        let micros = elapsed.as_micros().max(1) as u64;
        let _ = self.hist.record(micros);
    }

    pub fn record_error(&mut self) {
        self.errors += 1;
    }

    /// Merge this worker's data into the aggregate [`Summary`].
    pub fn add_to(&self, summary: &mut Summary) {
        let _ = summary.hist.add(&self.hist);
        summary.errors += self.errors;
    }
}

impl Default for WorkerStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate across all workers, used for the final report.
pub struct Summary {
    hist: Histogram<u64>,
    pub errors: u64,
}

impl Summary {
    pub fn new() -> Self {
        let hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        Self { hist, errors: 0 }
    }

    pub fn total(&self) -> u64 {
        self.hist.len()
    }

    pub fn min_us(&self) -> u64 {
        self.hist.min().max(1)
    }

    pub fn max_us(&self) -> u64 {
        self.hist.max()
    }

    pub fn mean_us(&self) -> f64 {
        self.hist.mean()
    }

    pub fn percentile_us(&self, p: f64) -> u64 {
        self.hist.value_at_quantile(p / 100.0)
    }
}

impl Default for Summary {
    fn default() -> Self {
        Self::new()
    }
}

/// A countdown of operations remaining in a fixed-count run.
///
/// Workers decrement until it hits zero; `try_claim` returns `false` to signal
/// "stop issuing". Used to bound a run to exactly `--ops` logical operations
/// across all workers.
pub struct OpBudget {
    remaining: AtomicU64,
}

impl OpBudget {
    pub fn new(count: u64) -> Self {
        Self {
            remaining: AtomicU64::new(count),
        }
    }

    /// Claim one operation slot. Returns `false` when the budget is exhausted.
    pub fn try_claim(&self) -> bool {
        loop {
            let cur = self.remaining.load(Ordering::Acquire);
            if cur == 0 {
                return false;
            }
            if self
                .remaining
                .compare_exchange(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }
}

/// A window of heap activity between two `brz_mem::heap()` snapshots, expressed
/// as totals and as per-request / per-million-request rates.
///
/// Bucket sizes are power-of-two (see `brz_mem::LayoutBuckets`), so byte
/// figures are upper bounds, not the exact bytes requested — they are the
/// allocator's bucket-rounded size. Object counts are exact.
pub struct MemoryWindow {
    enabled: bool,
    objects_per_request: u64,
    bytes_per_request: u64,
    allocated_objects: u64,
    allocated_bytes: u64,
    outstanding_objects_delta: i128,
    outstanding_bytes_delta: i128,
}

impl MemoryWindow {
    /// Build a window from a before/after heap snapshot over `requests` ops.
    pub fn between(
        before: Option<brz_mem::HeapStats>,
        after: Option<brz_mem::HeapStats>,
        requests: u64,
    ) -> Self {
        let (Some(before), Some(after)) = (before, after) else {
            return Self::disabled();
        };
        let allocated_objects = after.total_objects.saturating_sub(before.total_objects);
        let allocated_bytes = after.total.saturating_sub(before.total);
        Self {
            enabled: true,
            objects_per_request: per_request(allocated_objects, requests),
            bytes_per_request: per_request(allocated_bytes, requests),
            allocated_objects,
            allocated_bytes,
            outstanding_objects_delta: i128::from(after.used_objects)
                - i128::from(before.used_objects),
            outstanding_bytes_delta: i128::from(after.used) - i128::from(before.used),
        }
    }

    const fn disabled() -> Self {
        Self {
            enabled: false,
            objects_per_request: 0,
            bytes_per_request: 0,
            allocated_objects: 0,
            allocated_bytes: 0,
            outstanding_objects_delta: 0,
            outstanding_bytes_delta: 0,
        }
    }

    /// Print the memory section of the report table.
    pub fn print(&self) {
        if !self.enabled {
            println!("alloc:          (memory-stats feature disabled)");
            return;
        }
        println!(
            "alloc/op:       {} objects  {} bucket-bytes",
            self.objects_per_request, self.bytes_per_request,
        );
        println!(
            "alloc total:    {} objects  {} bucket-bytes  (Δoutstanding {} obj / {} bytes)",
            self.allocated_objects,
            self.allocated_bytes,
            self.outstanding_objects_delta,
            self.outstanding_bytes_delta,
        );
    }
}

fn per_request(value: u64, requests: u64) -> u64 {
    rate(value, 1, u128::from(requests))
}

fn rate(value: u64, scale: u128, divisor: u128) -> u64 {
    if divisor == 0 {
        return 0;
    }
    let result = u128::from(value).saturating_mul(scale) / divisor;
    u64::try_from(result).unwrap_or(u64::MAX)
}
