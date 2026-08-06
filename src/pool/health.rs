//! The per-pool circuit breaker, ported from the Java `EndpointPoolImpl`
//! health state machine.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// What a caller should do after recording a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FailureOutcome {
    /// The consecutive-failure count crossed the min threshold: the current IP
    /// set may be stale, so an immediate DNS re-lookup is warranted.
    pub trigger_dns_relookup: bool,
    /// The count reached the max threshold: the breaker just tripped open.
    pub tripped: bool,
}

/// Tracks the health of a pool and gates whether it will serve requests.
///
/// Two independent switches:
/// - `keep_service`: an operator kill-switch (drain for maintenance).
/// - `healthy`: an automatic breaker driven by consecutive failures.
#[derive(Debug)]
pub struct HealthState {
    healthy: AtomicBool,
    keep_service: AtomicBool,
    continue_false_count: AtomicU32,
    min_threshold: u32,
    max_threshold: u32,
}

impl HealthState {
    /// Build a breaker with the given failure thresholds.
    pub fn new(min_threshold: u32, max_threshold: u32) -> Self {
        HealthState {
            healthy: AtomicBool::new(true),
            keep_service: AtomicBool::new(true),
            continue_false_count: AtomicU32::new(0),
            min_threshold: min_threshold.max(1),
            max_threshold: max_threshold.max(1),
        }
    }

    /// Whether the pool will currently serve borrow requests.
    pub fn can_serve(&self) -> bool {
        self.keep_service.load(Ordering::Acquire) && self.healthy.load(Ordering::Acquire)
    }

    /// Whether the automatic breaker considers the pool healthy.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Whether the operator kill-switch is engaged (service kept up).
    pub fn is_kept(&self) -> bool {
        self.keep_service.load(Ordering::Acquire)
    }

    /// Record a successful operation: clears the consecutive-failure count.
    pub fn on_success(&self) {
        self.continue_false_count.store(0, Ordering::Release);
    }

    /// Record a failed operation and report what follow-up action is warranted.
    pub fn on_failure(&self) -> FailureOutcome {
        let count = self.continue_false_count.fetch_add(1, Ordering::AcqRel) + 1;
        let mut outcome = FailureOutcome::default();
        if count >= self.max_threshold {
            // Trip open and reset so recovery starts from a clean slate.
            self.healthy.store(false, Ordering::Release);
            self.continue_false_count.store(0, Ordering::Release);
            outcome.tripped = true;
        } else if count >= self.min_threshold {
            outcome.trigger_dns_relookup = true;
        }
        outcome
    }

    /// Close the breaker after a successful recovery probe.
    pub fn recover(&self) {
        self.continue_false_count.store(0, Ordering::Release);
        self.healthy.store(true, Ordering::Release);
    }

    /// Operator: stop serving (drain for maintenance).
    pub fn pause(&self) {
        self.keep_service.store(false, Ordering::Release);
    }

    /// Operator: resume serving.
    pub fn restart(&self) {
        self.keep_service.store(true, Ordering::Release);
        self.recover();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trips_after_max_failures() {
        let health = HealthState::new(2, 4);
        assert!(health.can_serve());

        assert!(!health.on_failure().trigger_dns_relookup); // 1
        let out = health.on_failure(); // 2 -> min
        assert!(out.trigger_dns_relookup && !out.tripped);
        assert!(health.on_failure().trigger_dns_relookup); // 3
        let out = health.on_failure(); // 4 -> max, trip
        assert!(out.tripped);
        assert!(!health.can_serve());
    }

    #[test]
    fn success_resets_and_recover_closes() {
        let health = HealthState::new(2, 3);
        health.on_failure();
        health.on_success();
        // Count reset, so the next failure starts over and does not trip.
        assert!(!health.on_failure().tripped);

        let health = HealthState::new(1, 1);
        health.on_failure(); // trips at 1
        assert!(!health.is_healthy());
        health.recover();
        assert!(health.is_healthy());
    }

    #[test]
    fn operator_pause_and_restart() {
        let health = HealthState::new(2, 4);
        health.pause();
        assert!(!health.can_serve());
        assert!(!health.is_kept());
        health.restart();
        assert!(health.can_serve());
    }
}
