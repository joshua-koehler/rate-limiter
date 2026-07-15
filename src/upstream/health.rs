//! Per-target active health state [P2.5].
//!
//! Each target carries a `HealthState` that the background prober
//! (`target::spawn_health`) drives. A target starts **healthy** (optimistic — a
//! freshly-booted gateway proxies immediately, before the first probe interval
//! elapses) and is marked **unhealthy** only after `unhealthy_threshold`
//! *consecutive* probe failures. Recovery is eager: a single successful probe
//! flips it healthy again and clears the failure count (DECISIONS.md).
//!
//! The flag is a plain `AtomicBool` because it is written by exactly one prober
//! task and read by every request thread — no read-modify-write race to guard,
//! so no lock. The request path also feeds *passive* failures into the target's
//! circuit breaker (see `target.rs`), so a target that dies mid-interval is
//! ejected by its breaker before the next scheduled probe even runs.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Liveness of a single target, maintained by the active prober.
pub struct HealthState {
    /// `true` until the prober has seen `unhealthy_threshold` consecutive
    /// failures; back to `true` on the first subsequent success.
    healthy: AtomicBool,
    /// Consecutive probe failures since the last success. Only the prober task
    /// mutates this, so a simple atomic (no CAS loop) is sufficient.
    consecutive_failures: AtomicU32,
}

impl HealthState {
    /// A target is healthy at construction — we proxy to it until a probe run
    /// proves otherwise (optimistic default per DECISIONS.md).
    pub fn new() -> Self {
        HealthState {
            healthy: AtomicBool::new(true),
            consecutive_failures: AtomicU32::new(0),
        }
    }

    /// Is this target currently in rotation (health-wise)? Read on the hot
    /// request path, so it is a single relaxed-ish atomic load.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Record a successful probe: recovery is immediate and the failure streak
    /// resets (DECISIONS.md — "recover on first success").
    pub fn record_probe_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.healthy.store(true, Ordering::Release);
    }

    /// Record a failed probe. Once the consecutive-failure streak reaches
    /// `threshold`, the target is marked unhealthy and drops out of rotation.
    pub fn record_probe_failure(&self, threshold: u32) {
        let streak = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if streak >= threshold.max(1) {
            self.healthy.store(false, Ordering::Release);
        }
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_healthy() {
        assert!(HealthState::new().is_healthy());
    }

    #[test]
    fn marks_unhealthy_only_after_threshold_consecutive_failures() {
        let h = HealthState::new();
        h.record_probe_failure(3);
        assert!(h.is_healthy(), "1 failure < threshold");
        h.record_probe_failure(3);
        assert!(h.is_healthy(), "2 failures < threshold");
        h.record_probe_failure(3);
        assert!(!h.is_healthy(), "3rd consecutive failure ejects the target");
    }

    #[test]
    fn a_success_resets_the_streak() {
        let h = HealthState::new();
        h.record_probe_failure(3);
        h.record_probe_failure(3);
        h.record_probe_success(); // streak back to 0
        assert!(h.is_healthy());
        h.record_probe_failure(3);
        h.record_probe_failure(3);
        assert!(h.is_healthy(), "streak restarted after the success, 2 < 3");
    }

    #[test]
    fn recovers_on_first_success_after_ejection() {
        let h = HealthState::new();
        h.record_probe_failure(2);
        h.record_probe_failure(2);
        assert!(!h.is_healthy());
        h.record_probe_success();
        assert!(h.is_healthy(), "one good probe brings it back");
    }
}
