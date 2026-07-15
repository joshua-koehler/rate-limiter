//! Per-target active health state [P2.5].
//!
//! Each target carries a `HealthState` that the background prober
//! (`target::spawn_health`) drives. A target starts **healthy** (optimistic — a
//! freshly-booted gateway proxies immediately, before the first probe interval
//! elapses) and is marked **unhealthy** only after `unhealthy_threshold`
//! *consecutive* probe failures. Recovery is eager: a single successful probe
//! flips it healthy again and clears the failure count (DECISIONS.md).
//!
//! **Passive ejection.** The request path also feeds *live-traffic* outcomes in
//! (`record_live_failure`/`record_live_success`) so a target that dies mid-
//! interval is ejected before its next scheduled probe — the `interval` gap is
//! not a blind spot. This works **whether or not a circuit breaker is
//! configured** (an earlier design routed passive ejection only through the
//! breaker, so a `health_check`-only route — like the spec's `/api/products` —
//! kept sending traffic to a dead target for a full interval). Passive signals
//! can only **eject**; **recovery is authoritative via the active prober**,
//! which is why passive ejection is only enabled when a `health_check` exists to
//! probe the target back in.
//!
//! Concurrency: `healthy` is an `AtomicBool`; the probe streak is written only by
//! the single prober task, while the passive streak is written by many request
//! threads via atomic `fetch_add` (a lock-free RMW, so concurrent live failures
//! don't lose updates). The two streaks are kept separate so probe and live
//! signals never conflate.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Liveness of a single target, maintained by the active prober and live traffic.
pub struct HealthState {
    /// `true` until either the prober sees `unhealthy_threshold` consecutive
    /// probe failures or live traffic sees `unhealthy_threshold` failures; back
    /// to `true` on the first successful **probe** (authoritative recovery).
    healthy: AtomicBool,
    /// Consecutive probe failures since the last probe success. Only the prober
    /// task mutates this, so a simple atomic (no CAS loop) is sufficient.
    consecutive_failures: AtomicU32,
    /// Consecutive live-traffic failures since the last live success. Written by
    /// many request threads → atomic RMW; separate from the probe streak so the
    /// two signals don't interfere.
    passive_failures: AtomicU32,
}

impl HealthState {
    /// A target is healthy at construction — we proxy to it until a probe run
    /// proves otherwise (optimistic default per DECISIONS.md).
    pub fn new() -> Self {
        HealthState {
            healthy: AtomicBool::new(true),
            consecutive_failures: AtomicU32::new(0),
            passive_failures: AtomicU32::new(0),
        }
    }

    /// Is this target currently in rotation (health-wise)? Read on the hot
    /// request path, so it is a single relaxed-ish atomic load.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Record a successful probe: recovery is immediate and **both** failure
    /// streaks reset (DECISIONS.md — "recover on first success"). The probe is the
    /// authoritative recovery signal, so it also clears the passive streak a
    /// recovered target may have accumulated before it was ejected.
    pub fn record_probe_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.passive_failures.store(0, Ordering::Release);
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

    /// Record a **live-traffic** success: clears the passive failure streak so an
    /// occasional blip doesn't accumulate toward ejection. Deliberately does *not*
    /// flip `healthy` back on — an ejected target receives no live traffic (it is
    /// skipped in selection), so recovery can only come from the active prober.
    pub fn record_live_success(&self) {
        self.passive_failures.store(0, Ordering::Release);
    }

    /// Record a **live-traffic** failure. After `threshold` consecutive live
    /// failures the target is ejected (passive ejection), closing the active-probe
    /// interval blind spot without needing a circuit breaker to be configured.
    pub fn record_live_failure(&self, threshold: u32) {
        let streak = self.passive_failures.fetch_add(1, Ordering::AcqRel) + 1;
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

    #[test]
    fn live_traffic_failures_eject_and_only_a_probe_recovers() {
        // Passive ejection with no breaker involved: `threshold` consecutive live
        // failures take the target out of rotation...
        let h = HealthState::new();
        h.record_live_failure(3);
        h.record_live_failure(3);
        assert!(h.is_healthy(), "2 live failures < threshold");
        h.record_live_failure(3);
        assert!(!h.is_healthy(), "3rd consecutive live failure ejects");
        // ...and a live success alone does NOT readmit (ejected targets get no
        // live traffic; recovery is the prober's job).
        h.record_live_success();
        assert!(!h.is_healthy(), "live success does not readmit an ejected target");
        h.record_probe_success();
        assert!(h.is_healthy(), "an active probe is the authoritative recovery");
    }

    #[test]
    fn a_live_success_clears_the_passive_streak() {
        let h = HealthState::new();
        h.record_live_failure(3);
        h.record_live_failure(3);
        h.record_live_success(); // streak back to 0
        h.record_live_failure(3);
        h.record_live_failure(3);
        assert!(h.is_healthy(), "streak restarted after the success, 2 < 3");
    }
}
