//! Per-target circuit breaker [P2.3].
//!
//! A breaker fronts a single upstream target and short-circuits traffic to it
//! once it looks unhealthy, giving the target room to recover instead of being
//! hammered while it is failing. State is guarded by a `Mutex` held only for the
//! brief check-and-mutate — the same "one short critical section" pattern the
//! rate limiter uses, so decisions stay race-free without serializing throughput
//! across *different* targets (each target owns its own breaker + lock).
//!
//! State machine (DECISIONS.md — per-target, single-probe half-open):
//!   * **Closed** — normal. Count failures (5xx / timeout / connection error)
//!     within a rolling `window`; at `threshold` failures the breaker trips to
//!     **Open** and records the trip time.
//!   * **Open** — reject every request (503) until `cooldown` elapses since the
//!     trip, reporting the seconds remaining; then the next request is allowed
//!     through as a single trial (→ **Half-Open**).
//!   * **Half-Open** — exactly ONE trial request is in flight; every other
//!     request is rejected. Trial success → **Closed** (reset); trial failure →
//!     **Open** again (cooldown restarts).
//!
//! Rolling-window approximation: rather than a timestamp log, we reset the
//! failure counter when `window` elapses since the *first* counted failure of
//! the current run. This is the same cheap, O(1)-per-target approximation the
//! rate limiter's fixed window uses; it can under-count a burst that straddles a
//! window boundary, which only ever makes the breaker *more* lenient (it never
//! trips spuriously). Timing is `std::time::Instant` (monotonic).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::config::CircuitBreaker;

/// Resolved breaker policy (owned copy of the config numbers).
#[derive(Clone, Copy)]
struct CircuitConfig {
    threshold: u32,
    window: Duration,
    cooldown: Duration,
}

impl CircuitConfig {
    fn from(cb: &CircuitBreaker) -> Self {
        CircuitConfig {
            // A threshold of 0 would trip before any failure; clamp to 1 so the
            // breaker is always meaningful (config validation doesn't forbid 0).
            threshold: cb.threshold.max(1),
            window: cb.window,
            cooldown: cb.cooldown,
        }
    }
}

/// Internal breaker state. `Half-Open` carries no data: it exists only while a
/// single trial request is in flight, so the mere fact of being in this state
/// means "a probe is out, reject everyone else".
enum State {
    Closed { failures: u32, window_start: Instant },
    Open { opened_at: Instant },
    HalfOpen,
}

/// The disposition a breaker returns for an incoming request.
#[derive(Debug, PartialEq, Eq)]
pub enum Allow {
    /// Closed — proceed normally.
    Permit,
    /// Half-open trial slot granted — proceed, but this is the single probe that
    /// decides whether the breaker closes or re-opens.
    PermitProbe,
    /// Open (or half-open with a probe already out) — do not contact upstream.
    /// `retry_after` is whole seconds until the cooldown is expected to elapse.
    Reject { retry_after: u64 },
}

/// A per-target circuit breaker. When the route has no `circuit_breaker` config,
/// `config` is `None` and the breaker is inert: it always permits and records
/// nothing, so a single code path serves configured and unconfigured targets.
pub struct Breaker {
    config: Option<CircuitConfig>,
    state: Mutex<State>,
}

impl Breaker {
    /// Build a breaker for a target. `None` yields an inert breaker.
    pub fn new(cb: Option<&CircuitBreaker>) -> Self {
        Breaker {
            config: cb.map(CircuitConfig::from),
            state: Mutex::new(State::Closed {
                failures: 0,
                window_start: Instant::now(),
            }),
        }
    }

    /// Decide whether a request may proceed, transitioning Open→Half-Open when
    /// the cooldown has elapsed (and claiming the single probe slot as it does).
    /// The transition side effect is why callers must only invoke this for a
    /// target they are about to use — see `RouteUpstream::select` in `target.rs`.
    pub fn allow(&self, now: Instant) -> Allow {
        let Some(cfg) = &self.config else {
            return Allow::Permit;
        };
        let mut state = self.state.lock().unwrap();
        match &*state {
            State::Closed { .. } => Allow::Permit,
            State::Open { opened_at } => {
                let elapsed = now.saturating_duration_since(*opened_at);
                if elapsed >= cfg.cooldown {
                    // Cooldown elapsed: claim the single trial slot.
                    *state = State::HalfOpen;
                    Allow::PermitProbe
                } else {
                    Allow::Reject {
                        retry_after: ceil_secs(cfg.cooldown - elapsed).max(1),
                    }
                }
            }
            // A probe is already out; hold everyone else off briefly.
            State::HalfOpen => Allow::Reject { retry_after: 1 },
        }
    }

    /// Record a successful outcome against this target.
    ///
    /// A success in **Half-Open** is the trial passing → close the breaker with a
    /// clean window. A success in **Closed** does **not** clear the rolling
    /// failure count: the spec counts failures *within `window`* toward the
    /// threshold, so an upstream that is only *partially* failing (e.g.
    /// 503,200,503,200…) must still accrue toward tripping — zeroing on every 2xx
    /// would let a steadily-degrading upstream never trip, defeating the exact
    /// case the breaker exists for. Failures instead age out by time (see
    /// [`record_failure`]'s window check). `Open` never sees a success (we don't
    /// call upstream while Open), but is handled defensively.
    pub fn record_success(&self, now: Instant) {
        if self.config.is_none() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if matches!(&*state, State::HalfOpen | State::Open { .. }) {
            *state = State::Closed {
                failures: 0,
                window_start: now,
            };
        }
        // Closed: leave the rolling window untouched — a success is not evidence
        // that earlier in-window failures didn't happen.
    }

    /// Record a failed outcome (5xx / timeout / connection error) against this
    /// target. In Closed this advances the rolling window count and may trip the
    /// breaker; in Half-Open the probe failed, so re-open with a fresh cooldown.
    pub fn record_failure(&self, now: Instant) {
        let Some(cfg) = &self.config else {
            return;
        };
        let mut state = self.state.lock().unwrap();
        match &mut *state {
            State::Closed {
                failures,
                window_start,
            } => {
                if now.saturating_duration_since(*window_start) >= cfg.window {
                    // Rolling window elapsed since the first counted failure:
                    // start a fresh window with this failure as its first.
                    *failures = 1;
                    *window_start = now;
                } else {
                    *failures += 1;
                }
                if *failures >= cfg.threshold {
                    *state = State::Open { opened_at: now };
                }
            }
            State::HalfOpen => {
                // The trial request failed — back to Open, cooldown restarts.
                *state = State::Open { opened_at: now };
            }
            // Already Open: a straggler failure changes nothing (the cooldown is
            // already running from the trip).
            State::Open { .. } => {}
        }
    }

    /// Test hook: is the breaker currently Closed (fully healthy)?
    #[cfg(test)]
    fn is_closed(&self) -> bool {
        matches!(&*self.state.lock().unwrap(), State::Closed { .. })
    }
}

/// Whole seconds rounded up, so a reported `retry_after` never tells a client to
/// come back before the cooldown has actually elapsed.
fn ceil_secs(d: Duration) -> u64 {
    let secs = d.as_secs();
    if d.subsec_nanos() > 0 {
        secs + 1
    } else {
        secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cb(threshold: u32, window_secs: u64, cooldown_secs: u64) -> CircuitBreaker {
        CircuitBreaker {
            threshold,
            window: Duration::from_secs(window_secs),
            cooldown: Duration::from_secs(cooldown_secs),
        }
    }

    #[test]
    fn inert_breaker_always_permits() {
        let b = Breaker::new(None);
        let now = Instant::now();
        b.record_failure(now);
        b.record_failure(now);
        assert_eq!(b.allow(now), Allow::Permit, "no config -> never trips");
    }

    #[test]
    fn trips_open_at_threshold_then_rejects() {
        let b = Breaker::new(Some(&cb(3, 60, 30)));
        let t0 = Instant::now();
        assert_eq!(b.allow(t0), Allow::Permit);
        b.record_failure(t0);
        b.record_failure(t0);
        assert_eq!(b.allow(t0), Allow::Permit, "2 < threshold, still closed");
        b.record_failure(t0); // 3rd failure trips
        match b.allow(t0) {
            Allow::Reject { retry_after } => assert_eq!(retry_after, 30),
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    #[test]
    fn reject_retry_after_decreases_toward_cooldown() {
        let b = Breaker::new(Some(&cb(1, 60, 30)));
        let t0 = Instant::now();
        b.record_failure(t0); // trips immediately (threshold 1)
        match b.allow(t0 + Duration::from_secs(10)) {
            Allow::Reject { retry_after } => assert_eq!(retry_after, 20),
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    #[test]
    fn half_open_probe_success_closes() {
        let b = Breaker::new(Some(&cb(1, 60, 30)));
        let t0 = Instant::now();
        b.record_failure(t0); // Open
        let t1 = t0 + Duration::from_secs(31); // past cooldown
        assert_eq!(b.allow(t1), Allow::PermitProbe, "cooldown elapsed -> probe");
        assert_eq!(
            b.allow(t1),
            Allow::Reject { retry_after: 1 },
            "second concurrent request rejected while probe is out"
        );
        b.record_success(t1); // probe succeeded
        assert!(b.is_closed());
        assert_eq!(b.allow(t1), Allow::Permit);
    }

    #[test]
    fn half_open_probe_failure_reopens_with_fresh_cooldown() {
        let b = Breaker::new(Some(&cb(1, 60, 30)));
        let t0 = Instant::now();
        b.record_failure(t0); // Open
        let t1 = t0 + Duration::from_secs(31);
        assert_eq!(b.allow(t1), Allow::PermitProbe);
        b.record_failure(t1); // probe failed -> reopen at t1
        match b.allow(t1 + Duration::from_secs(5)) {
            Allow::Reject { retry_after } => assert_eq!(retry_after, 25, "cooldown restarted"),
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    #[test]
    fn interleaved_successes_do_not_reset_the_window_count() {
        // The degrading-upstream case: 503,200,503,200,503 within the window must
        // still trip a threshold-3 breaker — a 2xx between failures is not proof
        // the earlier in-window failures didn't happen. (Regression guard: a prior
        // impl zeroed the count on every success and never tripped here.)
        let b = Breaker::new(Some(&cb(3, 60, 30)));
        let t0 = Instant::now();
        b.record_failure(t0); // 1
        b.record_success(t0); // interleaved 2xx — must NOT reset the count
        b.record_failure(t0); // 2
        b.record_success(t0);
        assert_eq!(b.allow(t0), Allow::Permit, "2 < threshold, still closed");
        b.record_failure(t0); // 3 → trips despite the interleaved successes
        assert!(
            matches!(b.allow(t0), Allow::Reject { .. }),
            "3 in-window failures trip even with successes between them"
        );
    }

    #[test]
    fn rolling_window_resets_failure_count() {
        let b = Breaker::new(Some(&cb(3, 10, 30)));
        let t0 = Instant::now();
        b.record_failure(t0);
        b.record_failure(t0); // 2 failures in the window
        // Window elapses before the 3rd failure: counter restarts, no trip.
        let t1 = t0 + Duration::from_secs(11);
        b.record_failure(t1);
        assert_eq!(b.allow(t1), Allow::Permit, "count reset across the window");
    }
}
