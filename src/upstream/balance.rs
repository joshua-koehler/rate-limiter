//! Load-balancing target ordering [P2.4].
//!
//! The balancer's sole job is to produce a **preference ordering** of a route's
//! targets for one request. Eligibility filtering (health + circuit breaker) is
//! layered on top by `RouteUpstream::select`, which walks this ordering and
//! takes the first target that is actually usable — so the balancer never has to
//! know about health or breakers, and skipping an ineligible target degrades
//! gracefully to the next in preference order.
//!
//! Two strategies (DECISIONS.md):
//!   * `round_robin` — a shared atomic cursor rotated once per request. **Weight
//!     is ignored.** Request *k* prefers target `k % n`, so a steady stream is
//!     split evenly across all targets.
//!   * `weighted_round_robin` — the classic **smooth weighted round-robin**
//!     (nginx SWRR): each request adds every target's weight to its running
//!     "current weight", the highest wins and has the total weight subtracted
//!     back off. Over `sum(weights)` requests each target is chosen exactly
//!     `weight` times, and — unlike naive WRR — the picks are *interleaved*
//!     rather than bursted (3:1 → A B A C A B A C…, not A A A B). The chosen
//!     target leads the preference order; the rest follow in index order so a
//!     retry/failover still has somewhere to go.
//!
//! All state is either an atomic (round-robin) or a briefly-locked `Mutex`
//! (SWRR's current-weight vector), matching the project's "no map-wide lock"
//! concurrency bar. `Single` is the degenerate one-target upstream — no state,
//! no balancing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Per-route balancing state.
pub enum Balancer {
    /// One target (single-`url` upstream, or a one-entry `targets`): always
    /// target 0.
    Single,
    /// Round-robin over all targets via a shared atomic cursor; weight ignored.
    RoundRobin { cursor: AtomicUsize },
    /// Smooth weighted round-robin. `current` holds each target's running
    /// current-weight; `weights` is the fixed configured weight per target.
    Weighted {
        weights: Vec<i64>,
        current: Mutex<Vec<i64>>,
    },
}

impl Balancer {
    /// Round-robin over `n` targets.
    pub fn round_robin() -> Self {
        Balancer::RoundRobin {
            cursor: AtomicUsize::new(0),
        }
    }

    /// Smooth weighted round-robin over the given per-target weights.
    pub fn weighted(weights: &[u32]) -> Self {
        let weights: Vec<i64> = weights.iter().map(|w| *w as i64).collect();
        let current = vec![0i64; weights.len()];
        Balancer::Weighted {
            weights,
            current: Mutex::new(current),
        }
    }

    /// Produce the preference ordering (a permutation of `0..n`) for one
    /// request. Advances the balancer's internal state exactly once per call, so
    /// callers must invoke it a single time per client request (retries reuse the
    /// returned ordering rather than re-selecting).
    pub fn preference_order(&self, n: usize) -> Vec<usize> {
        match self {
            Balancer::Single => vec![0],
            Balancer::RoundRobin { cursor } => {
                // fetch_add wraps astronomically far out; the modulo keeps it in
                // range and the rotation is what produces the even split.
                let start = cursor.fetch_add(1, Ordering::Relaxed) % n;
                (0..n).map(|i| (start + i) % n).collect()
            }
            Balancer::Weighted { weights, current } => {
                let primary = self.swrr_pick(weights, current);
                // Primary leads; the remaining targets follow in index order so a
                // failover retry has a well-defined next choice.
                let mut order = Vec::with_capacity(n);
                order.push(primary);
                order.extend((0..n).filter(|&i| i != primary));
                order
            }
        }
    }

    /// One smooth-weighted-round-robin step. Mutates `current` under its lock and
    /// returns the index of the selected target.
    fn swrr_pick(&self, weights: &[i64], current: &Mutex<Vec<i64>>) -> usize {
        let total: i64 = weights.iter().sum();
        let mut cur = current.lock().unwrap();
        let mut best = 0usize;
        let mut best_weight = i64::MIN;
        for (i, w) in weights.iter().enumerate() {
            cur[i] += *w;
            if cur[i] > best_weight {
                best_weight = cur[i];
                best = i;
            }
        }
        cur[best] -= total;
        best
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn round_robin_rotates_and_covers_evenly() {
        let b = Balancer::round_robin();
        // First choice of each of the first 3 requests over 3 targets: 0,1,2.
        assert_eq!(b.preference_order(3)[0], 0);
        assert_eq!(b.preference_order(3)[0], 1);
        assert_eq!(b.preference_order(3)[0], 2);
        assert_eq!(b.preference_order(3)[0], 0);
        // Each ordering is a full permutation (failover always has a next hop).
        let mut order = b.preference_order(3);
        order.sort();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn weighted_splits_roughly_by_weight_and_interleaves() {
        // 3:1 over two targets: over 4 requests A wins 3, B wins 1.
        let b = Balancer::weighted(&[3, 1]);
        let mut counts: HashMap<usize, usize> = HashMap::new();
        let mut seq = Vec::new();
        for _ in 0..400 {
            let primary = b.preference_order(2)[0];
            *counts.entry(primary).or_default() += 1;
            seq.push(primary);
        }
        assert_eq!(counts[&0], 300, "weight-3 target wins 3/4");
        assert_eq!(counts[&1], 100, "weight-1 target wins 1/4");
        // Smoothness: the sole weight-1 pick is not bursted — within the first
        // 4-request cycle target 1 appears exactly once, interleaved.
        assert_eq!(seq[..4].iter().filter(|&&t| t == 1).count(), 1);
    }

    #[test]
    fn single_always_zero() {
        let b = Balancer::Single;
        assert_eq!(b.preference_order(1), vec![0]);
    }
}
