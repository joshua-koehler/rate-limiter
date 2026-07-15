//! Per-route upstream runtime: the target pool plus the coherent core the whole
//! P2 resilience layer revolves around.
//!
//! The [`UpstreamRegistry`] is indexed 1:1 with `config.routes` (mirroring
//! `RateLimiterRegistry`) and owns, per route, a [`RouteUpstream`]: the list of
//! [`TargetRuntime`]s (each with its own circuit breaker + health state) and the
//! [`Balancer`] that orders them. A single-`url` upstream is modelled as a
//! one-target pool so health checks and breakers apply to it uniformly
//! (DECISIONS.md).
//!
//! **Eligibility & selection.** A target is *eligible* when it is health-check
//! healthy AND its breaker permits (Closed, or a granted half-open probe). The
//! request path asks the balancer for a preference ordering once, then walks it
//! via [`RouteUpstream::select`], taking the first eligible target and (on a
//! retry) advancing to the next. When nothing is eligible the exclusion reason
//! drives the error: any target held out by an **Open breaker** →
//! `CircuitOpen { retry_after }` (soonest cooldown remaining); otherwise every
//! exclusion was a **health** ejection → `AllTargetsUnhealthy`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hyper::{Method, Request};

use crate::config::{Balance, CircuitBreaker, Config};
use crate::error::full;
use crate::state::HttpClient;

use super::balance::Balancer;
use super::breaker::{Allow, Breaker};
use super::health::HealthState;

/// How long a single active health probe may run before it counts as a failed
/// probe. Short by design so one hung target can't stall the prober loop for the
/// other targets on the route.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// One upstream target: its base URL and weight plus the mutable resilience
/// state shared between the request path and the background prober (hence the
/// `Arc` wrapping wherever it is held).
pub struct TargetRuntime {
    /// Base URL as configured (e.g. `http://host:port`), trailing slash trimmed
    /// at request-build time.
    pub url: String,
    /// Static weight (round-robin ignores it; weighted RR honours it).
    pub weight: u32,
    /// Per-target circuit breaker (inert when the route has no `circuit_breaker`).
    pub breaker: Breaker,
    /// Per-target active-health flag driven by the prober.
    pub health: HealthState,
}

impl TargetRuntime {
    fn new(url: String, weight: u32, cb: Option<&CircuitBreaker>) -> Self {
        TargetRuntime {
            url,
            weight,
            breaker: Breaker::new(cb),
            health: HealthState::new(),
        }
    }
}

/// Active-probe parameters for a route (resolved from `upstream.health_check`).
struct HealthProbe {
    path: String,
    interval: Duration,
    unhealthy_threshold: u32,
}

/// The outcome of a target-selection scan.
pub enum Selection {
    /// An eligible target and the position it occupied in the preference order
    /// (so a retry can resume the scan *after* it — round-robin failover).
    Target {
        target: Arc<TargetRuntime>,
        order_pos: usize,
    },
    /// No eligible target. `open` distinguishes the two 503 flavours.
    Unavailable {
        /// At least one target was held out by an **Open** breaker.
        open: bool,
        /// Soonest breaker cooldown remaining (seconds), when `open`.
        retry_after: u64,
    },
}

/// Everything a route needs to pick and reach a target.
pub struct RouteUpstream {
    targets: Vec<Arc<TargetRuntime>>,
    balancer: Balancer,
    health_check: Option<HealthProbe>,
}

impl RouteUpstream {
    /// The balancer's preference ordering for one request — call once, then walk
    /// it across attempts (see `select`).
    pub fn preference_order(&self) -> Vec<usize> {
        self.balancer.preference_order(self.targets.len())
    }

    /// Scan `order` for the first eligible target, starting at position `start`
    /// and wrapping. Health-unhealthy targets are skipped silently; breaker-Open
    /// targets are skipped but recorded so the caller can map "all Open" to a
    /// circuit-open 503. A granted half-open probe counts as eligible (this is
    /// where the single trial request gets through).
    pub fn select(&self, order: &[usize], start: usize, now: Instant) -> Selection {
        let n = order.len();
        let mut saw_open = false;
        let mut min_retry = u64::MAX;
        for step in 0..n {
            let pos = (start + step) % n;
            let target = &self.targets[order[pos]];
            if !target.health.is_healthy() {
                continue;
            }
            match target.breaker.allow(now) {
                Allow::Permit | Allow::PermitProbe => {
                    return Selection::Target {
                        target: Arc::clone(target),
                        order_pos: pos,
                    };
                }
                Allow::Reject { retry_after } => {
                    saw_open = true;
                    min_retry = min_retry.min(retry_after);
                }
            }
        }
        Selection::Unavailable {
            open: saw_open,
            retry_after: if min_retry == u64::MAX { 0 } else { min_retry },
        }
    }
}

/// Per-route upstream table, indexed by `route_index` (aligned 1:1 with
/// `config.routes`) — the same shape as `RateLimiterRegistry`.
pub struct UpstreamRegistry {
    routes: Vec<RouteUpstream>,
}

impl UpstreamRegistry {
    /// Build the pool for every route from its parsed config. Runs before
    /// `config` is moved into its `Arc` (the registry owns copies of every URL /
    /// weight / policy number, so it borrows nothing afterwards).
    pub fn build(config: &Config) -> Self {
        let routes = config
            .routes
            .iter()
            .map(|route| {
                let up = &route.upstream;
                let cb = route.circuit_breaker.as_ref();

                // Single `url` → a one-target pool; `targets` → one runtime each.
                let targets: Vec<Arc<TargetRuntime>> = if let Some(url) = &up.url {
                    vec![Arc::new(TargetRuntime::new(url.clone(), 1, cb))]
                } else {
                    up.targets
                        .iter()
                        .map(|t| Arc::new(TargetRuntime::new(t.url.clone(), t.weight, cb)))
                        .collect()
                };

                // Resolve the balancer. Explicit `balance` wins; with multiple
                // targets and no `balance` we default to round-robin; a lone
                // target needs no balancing at all.
                let balancer = match up.balance {
                    Some(Balance::RoundRobin) => Balancer::round_robin(),
                    Some(Balance::WeightedRoundRobin) => {
                        let weights: Vec<u32> = up.targets.iter().map(|t| t.weight).collect();
                        Balancer::weighted(&weights)
                    }
                    None => {
                        if targets.len() > 1 {
                            Balancer::round_robin()
                        } else {
                            Balancer::Single
                        }
                    }
                };

                let health_check = up.health_check.as_ref().map(|hc| HealthProbe {
                    path: hc.path.clone(),
                    interval: hc.interval,
                    unhealthy_threshold: hc.unhealthy_threshold,
                });

                RouteUpstream {
                    targets,
                    balancer,
                    health_check,
                }
            })
            .collect();
        UpstreamRegistry { routes }
    }

    /// Borrow a route's upstream pool.
    pub fn route(&self, route_index: usize) -> &RouteUpstream {
        &self.routes[route_index]
    }

    /// Spawn one background active-health prober per route that configures
    /// `health_check`. Mirrors `RateLimiterRegistry::spawn_sweeper`: a no-op when
    /// no route has health checks, and must be called from within a Tokio runtime
    /// (it is — `AppState::new` runs under `#[tokio::main]`). Each prober owns
    /// `Arc` clones of its targets and the shared HTTP client, so it never blocks
    /// request handling.
    pub fn spawn_health(self: Arc<Self>, client: HttpClient) {
        for route in &self.routes {
            let Some(probe) = &route.health_check else {
                continue;
            };
            let targets = route.targets.clone();
            let path = probe.path.clone();
            let interval = probe.interval;
            let threshold = probe.unhealthy_threshold;
            let client = client.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                // `interval` fires immediately on the first tick; skip it so the
                // first probe is one interval out (targets start optimistically
                // healthy, so there's nothing to check at t=0).
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    for target in &targets {
                        if probe_once(&client, &target.url, &path).await {
                            target.health.record_probe_success();
                        } else {
                            target.health.record_probe_failure(threshold);
                        }
                    }
                }
            });
        }
    }
}

/// Issue one health probe (GET `base + path`) with a short timeout. A probe
/// "passes" only on a 2xx; any non-2xx status, transport error, or timeout is a
/// failure (DECISIONS.md).
async fn probe_once(client: &HttpClient, base: &str, path: &str) -> bool {
    let uri = format!("{}{}", base.trim_end_matches('/'), path);
    let req = match Request::builder()
        .method(Method::GET)
        .uri(&uri)
        .body(full(Bytes::new()))
    {
        Ok(r) => r,
        Err(_) => return false, // an unbuildable probe URI is treated as a failure
    };
    match tokio::time::timeout(PROBE_TIMEOUT, client.request(req)).await {
        Ok(Ok(resp)) => resp.status().is_success(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(cb: Option<&CircuitBreaker>) -> Arc<TargetRuntime> {
        Arc::new(TargetRuntime::new("http://t".to_string(), 1, cb))
    }

    fn route(targets: Vec<Arc<TargetRuntime>>) -> RouteUpstream {
        let n = targets.len();
        RouteUpstream {
            targets,
            balancer: if n > 1 {
                Balancer::round_robin()
            } else {
                Balancer::Single
            },
            health_check: None,
        }
    }

    #[test]
    fn selects_the_only_healthy_target() {
        let ru = route(vec![target(None)]);
        let order = ru.preference_order();
        let now = Instant::now();
        assert!(matches!(
            ru.select(&order, 0, now),
            Selection::Target { .. }
        ));
    }

    #[test]
    fn all_unhealthy_maps_to_targets_unhealthy_not_circuit_open() {
        let t = target(None);
        t.health.record_probe_failure(1); // eject via health
        let ru = route(vec![t]);
        let order = ru.preference_order();
        match ru.select(&order, 0, Instant::now()) {
            Selection::Unavailable { open, .. } => {
                assert!(!open, "health ejection -> AllTargetsUnhealthy, not CircuitOpen")
            }
            _ => panic!("expected Unavailable"),
        }
    }

    #[test]
    fn open_breaker_maps_to_circuit_open_with_retry_after() {
        let cb = CircuitBreaker {
            threshold: 1,
            window: Duration::from_secs(60),
            cooldown: Duration::from_secs(30),
        };
        let t = target(Some(&cb));
        let now = Instant::now();
        t.breaker.record_failure(now); // trips Open
        let ru = route(vec![t]);
        let order = ru.preference_order();
        match ru.select(&order, 0, now) {
            Selection::Unavailable { open, retry_after } => {
                assert!(open, "breaker Open -> CircuitOpen");
                assert_eq!(retry_after, 30);
            }
            _ => panic!("expected Unavailable"),
        }
    }

    #[test]
    fn failover_skips_the_open_target_for_the_healthy_one() {
        let cb = CircuitBreaker {
            threshold: 1,
            window: Duration::from_secs(60),
            cooldown: Duration::from_secs(30),
        };
        let bad = target(Some(&cb));
        let good = target(None);
        let now = Instant::now();
        bad.breaker.record_failure(now); // bad is Open
        // Order [bad, good]: selection must skip bad and land on good.
        let ru = RouteUpstream {
            targets: vec![Arc::clone(&bad), Arc::clone(&good)],
            balancer: Balancer::round_robin(),
            health_check: None,
        };
        match ru.select(&[0, 1], 0, now) {
            Selection::Target { target, .. } => {
                assert!(Arc::ptr_eq(&target, &good), "picked the healthy target")
            }
            _ => panic!("expected a Target"),
        }
    }
}
