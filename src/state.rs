//! Shared application state: cheaply cloneable, `Arc`-backed, handed to every
//! connection.

use std::sync::Arc;
use std::time::Instant;

use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::config::Config;
use crate::error::BoxBody;
use crate::router::Router;

/// Outbound HTTP client used to reach upstreams. The legacy `hyper-util` client
/// is a connection-pooling wrapper over hyper's low-level client — not a proxy
/// framework. It's internally `Arc`-based, so cloning `AppState` is cheap.
pub type HttpClient = Client<HttpConnector, BoxBody>;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub router: Arc<Router>,
    pub client: HttpClient,
    /// Process start, source of `/health`'s `uptime_seconds`.
    pub start: Instant,
    // ── Seam for later tiers ────────────────────────────────────────────────
    // Per-route runtime policy state goes here, keyed by route index, all
    // in-memory and concurrency-safe (atomics / Mutex / DashMap). Adding a
    // field here is how P1–P2 wire in without touching the pipeline plumbing:
    //   pub rate_limiters: Arc<RateLimiterRegistry>,  // P1  (per: ip|global)
    //   pub breakers:      Arc<BreakerRegistry>,       // P2  circuit breaker
    //   pub balancers:     Arc<BalancerRegistry>,      // P2  round-robin cursor
    //   pub health:        Arc<HealthRegistry>,        // P2  active checks
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let router = Router::build(&config);
        let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
        AppState {
            config: Arc::new(config),
            router: Arc::new(router),
            client,
            start: Instant::now(),
        }
    }
}
