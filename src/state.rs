//! Shared application state: cheaply cloneable, `Arc`-backed, handed to every
//! connection.

use std::sync::Arc;
use std::time::Instant;

use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::config::Config;
use crate::error::BoxBody;
use crate::pipeline::{self, Stage};
use crate::rate_limit::RateLimiterRegistry;
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
    /// Per-route pipeline chains, indexed by route index (parallel to
    /// `config.routes`). Assembled once at startup from the config; iterated
    /// per request by the pipeline. `Arc<dyn Stage>` entries are shared, so a
    /// clone of `AppState` shares the same stages.
    pub stages: Arc<Vec<Vec<Arc<dyn Stage>>>>,
    /// P1 rate-limit counters, keyed by route index (per: ip → sharded map,
    /// per: global → one bucket). Shared across every connection so limits are
    /// enforced process-wide; the `RateLimitStage` reads it via `ctx.state`.
    pub rate_limiters: Arc<RateLimiterRegistry>,
    // ── Seam for later tiers ────────────────────────────────────────────────
    // Per-route runtime policy *state* goes here, keyed by route index, all
    // in-memory and concurrency-safe (atomics / Mutex / sharded map). Stages
    // above are stateless policy logic; the mutable counters they touch live
    // here so a stage can read them via `ctx.state`:
    //   pub breakers:      Arc<BreakerRegistry>,       // P2  circuit breaker
    //   pub balancers:     Arc<BalancerRegistry>,      // P2  round-robin cursor
    //   pub health:        Arc<HealthRegistry>,        // P2  active checks
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let router = Router::build(&config);
        let stages = Arc::new(pipeline::assemble(&config));
        // Build the registry from `&config` *before* `config` is moved into its
        // Arc below. The registry owns its counters (no borrow of `config`).
        let rate_limiters = Arc::new(RateLimiterRegistry::build(&config));
        // Start the background idle-key sweeper. `AppState::new` runs under
        // `#[tokio::main]`, so a Tokio runtime is active and `tokio::spawn`
        // works; the call is a no-op when no route is rate-limited.
        rate_limiters.clone().spawn_sweeper();
        let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
        AppState {
            config: Arc::new(config),
            router: Arc::new(router),
            client,
            start: Instant::now(),
            stages,
            rate_limiters,
        }
    }
}
