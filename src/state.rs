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
use crate::upstream::UpstreamRegistry;

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
    /// P2 resilience runtime, keyed by route index: per-target circuit breakers,
    /// active-health flags, and load-balancer cursors, plus the target pool the
    /// upstream call selects from. All in-memory and concurrency-safe (atomics /
    /// briefly-locked `Mutex`), read by `upstream::proxy` via `ctx.state`. The
    /// three "seam" registries the earlier tiers reserved (breaker / balancer /
    /// health) are unified here because they all revolve around per-target state
    /// and the terminal call — one registry, indexed like the rate limiter.
    pub upstreams: Arc<UpstreamRegistry>,
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
        // Build the per-route upstream runtime (breakers/health/balancers +
        // target pools) from `&config`, before `config` is moved into its Arc.
        let upstreams = Arc::new(UpstreamRegistry::build(&config));
        let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
        // Start the active health probers (one per route with `health_check`).
        // They need the HTTP client to reach targets — clone it in, mirroring how
        // the sweeper is spawned. No-op when no route configures health checks.
        upstreams.clone().spawn_health(client.clone());
        AppState {
            config: Arc::new(config),
            router: Arc::new(router),
            client,
            start: Instant::now(),
            stages,
            rate_limiters,
            upstreams,
        }
    }
}
