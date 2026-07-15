//! The request pipeline: a **pluggable, per-route chain of stages**.
//!
//! This is the load-bearing extensibility decision (criteria.md: "another
//! engineer could extend with a new config feature in an afternoon"). Instead of
//! a hard-coded sequence of `if` blocks, the pipeline is an ordered list of
//! [`Stage`] trait objects, **assembled per route from the parsed config** at
//! startup ([`assemble`]) and iterated in order. Adding a config feature is:
//!   1. add the config struct (in `config`),
//!   2. add one file implementing [`Stage`] (in this module),
//!   3. `push` it in [`assemble`] under the right condition — no change to the
//!      core loop below.
//!
//! Intended full order (later tiers slot stages into `assemble` at the seams):
//!   route match (selects the chain)  → method → auth → rate limit
//!   → request transform → circuit-breaker gate → target select
//!   → timeout+retry around the upstream call → response transform → return
//!
//! Fast-reject stages (404 pre-chain, 405/401/429/503…) short-circuit *before*
//! any upstream work by returning [`Flow::ShortCircuit`].

mod auth;
mod method;
mod request_transform;
mod response_transform;
mod transform;

// Re-exported for the upstream module: request *body* mapping runs at the body
// buffer boundary in `upstream::proxy` (the retry loop already buffers the body),
// not as a pipeline Stage — see `assemble` and DECISIONS.md.
pub use request_transform::apply_body_mapping;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};

use crate::config::Config;
use crate::error::{BoxBody, GatewayError};
use crate::rate_limit::{effective_rate_limit, RateLimitStage};
use crate::state::AppState;
use crate::{health, upstream};

use auth::AuthStage;
use method::MethodStage;

/// Outcome of a stage: either let the request proceed, or terminate it now with
/// a fully-formed response (a fast rejection or a short-circuit answer).
pub enum Flow {
    Continue,
    ShortCircuit(Response<BoxBody>),
}

/// A composable pipeline step. Each stage inspects/mutates the shared
/// [`RequestCtx`] and returns a [`Flow`]. Stages are held as `Arc<dyn Stage>`,
/// so they must be `Send + Sync`; `#[async_trait]` boxes the returned future so
/// the trait stays object-safe.
#[async_trait]
pub trait Stage: Send + Sync {
    async fn apply(&self, ctx: &mut RequestCtx) -> Flow;
}

/// Mutable per-request context threaded through the stage chain and consumed by
/// the terminal upstream call. Stages read the request head, may mutate the
/// (later) buffered body/headers, and can reach shared state via `state`.
pub struct RequestCtx {
    /// Cheaply-cloned shared state (config, client, per-route runtime handles).
    pub state: AppState,
    /// Index into `state.config.routes` chosen by the router.
    pub route_index: usize,
    /// Path remainder after the matched prefix (consumed by P1 `strip_prefix`).
    pub tail: String,
    /// Socket peer, captured at accept — the `per: ip` rate-limit key (P1). We
    /// trust the socket peer, not `X-Forwarded-For`.
    #[allow(dead_code)]
    pub client_addr: SocketAddr,
    /// The in-flight request; the terminal upstream call takes its body.
    pub req: Request<Incoming>,
}

impl RequestCtx {
    /// The matched route's configured path (used for logging).
    fn route_path(&self) -> &str {
        &self.state.config.routes[self.route_index].path
    }
}

/// A composable **post-upstream** step: transforms the upstream response before
/// it returns to the client. The response-phase analogue of [`Stage`]. Registering
/// a P3 response transform is "add a struct + impl this + push in
/// [`assemble_response`]" — symmetric with the request phase, so the pipeline
/// extends the same way in *both* directions with no change to [`handle`].
#[async_trait]
pub trait ResponseStage: Send + Sync {
    async fn apply(&self, ctx: &mut ResponseCtx);
}

/// Per-response context threaded through the response-transform chain. Stages
/// mutate `resp` in place (headers, body envelope). Built only for real upstream
/// responses — gateway-generated errors (404/401/429/503/504…) skip this phase
/// (DECISIONS.md: envelope/transform applies only to genuine upstream responses).
/// `state`/`route_index` are the seam's surface for P3 (e.g. `$route_path`,
/// `response_transform` config); unused until then.
#[allow(dead_code)]
pub struct ResponseCtx {
    pub state: AppState,
    pub route_index: usize,
    pub resp: Response<BoxBody>,
}

impl ResponseCtx {
    /// Matched route path — the `$route_path` placeholder source for envelopes.
    #[allow(dead_code)]
    pub fn route_path(&self) -> &str {
        &self.state.config.routes[self.route_index].path
    }
}

/// Entry point invoked per request by the server's connection service.
///
/// Health and routing run *ahead* of the per-route chain: health can never be
/// shadowed by config, and routing is what selects which chain to run.
pub async fn handle(
    state: AppState,
    client_addr: SocketAddr,
    req: Request<Incoming>,
) -> Response<BoxBody> {
    let start = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // /health — always available, never routed/authed/rate-limited. GET only
    // (spec: `GET /health`); other methods get 405 rather than a health body.
    if path == "/health" {
        let resp = if method == Method::GET {
            health::health_response(&state)
        } else {
            GatewayError::method_not_allowed(&["GET".to_string()]).into_response()
        };
        access_log(&method, &path, "-", resp.status(), start);
        return resp;
    }

    // Route match: longest segment-boundary prefix; no match -> 404.
    let Some(m) = state.router.match_route(&path) else {
        let resp = GatewayError::NotFound.into_response();
        access_log(&method, &path, "-", resp.status(), start);
        return resp;
    };
    let route_index = m.route_index;

    let mut ctx = RequestCtx {
        state: state.clone(),
        route_index,
        tail: m.tail,
        client_addr,
        req,
    };

    // Per-route policy chain. Any stage may short-circuit before upstream work.
    for stage in &state.stages[route_index] {
        if let Flow::ShortCircuit(resp) = stage.apply(&mut ctx).await {
            access_log(&method, &path, ctx.route_path(), resp.status(), start);
            return resp;
        }
    }

    // Terminal upstream call (P1/P2 wrap this in timeout + retry + LB/breaker).
    let route_path = ctx.route_path().to_string();
    let resp = match upstream::proxy(ctx).await {
        // Response-transform phase: real upstream responses flow through the
        // route's response chain before returning. Gateway-generated errors below
        // deliberately skip it — envelopes/transforms apply only to genuine
        // upstream responses.
        Ok(resp) => run_response_stages(&state, route_index, resp).await,
        Err(e) => e.into_response(),
    };
    access_log(&method, &path, &route_path, resp.status(), start);
    resp
}

/// Run a route's response-phase chain over a real upstream response. A no-op
/// (and allocation-free) when the route registers no response stages, which is
/// every route until P3 transforms land.
async fn run_response_stages(
    state: &AppState,
    route_index: usize,
    resp: Response<BoxBody>,
) -> Response<BoxBody> {
    let stages = &state.response_stages[route_index];
    if stages.is_empty() {
        return resp;
    }
    let mut ctx = ResponseCtx {
        state: state.clone(),
        route_index,
        resp,
    };
    for stage in stages {
        stage.apply(&mut ctx).await;
    }
    ctx.resp
}

/// Assemble each route's stage chain once, at startup, from the parsed config.
/// The returned outer `Vec` is indexed by route index (parallel to
/// `config.routes`). **This function is the feature registry** — later tiers add
/// a `push` per feature here; the core loop in [`handle`] never changes.
pub fn assemble(config: &Config) -> Vec<Vec<Arc<dyn Stage>>> {
    config
        .routes
        .iter()
        .map(|route| {
            let mut stages: Vec<Arc<dyn Stage>> = Vec::new();

            // P0 — method filtering (405 + Allow).
            stages.push(Arc::new(MethodStage::new(&route.methods)));

            // P2 — api_key auth (401). Pushed only for routes that declare
            // `auth`, so open routes pay nothing. Runs after method but BEFORE
            // rate limiting: the key compare is guarded first, and rate-limit
            // bucketing needs no client identity (DECISIONS.md).
            if let Some(a) = &route.auth {
                stages.push(Arc::new(AuthStage::new(a)));
            }

            // P1 — rate limiting (429 + Retry-After). Pushed only when the route
            // has an effective limit, so unlimited routes carry no stage and pay
            // nothing. Runs after method and auth.
            if effective_rate_limit(route, config).is_some() {
                stages.push(Arc::new(RateLimitStage));
            }

            // SEAM — later tiers register their stages here, in pipeline order,
            // gated on the route's config so absent blocks add no overhead:
            //   if let Some(rt) = &route.request_transform { stages.push(Arc::new(RequestTransform)) }   // P3
            // (circuit-breaker gate + target selection wrap the terminal call.)

            stages
        })
        .collect()
}

/// Assemble each route's **response-phase** chain (P3 response transforms),
/// indexed by route index like [`assemble`]. Empty today — the seam is live, so a
/// response transform registers here exactly as a request stage registers above,
/// with no change to [`handle`]'s loop. This is what makes the "extend in an
/// afternoon" claim hold for the response side, not just the request side.
pub fn assemble_response(config: &Config) -> Vec<Vec<Arc<dyn ResponseStage>>> {
    config
        .routes
        .iter()
        .map(|_route| {
            let stages: Vec<Arc<dyn ResponseStage>> = Vec::new();
            // SEAM — P3 response transforms register here, gated on route config:
            //   if let Some(rt) = &_route.response_transform {
            //       stages.push(Arc::new(ResponseTransformStage::new(rt)));
            //   }
            stages
        })
        .collect()
}

/// One structured access-log line per request (observability — makes "upstream
/// down" and rate-limit rejections visible). Chosen-target logging arrives with
/// P2 load balancing; for now `target` is the matched route (or `-`).
fn access_log(method: &Method, path: &str, route: &str, status: StatusCode, start: Instant) {
    let latency_ms = start.elapsed().as_millis();
    eprintln!(
        "access method={method} path={path} route={route} status={} latency_ms={latency_ms}",
        status.as_u16()
    );
}
