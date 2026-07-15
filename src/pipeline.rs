//! The single, ordered request pipeline.
//!
//! Intended full order (later tiers insert stages at the numbered seams):
//!   1. health check   — GET /health, never routed/authed/rate-limited   [P0]
//!   2. match route    — longest-prefix; no match -> 404                  [P0]
//!   3. method filter  — method not in route.methods -> 405              [P0]
//!   4. auth           — api_key; missing/invalid -> 401                 [P2]
//!   5. rate limit     — fixed/sliding window, per ip|global -> 429      [P1]
//!   6. request transform  — headers add/remove, body mapping           [P3]
//!   7. circuit-breaker gate — open -> 503 envelope                     [P2]
//!   8. select target  — round_robin / weighted, skip unhealthy         [P2]
//!   9. timeout + retry — around the upstream call -> 504 / retry       [P1/P2]
//!  10. proxy          — forward upstream; connect error -> 502          [P0]
//!  11. response transform — headers, body envelope                      [P3]
//!  12. return
//!
//! Fast-reject stages (404/405, later 401/429/503) short-circuit *before* any
//! upstream work by returning `ControlFlow::Break(response)`.

use std::net::SocketAddr;
use std::ops::ControlFlow;

use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::error::{BoxBody, GatewayError};
use crate::state::AppState;
use crate::{health, proxy};

/// Mutable per-request context that stages fill in as the request flows down.
struct Ctx {
    route_index: Option<usize>,
    tail: String,
    /// Peer address captured at accept. Unused in P0; P1's rate limiter uses it
    /// as the `per: ip` bucket key (we trust the socket peer, not `X-Forwarded-For`).
    #[allow(dead_code)]
    client_addr: SocketAddr,
}

/// Entry point invoked per request by the server's connection service.
pub async fn handle(
    state: AppState,
    client_addr: SocketAddr,
    req: Request<Incoming>,
) -> Response<BoxBody> {
    // Stage 1 — health, ahead of routing so config can never shadow it.
    if req.uri().path() == "/health" {
        return health::health_response(&state);
    }

    let mut ctx = Ctx {
        route_index: None,
        tail: String::new(),
        client_addr,
    };

    // Fast-reject stages: each returns Break(response) to short-circuit.
    if let ControlFlow::Break(resp) = stage_match(&state, &mut ctx, &req) {
        return resp;
    }
    if let ControlFlow::Break(resp) = stage_method(&state, &ctx, &req) {
        return resp;
    }
    // SEAM: 4 auth · 5 rate limit · 6 request transform · 7 CB gate · 8 target select
    //   each inserts here as: `if let ControlFlow::Break(r) = stage_x(..) { return r; }`

    // Stage 10 — terminal upstream call (P1/P2 wrap this in timeout+retry).
    let route = &state.config.routes[ctx.route_index.expect("route set by stage_match")];
    match proxy::proxy(&state, route, &ctx.tail, req).await {
        // SEAM: 11 response transform applied here before returning.
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

/// Stage 2 — longest-prefix route match; no match -> 404.
fn stage_match(
    state: &AppState,
    ctx: &mut Ctx,
    req: &Request<Incoming>,
) -> ControlFlow<Response<BoxBody>> {
    match state.router.match_route(req.uri().path()) {
        Some(m) => {
            ctx.route_index = Some(m.route_index);
            ctx.tail = m.tail;
            ControlFlow::Continue(())
        }
        None => ControlFlow::Break(GatewayError::NotFound.into_response()),
    }
}

/// Stage 3 — method filter; method not in the route's list -> 405.
fn stage_method(
    state: &AppState,
    ctx: &Ctx,
    req: &Request<Incoming>,
) -> ControlFlow<Response<BoxBody>> {
    let route = &state.config.routes[ctx.route_index.expect("route set by stage_match")];
    let method = req.method().as_str();
    if route.methods.iter().any(|m| m.eq_ignore_ascii_case(method)) {
        ControlFlow::Continue(())
    } else {
        ControlFlow::Break(GatewayError::method_not_allowed(&route.methods).into_response())
    }
}
