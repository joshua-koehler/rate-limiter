//! Upstream module: the outbound HTTP call and proxy hygiene. Owns target
//! selection and faithful relay of the upstream response.
//!
//! P0 selects a single upstream (or the first `targets` entry as a fallback) and
//! forwards the original path+query. P2 replaces [`select_upstream`] with
//! balance-aware, health-aware, breaker-aware selection; the seam is here so the
//! pipeline core never has to change.

use std::time::Duration;

use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Request, Response, Uri};

use crate::config::{Gateway, Route, Upstream};
use crate::error::{BoxBody, BoxError, GatewayError};
use crate::pipeline::RequestCtx;

/// Forward the request to the route's upstream and relay the response.
///
/// Consumes the [`RequestCtx`] — this is the terminal step of the pipeline, so
/// it takes ownership of the request body. `tail` (the post-prefix remainder) is
/// used by `strip_prefix` (P1); otherwise the original path+query is forwarded.
pub async fn proxy(ctx: RequestCtx) -> Result<Response<BoxBody>, GatewayError> {
    let RequestCtx {
        state,
        route_index,
        tail,
        req,
        ..
    } = ctx;

    let route = &state.config.routes[route_index];
    let base = select_upstream(&route.upstream)?;

    // Build the forwarded path. `strip_prefix` swaps the matched prefix for the
    // router `tail` (which the router already collapses to `/` on an exact
    // match). The `tail` is path-only, so we re-attach the original query to
    // preserve it; without strip, `path_and_query` already carries the query.
    let path_and_query = if route.strip_prefix {
        match req.uri().query() {
            Some(q) => format!("{tail}?{q}"),
            None => tail.clone(),
        }
    } else {
        req.uri()
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| "/".to_string())
    };
    let uri_str = format!("{}{}", base.trim_end_matches('/'), path_and_query);
    let uri: Uri = uri_str
        .parse()
        .map_err(|e| GatewayError::BadGateway(format!("invalid upstream URI '{uri_str}': {e}")))?;

    // The Host we present upstream is the *target's* authority, not the client's.
    let authority = uri
        .authority()
        .map(|a| a.as_str().to_string())
        .ok_or_else(|| GatewayError::BadGateway(format!("upstream URI '{uri_str}' has no host")))?;

    let (parts, body) = req.into_parts();
    let mut builder = Request::builder().method(parts.method).uri(uri);
    if let Some(headers) = builder.headers_mut() {
        for (name, value) in parts.headers.iter() {
            // Drop hop-by-hop headers and the client's Host — we rewrite Host below.
            if is_hop_by_hop(name.as_str()) || name.as_str() == "host" {
                continue;
            }
            headers.append(name, value.clone());
        }
        // Rewrite Host to the upstream authority (real-proxy hygiene).
        if let Ok(host) = HeaderValue::from_str(&authority) {
            headers.insert(header::HOST, host);
        }
    }
    let upstream_req = builder
        .body(body.map_err(|e| Box::new(e) as BoxError).boxed_unsync())
        .map_err(|e| GatewayError::BadGateway(format!("building upstream request: {e}")))?;

    // Per-attempt timeout: bounds a single upstream call, not the whole request.
    // P2 retry adds an *overall* wall-clock budget spanning every attempt (the
    // timeout+retry seam) — this stays per attempt. With no effective timeout
    // configured we await the call unbounded.
    let call = state.client.request(upstream_req);
    let result = match effective_timeout(route, &state.config.gateway) {
        Some(dur) => match tokio::time::timeout(dur, call).await {
            Ok(inner) => inner,
            Err(_elapsed) => return Err(GatewayError::GatewayTimeout),
        },
        None => call.await,
    };

    match result {
        Ok(resp) => Ok(relay_response(resp)),
        Err(e) => {
            // Detailed transport error is logged, not leaked to the client.
            eprintln!("upstream request to '{base}' failed: {e}");
            Err(GatewayError::BadGateway(e.to_string()))
        }
    }
}

/// Resolve the per-attempt timeout, first-present-wins: a route-level `timeout`
/// beats an `upstream.timeout`, which beats the gateway `global_timeout`. When
/// none is set anywhere the call is left unbounded (returns `None`).
fn effective_timeout(route: &Route, gateway: &Gateway) -> Option<Duration> {
    route
        .timeout
        .or(route.upstream.timeout)
        .or(gateway.global_timeout)
}

/// Relay an upstream response: keep status + end-to-end headers + body, drop
/// hop-by-hop headers and let hyper reframe the outgoing body.
fn relay_response(resp: Response<Incoming>) -> Response<BoxBody> {
    let (mut parts, body) = resp.into_parts();
    let hop: Vec<_> = parts
        .headers
        .keys()
        .filter(|k| is_hop_by_hop(k.as_str()))
        .cloned()
        .collect();
    for name in hop {
        parts.headers.remove(&name);
    }
    let body = body.map_err(|e| Box::new(e) as BoxError).boxed_unsync();
    Response::from_parts(parts, body)
}

/// Pick the upstream base URL. P0 has no load balancing: a single `url` is used
/// directly, and `targets` configs fall back to the first target so they still
/// proxy. P2 replaces this with balance-aware, health-aware target selection.
fn select_upstream(upstream: &Upstream) -> Result<&str, GatewayError> {
    if let Some(url) = &upstream.url {
        return Ok(url);
    }
    if let Some(first) = upstream.targets.first() {
        return Ok(&first.url);
    }
    // validate() rejects this at load, so it is effectively unreachable.
    Err(GatewayError::BadGateway(
        "route upstream has no url or targets".to_string(),
    ))
}

/// RFC 7230 hop-by-hop headers, which a proxy must not forward end-to-end.
/// `HeaderName` is always lowercase, so a direct compare is safe.
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-connection"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}
