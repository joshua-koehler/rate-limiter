//! Upstream proxying: forward the request to the route's upstream and relay the
//! response faithfully (status + headers + body). Connect errors map to 502,
//! and an exceeded per-attempt timeout maps to 504.

use std::time::Duration;

use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response, Uri};

use crate::config::{Gateway, Route, Upstream};
use crate::error::{BoxBody, BoxError, GatewayError};
use crate::state::AppState;

/// Forward `req` to the route's upstream and return the relayed response.
///
/// `tail` is the post-prefix path remainder from the router (e.g. prefix
/// `/api/products`, path `/api/products/123` -> `/123`; an exact-prefix match
/// yields `/`). With `strip_prefix` we forward that tail; otherwise the full
/// original path is forwarded unchanged.
pub async fn proxy(
    state: &AppState,
    route: &Route,
    tail: &str,
    req: Request<Incoming>,
) -> Result<Response<BoxBody>, GatewayError> {
    let base = select_upstream(&route.upstream)?;

    // Build the forwarded path. `strip_prefix` swaps the matched prefix for the
    // router `tail` (which the router already collapses to `/` on an exact
    // match). The `tail` is path-only, so we must re-attach the original query
    // ourselves to preserve it; without strip, `path_and_query` already carries
    // the query, so it is forwarded verbatim.
    let original = req.uri();
    let path_and_query = if route.strip_prefix {
        match original.query() {
            Some(q) => format!("{tail}?{q}"),
            None => tail.to_string(),
        }
    } else {
        original
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| "/".to_string())
    };
    let uri_str = format!("{}{}", base.trim_end_matches('/'), path_and_query);
    let uri: Uri = uri_str
        .parse()
        .map_err(|e| GatewayError::BadGateway(format!("invalid upstream URI '{uri_str}': {e}")))?;

    let (parts, body) = req.into_parts();
    let mut builder = Request::builder().method(parts.method).uri(uri);
    if let Some(headers) = builder.headers_mut() {
        for (name, value) in parts.headers.iter() {
            // Drop hop-by-hop headers; let the client set Host from the target.
            if is_hop_by_hop(name.as_str()) || name.as_str() == "host" {
                continue;
            }
            headers.append(name, value.clone());
        }
    }
    let upstream_req = builder
        .body(body.map_err(|e| Box::new(e) as BoxError).boxed_unsync())
        .map_err(|e| GatewayError::BadGateway(format!("building upstream request: {e}")))?;

    // Per-attempt timeout: this bounds a single upstream call, not the whole
    // request. P2 retry adds an *overall* wall-clock budget spanning every
    // attempt (the timeout+retry seam in the pipeline) — this stays per attempt.
    // With no effective timeout configured we await the call unbounded.
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
