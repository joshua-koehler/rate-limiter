//! Upstream module: the terminal outbound call plus the whole P2 resilience
//! layer wrapped around it — target selection (load balancing + health +
//! circuit breaker) and a retry/backoff loop, all preserving the P0/P1 proxy
//! hygiene (strip_prefix, Host rewrite, hop-by-hop stripping, per-attempt
//! timeout, faithful relay).
//!
//! The per-target *state* (breakers, health, balancer cursors) lives in
//! [`UpstreamRegistry`] on `AppState`, keyed by route index; this module is the
//! stateless policy logic that reads it. Submodules:
//!   * [`balance`] — preference ordering (round-robin / smooth weighted RR).
//!   * [`breaker`] — per-target circuit breaker.
//!   * [`health`]  — per-target active-health flag.
//!   * [`target`]  — the registry, target runtimes, and eligibility/selection.
//!
//! ## Request flow
//! 1. Buffer the request body once (bounded — [`MAX_BODY_BYTES`], else 413) so
//!    each retry can re-send it.
//! 2. Ask the balancer for a preference ordering of the route's targets.
//! 3. Up to `attempts` times: select the next *eligible* target (healthy + a
//!    permitting breaker), forward the buffered request with the per-attempt
//!    timeout, and relay/retry based on the outcome.
//!
//! ## Breaker accounting (per-request vs per-attempt — reconciled)
//! DECISIONS.md says a fully-failed request is **one** breaker failure (not one
//! per attempt); the retry loop also fails *over* to other targets. We reconcile
//! the two by recording outcomes **per target**, but **at most one failure per
//! target per request** (tracked in `failed_targets`). So a single-`url` upstream
//! retried 3× and failing = one failure toward that target's breaker (the
//! decision), while with failover each *distinct* target that failed gets its own
//! single failure. A successful attempt records success on the target it hit.

mod balance;
mod breaker;
mod health;
mod target;

pub use target::UpstreamRegistry;

use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{self, HeaderMap, HeaderValue};
use hyper::{Method, Request, Response, Uri};

use crate::config::{Backoff, Gateway, Retry, Route};
use crate::error::{full, BoxBody, BoxError, GatewayError};
use crate::pipeline::RequestCtx;

use target::{Selection, TargetRuntime};

/// Maximum request body the gateway will buffer for retrying. Retry requires
/// re-sending the body, so we read it fully into memory up front; capping that
/// read is both a correctness bound and a DoS guard (an unbounded body would let
/// a client OOM the gateway). 2 MiB comfortably covers JSON API payloads; larger
/// bodies get a `413` rather than being streamed unbounded.
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Forward the request to one of the route's upstream targets and relay the
/// response, applying load balancing, health/breaker eligibility, and
/// retry+backoff. Terminal pipeline step: consumes the [`RequestCtx`].
pub async fn proxy(ctx: RequestCtx) -> Result<Response<BoxBody>, GatewayError> {
    let RequestCtx {
        state,
        route_index,
        tail,
        req,
        ..
    } = ctx;

    let route = &state.config.routes[route_index];
    let route_upstream = state.upstreams.route(route_index);

    // Forwarded path+query — identical across attempts/targets, so compute once.
    // `strip_prefix` swaps the matched prefix for the router `tail` (path-only,
    // so we re-attach the original query); otherwise the original path+query is
    // forwarded verbatim.
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

    // Split head from body and buffer the body once (bounded → 413). Each attempt
    // rebuilds a fresh request from these buffered `Bytes`.
    let (parts, body) = req.into_parts();
    let method = parts.method;
    let headers = parts.headers;
    let body_bytes = buffer_body(body).await?;

    // Retry policy + per-attempt timeout (route → upstream → global precedence).
    let retry = route.retry.as_ref();
    let total_attempts = retry.map(|r| r.attempts).unwrap_or(1).max(1);
    let per_attempt = effective_timeout(route, &state.config.gateway);

    // Overall wall-clock budget: with a per-attempt timeout set, cap total time
    // at `attempts * per_attempt + sum(backoffs)` — the natural worst case — so
    // retries+backoff can't run unbounded. With no timeout there's no per-call
    // bound to sum, so we let the (finite) attempt count + backoff be the bound.
    let total_backoff: Duration = (1..total_attempts)
        .filter_map(|n| backoff_delay(retry, n))
        .sum();
    let deadline = per_attempt
        .map(|t| Instant::now() + t.saturating_mul(total_attempts) + total_backoff);

    let order = route_upstream.preference_order();

    // Walk the preference order across attempts. `cursor` advances past each
    // target we try so a retry fails over to the *next* eligible target.
    let mut cursor = 0usize;
    let mut failed_targets: Vec<usize> = Vec::new();
    let mut last_response: Option<Response<BoxBody>> = None;
    let mut last_kind = LastKind::None;
    let mut attempt = 0u32;

    loop {
        if attempt >= total_attempts {
            break;
        }
        let now = Instant::now();
        let (target, order_pos) = match route_upstream.select(&order, cursor, now) {
            Selection::Target { target, order_pos } => (target, order_pos),
            Selection::Unavailable { open, retry_after } => {
                if attempt == 0 {
                    // Nothing eligible before we ever called upstream → map the
                    // exclusion reason: any Open breaker → circuit-open 503;
                    // otherwise every exclusion was a health ejection.
                    return Err(if open {
                        GatewayError::CircuitOpen {
                            retry_after: retry_after.max(1),
                        }
                    } else {
                        GatewayError::AllTargetsUnhealthy
                    });
                }
                // Ran out of eligible targets mid-retry → surface the last error.
                break;
            }
        };
        let pool_idx = order[order_pos];
        cursor = order_pos + 1;

        let outcome = attempt_call(
            &state.client,
            &target.url,
            &path_and_query,
            &method,
            &headers,
            &body_bytes,
            per_attempt,
        )
        .await;
        attempt += 1;

        match outcome {
            AttemptOutcome::Response(resp) => {
                // Breaker: a 5xx is a failure (once per target/request); anything
                // else means the target is alive → success.
                if resp.status().is_server_error() {
                    note_failure(&mut failed_targets, pool_idx, &target, now);
                } else {
                    target.breaker.record_success(now);
                }
                // Retry only on configured statuses; otherwise relay as-is.
                let retryable = retry.map_or(false, |r| r.on.contains(&resp.status().as_u16()));
                if retryable && attempt < total_attempts {
                    last_response = Some(resp);
                    last_kind = LastKind::Response;
                } else {
                    return Ok(resp);
                }
            }
            AttemptOutcome::Timeout => {
                note_failure(&mut failed_targets, pool_idx, &target, now);
                last_response = None;
                last_kind = LastKind::Timeout;
            }
            AttemptOutcome::Transport(e) => {
                note_failure(&mut failed_targets, pool_idx, &target, now);
                last_response = None;
                last_kind = LastKind::Transport(e);
            }
        }

        // Backoff before the next attempt (only reached when retrying).
        if attempt < total_attempts {
            if let Some(delay) = backoff_delay(retry, attempt) {
                if let Some(dl) = deadline {
                    if Instant::now() + delay >= dl {
                        break; // backing off would blow the overall budget
                    }
                }
                tokio::time::sleep(delay).await;
            }
        }
    }

    // Retries exhausted (or budget/eligibility ran out): surface the last outcome.
    match last_kind {
        LastKind::Response => Ok(last_response.expect("Response kind carries a response")),
        LastKind::Timeout => Err(GatewayError::GatewayTimeout),
        LastKind::Transport(e) => {
            eprintln!("upstream request failed after {attempt} attempt(s): {e}");
            Err(GatewayError::BadGateway(e))
        }
        LastKind::None => Err(GatewayError::BadGateway(
            "no upstream attempt was made".to_string(),
        )),
    }
}

/// The last non-final outcome carried out of the retry loop, so exhaustion can be
/// mapped to the right terminal error.
enum LastKind {
    None,
    /// Last attempt returned a retryable HTTP response (relayed on exhaustion).
    Response,
    /// Last attempt timed out → 504.
    Timeout,
    /// Last attempt was a transport error (message) → 502.
    Transport(String),
}

/// One attempt's outcome.
enum AttemptOutcome {
    Response(Response<BoxBody>),
    Timeout,
    Transport(String),
}

/// Record a failure against `target`'s breaker at most **once per request** (the
/// per-request-not-per-attempt reconciliation — see module docs).
fn note_failure(failed: &mut Vec<usize>, pool_idx: usize, target: &TargetRuntime, now: Instant) {
    if !failed.contains(&pool_idx) {
        target.breaker.record_failure(now);
        failed.push(pool_idx);
    }
}

/// Build and send one upstream attempt to `base`, applying proxy hygiene and the
/// per-attempt timeout. Never fails the whole request — every failure mode maps
/// to an [`AttemptOutcome`] the retry loop decides on.
#[allow(clippy::too_many_arguments)]
async fn attempt_call(
    client: &crate::state::HttpClient,
    base: &str,
    path_and_query: &str,
    method: &Method,
    headers: &HeaderMap,
    body: &Bytes,
    per_attempt: Option<Duration>,
) -> AttemptOutcome {
    let uri_str = format!("{}{}", base.trim_end_matches('/'), path_and_query);
    let uri: Uri = match uri_str.parse() {
        Ok(u) => u,
        Err(e) => return AttemptOutcome::Transport(format!("invalid upstream URI '{uri_str}': {e}")),
    };
    // The Host we present upstream is the *target's* authority, not the client's.
    let authority = match uri.authority().map(|a| a.as_str().to_string()) {
        Some(a) => a,
        None => return AttemptOutcome::Transport(format!("upstream URI '{uri_str}' has no host")),
    };

    let mut builder = Request::builder().method(method.clone()).uri(uri);
    if let Some(out) = builder.headers_mut() {
        for (name, value) in headers.iter() {
            // Drop hop-by-hop, the client's Host (rewritten below), and the
            // client's Content-Length — the buffered `Full` body has a known
            // length that hyper frames afresh (recompute-on-body-change hygiene).
            if is_hop_by_hop(name.as_str())
                || name.as_str() == "host"
                || name.as_str() == "content-length"
            {
                continue;
            }
            out.append(name, value.clone());
        }
        if let Ok(host) = HeaderValue::from_str(&authority) {
            out.insert(header::HOST, host);
        }
    }
    let upstream_req = match builder.body(full(body.clone())) {
        Ok(r) => r,
        Err(e) => return AttemptOutcome::Transport(format!("building upstream request: {e}")),
    };

    let call = client.request(upstream_req);
    let result = match per_attempt {
        Some(dur) => match tokio::time::timeout(dur, call).await {
            Ok(inner) => inner,
            Err(_elapsed) => return AttemptOutcome::Timeout,
        },
        None => call.await,
    };
    match result {
        Ok(resp) => AttemptOutcome::Response(relay_response(resp)),
        Err(e) => AttemptOutcome::Transport(e.to_string()),
    }
}

/// Buffer the request body into `Bytes`, enforcing [`MAX_BODY_BYTES`] as we go so
/// an oversize body is rejected (413) *without* first reading it all into memory.
async fn buffer_body(body: Incoming) -> Result<Bytes, GatewayError> {
    let mut body = body;
    let mut collected: Vec<u8> = Vec::new();
    while let Some(next) = body.frame().await {
        let frame =
            next.map_err(|e| GatewayError::BadGateway(format!("reading request body: {e}")))?;
        if let Ok(data) = frame.into_data() {
            if collected.len() + data.len() > MAX_BODY_BYTES {
                return Err(GatewayError::PayloadTooLarge);
            }
            collected.extend_from_slice(&data);
        }
    }
    Ok(Bytes::from(collected))
}

/// Backoff before the attempt that follows the `n`-th completed attempt
/// (1-indexed). `fixed` → `initial_delay` every time; `exponential` →
/// `initial_delay * 2^(n-1)`. `None` when the route has no retry policy.
fn backoff_delay(retry: Option<&Retry>, n: u32) -> Option<Duration> {
    let r = retry?;
    let delay = match r.backoff {
        Backoff::Fixed => r.initial_delay,
        Backoff::Exponential => {
            // Cap the shift so a large `attempts` can't overflow the multiply;
            // the saturating_mul then caps the duration itself.
            let shift = n.saturating_sub(1).min(16);
            r.initial_delay.saturating_mul(1u32 << shift)
        }
    };
    Some(delay)
}

/// Hard backstop applied when a valid config sets no timeout anywhere (all three
/// knobs are optional in the schema). Without it, a connected-but-hung upstream
/// would pin a request/task forever — no config is a reason to be unbounded. 30s
/// mirrors the spec's example `global_timeout`; an explicit config value always
/// wins over it.
const DEFAULT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// Resolve the per-attempt timeout, first-present-wins: route `timeout` beats
/// `upstream.timeout` beats the gateway `global_timeout`, and a hard default
/// backstops the case where the config sets none. Always `Some`, so every attempt
/// is bounded and the overall retry deadline is always finite.
///
/// Scope: this bounds time-to-**response-headers** (when hyper resolves the
/// request future), not the streamed response body. A well-behaved upstream that
/// sends headers then trickles the body is not covered here; a body/idle timeout
/// is future work (noted in DECISIONS.md), kept out of P1/P2 to avoid a second
/// body-streaming design pass.
fn effective_timeout(route: &Route, gateway: &Gateway) -> Option<Duration> {
    Some(
        route
            .timeout
            .or(route.upstream.timeout)
            .or(gateway.global_timeout)
            .unwrap_or(DEFAULT_ATTEMPT_TIMEOUT),
    )
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
