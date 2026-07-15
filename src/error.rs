//! Gateway errors and the shared response body type.
//!
//! Every failure mode maps to an HTTP status + a small JSON body via
//! [`GatewayError::into_response`]. No panic ever reaches the client — the
//! server's connection handler and this enum together guarantee a response.

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::{header, Response, StatusCode};

/// Boxed error type shared by all response/request bodies.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The one body type flowing out of the pipeline: either buffered gateway
/// bodies (health/errors, via [`full`]) or a streamed upstream body (in
/// `proxy.rs`), both erased to the same type so stages can be uniform.
/// `Unsync` avoids requiring `Sync` on hyper's streaming `Incoming` body.
pub type BoxBody = UnsyncBoxBody<Bytes, BoxError>;

/// Wrap in-memory bytes as a [`BoxBody`].
pub fn full(body: impl Into<Bytes>) -> BoxBody {
    Full::new(body.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// Errors that terminate a request with a specific HTTP status.
///
/// Future tiers add variants here (401/429/503/504); [`into_response`] and the
/// pipeline's error arm already funnel any variant to a clean JSON response.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("no route matches the request path")]
    NotFound,
    #[error("method not allowed (allowed: {allow})")]
    MethodNotAllowed { allow: String },
    #[error("bad gateway: {0}")]
    BadGateway(String),
    /// P1 rate limiting: over the configured limit. `retry_after` is whole
    /// seconds until capacity, surfaced in the `Retry-After` header.
    #[error("rate limit exceeded (retry after {retry_after}s)")]
    RateLimited { retry_after: u64 },
    /// P1 timeouts: the upstream did not respond within the effective timeout.
    #[error("upstream timed out")]
    GatewayTimeout,
    /// P2 api_key auth: missing or invalid credential (no 403 distinction —
    /// see DECISIONS.md).
    #[error("unauthorized")]
    Unauthorized,
    /// P2 circuit breaker: the target's breaker is Open, so we reject without
    /// contacting upstream. Renders the spec's exact 503 envelope
    /// `{ "error": "service_unavailable", "retry_after": <s> }`.
    #[error("circuit open (retry after {retry_after}s)")]
    CircuitOpen { retry_after: u64 },
    /// P2 health checks / load balancing: every target for the route is
    /// unhealthy or Open, so there is nothing to proxy to → 503.
    #[error("all upstream targets are unhealthy")]
    AllTargetsUnhealthy,
    /// Cross-cutting body cap (lands with P2 retry buffering): the request body
    /// exceeds the maximum the gateway will buffer → 413.
    #[error("request body too large")]
    PayloadTooLarge,
}

impl GatewayError {
    /// Build a 405 carrying the route's allowed methods for the `Allow` header.
    pub fn method_not_allowed(methods: &[String]) -> Self {
        GatewayError::MethodNotAllowed {
            allow: methods.join(", "),
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            GatewayError::NotFound => StatusCode::NOT_FOUND,
            GatewayError::MethodNotAllowed { .. } => StatusCode::METHOD_NOT_ALLOWED,
            GatewayError::BadGateway(_) => StatusCode::BAD_GATEWAY,
            GatewayError::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
            GatewayError::GatewayTimeout => StatusCode::GATEWAY_TIMEOUT,
            GatewayError::Unauthorized => StatusCode::UNAUTHORIZED,
            GatewayError::CircuitOpen { .. } => StatusCode::SERVICE_UNAVAILABLE,
            GatewayError::AllTargetsUnhealthy => StatusCode::SERVICE_UNAVAILABLE,
            GatewayError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        }
    }

    /// Stable machine-readable error code returned to clients.
    fn code(&self) -> &'static str {
        match self {
            GatewayError::NotFound => "not_found",
            GatewayError::MethodNotAllowed { .. } => "method_not_allowed",
            GatewayError::BadGateway(_) => "bad_gateway",
            GatewayError::RateLimited { .. } => "rate_limited",
            GatewayError::GatewayTimeout => "gateway_timeout",
            GatewayError::Unauthorized => "unauthorized",
            // Both 503s speak the spec's `service_unavailable` code; CircuitOpen
            // additionally carries `retry_after` via the custom body below.
            GatewayError::CircuitOpen { .. } => "service_unavailable",
            GatewayError::AllTargetsUnhealthy => "service_unavailable",
            GatewayError::PayloadTooLarge => "payload_too_large",
        }
    }

    /// Render as an HTTP response. Detailed/dynamic error text (e.g. the
    /// upstream connect error) is logged server-side, never leaked in the body,
    /// so the body is a fixed, injection-safe JSON document.
    pub fn into_response(self) -> Response<BoxBody> {
        let status = self.status();
        // Most errors render `{"error":"<code>"}`; the circuit-breaker 503 adds
        // the spec's `retry_after` field to the same envelope.
        let body = match &self {
            GatewayError::CircuitOpen { retry_after } => format!(
                "{{\"error\":\"{}\",\"retry_after\":{retry_after}}}",
                self.code()
            ),
            _ => format!("{{\"error\":\"{}\"}}", self.code()),
        };
        let mut builder = Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json");
        if let GatewayError::MethodNotAllowed { allow } = &self {
            // `allow` derives from config-supplied method strings; guard against
            // illegal header bytes so a bad config can never panic a request.
            if let Ok(value) = header::HeaderValue::from_str(allow) {
                builder = builder.header(header::ALLOW, value);
            }
        }
        if let GatewayError::RateLimited { retry_after } = &self {
            builder = builder.header(header::RETRY_AFTER, retry_after.to_string());
        }
        builder.body(full(body)).unwrap_or_else(|_| {
            // All header values above are static or validated, so this is
            // effectively unreachable; fall back to a bare status response
            // rather than panicking if that ever changes.
            Response::builder()
                .status(status)
                .body(full("{\"error\":\"internal\"}"))
                .expect("status-only response with no custom headers is infallible")
        })
    }
}
