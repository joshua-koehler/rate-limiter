//! P3 response transform: header rewriting + body enveloping on real upstream
//! responses.
//!
//! This is the response-direction sibling of the request transformer. It runs
//! *after* the terminal upstream call, on the [`hyper::Response`] we got back
//! from a real backend — the caller (`upstream::proxy` / the pipeline) is
//! responsible for ensuring it only ever hands us **genuine upstream
//! responses**, never a gateway-generated error envelope. That contract matters
//! a lot: it means we can transform unconditionally here without having to
//! second-guess whether a `502`/`503`/`429` body is "ours" (which must reach the
//! client verbatim) or the backend's. So this module does exactly one thing —
//! apply the configured transform — and trusts that boundary.
//!
//! Two independent transforms, either or both configured:
//!   * **Headers** — remove named response headers, then add/overwrite a set of
//!     computed ones (reusing [`super::transform::resolve_value`] so header
//!     values speak the same `$literal:`/`$response_time` mini-language as the
//!     request side).
//!   * **Body envelope** — wrap the upstream body inside an arbitrary nested
//!     template (`$body`/`$response_time`/`$route_path` placeholders). This is
//!     the feature that lets a route present a legacy backend's payload inside a
//!     modern `{ "data": …, "gateway_metadata": … }` shape without the backend
//!     changing.
//!
//! Design choices worth calling out:
//!   * The per-response timestamp is computed **once** (see
//!     [`super::transform::now_rfc3339`]) and threaded into both the header and
//!     body substitutions, so a `X-Served-At` header and a
//!     `gateway_metadata.served_at` field always agree to the second.
//!   * We only buffer the upstream body when an envelope is actually configured.
//!     With no body transform we reattach the original *streamed* body untouched
//!     — no needless memory blowup on large/streaming responses.
//!   * Enveloping changes the body length, so we strip stale `Content-Length` /
//!     `Transfer-Encoding` framing and let hyper reframe, and we force
//!     `Content-Type: application/json` because the envelope is JSON regardless
//!     of what the backend sent.
//!   * We **never** turn a non-JSON or empty upstream body into a 5xx: it is
//!     embedded as a JSON string instead (DECISIONS.md — enveloping is a
//!     presentation concern and must not fail an otherwise-good response). The
//!     only failure that yields a 502 here is a genuine I/O error while reading
//!     the upstream body, which means we have no complete response to send.

use async_trait::async_trait;
use hyper::header::{HeaderName, HeaderValue};
use http_body_util::BodyExt;

use crate::config::ResponseTransform;
use crate::error::{full, BoxBody, GatewayError};

use super::{ResponseCtx, ResponseStage};

/// The registered [`ResponseStage`] for P3 response transforms — the response
/// sibling of `RequestTransformStage`. A zero-sized marker (like `RateLimitStage`):
/// it reads the route's `response_transform` config and `$route_path` straight
/// from the [`ResponseCtx`] at apply time, so `assemble_response` can push it
/// without cloning any config into the stage. Only registered for routes that
/// declare the block, and only ever run on genuine upstream responses.
pub struct ResponseTransformStage;

#[async_trait]
impl ResponseStage for ResponseTransformStage {
    async fn apply(&self, ctx: &mut ResponseCtx) {
        // Clone the `Arc<Config>` so the immutable borrow of the route config does
        // not overlap the `&mut` swap of `ctx.resp` below.
        let config = ctx.state.config.clone();
        let route = &config.routes[ctx.route_index];
        let Some(rt) = route.response_transform.as_ref() else {
            return; // registered only when present, but stay total.
        };
        // `apply` consumes and returns the response; take it out behind a trivial
        // placeholder we immediately overwrite.
        let taken = std::mem::replace(&mut ctx.resp, hyper::Response::new(full("")));
        ctx.resp = apply(taken, rt, &route.path).await;
    }
}

/// Apply a route's response transform to a **real upstream response**.
///
/// The caller guarantees `resp` is a genuine backend response (never a
/// gateway-generated error), so we transform unconditionally. Header changes are
/// applied in place; body enveloping (when configured) buffers the upstream body
/// and replaces it. See the module docs for the framing/`Content-Type` handling
/// and the non-JSON/empty-body policy.
pub async fn apply(
    resp: hyper::Response<BoxBody>,
    rt: &ResponseTransform,
    route_path: &str,
) -> hyper::Response<BoxBody> {
    // One timestamp for the whole response so every `$response_time` — in a
    // header and in the body envelope — is identical.
    let response_time = super::transform::now_rfc3339();

    let (mut parts, body) = resp.into_parts();

    // ── Header transform ─────────────────────────────────────────────────────
    // Removals happen before adds so a config that both removes and re-adds the
    // same name ends with the added (computed) value.
    if let Some(ht) = &rt.headers {
        for name in &ht.remove {
            // A malformed header name in config can never take down a response;
            // skip it rather than propagate an error.
            if let Ok(hn) = HeaderName::from_bytes(name.as_bytes()) {
                parts.headers.remove(hn);
            }
        }
        for (name, spec) in &ht.add {
            let value = super::transform::resolve_value(spec, &response_time);
            match (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(&value),
            ) {
                // `insert` overwrites, matching "set this header" semantics.
                (Ok(hn), Ok(hv)) => {
                    parts.headers.insert(hn, hv);
                }
                // Illegal bytes in either half -> skip this one pair, keep going.
                _ => continue,
            }
        }
    }

    // ── Body envelope ────────────────────────────────────────────────────────
    // Only when a body transform *and* an envelope template are present. Both
    // `rt.body == None` and `rt.body.envelope == None` mean "no body transform",
    // in which case we fall through and reattach the original streamed body.
    if let Some(ResponseBodyEnvelope(template)) = body_envelope(rt) {
        // Buffer the upstream body. A read error here is the one case we cannot
        // paper over: we have no complete body to envelope, so we surface a 502.
        let bytes = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                eprintln!("response_transform: reading upstream response body failed: {e}");
                return GatewayError::BadGateway("reading upstream response body".to_string())
                    .into_response();
            }
        };

        // `$body` value: the parsed JSON if the body is valid JSON, otherwise the
        // raw bytes embedded as a JSON string (also covers an empty body). We
        // never 500 on a non-JSON/empty payload — enveloping is presentation.
        let body_value = if bytes.is_empty() {
            serde_json::Value::String(String::new())
        } else {
            match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(v) => v,
                Err(_) => {
                    serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
                }
            }
        };

        let enveloped = substitute(template, &body_value, &response_time, route_path);

        // Serialization of a plain JSON value is effectively infallible; fall
        // back to an empty object rather than panic if it ever weren't.
        let new_bytes = serde_json::to_vec(&enveloped).unwrap_or_else(|_| b"{}".to_vec());

        // The body length changed, so stale framing headers would be wrong; drop
        // them and let hyper reframe. Force JSON content-type for the envelope.
        parts.headers.remove(hyper::header::CONTENT_LENGTH);
        parts.headers.remove(hyper::header::TRANSFER_ENCODING);
        parts.headers.insert(
            hyper::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        return hyper::Response::from_parts(parts, full(new_bytes));
    }

    // No body transform: reattach the original streamed body untouched, carrying
    // only whatever header changes were applied above.
    hyper::Response::from_parts(parts, body)
}

/// Newtype so [`body_envelope`] can return the envelope template with a name at
/// the call site (`if let Some(ResponseBodyEnvelope(template)) = …`), keeping the
/// two-level `Option` unwrap (`rt.body` then `.envelope`) readable.
struct ResponseBodyEnvelope<'a>(&'a serde_yaml::Value);

/// Collapse the `rt.body: Option<…>` / `.envelope: Option<…>` pair into a single
/// optional template reference: `Some` only when a body transform *and* an
/// envelope are both configured.
fn body_envelope(rt: &ResponseTransform) -> Option<ResponseBodyEnvelope<'_>> {
    rt.body
        .as_ref()
        .and_then(|b| b.envelope.as_ref())
        .map(ResponseBodyEnvelope)
}

/// Recursively convert a YAML envelope `template` into a concrete JSON value,
/// resolving placeholders against this response's `body`, `response_time`, and
/// `route_path`.
///
/// Pure and total (no panics, no I/O) so it is trivially unit-testable and can
/// never fail a request:
///   * `"$body"` → the upstream body value (already JSON or a JSON string),
///   * `"$response_time"` / `"$route_path"` → the respective string,
///   * `"$literal:<v>"` → `<v>` verbatim (escape hatch for values starting `$`),
///   * any other string → itself,
///   * mappings/sequences recurse; scalars map across; a non-finite float or an
///     unrepresentable/tagged node degrades to `null` rather than erroring.
fn substitute(
    template: &serde_yaml::Value,
    body: &serde_json::Value,
    response_time: &str,
    route_path: &str,
) -> serde_json::Value {
    use serde_json::Value as J;
    use serde_yaml::Value as Y;

    match template {
        Y::String(s) => {
            if s == "$body" {
                body.clone()
            } else if s == "$response_time" {
                J::String(response_time.to_string())
            } else if s == "$route_path" {
                J::String(route_path.to_string())
            } else if let Some(rest) = s.strip_prefix("$literal:") {
                J::String(rest.to_string())
            } else {
                J::String(s.clone())
            }
        }
        Y::Mapping(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                // Keys are stringified; a non-string key (rare in these
                // templates) degrades to an empty key rather than dropping data.
                let key = match k {
                    Y::String(s) => s.clone(),
                    Y::Bool(b) => b.to_string(),
                    Y::Number(n) => n.to_string(),
                    _ => k.as_str().unwrap_or_default().to_string(),
                };
                obj.insert(key, substitute(v, body, response_time, route_path));
            }
            J::Object(obj)
        }
        Y::Sequence(seq) => J::Array(
            seq.iter()
                .map(|v| substitute(v, body, response_time, route_path))
                .collect(),
        ),
        Y::Bool(b) => J::Bool(*b),
        Y::Number(n) => {
            if let Some(i) = n.as_i64() {
                J::Number(i.into())
            } else if let Some(u) = n.as_u64() {
                J::Number(u.into())
            } else if let Some(f) = n.as_f64() {
                // JSON has no NaN/Infinity; those degrade to null.
                serde_json::Number::from_f64(f).map(J::Number).unwrap_or(J::Null)
            } else {
                J::Null
            }
        }
        Y::Null => J::Null,
        // Tagged values and any future variant have no meaningful envelope
        // representation; degrade to null rather than fail.
        _ => J::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The canonical spec envelope: wrap the body under `data` and attach
    /// gateway metadata. Placeholders resolve to the threaded values, and a JSON
    /// object body is embedded structurally (not stringified).
    #[test]
    fn substitute_builds_spec_envelope() {
        let template: serde_yaml::Value = serde_yaml::from_str(
            r#"
data: "$body"
gateway_metadata:
  served_at: "$response_time"
  route: "$route_path"
"#,
        )
        .unwrap();

        let body = json!({"ok": true});
        let out = substitute(&template, &body, "2021-01-01T00:00:00Z", "/api/legacy");

        assert_eq!(
            out,
            json!({
                "data": {"ok": true},
                "gateway_metadata": {
                    "served_at": "2021-01-01T00:00:00Z",
                    "route": "/api/legacy",
                }
            })
        );
    }

    /// A non-JSON upstream body reaches `substitute` already as a JSON string
    /// (the caller's fallback); it must land under `data` verbatim, unquoted-ness
    /// preserved as a JSON string value.
    #[test]
    fn substitute_embeds_non_json_body_as_string() {
        let template: serde_yaml::Value = serde_yaml::from_str(r#"data: "$body""#).unwrap();
        let body = serde_json::Value::String("plain-text".into());

        let out = substitute(&template, &body, "T", "/r");
        assert_eq!(out, json!({"data": "plain-text"}));
    }

    /// `$literal:` is the escape hatch for values that must start with `$` or
    /// otherwise look like a placeholder.
    #[test]
    fn substitute_resolves_literal() {
        let template: serde_yaml::Value =
            serde_yaml::from_str(r#"marker: "$literal:$response_time""#).unwrap();
        let out = substitute(&template, &json!(null), "T", "/r");
        assert_eq!(out, json!({"marker": "$response_time"}));
    }

    /// Scalars, sequences, and non-finite floats round-trip / degrade sanely.
    #[test]
    fn substitute_maps_scalars_and_sequences() {
        let template: serde_yaml::Value = serde_yaml::from_str(
            r#"
flag: true
count: 3
ratio: 1.5
items:
  - "$route_path"
  - "$literal:x"
"#,
        )
        .unwrap();
        let out = substitute(&template, &json!(null), "T", "/api");
        assert_eq!(
            out,
            json!({
                "flag": true,
                "count": 3,
                "ratio": 1.5,
                "items": ["/api", "x"],
            })
        );
    }

    /// End-to-end `apply`: a JSON upstream body is enveloped, framing headers are
    /// reset, and `Content-Type` is forced to JSON.
    #[tokio::test]
    async fn apply_envelopes_upstream_body() {
        use crate::config::{ResponseBodyTransform, ResponseTransform};

        let envelope: serde_yaml::Value = serde_yaml::from_str(
            r#"
data: "$body"
gateway_metadata:
  route: "$route_path"
"#,
        )
        .unwrap();
        let rt = ResponseTransform {
            headers: None,
            body: Some(ResponseBodyTransform {
                envelope: Some(envelope),
            }),
        };

        let upstream = hyper::Response::builder()
            .status(200)
            // A stale Content-Length that no longer matches after enveloping —
            // apply() must drop it.
            .header(hyper::header::CONTENT_LENGTH, "13")
            .body(full(r#"{"ok":true}"#))
            .unwrap();

        let out = apply(upstream, &rt, "/api/legacy").await;

        assert_eq!(
            out.headers().get(hyper::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert!(out.headers().get(hyper::header::CONTENT_LENGTH).is_none());

        let (_parts, body) = out.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value,
            json!({
                "data": {"ok": true},
                "gateway_metadata": {"route": "/api/legacy"},
            })
        );
    }

    /// With header changes but no body envelope, `apply` leaves the original body
    /// intact and only rewrites headers.
    #[tokio::test]
    async fn apply_header_only_preserves_body() {
        use std::collections::BTreeMap;

        use crate::config::{HeaderTransform, ResponseTransform};

        let mut add = BTreeMap::new();
        add.insert("X-Served-By".to_string(), "$literal:gatewaykit".to_string());
        let rt = ResponseTransform {
            headers: Some(HeaderTransform {
                add,
                remove: vec!["X-Internal".to_string()],
            }),
            body: None,
        };

        let upstream = hyper::Response::builder()
            .status(200)
            .header("X-Internal", "secret")
            .body(full("untouched-body"))
            .unwrap();

        let out = apply(upstream, &rt, "/api/x").await;

        assert_eq!(out.headers().get("X-Served-By").unwrap(), "gatewaykit");
        assert!(out.headers().get("X-Internal").is_none());

        let (_parts, body) = out.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        assert_eq!(&bytes[..], b"untouched-body");
    }
}
