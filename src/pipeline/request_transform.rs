//! Request-direction transform [P3]: rewrite headers and (separately) remap the
//! JSON body of an in-flight request before it is proxied upstream.
//!
//! Two concerns live here, split by *when* they can run in the pipeline:
//!   * [`RequestTransformStage`] — a header `add`/`remove` [`Stage`]. Headers are
//!     available on the request head immediately, so this runs inline in the
//!     stage chain and mutates `ctx.req.headers_mut()`.
//!   * [`apply_body_mapping`] — a pure body remapper. The body isn't a `Stage`
//!     because it needs the *buffered* bytes, which the upstream module only has
//!     after it reads `Incoming`. So the upstream call invokes this function
//!     directly on the buffered body rather than through the chain.
//!
//! Both read the single per-request `$request_time` string that the caller
//! computed once (`ctx.request_time`), so a header injected as `$request_time`
//! and a body field mapped from `$request_time` are byte-for-byte identical.
//!
//! Everything here is fed config- and client-derived input, so nothing panics:
//! an unparseable header name/value is skipped, and a non-JSON body is passed
//! through untouched (see DECISIONS.md).

use async_trait::async_trait;
use std::collections::BTreeMap;

use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use crate::config::HeaderTransform;

use super::{Flow, RequestCtx, Stage};

/// Header rewriter for the request direction. Holds the `add` pairs and `remove`
/// list as OWNED data (cloned out of the config borrow, mirroring `AuthStage`)
/// so the stage outlives the config used to build it.
///
/// `add` is kept as a [`BTreeMap`] purely for its deterministic (sorted) iteration
/// order: the wire order of injected headers is stable across runs, which keeps
/// tests and any downstream signature/canonicalization reproducible.
pub struct RequestTransformStage {
    /// dest header name -> value spec (resolved at apply time via
    /// `transform::resolve_value`, so `$request_time` reflects *this* request).
    add: BTreeMap<String, String>,
    /// header names to strip before proxying upstream.
    remove: Vec<String>,
}

impl RequestTransformStage {
    pub fn new(h: &HeaderTransform) -> Self {
        RequestTransformStage {
            add: h.add.clone(),
            remove: h.remove.clone(),
        }
    }

    /// Apply the remove-then-add rewrite to a raw [`HeaderMap`], resolving value
    /// specs against `request_time`. Factored out of [`Stage::apply`] so it is
    /// unit-testable without constructing a full `RequestCtx` (which would need an
    /// `AppState`); `apply` is a thin wrapper over this.
    ///
    /// Remove runs before add so that a name appearing in both is first stripped
    /// and then re-added with the configured value (add wins). Parse failures on
    /// either a name or a value are skipped rather than panicking — the transform
    /// is config-derived and must never take down a request.
    fn rewrite(&self, headers: &mut HeaderMap, request_time: &str) {
        // Strip configured names. `HeaderMap::remove` is case-insensitive given a
        // valid `HeaderName`; an unparseable configured name simply matches
        // nothing, so skipping it is correct.
        for name in &self.remove {
            if let Ok(hn) = HeaderName::from_bytes(name.as_bytes()) {
                headers.remove(&hn);
            }
        }

        // Add/overwrite. `insert` (not `append`) gives add/overwrite semantics
        // per spec: a single configured value replaces any existing header of
        // that name. BTreeMap iteration is sorted → deterministic wire order.
        for (name, spec) in &self.add {
            let value = super::transform::resolve_value(spec, request_time);
            match (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(&value),
            ) {
                (Ok(hn), Ok(hv)) => {
                    headers.insert(hn, hv);
                }
                // Invalid header name or a value with illegal bytes (e.g. control
                // chars): skip this one header, keep processing the rest.
                _ => continue,
            }
        }
    }
}

#[async_trait]
impl Stage for RequestTransformStage {
    async fn apply(&self, ctx: &mut RequestCtx) -> Flow {
        self.rewrite(ctx.req.headers_mut(), &ctx.request_time);
        // Header rewriting never rejects a request — always proceed.
        Flow::Continue
    }
}

/// Build a NEW JSON request body from `body` according to `mapping`
/// (dest dot-path <- source spec). Pure function: the upstream module calls it
/// on the buffered body after reading it off the wire.
///
/// Returns `None` — meaning "keep the original body unchanged" — when either:
///   * `mapping` is empty (nothing to do), or
///   * `body` does not parse as JSON (form data, plain text, empty, …). Non-JSON
///     passes through untouched by design (DECISIONS.md); we only remap bodies we
///     can actually understand.
///
/// When it does remap, the output contains ONLY the mapped destinations — the
/// incoming body is a *source* to read from, not a base to merge into, so
/// unmapped incoming fields are dropped. Source values are `clone`d out of the
/// parsed input, so their JSON type is preserved (a numeric `userId` stays a JSON
/// number, not a stringified one).
pub fn apply_body_mapping(
    body: &[u8],
    mapping: &BTreeMap<String, String>,
    request_time: &str,
) -> Option<bytes::Bytes> {
    if mapping.is_empty() {
        return None;
    }
    // Non-JSON in → pass through (caller keeps the original bytes).
    let incoming: Value = serde_json::from_slice(body).ok()?;

    let mut out = Value::Object(Default::default());
    // BTreeMap → sorted iteration → deterministic construction order.
    for (dest, source) in mapping {
        let value = if let Some(literal) = source.strip_prefix("$literal:") {
            Value::String(literal.to_string())
        } else if source == "$request_time" {
            Value::String(request_time.to_string())
        } else {
            // Dot-path read of the incoming body. A missing source is skipped
            // entirely (we do NOT insert null) so the shape stays clean.
            match super::transform::json_get(&incoming, source) {
                Some(v) => v.clone(),
                None => continue,
            }
        };
        super::transform::json_set(&mut out, dest, value);
    }

    // Serialization of a value we just built cannot fail; fall back to the
    // original body rather than panic in the impossible case.
    let vec = serde_json::to_vec(&out).ok()?;
    Some(bytes::Bytes::from(vec))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(add: &[(&str, &str)], remove: &[&str]) -> RequestTransformStage {
        RequestTransformStage::new(&HeaderTransform {
            add: add
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            remove: remove.iter().map(|s| s.to_string()).collect(),
        })
    }

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn rewrite_adds_and_overwrites() {
        let s = stage(
            &[("X-Added", "$literal:v"), ("X-Trace", "$request_time")],
            &[],
        );
        let mut headers = HeaderMap::new();
        // A pre-existing value for a to-be-added name must be overwritten.
        headers.insert("x-added", "old".parse().unwrap());
        s.rewrite(&mut headers, "2021-01-01T00:00:00Z");

        assert_eq!(headers.get("X-Added").unwrap(), "v");
        // $request_time resolves to the passed-in per-request timestamp.
        assert_eq!(headers.get("X-Trace").unwrap(), "2021-01-01T00:00:00Z");
    }

    #[test]
    fn rewrite_removes_case_insensitively() {
        let s = stage(&[], &["X-Drop-Me"]);
        let mut headers = HeaderMap::new();
        // Sent with different casing than configured — HTTP header match is
        // case-insensitive, so it must still be stripped.
        headers.insert("x-drop-me", "secret".parse().unwrap());
        headers.insert("x-keep", "ok".parse().unwrap());
        s.rewrite(&mut headers, "T");

        assert!(headers.get("X-Drop-Me").is_none());
        assert_eq!(headers.get("x-keep").unwrap(), "ok");
    }

    #[test]
    fn rewrite_remove_then_add_lets_add_win() {
        // A name in both lists ends up with the add value, not removed.
        let s = stage(&[("X-Both", "$literal:new")], &["X-Both"]);
        let mut headers = HeaderMap::new();
        headers.insert("x-both", "old".parse().unwrap());
        s.rewrite(&mut headers, "T");
        assert_eq!(headers.get("X-Both").unwrap(), "new");
    }

    #[test]
    fn rewrite_skips_invalid_names_without_panicking() {
        // A space is illegal in a header name; the entry is silently skipped and
        // a valid sibling still lands.
        let s = stage(&[("Bad Name", "$literal:x"), ("X-Good", "$literal:y")], &[]);
        let mut headers = HeaderMap::new();
        s.rewrite(&mut headers, "T");
        assert!(headers.get("X-Good").is_some());
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn body_mapping_matches_spec_example() {
        let incoming = br#"{"userId":"u1","userName":"Ada","extra":9}"#;
        let mapping = map(&[
            ("user.id", "userId"),
            ("user.name", "userName"),
            ("meta.source", "$literal:gateway"),
            ("meta.timestamp", "$request_time"),
        ]);
        let out = apply_body_mapping(incoming, &mapping, "2021-01-01T00:00:00Z")
            .expect("JSON body should remap");
        let got: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            got,
            serde_json::json!({
                "user": {"id": "u1", "name": "Ada"},
                "meta": {"source": "gateway", "timestamp": "2021-01-01T00:00:00Z"},
            })
        );
        // `extra` was not mapped, so it does not survive.
        assert!(got.get("extra").is_none());
    }

    #[test]
    fn body_mapping_preserves_numeric_type() {
        let out = apply_body_mapping(br#"{"n":7}"#, &map(&[("x", "n")]), "T").unwrap();
        let got: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(got, serde_json::json!({"x": 7}));
        // Specifically a JSON number, not the string "7".
        assert!(got.get("x").unwrap().is_number());
    }

    #[test]
    fn body_mapping_passes_through_non_json() {
        assert!(apply_body_mapping(b"not json", &map(&[("x", "n")]), "T").is_none());
    }

    #[test]
    fn body_mapping_empty_mapping_is_noop() {
        assert!(apply_body_mapping(br#"{"a":1}"#, &BTreeMap::new(), "T").is_none());
    }

    #[test]
    fn body_mapping_skips_missing_source() {
        // `nope` is absent from the incoming body → its dest is omitted entirely.
        let out = apply_body_mapping(
            br#"{"present":1}"#,
            &map(&[("kept", "present"), ("dropped", "nope")]),
            "T",
        )
        .unwrap();
        let got: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(got, serde_json::json!({"kept": 1}));
        assert!(got.get("dropped").is_none());
    }
}
