//! Shared helpers for the P3 transform stages (request + response).
//!
//! Three concerns live here because both the request and response transformers
//! need them, and co-locating keeps `$request_time`/`$response_time` formatting
//! identical across the two:
//!   * [`now_rfc3339`] / [`format_rfc3339_utc`] — an RFC-3339 UTC timestamp
//!     formatted via the `time` crate (a date utility library, not a gateway
//!     framework, so the transport-only boundary is intact). The caller computes
//!     the timestamp **once** per request/response and threads it, so a header
//!     and a body mapping that both reference `$request_time` always agree.
//!   * [`resolve_value`] — turn a config value spec (`$literal:…`,
//!     `$request_time`/`$response_time`, or a plain literal) into a concrete
//!     string. Used for header `add` values in both directions.
//!   * [`json_get`] / [`json_set`] — dot-path read/write over a JSON value, used
//!     by request body mapping and (via the envelope walk) response enveloping.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};
use time::macros::format_description;
use time::OffsetDateTime;

/// Format a [`SystemTime`] as RFC-3339 / ISO-8601 UTC at second precision:
/// `YYYY-MM-DDTHH:MM:SSZ`. Times before the Unix epoch (which never occur for
/// `SystemTime::now()` on a sane clock) clamp to the epoch.
pub fn format_rfc3339_utc(t: SystemTime) -> String {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let fmt = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");
    OffsetDateTime::from_unix_timestamp(secs)
        .ok()
        .and_then(|dt| dt.format(&fmt).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

/// `SystemTime::now()` formatted as RFC-3339 UTC. Call **once** per
/// request/response and reuse the string for every placeholder in that phase.
pub fn now_rfc3339() -> String {
    format_rfc3339_utc(SystemTime::now())
}

/// Resolve a config value spec into a concrete string:
///   * `$literal:<v>` → `<v>` verbatim (the escape hatch for values that must
///     start with `$`),
///   * `$request_time` / `$response_time` → `time` (the single per-phase
///     timestamp the caller passes),
///   * anything else → the spec itself (a plain literal).
pub fn resolve_value(spec: &str, time: &str) -> String {
    if let Some(literal) = spec.strip_prefix("$literal:") {
        literal.to_string()
    } else if spec == "$request_time" || spec == "$response_time" {
        time.to_string()
    } else {
        spec.to_string()
    }
}

/// Read the JSON value at a dot-path (`"user.id"`, `"a.b.c"`). Returns `None` if
/// any segment is absent or traverses a non-object. An empty path yields the
/// root.
pub fn json_get<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(root);
    }
    let mut cur = root;
    for seg in path.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

/// Write `value` at a dot-path in `root`, creating intermediate objects as
/// needed. If an intermediate segment currently holds a non-object, it is
/// replaced with an object (last-writer-wins for the constructed shape). An
/// empty path replaces the whole root.
pub fn json_set(root: &mut Value, path: &str, value: Value) {
    if path.is_empty() {
        *root = value;
        return;
    }
    let segs: Vec<&str> = path.split('.').collect();
    let mut cur = root;
    for seg in &segs[..segs.len() - 1] {
        if !cur.is_object() {
            *cur = Value::Object(Map::new());
        }
        cur = cur
            .as_object_mut()
            .unwrap()
            .entry((*seg).to_string())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    if !cur.is_object() {
        *cur = Value::Object(Map::new());
    }
    cur.as_object_mut()
        .unwrap()
        .insert(segs[segs.len() - 1].to_string(), value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(secs: u64) -> String {
        format_rfc3339_utc(UNIX_EPOCH + Duration::from_secs(secs))
    }

    #[test]
    fn formats_known_epochs() {
        assert_eq!(at(0), "1970-01-01T00:00:00Z");
        // 2021-01-01T00:00:00Z
        assert_eq!(at(1_609_459_200), "2021-01-01T00:00:00Z");
        // 2001-09-09T01:46:40Z (the classic 1e9 timestamp)
        assert_eq!(at(1_000_000_000), "2001-09-09T01:46:40Z");
        // A leap day: 2020-02-29T12:34:56Z
        assert_eq!(at(1_582_979_696), "2020-02-29T12:34:56Z");
    }

    #[test]
    fn resolves_value_specs() {
        assert_eq!(resolve_value("$literal:gateway", "T"), "gateway");
        assert_eq!(resolve_value("$request_time", "T"), "T");
        assert_eq!(resolve_value("$response_time", "T"), "T");
        assert_eq!(resolve_value("plain", "T"), "plain");
        // $literal preserves a leading `$` in the payload.
        assert_eq!(resolve_value("$literal:$request_time", "T"), "$request_time");
    }

    #[test]
    fn json_get_reads_dot_paths() {
        let v: Value = serde_json::json!({"user": {"id": 7, "name": "ada"}, "top": 1});
        assert_eq!(json_get(&v, "user.id"), Some(&Value::from(7)));
        assert_eq!(json_get(&v, "user.name"), Some(&Value::from("ada")));
        assert_eq!(json_get(&v, "top"), Some(&Value::from(1)));
        assert_eq!(json_get(&v, "user.missing"), None);
        assert_eq!(json_get(&v, "top.nope"), None); // traverse into non-object
    }

    #[test]
    fn json_set_builds_nested_objects() {
        let mut v = Value::Object(Map::new());
        json_set(&mut v, "user.id", Value::from(7));
        json_set(&mut v, "user.name", Value::from("ada"));
        json_set(&mut v, "meta.source", Value::from("gateway"));
        assert_eq!(v, serde_json::json!({
            "user": {"id": 7, "name": "ada"},
            "meta": {"source": "gateway"},
        }));
    }
}
