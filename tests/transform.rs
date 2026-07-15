//! P3 acceptance tests: request/response transforms (header add/remove, body
//! mapping, response enveloping, dynamic `$request_time`/`$response_time`/
//! `$route_path` values). Each test spawns the real gateway binary against the
//! shared in-process mock upstream (see `tests/common/mod.rs`).
//!
//! The mock reflects every forwarded request header back as
//! `echo-req-<lowercased-name>` and always emits `Server`/`X-Powered-By`
//! response headers, so request-header and response-header transforms are both
//! observable from the client side.

mod common;

use common::{client, free_port, get, send, spawn_gateway, spawn_mock_upstream};
use hyper::Method;
use serde_json::Value;

/// Build a gateway config with a single route body (`route`) pointed at `mock`.
/// `route` is the YAML for one list entry under `routes:` (already indented for
/// a `-` item at two spaces), keeping each test's config compact and readable.
fn config(port: u16, route: &str) -> String {
    format!(
        r#"
gateway:
  port: {port}
routes:
{route}
"#
    )
}

/// Assert a string looks like an RFC-3339 UTC timestamp (e.g.
/// `2026-07-15T12:34:56Z`): contains 'T', ends with 'Z', ~20 chars.
fn looks_like_rfc3339_utc(s: &str) {
    assert!(s.contains('T'), "expected RFC-3339 timestamp, got {s:?}");
    assert!(s.ends_with('Z'), "expected UTC 'Z' suffix, got {s:?}");
    assert!(
        (19..=30).contains(&s.len()),
        "expected ~20-char timestamp, got {s:?} (len {})",
        s.len()
    );
}

#[tokio::test]
async fn request_header_add() {
    // request_transform adds a static header and a dynamic `$request_time`; the
    // mock reflects both back as `echo-req-*` so we can observe the forward.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let route = format!(
        r#"  - path: "/add"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
    request_transform:
      headers:
        add:
          X-Gateway: "gatewaykit"
          X-Request-Start: "$request_time""#
    );
    let gw = spawn_gateway(&config(port, &route), port).await;
    let cl = client();

    let resp = get(&cl, &gw.url("/add")).await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("echo-req-x-gateway").as_deref(),
        Some("gatewaykit"),
        "static request header forwarded to upstream"
    );
    let ts = resp
        .header("echo-req-x-request-start")
        .expect("dynamic $request_time header forwarded");
    looks_like_rfc3339_utc(&ts);
}

#[tokio::test]
async fn request_header_remove() {
    // Client sends X-Debug/X-Internal; request_transform removes them, so the
    // mock must not observe them (no echo-req-* reflection).
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let route = format!(
        r#"  - path: "/remove"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
    request_transform:
      headers:
        remove: ["X-Debug", "X-Internal"]"#
    );
    let gw = spawn_gateway(&config(port, &route), port).await;
    let cl = client();

    let resp = send(
        &cl,
        Method::GET,
        &gw.url("/remove"),
        &[("X-Debug", "1"), ("X-Internal", "secret")],
        "",
    )
    .await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("echo-req-x-debug"),
        None,
        "X-Debug stripped before forwarding"
    );
    assert_eq!(
        resp.header("echo-req-x-internal"),
        None,
        "X-Internal stripped before forwarding"
    );
}

/// A route whose request_transform remaps the JSON body per the spec example.
fn body_mapping_route(mock: &str) -> String {
    format!(
        r#"  - path: "/map"
    methods: ["POST"]
    strip_prefix: false
    upstream:
      url: "{mock}"
    request_transform:
      body:
        mapping:
          user.id: "userId"
          user.name: "userName"
          meta.source: "$literal:gateway"
          meta.timestamp: "$request_time""#
    )
}

#[tokio::test]
async fn request_body_mapping() {
    // The mock echoes the forwarded (transformed) body back as its response
    // body, so parsing the client response shows the remapped JSON.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&config(port, &body_mapping_route(&mock)), port).await;
    let cl = client();

    let resp = send(
        &cl,
        Method::POST,
        &gw.url("/map"),
        &[("content-type", "application/json")],
        r#"{"userId":"u1","userName":"Ada","extra":9}"#,
    )
    .await;
    assert_eq!(resp.status, 200);

    let v: Value = serde_json::from_slice(&resp.body).expect("forwarded body is JSON");
    assert_eq!(v["user"]["id"], "u1");
    assert_eq!(v["user"]["name"], "Ada");
    assert_eq!(v["meta"]["source"], "gateway");
    let ts = v["meta"]["timestamp"]
        .as_str()
        .expect("meta.timestamp is a string");
    assert!(!ts.is_empty(), "meta.timestamp present and non-empty");
    assert!(v.get("extra").is_none(), "unmapped `extra` dropped");
}

#[tokio::test]
async fn request_body_non_json_passthrough() {
    // A non-JSON request body cannot be remapped; it must pass through unchanged.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&config(port, &body_mapping_route(&mock)), port).await;
    let cl = client();

    let resp = send(
        &cl,
        Method::POST,
        &gw.url("/map"),
        &[],
        "not-json-at-all",
    )
    .await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.body_str(),
        "not-json-at-all",
        "non-JSON body passes through the mapping unchanged"
    );
}

#[tokio::test]
async fn response_header_add_remove() {
    // response_transform adds X-Served-By and strips the mock's Server /
    // X-Powered-By; assertions are on the CLIENT response headers.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let route = format!(
        r#"  - path: "/resp-hdr"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
    response_transform:
      headers:
        add:
          X-Served-By: "gatewaykit"
        remove: ["Server", "X-Powered-By"]"#
    );
    let gw = spawn_gateway(&config(port, &route), port).await;
    let cl = client();

    let resp = get(&cl, &gw.url("/resp-hdr")).await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("x-served-by").as_deref(),
        Some("gatewaykit"),
        "response header added"
    );
    assert_eq!(resp.header("server"), None, "Server stripped");
    assert_eq!(resp.header("x-powered-by"), None, "X-Powered-By stripped");
}

/// A route that wraps the upstream response in the spec's envelope.
fn envelope_route(mock: &str, methods: &str) -> String {
    format!(
        r#"  - path: "/env"
    methods: {methods}
    strip_prefix: false
    upstream:
      url: "{mock}"
    response_transform:
      body:
        envelope:
          data: "$body"
          gateway_metadata:
            served_at: "$response_time"
            route: "$route_path""#
    )
}

#[tokio::test]
async fn response_body_envelope() {
    // The mock echoes the request body, so POSTing JSON makes the "upstream
    // response body" be that JSON; the envelope wraps it under `data`.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&config(port, &envelope_route(&mock, r#"["POST"]"#)), port).await;
    let cl = client();

    let resp = send(
        &cl,
        Method::POST,
        &gw.url("/env"),
        &[("content-type", "application/json")],
        r#"{"hello":"world"}"#,
    )
    .await;
    assert_eq!(resp.status, 200);

    let v: Value = serde_json::from_slice(&resp.body).expect("envelope is JSON");
    assert_eq!(v["data"], serde_json::json!({"hello": "world"}));
    assert_eq!(
        v["gateway_metadata"]["route"], "/env",
        "route path recorded in envelope"
    );
    let served_at = v["gateway_metadata"]["served_at"]
        .as_str()
        .expect("served_at is a timestamp string");
    assert!(!served_at.is_empty(), "served_at present and non-empty");
}

#[tokio::test]
async fn response_envelope_non_json_body() {
    // Empty request body → the mock replies with plain-text `mock-upstream-ok`.
    // The envelope embeds a non-JSON upstream body as a JSON STRING under `data`.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&config(port, &envelope_route(&mock, r#"["GET"]"#)), port).await;
    let cl = client();

    let resp = get(&cl, &gw.url("/env")).await;
    assert_eq!(resp.status, 200, "non-JSON upstream body still enveloped, not 500");

    let v: Value = serde_json::from_slice(&resp.body).expect("envelope is JSON");
    assert_eq!(
        v["data"],
        Value::String("mock-upstream-ok".to_string()),
        "non-JSON body embedded as a JSON string"
    );
}

#[tokio::test]
async fn envelope_not_applied_to_gateway_errors() {
    // The envelope route only allows GET; a POST is a gateway-generated 405 and
    // must NOT be wrapped — proving the envelope skips gateway errors.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&config(port, &envelope_route(&mock, r#"["GET"]"#)), port).await;
    let cl = client();

    let resp = send(&cl, Method::POST, &gw.url("/env"), &[], "").await;
    assert_eq!(resp.status, 405);

    let v: Value = serde_json::from_slice(&resp.body).expect("error body is JSON");
    assert_eq!(v["error"], "method_not_allowed", "plain gateway error body");
    assert!(v.get("data").is_none(), "gateway error not enveloped (no data)");
    assert!(
        v.get("gateway_metadata").is_none(),
        "gateway error not enveloped (no gateway_metadata)"
    );
}
