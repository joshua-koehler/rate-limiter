//! P0 acceptance tests: config boots + binds, /health shape, faithful proxying,
//! 404 on unmatched, 405 on wrong method, and longest-prefix route matching.
//! Each test spawns the real gateway binary + an in-process mock upstream.

mod common;

use common::{free_port, get, send, spawn_gateway, spawn_mock_upstream};
use hyper::Method;

/// Two routes: an echo route (GET+POST) and a GET-only route.
fn basic_config(port: u16, mock: &str) -> String {
    format!(
        r#"
gateway:
  port: {port}
  global_timeout: "30s"
routes:
  - path: "/api/echo"
    methods: ["GET", "POST"]
    strip_prefix: false
    upstream:
      url: "{mock}"
  - path: "/api/get-only"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
"#
    )
}

#[tokio::test]
async fn health_endpoint_returns_expected_shape() {
    // P0.1 boot/bind + P0.2 health. spawn_gateway already waits on /health,
    // so reaching this point proves the configured port is bound.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&basic_config(port, &mock), port).await;
    let client = common::client();

    let resp = get(&client, &gw.url("/health")).await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("content-type").as_deref(),
        Some("application/json")
    );
    let body = resp.body_str();
    assert!(body.contains("\"status\":\"healthy\""), "body: {body}");
    assert!(body.contains("\"uptime_seconds\":"), "body: {body}");
    // uptime is an integer immediately after the key.
    let after = body.split("\"uptime_seconds\":").nth(1).unwrap();
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    assert!(!digits.is_empty(), "uptime not an int: {body}");
}

#[tokio::test]
async fn proxies_and_relays_status_headers_body_faithfully() {
    // P0.3 proxying: status + headers + body all relayed, and method + path are
    // forwarded to the upstream unchanged (P0 forwards the original path).
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&basic_config(port, &mock), port).await;
    let client = common::client();

    let resp = send(
        &client,
        Method::POST,
        &gw.url("/api/echo?q=1"),
        &[("x-mock-status", "201")],
        "payload-123",
    )
    .await;

    assert_eq!(resp.status, 201, "upstream status relayed");
    assert_eq!(
        resp.header("x-upstream").as_deref(),
        Some("mock-upstream"),
        "upstream response header relayed"
    );
    assert_eq!(resp.header("x-echo-method").as_deref(), Some("POST"));
    assert_eq!(resp.header("x-echo-path").as_deref(), Some("/api/echo?q=1"));
    assert_eq!(resp.body_str(), "payload-123", "body relayed faithfully");
}

#[tokio::test]
async fn unmatched_path_returns_404() {
    // P0.3 unmatched -> 404.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&basic_config(port, &mock), port).await;
    let client = common::client();

    let resp = get(&client, &gw.url("/nothing/here")).await;
    assert_eq!(resp.status, 404);
    assert!(resp.body_str().contains("not_found"));
}

#[tokio::test]
async fn wrong_method_returns_405_with_allow_header() {
    // P0.4 method filter: POST to a GET-only route -> 405; GET passes.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&basic_config(port, &mock), port).await;
    let client = common::client();

    let denied = send(&client, Method::POST, &gw.url("/api/get-only"), &[], "").await;
    assert_eq!(denied.status, 405);
    let allow = denied.header("allow").unwrap_or_default();
    assert!(allow.contains("GET"), "Allow header: {allow}");

    let allowed = get(&client, &gw.url("/api/get-only")).await;
    assert_eq!(allowed.status, 200);
}

/// Overlapping prefixes with disjoint methods let us prove *which* route was
/// selected purely from the status code.
fn overlap_config(port: u16, mock: &str) -> String {
    format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/api"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
  - path: "/api/special"
    methods: ["POST"]
    strip_prefix: false
    upstream:
      url: "{mock}"
"#
    )
}

#[tokio::test]
async fn longest_prefix_match_selects_the_more_specific_route() {
    // P0.3 route matching = longest matching prefix.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&overlap_config(port, &mock), port).await;
    let client = common::client();

    // POST /api/special -> longest match is "/api/special" (POST allowed) -> 200.
    // If it had wrongly matched "/api" (GET only), this would be 405.
    let specific = send(&client, Method::POST, &gw.url("/api/special"), &[], "").await;
    assert_eq!(specific.status, 200, "should match /api/special");

    // GET /api/special -> still the specific route (POST-only) -> 405.
    // If "/api" had matched, GET would be allowed (200); 405 proves specificity.
    let specific_get = get(&client, &gw.url("/api/special")).await;
    assert_eq!(specific_get.status, 405, "still the specific route");

    // GET /api/other -> falls back to the shorter "/api" prefix -> 200.
    let shorter = get(&client, &gw.url("/api/other")).await;
    assert_eq!(shorter.status, 200, "should match /api");
}
