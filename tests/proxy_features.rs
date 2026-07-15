//! P1 proxy-layer acceptance tests: `strip_prefix` path rewriting (with query
//! preservation and the whole-prefix edge case) and per-attempt timeouts that
//! map to 504, including route-over-global precedence.
//! Each test spawns the real gateway binary + an in-process mock upstream.

mod common;

use common::{free_port, get, send, spawn_gateway, spawn_mock_upstream};
use hyper::Method;

/// One strip route and one non-strip route over the same mock, so a single
/// config exercises both forwarding modes.
fn strip_config(port: u16, mock: &str) -> String {
    format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/api/products"
    methods: ["GET"]
    strip_prefix: true
    upstream:
      url: "{mock}"
  - path: "/api/legacy"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
"#
    )
}

#[tokio::test]
async fn strip_prefix_true_rewrites_path_and_preserves_query() {
    // P1.2: with strip_prefix the matched prefix is dropped and the router tail
    // is forwarded; the original query must survive the rewrite.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&strip_config(port, &mock), port).await;
    let client = common::client();

    let resp = get(&client, &gw.url("/api/products/123")).await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("x-echo-path").as_deref(),
        Some("/123"),
        "prefix stripped, tail forwarded"
    );

    let with_query = get(&client, &gw.url("/api/products/123?q=1")).await;
    assert_eq!(with_query.status, 200);
    assert_eq!(
        with_query.header("x-echo-path").as_deref(),
        Some("/123?q=1"),
        "query preserved across strip"
    );
}

#[tokio::test]
async fn strip_prefix_whole_path_yields_root() {
    // P1.2 edge: requesting exactly the prefix strips down to "/".
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&strip_config(port, &mock), port).await;
    let client = common::client();

    let resp = get(&client, &gw.url("/api/products")).await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("x-echo-path").as_deref(),
        Some("/"),
        "exact-prefix match strips to root"
    );

    // With a query but no extra path, the tail is still "/".
    let with_query = get(&client, &gw.url("/api/products?q=1")).await;
    assert_eq!(with_query.status, 200);
    assert_eq!(with_query.header("x-echo-path").as_deref(), Some("/?q=1"));
}

#[tokio::test]
async fn strip_prefix_false_forwards_original_path() {
    // P1.2: without strip_prefix the full original path+query is forwarded.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&strip_config(port, &mock), port).await;
    let client = common::client();

    let resp = get(&client, &gw.url("/api/legacy/v1/data?a=b")).await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("x-echo-path").as_deref(),
        Some("/api/legacy/v1/data?a=b"),
        "original path + query forwarded unchanged"
    );
}

/// A route with a short per-route timeout; the mock is told to sleep longer via
/// the `x-mock-sleep-ms` request header to force the timeout to fire.
fn timeout_config(port: u16, mock: &str) -> String {
    format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/slow"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
      timeout: "500ms"
"#
    )
}

#[tokio::test]
async fn upstream_timeout_returns_504_and_fast_request_passes() {
    // P1.3: mock sleeps past the 500ms timeout -> 504; a fast call -> 200.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&timeout_config(port, &mock), port).await;
    let client = common::client();

    let slow = send(
        &client,
        Method::GET,
        &gw.url("/slow"),
        &[("x-mock-sleep-ms", "1500")],
        "",
    )
    .await;
    assert_eq!(slow.status, 504, "upstream exceeded timeout");
    assert!(slow.body_str().contains("gateway_timeout"), "{}", slow.body_str());

    let fast = get(&client, &gw.url("/slow")).await;
    assert_eq!(fast.status, 200, "fast request under the timeout");
}

/// A short route-level timeout alongside a long global timeout, so the response
/// proves route beats global in the precedence chain.
fn precedence_config(port: u16, mock: &str) -> String {
    format!(
        r#"
gateway:
  port: {port}
  global_timeout: "30s"
routes:
  - path: "/slow"
    methods: ["GET"]
    strip_prefix: false
    timeout: "500ms"
    upstream:
      url: "{mock}"
"#
    )
}

#[tokio::test]
async fn route_timeout_overrides_longer_global_timeout() {
    // P1.3 precedence: route.timeout (500ms) wins over global (30s), so a 1500ms
    // upstream still 504s rather than waiting on the global budget.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&precedence_config(port, &mock), port).await;
    let client = common::client();

    let slow = send(
        &client,
        Method::GET,
        &gw.url("/slow"),
        &[("x-mock-sleep-ms", "1500")],
        "",
    )
    .await;
    assert_eq!(slow.status, 504, "route timeout overrides longer global");
}
