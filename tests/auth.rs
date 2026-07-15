//! P2.1 api_key auth acceptance tests, driving the real gateway binary over HTTP
//! against an in-process mock upstream.
//!
//! Covers: a valid key proxies through to upstream; missing header and wrong key
//! both yield `401 {"error":"unauthorized"}`; a route without `auth` is
//! unaffected; and auth runs BEFORE rate limiting (a bad key on an authed +
//! strictly-rate-limited route returns 401, never 429).

mod common;

use common::{free_port, get, send, spawn_gateway, spawn_mock_upstream};
use hyper::Method;

/// One authed route (`/secure`, header `X-API-Key`, two valid keys) and one open
/// route (`/open`, no auth) over the same mock, so a single config exercises both
/// "auth applies" and "auth doesn't touch routes that don't declare it".
fn auth_config(port: u16, mock: &str) -> String {
    format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/secure"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
    auth:
      type: "api_key"
      header: "X-API-Key"
      keys: ["sk_live_abc123", "sk_live_def456"]
  - path: "/open"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
"#
    )
}

#[tokio::test]
async fn valid_key_reaches_upstream() {
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&auth_config(port, &mock), port).await;
    let client = common::client();

    let resp = send(
        &client,
        Method::GET,
        &gw.url("/secure"),
        &[("X-API-Key", "sk_live_abc123")],
        "",
    )
    .await;
    assert_eq!(resp.status, 200, "a valid key should proxy through");
    assert_eq!(
        resp.header("x-upstream").as_deref(),
        Some("mock-upstream"),
        "response should be the upstream's (echo header present)"
    );

    // The second configured key must work too.
    let resp2 = send(
        &client,
        Method::GET,
        &gw.url("/secure"),
        &[("X-API-Key", "sk_live_def456")],
        "",
    )
    .await;
    assert_eq!(resp2.status, 200, "the other valid key should also pass");
}

#[tokio::test]
async fn missing_header_is_unauthorized() {
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&auth_config(port, &mock), port).await;
    let client = common::client();

    let resp = get(&client, &gw.url("/secure")).await;
    assert_eq!(resp.status, 401, "missing key header must be 401");
    assert_eq!(
        resp.body_str(),
        "{\"error\":\"unauthorized\"}",
        "401 body must be the unauthorized envelope"
    );
}

#[tokio::test]
async fn invalid_key_is_unauthorized() {
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&auth_config(port, &mock), port).await;
    let client = common::client();

    let resp = send(
        &client,
        Method::GET,
        &gw.url("/secure"),
        &[("X-API-Key", "sk_live_not_a_real_key")],
        "",
    )
    .await;
    assert_eq!(resp.status, 401, "a wrong key must be 401");
    assert!(resp.body_str().contains("unauthorized"));
}

#[tokio::test]
async fn route_without_auth_is_unaffected() {
    // Proves auth is assembled only for routes that declare it: /open has no
    // `auth` block, so it proxies with no credential at all.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&auth_config(port, &mock), port).await;
    let client = common::client();

    let resp = get(&client, &gw.url("/open")).await;
    assert_eq!(resp.status, 200, "a route without auth needs no key");
    assert_eq!(resp.header("x-upstream").as_deref(), Some("mock-upstream"));
}

#[tokio::test]
async fn auth_runs_before_rate_limit() {
    // A route with BOTH auth and a strict (requests: 1) rate_limit. A bad key
    // must return 401, not 429 — proving the auth stage runs ahead of the
    // rate-limit stage (DECISIONS.md ordering). We fire several bad-key requests;
    // if rate limiting ran first, some would come back 429.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/guarded"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
    auth:
      type: "api_key"
      header: "X-API-Key"
      keys: ["sk_live_abc123"]
    rate_limit:
      requests: 1
      window: "60s"
      strategy: "fixed_window"
      per: "ip"
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let client = common::client();
    let url = gw.url("/guarded");

    for i in 0..5 {
        let resp = send(
            &client,
            Method::GET,
            &url,
            &[("X-API-Key", "wrong-key")],
            "",
        )
        .await;
        assert_eq!(
            resp.status, 401,
            "request {i}: bad key must be 401 (auth before rate limit), never 429"
        );
    }
}
