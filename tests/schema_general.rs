//! P0.5 schema-general + P0.1 fail-fast tests.
//!
//! Proves the gateway works with a *different* config using the same schema
//! (different routes/paths/port, all optional P1–P3 blocks present so a full
//! config parses), and that malformed/invalid configs fail fast with a
//! non-zero exit and a readable message rather than booting half-configured.

mod common;

use common::{free_port, get, run_binary_expecting_exit, send, spawn_gateway, spawn_mock_upstream};
use hyper::Method;

/// A config that differs from the P0 core tests in every way (paths, port,
/// methods) and exercises *every* optional block to prove they all parse.
fn kitchen_sink_config(port: u16, mock: &str) -> String {
    format!(
        r#"
gateway:
  port: {port}
  global_timeout: "10s"
  global_rate_limit:
    requests: 100
    window: "60s"
    strategy: "fixed_window"
    per: "ip"
routes:
  - path: "/v2/widgets"
    methods: ["GET", "PUT"]
    strip_prefix: true
    upstream:
      targets:
        - url: "{mock}"
          weight: 3
        - url: "{mock}"
          weight: 1
      balance: "weighted_round_robin"
      timeout: "5s"
      health_check:
        path: "/healthz"
        interval: "30s"
        unhealthy_threshold: 3
  - path: "/secure/data"
    methods: ["POST"]
    strip_prefix: false
    upstream:
      url: "{mock}"
    auth:
      type: "api_key"
      header: "X-API-Key"
      keys: ["sk_test_1", "sk_test_2"]
    retry:
      attempts: 3
      backoff: "exponential"
      initial_delay: "1s"
      on: [502, 503, 504]
    circuit_breaker:
      threshold: 5
      window: "60s"
      cooldown: "30s"
    rate_limit:
      requests: 10
      window: "10s"
      strategy: "sliding_window"
      per: "ip"
    request_transform:
      headers:
        add:
          X-Gateway: "gatewaykit"
        remove: ["X-Debug"]
      body:
        mapping:
          user.id: "userId"
    response_transform:
      headers:
        add:
          X-Served-By: "gatewaykit"
        remove: ["Server"]
      body:
        envelope:
          data: "$body"
          gateway_metadata:
            route: "$route_path"
"#
    )
}

#[tokio::test]
async fn second_config_with_different_routes_and_all_optional_blocks_works() {
    // Booting at all proves every optional P1–P3 block parses (P0.5); the
    // requests prove routing/method/proxy work against a brand-new schema-valid
    // config with no code changes.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&kitchen_sink_config(port, &mock), port).await;
    let client = common::client();

    // Route defined via `targets` (LB unimplemented in P0) still proxies.
    let widgets = get(&client, &gw.url("/v2/widgets/42")).await;
    assert_eq!(widgets.status, 200);
    assert_eq!(widgets.header("x-echo-method").as_deref(), Some("GET"));

    // Method filtering on the new routes.
    let bad_method = send(&client, Method::DELETE, &gw.url("/v2/widgets"), &[], "").await;
    assert_eq!(bad_method.status, 405);

    // A route carrying auth/rate_limit/CB/transform blocks proxies in P0
    // (those policies are enforced by later tiers; parsing them is P0.5).
    let secure = send(&client, Method::POST, &gw.url("/secure/data"), &[], "hi").await;
    assert_eq!(secure.status, 200);
    assert_eq!(secure.body_str(), "hi");

    // /health remains available regardless of the new config.
    assert_eq!(get(&client, &gw.url("/health")).await.status, 200);
}

#[test]
fn malformed_yaml_fails_fast_with_nonzero_exit() {
    // P0.1: broken YAML -> non-zero exit + readable stderr, no server.
    let out = run_binary_expecting_exit("gateway:\n  port: 8080\n  this: : : broken");
    assert!(!out.status.success(), "should exit non-zero on broken YAML");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.trim().is_empty(), "should print an error message");
}

#[test]
fn unknown_enum_value_fails_fast() {
    // P0.5: unknown enum values are rejected at load.
    let cfg = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream:
      url: "http://localhost:1"
    rate_limit:
      requests: 1
      window: "1s"
      strategy: "not_a_real_strategy"
      per: "ip"
"#;
    let out = run_binary_expecting_exit(cfg);
    assert!(!out.status.success(), "unknown enum should fail fast");
}

#[test]
fn missing_required_field_fails_fast() {
    // P0.1: a route without an upstream is rejected before binding.
    let cfg = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
"#;
    let out = run_binary_expecting_exit(cfg);
    assert!(!out.status.success(), "missing upstream should fail fast");
}

#[test]
fn unparseable_duration_fails_fast() {
    // P0.1: bad duration string rejected at load.
    let cfg = "gateway:\n  port: 8080\n  global_timeout: \"soon\"\nroutes: []\n";
    let out = run_binary_expecting_exit(cfg);
    assert!(!out.status.success(), "bad duration should fail fast");
}
