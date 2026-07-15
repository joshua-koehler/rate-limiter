//! P1 rate-limiting acceptance tests, driving the real gateway binary over HTTP
//! against an in-process mock upstream.
//!
//! The marquee case is `fifty_concurrent_requests_admit_exactly_ten`: it fires
//! 50 simultaneous requests at a `requests: 10` route and asserts EXACTLY 10 are
//! admitted and 40 rejected — the proof that the check-and-increment is race-free
//! (no lost updates) under concurrency. All requests come from loopback, so they
//! share one `per: ip` bucket, which is exactly what makes the count exact.
//!
//! (Per-ip *isolation*, `per: global`, fixed-window reset, sliding-window decay,
//! and eviction are covered by the unit tests in `src/rate_limit.rs`, where we
//! can drive distinct IPs and a controlled clock — impossible over loopback.)

mod common;

use std::time::Duration;

use common::{free_port, get, spawn_gateway, spawn_mock_upstream};

/// A single rate-limited route. `strategy`/`per`/`requests`/`window` are
/// interpolated so each test can shape the policy it needs.
fn limited_config(
    port: u16,
    mock: &str,
    requests: u64,
    window: &str,
    strategy: &str,
    per: &str,
) -> String {
    format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/limited"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
    rate_limit:
      requests: {requests}
      window: "{window}"
      strategy: "{strategy}"
      per: "{per}"
"#
    )
}

#[tokio::test]
async fn fixed_window_rejects_over_limit_with_retry_after() {
    // requests: 3 over a long window so all sends land in the same window.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let cfg = limited_config(port, &mock, 3, "60s", "fixed_window", "ip");
    let gw = spawn_gateway(&cfg, port).await;
    let client = common::client();
    let url = gw.url("/limited");

    // First 3 admitted.
    for i in 0..3 {
        let r = get(&client, &url).await;
        assert_eq!(r.status, 200, "request {i} should be admitted");
    }
    // 4th over the limit -> 429 with a Retry-After header.
    let denied = get(&client, &url).await;
    assert_eq!(denied.status, 429, "4th request should be rate-limited");
    let retry_after = denied
        .header("retry-after")
        .expect("429 must carry a Retry-After header");
    let secs: u64 = retry_after
        .parse()
        .expect("Retry-After should be whole seconds");
    assert!(secs >= 1, "Retry-After should be at least 1s, got {secs}");
    assert!(denied.body_str().contains("rate_limited"));
}

#[tokio::test]
async fn fifty_concurrent_requests_admit_exactly_ten() {
    // THE marquee concurrency test. 50 simultaneous requests, limit 10, one
    // shared (loopback) bucket -> exactly 10 admitted, 40 rejected. Any lost
    // update under the shard lock would let >10 through and fail this.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let cfg = limited_config(port, &mock, 10, "60s", "fixed_window", "ip");
    let gw = spawn_gateway(&cfg, port).await;
    let client = common::client();
    let url = gw.url("/limited");

    let mut handles = Vec::with_capacity(50);
    for _ in 0..50 {
        let c = client.clone();
        let u = url.clone();
        handles.push(tokio::spawn(async move { get(&c, &u).await.status }));
    }

    let mut admitted = 0;
    let mut rejected = 0;
    for h in handles {
        let status = h.await.expect("request task panicked");
        if status == 200 {
            admitted += 1;
        } else if status == 429 {
            rejected += 1;
        } else {
            panic!("unexpected status under load: {status}");
        }
    }

    assert_eq!(admitted, 10, "exactly the limit should be admitted");
    assert_eq!(rejected, 40, "everything over the limit should be 429");
}

#[tokio::test]
async fn sliding_window_capacity_returns_after_window() {
    // 5 per 1s sliding. Exhaust the limit, then after the window fully elapses
    // (generously > 2 windows), capacity returns. Timing kept tolerant so the
    // test is robust across the process boundary and real HTTP.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let cfg = limited_config(port, &mock, 5, "1s", "sliding_window", "ip");
    let gw = spawn_gateway(&cfg, port).await;
    let client = common::client();
    let url = gw.url("/limited");

    // Hammer past the limit; at least one must be rejected.
    let mut saw_429 = false;
    for _ in 0..10 {
        if get(&client, &url).await.status == 429 {
            saw_429 = true;
        }
    }
    assert!(saw_429, "sliding window should reject once the limit is exhausted");

    // Wait out the window(s); a fresh request should be admitted again.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let after = get(&client, &url).await;
    assert_eq!(after.status, 200, "capacity should return after the window elapses");
}

/// Global default plus a per-route override and a route that inherits the global.
fn override_config(port: u16, mock: &str) -> String {
    format!(
        r#"
gateway:
  port: {port}
  global_rate_limit:
    requests: 100
    window: "60s"
    strategy: "fixed_window"
    per: "ip"
routes:
  - path: "/strict"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
    rate_limit:
      requests: 2
      window: "60s"
      strategy: "fixed_window"
      per: "ip"
  - path: "/default"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
"#
    )
}

#[tokio::test]
async fn per_route_limit_overrides_global() {
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let gw = spawn_gateway(&override_config(port, &mock), port).await;
    let client = common::client();

    // /strict overrides the global 100 with a stricter 2: 2 pass, 3rd is 429.
    let strict = gw.url("/strict");
    assert_eq!(get(&client, &strict).await.status, 200);
    assert_eq!(get(&client, &strict).await.status, 200);
    assert_eq!(
        get(&client, &strict).await.status,
        429,
        "route override (2) must apply, not the global (100)"
    );

    // /default inherits the global 100: many requests well under 100 all pass.
    let default = gw.url("/default");
    for i in 0..10 {
        assert_eq!(
            get(&client, &default).await.status,
            200,
            "request {i} to the global-limited route (100) should pass"
        );
    }
}

#[tokio::test]
async fn route_without_any_limit_is_unlimited() {
    // No global, no route limit -> no limiter -> unbounded.
    let mock = spawn_mock_upstream().await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/open"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let client = common::client();
    let url = gw.url("/open");

    for i in 0..25 {
        assert_eq!(
            get(&client, &url).await.status,
            200,
            "request {i} to an unlimited route should always pass"
        );
    }
}
