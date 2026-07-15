//! P2 resilience acceptance tests: load balancing (round-robin + smooth weighted
//! round-robin), retry/backoff, circuit breaker (trip → 503 envelope → recover),
//! and active + passive health ejection.
//!
//! Each test spawns the real gateway binary (via `common::spawn_gateway`) against
//! its own tailored in-process mock upstreams. The shared `common` mock is too
//! plain for these (it can't count hits, fail-then-succeed, identify itself, or
//! toggle health), so this file spins up its own hyper mock servers backed by
//! atomics — copying the `spawn_mock_upstream` pattern but with the knobs each
//! resilience feature needs.

mod common;

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use common::{client, free_port, get, spawn_gateway};

// ── Tailored mock upstream ───────────────────────────────────────────────────

/// Runtime knobs for a mock upstream, shared with the test via `Arc` so a test
/// can flip behaviour (status, health) while the gateway is running.
struct Ctl {
    /// Status returned on normal (non-`/healthz`) paths once past `flaky_fails`.
    status: AtomicU16,
    /// The first N normal-path requests return 503 regardless of `status`
    /// (models a target that fails then recovers — for retry tests).
    flaky_fails: AtomicU64,
    /// Count of normal-path requests actually served (probes excluded).
    hits: AtomicU64,
    /// `/healthz` returns 200 when true, 503 when false.
    healthy: AtomicBool,
}

impl Ctl {
    fn new() -> Arc<Self> {
        Arc::new(Ctl {
            status: AtomicU16::new(200),
            flaky_fails: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
        })
    }
    fn with_status(status: u16) -> Arc<Self> {
        let c = Ctl::new();
        c.status.store(status, Ordering::SeqCst);
        c
    }
}

/// Spawn a mock upstream tagged with `id` (echoed in the `x-server` response
/// header so distribution tests can tell targets apart). Returns its base URL.
async fn spawn_mock(id: &'static str, ctl: Arc<Ctl>) -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let ctl = ctl.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| handle(req, id, ctl.clone())),
                    )
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

async fn handle(
    req: Request<Incoming>,
    id: &'static str,
    ctl: Arc<Ctl>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    let _ = req.into_body().collect().await; // drain

    // Health probes are answered from the toggle and never count as hits.
    if path.ends_with("/healthz") {
        let code = if ctl.healthy.load(Ordering::SeqCst) {
            200
        } else {
            503
        };
        return Ok(Response::builder()
            .status(code)
            .header("x-server", id)
            .body(Full::new(Bytes::from_static(b"health")))
            .unwrap());
    }

    let hit = ctl.hits.fetch_add(1, Ordering::SeqCst);
    let fails = ctl.flaky_fails.load(Ordering::SeqCst);
    let code = if hit < fails {
        503
    } else {
        ctl.status.load(Ordering::SeqCst)
    };
    Ok(Response::builder()
        .status(code)
        .header("x-server", id)
        .body(Full::new(Bytes::from(format!("{id}:{code}"))))
        .unwrap())
}

/// Pull `retry_after` out of a `{"error":"service_unavailable","retry_after":N}`
/// body. Panics if absent — the tests only call it on circuit-open responses.
fn parse_retry_after(body: &str) -> u64 {
    let key = "\"retry_after\":";
    let start = body.find(key).expect("retry_after in body") + key.len();
    let rest = &body[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().expect("numeric retry_after")
}

// ── Load balancing ───────────────────────────────────────────────────────────

#[tokio::test]
async fn round_robin_splits_evenly_and_ignores_weight() {
    // Weights 3:1 but `round_robin` must ignore them → ~even split across A/B.
    let a = spawn_mock("A", Ctl::new()).await;
    let b = spawn_mock("B", Ctl::new()).await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/lb"
    methods: ["GET"]
    upstream:
      balance: "round_robin"
      targets:
        - url: "{a}"
          weight: 3
        - url: "{b}"
          weight: 1
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    let mut count_a = 0;
    let mut count_b = 0;
    for _ in 0..40 {
        let resp = get(&cl, &gw.url("/lb")).await;
        assert_eq!(resp.status, 200);
        match resp.header("x-server").as_deref() {
            Some("A") => count_a += 1,
            Some("B") => count_b += 1,
            other => panic!("unexpected server tag {other:?}"),
        }
    }
    assert_eq!(count_a, 20, "round-robin ignores weight → even split");
    assert_eq!(count_b, 20, "round-robin ignores weight → even split");
}

#[tokio::test]
async fn weighted_round_robin_splits_by_weight() {
    // 3:1 weights → target A served 3× as often as B.
    let a = spawn_mock("A", Ctl::new()).await;
    let b = spawn_mock("B", Ctl::new()).await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/lb"
    methods: ["GET"]
    upstream:
      balance: "weighted_round_robin"
      targets:
        - url: "{a}"
          weight: 3
        - url: "{b}"
          weight: 1
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    let mut count_a = 0;
    let mut count_b = 0;
    for _ in 0..40 {
        let resp = get(&cl, &gw.url("/lb")).await;
        assert_eq!(resp.status, 200);
        match resp.header("x-server").as_deref() {
            Some("A") => count_a += 1,
            Some("B") => count_b += 1,
            other => panic!("unexpected server tag {other:?}"),
        }
    }
    // Smooth WRR is deterministic given all-healthy targets: exactly 30/10.
    assert_eq!(count_a, 30, "weight-3 target gets 3/4 of traffic");
    assert_eq!(count_b, 10, "weight-1 target gets 1/4 of traffic");
}

// ── Retry / backoff ──────────────────────────────────────────────────────────

#[tokio::test]
async fn retry_recovers_after_transient_failure() {
    // First upstream call 503, second 200; retry on [503] with 2 attempts → the
    // client sees the eventual 200.
    let ctl = Ctl::new();
    ctl.flaky_fails.store(1, Ordering::SeqCst);
    let up = spawn_mock("single", ctl.clone()).await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/flaky"
    methods: ["GET"]
    upstream:
      url: "{up}"
    retry:
      attempts: 2
      backoff: "fixed"
      initial_delay: "50ms"
      on: [503]
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    let resp = get(&cl, &gw.url("/flaky")).await;
    assert_eq!(resp.status, 200, "retry turned a 503 into a 200");
    assert_eq!(ctl.hits.load(Ordering::SeqCst), 2, "took exactly two tries");
}

#[tokio::test]
async fn retry_observes_backoff_delay() {
    // One failure then success, fixed 300ms backoff → the round trip must span at
    // least the backoff.
    let ctl = Ctl::new();
    ctl.flaky_fails.store(1, Ordering::SeqCst);
    let up = spawn_mock("single", ctl).await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/flaky"
    methods: ["GET"]
    upstream:
      url: "{up}"
    retry:
      attempts: 2
      backoff: "fixed"
      initial_delay: "300ms"
      on: [503]
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    let t0 = Instant::now();
    let resp = get(&cl, &gw.url("/flaky")).await;
    let elapsed = t0.elapsed();
    assert_eq!(resp.status, 200);
    assert!(
        elapsed >= Duration::from_millis(300),
        "observed backoff {elapsed:?} < initial_delay"
    );
}

#[tokio::test]
async fn retry_exhaustion_surfaces_last_upstream_error() {
    // Upstream always 503; 2 attempts both fail → the client gets the relayed
    // upstream 503 (no circuit breaker configured, so not a circuit-open body).
    let up = spawn_mock("single", Ctl::with_status(503)).await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/down"
    methods: ["GET"]
    upstream:
      url: "{up}"
    retry:
      attempts: 2
      backoff: "fixed"
      initial_delay: "20ms"
      on: [503]
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    let resp = get(&cl, &gw.url("/down")).await;
    assert_eq!(resp.status, 503, "exhausted retries surface the last 503");
    assert_eq!(resp.body_str(), "single:503", "relayed upstream body, not a gateway envelope");
}

// ── Circuit breaker ──────────────────────────────────────────────────────────

#[tokio::test]
async fn circuit_breaker_trips_then_recovers() {
    // threshold 3, cooldown 3s. Three upstream 503s trip the breaker; the next
    // request is short-circuited with the spec's 503 envelope; after the cooldown
    // a half-open probe against a now-healthy upstream closes it again.
    let ctl = Ctl::with_status(503);
    let up = spawn_mock("cb", ctl.clone()).await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/cb"
    methods: ["GET"]
    upstream:
      url: "{up}"
    circuit_breaker:
      threshold: 3
      window: "60s"
      cooldown: "3s"
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    // Three failing requests (relayed upstream 503s) trip the breaker.
    for _ in 0..3 {
        let resp = get(&cl, &gw.url("/cb")).await;
        assert_eq!(resp.status, 503);
        assert_eq!(resp.body_str(), "cb:503", "relayed upstream 503 while closed");
    }

    // Now Open: rejected without hitting upstream, with the circuit-open envelope.
    let open1 = get(&cl, &gw.url("/cb")).await;
    assert_eq!(open1.status, 503);
    assert!(
        open1.body_str().contains("service_unavailable"),
        "{}",
        open1.body_str()
    );
    let ra1 = parse_retry_after(&open1.body_str());
    let hits_after_trip = ctl.hits.load(Ordering::SeqCst);
    assert_eq!(hits_after_trip, 3, "open breaker does not contact upstream");

    // retry_after decreases as the cooldown elapses.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let open2 = get(&cl, &gw.url("/cb")).await;
    assert_eq!(open2.status, 503);
    let ra2 = parse_retry_after(&open2.body_str());
    assert!(ra2 < ra1, "retry_after should decrease ({ra1} -> {ra2})");

    // Heal the upstream, wait out the cooldown, and the half-open probe recovers.
    ctl.status.store(200, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let recovered = get(&cl, &gw.url("/cb")).await;
    assert_eq!(recovered.status, 200, "half-open probe closed the breaker");
    assert_eq!(recovered.body_str(), "cb:200");
}

#[tokio::test]
async fn passive_ejection_removes_a_failing_target_via_its_breaker() {
    // Two targets, A healthy and B always-503, breaker threshold 2, round-robin.
    // B's live failures trip its breaker; thereafter every request fails over to
    // A. Exactly `threshold` 503s escape before B is ejected.
    let a = spawn_mock("A", Ctl::new()).await;
    let b = spawn_mock("B", Ctl::with_status(503)).await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/pe"
    methods: ["GET"]
    upstream:
      balance: "round_robin"
      targets:
        - url: "{a}"
        - url: "{b}"
    circuit_breaker:
      threshold: 2
      window: "60s"
      cooldown: "30s"
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    let mut fails = 0;
    let mut a_hits = 0;
    for _ in 0..12 {
        let resp = get(&cl, &gw.url("/pe")).await;
        match resp.header("x-server").as_deref() {
            Some("A") => {
                assert_eq!(resp.status, 200);
                a_hits += 1;
            }
            Some("B") => {
                assert_eq!(resp.status, 503);
                fails += 1;
            }
            other => panic!("unexpected server {other:?}"),
        }
    }
    assert_eq!(fails, 2, "B ejected after `threshold` live failures");
    assert_eq!(a_hits, 10, "remaining traffic fails over to the healthy target");
}

#[tokio::test]
async fn passive_ejection_works_with_health_check_but_no_breaker() {
    // The spec's `/api/products` shape: a load-balanced route with `health_check`
    // and NO `circuit_breaker`. B fails live traffic while its `/healthz` still
    // answers 200 (so active probing alone would never eject it). A long probe
    // interval keeps the prober from firing within the test, isolating the
    // *passive* path: B's live failures must eject it after `unhealthy_threshold`.
    let a = spawn_mock("A", Ctl::new()).await;
    let b = spawn_mock("B", Ctl::with_status(503)).await; // healthz stays 200
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/pe2"
    methods: ["GET"]
    upstream:
      balance: "round_robin"
      targets:
        - url: "{a}"
        - url: "{b}"
      health_check:
        path: "/healthz"
        interval: "60s"
        unhealthy_threshold: 2
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    let mut fails = 0;
    let mut a_hits = 0;
    for _ in 0..12 {
        let resp = get(&cl, &gw.url("/pe2")).await;
        match resp.header("x-server").as_deref() {
            Some("A") => {
                assert_eq!(resp.status, 200);
                a_hits += 1;
            }
            Some("B") => {
                assert_eq!(resp.status, 503);
                fails += 1;
            }
            other => panic!("unexpected server {other:?}"),
        }
    }
    assert_eq!(fails, 2, "B passively ejected after `unhealthy_threshold` live failures");
    assert_eq!(a_hits, 10, "traffic fails over to A with no breaker configured");
}

// ── Health checks ────────────────────────────────────────────────────────────

#[tokio::test]
async fn active_health_check_ejects_and_readmits_a_target() {
    // Two targets under an active health check. Flip B's /healthz to failing and,
    // after `unhealthy_threshold` probes, all traffic goes to A; flip it back and
    // B rejoins.
    let a = spawn_mock("A", Ctl::new()).await;
    let ctl_b = Ctl::new();
    let b = spawn_mock("B", ctl_b.clone()).await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/h"
    methods: ["GET"]
    upstream:
      balance: "round_robin"
      targets:
        - url: "{a}"
        - url: "{b}"
      health_check:
        path: "/healthz"
        interval: "200ms"
        unhealthy_threshold: 2
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    // Both healthy initially → B does receive some traffic.
    let mut saw_b = false;
    for _ in 0..8 {
        if get(&cl, &gw.url("/h")).await.header("x-server").as_deref() == Some("B") {
            saw_b = true;
        }
    }
    assert!(saw_b, "both targets in rotation while healthy");

    // Fail B's health endpoint; after ~2 probe intervals it drops out.
    ctl_b.healthy.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(900)).await;
    for _ in 0..10 {
        let resp = get(&cl, &gw.url("/h")).await;
        assert_eq!(
            resp.header("x-server").as_deref(),
            Some("A"),
            "unhealthy B is out of rotation"
        );
        assert_eq!(resp.status, 200);
    }

    // Recover B; one good probe brings it back.
    ctl_b.healthy.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut saw_b_again = false;
    for _ in 0..8 {
        if get(&cl, &gw.url("/h")).await.header("x-server").as_deref() == Some("B") {
            saw_b_again = true;
        }
    }
    assert!(saw_b_again, "recovered B rejoins the rotation");
}

#[tokio::test]
async fn all_targets_unhealthy_returns_503() {
    // Both targets' health endpoints fail → every target ejected → 503.
    let ctl_a = Ctl::new();
    let a = spawn_mock("A", ctl_a.clone()).await;
    let ctl_b = Ctl::new();
    let b = spawn_mock("B", ctl_b.clone()).await;
    let port = free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/h"
    methods: ["GET"]
    upstream:
      balance: "round_robin"
      targets:
        - url: "{a}"
        - url: "{b}"
      health_check:
        path: "/healthz"
        interval: "200ms"
        unhealthy_threshold: 2
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let cl = client();

    ctl_a.healthy.store(false, Ordering::SeqCst);
    ctl_b.healthy.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(900)).await;

    let resp = get(&cl, &gw.url("/h")).await;
    assert_eq!(resp.status, 503, "no eligible target → 503");
    assert!(
        resp.body_str().contains("service_unavailable"),
        "{}",
        resp.body_str()
    );
}
