//! Self-contained integration-test harness: a mock upstream, an HTTP test
//! client, and a helper that boots the real gateway binary against a temp
//! config and waits for it to become healthy. No external services.
//!
//! Each test binary that does `mod common;` compiles this module separately, so
//! helpers used by only some binaries look "unused" to the others — expected.
#![allow(dead_code)]

use std::convert::Infallible;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::HeaderMap;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

// ── Test HTTP client ─────────────────────────────────────────────────────────

pub type TestClient = Client<HttpConnector, Full<Bytes>>;

pub fn client() -> TestClient {
    Client::builder(TokioExecutor::new()).build_http()
}

/// A fully-buffered response captured by the test client.
pub struct TestResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl TestResponse {
    pub fn body_str(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }
}

pub async fn send(
    client: &TestClient,
    method: Method,
    url: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> TestResponse {
    let mut builder = Request::builder().method(method).uri(url);
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let req = builder
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("build test request");
    let resp = client.request(req).await.expect("request to gateway failed");
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect response body")
        .to_bytes();
    TestResponse {
        status,
        headers,
        body,
    }
}

pub async fn get(client: &TestClient, url: &str) -> TestResponse {
    send(client, Method::GET, url, &[], "").await
}

// ── Mock upstream ────────────────────────────────────────────────────────────

/// Spawn the mock upstream and return its base URL (e.g. `http://127.0.0.1:PORT`).
///
/// Behavior is path-agnostic (driven by request headers so it works behind any
/// route prefix). The mock always echoes the request body and reports what it
/// received, letting tests assert faithful relay of method/path/headers/body.
///
/// Request knobs: `x-mock-status: <code>` sets the response status (default
/// 200); `x-mock-sleep-ms: <millis>` adds delay (default 0; for P1 tests).
/// Response always carries `x-upstream: mock-upstream`, `x-echo-method`,
/// `x-echo-path`, and `x-echo-host` (the incoming `Host` header), with the body
/// echoing the request body (or `mock-upstream-ok`).
pub async fn spawn_mock_upstream() -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(io, service_fn(mock_handler))
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

async fn mock_handler(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method().to_string();
    let path = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_default();
    let host = req
        .headers()
        .get(hyper::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let status = req
        .headers()
        .get("x-mock-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u16>().ok())
        .and_then(|c| StatusCode::from_u16(c).ok())
        .unwrap_or(StatusCode::OK);
    let sleep_ms = req
        .headers()
        .get("x-mock-sleep-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // P3 tests need to observe forwarded request headers: reflect every incoming
    // request header back as `echo-req-<lowercased-name>: <value>`. Non-ASCII
    // values (whose `to_str()` fails) are skipped to avoid panics. This lets a
    // test assert that the gateway's request_transform added/removed a header.
    let echo_req_headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (format!("echo-req-{}", name.as_str().to_lowercase()), v.to_string()))
        })
        .collect();

    let body = req
        .into_body()
        .collect()
        .await
        .map(|b| b.to_bytes())
        .unwrap_or_default();

    if sleep_ms > 0 {
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    let echo = if body.is_empty() {
        Bytes::from_static(b"mock-upstream-ok")
    } else {
        body
    };

    let mut builder = Response::builder()
        .status(status)
        .header("x-upstream", "mock-upstream")
        .header("x-echo-method", method)
        .header("x-echo-path", path)
        .header("x-echo-host", host)
        // P3 tests need removable response headers: the gateway's
        // response_transform strips these in the relevant test.
        .header("Server", "mock-upstream")
        .header("X-Powered-By", "mock");
    // Reflect the incoming request headers (see above) so request-header
    // add/remove is observable from the client response headers.
    for (name, value) in echo_req_headers {
        builder = builder.header(name, value);
    }
    let resp = builder.body(Full::new(echo)).unwrap();
    Ok(resp)
}

// ── Gateway process harness ──────────────────────────────────────────────────

/// Pick an ephemeral port by binding :0, reading it back, and releasing it.
pub fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// A running gateway process plus its temp config, both cleaned up on drop.
pub struct Gateway {
    pub port: u16,
    child: Child,
    config_path: PathBuf,
}

impl Gateway {
    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url())
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.config_path);
    }
}

fn write_temp_config(contents: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "gatewaykit-test-{}-{n}-{nanos}.yaml",
        std::process::id()
    ));
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    path
}

/// Spawn the built gateway binary against `config_yaml` and wait until its
/// `/health` endpoint responds 200.
pub async fn spawn_gateway(config_yaml: &str, port: u16) -> Gateway {
    let config_path = write_temp_config(config_yaml);
    let child = Command::new(env!("CARGO_BIN_EXE_gatewaykit"))
        .arg("--config")
        .arg(&config_path)
        .spawn()
        .expect("spawn gatewaykit binary");
    let gw = Gateway {
        port,
        child,
        config_path,
    };
    wait_until_healthy(&gw).await;
    gw
}

async fn wait_until_healthy(gw: &Gateway) {
    let client = client();
    let url = gw.url("/health");
    for _ in 0..100 {
        if let Ok(resp) = client
            .request(
                Request::builder()
                    .uri(&url)
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
        {
            if resp.status() == StatusCode::OK {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("gateway did not become healthy on port {}", gw.port);
}

/// Run the gateway binary once against `config_yaml` and return its exit output.
/// Used for fail-fast config tests (invalid configs cause the process to exit).
pub fn run_binary_expecting_exit(config_yaml: &str) -> std::process::Output {
    let path = write_temp_config(config_yaml);
    let out = Command::new(env!("CARGO_BIN_EXE_gatewaykit"))
        .arg("--config")
        .arg(&path)
        .output()
        .expect("run gatewaykit binary");
    let _ = std::fs::remove_file(&path);
    out
}
