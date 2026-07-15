//! Proxy hygiene: the gateway must present the *upstream's* authority as `Host`
//! to the upstream, not the client's inbound `Host`. (Hop-by-hop stripping is
//! also implemented in `upstream::proxy`; asserting it end-to-end needs the mock
//! to report the raw headers it received, which would mean editing the shared
//! harness — deferred so this file stays self-contained.)

mod common;

use common::{get, spawn_gateway, spawn_mock_upstream};

#[tokio::test]
async fn host_header_is_rewritten_to_the_upstream_authority() {
    let mock = spawn_mock_upstream().await; // "http://127.0.0.1:<mock_port>"
    let upstream_authority = mock.trim_start_matches("http://").to_string();

    // The client reaches the gateway on its own port, so the inbound Host the
    // gateway sees is "127.0.0.1:<gateway_port>". If the rewrite works, the mock
    // instead reports its *own* authority (a different port) as the Host.
    let port = common::free_port();
    let cfg = format!(
        r#"
gateway:
  port: {port}
routes:
  - path: "/api/echo"
    methods: ["GET"]
    strip_prefix: false
    upstream:
      url: "{mock}"
"#
    );
    let gw = spawn_gateway(&cfg, port).await;
    let client = common::client();

    let resp = get(&client, &gw.url("/api/echo")).await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("x-echo-host").as_deref(),
        Some(upstream_authority.as_str()),
        "upstream should see its own authority as Host, not the client's inbound Host"
    );
}
