//! hyper 1.x accept loop. Each accepted connection captures its peer address
//! and is served on its own task; a connection error never takes down the
//! gateway. Ctrl-C stops accepting new connections (graceful-ish).

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;

/// Cap the time a client may take to send its request headers, so a slow-loris
/// connection that dribbles (or never finishes) the header block can't pin a
/// connection task. The request-body read is bounded separately in `upstream`.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

use crate::pipeline;
use crate::state::AppState;

pub async fn run(state: AppState) -> Result<()> {
    // Bind all interfaces on the configured port (the only network knob in the
    // schema). Loopback clients still connect via 127.0.0.1.
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.gateway.port));
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding gateway listener on {addr}"))?;
    eprintln!("GatewayKit listening on http://{addr}");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("accepting connection")?;
                let state = state.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    // Peer captured at accept; threaded into the pipeline for
                    // the (P1) `per: ip` rate-limit bucket key.
                    let service = service_fn(move |req| {
                        let state = state.clone();
                        async move { Ok::<_, Infallible>(pipeline::handle(state, peer, req).await) }
                    });
                    if let Err(err) = http1::Builder::new()
                        // `header_read_timeout` needs a hyper timer wired in.
                        .timer(TokioTimer::new())
                        .header_read_timeout(HEADER_READ_TIMEOUT)
                        .serve_connection(io, service)
                        .await
                    {
                        eprintln!("connection from {peer} ended with error: {err}");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("received shutdown signal; no longer accepting connections");
                break;
            }
        }
    }
    Ok(())
}
