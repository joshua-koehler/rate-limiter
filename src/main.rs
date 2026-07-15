//! GatewayKit — a lightweight, config-driven API gateway.
//!
//! Entry point: resolve the config path (`--config <path>` or `CONFIG` env),
//! load + validate the config (failing fast, non-zero exit, on any error),
//! build shared state, and run the server.

mod config;
mod error;
mod health;
mod pipeline;
mod proxy;
mod router;
mod server;
mod state;

use std::path::PathBuf;

use anyhow::{bail, Context, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = resolve_config_path()?;
    let config = config::load(&config_path)
        .with_context(|| format!("loading config from '{}'", config_path.display()))?;
    let state = state::AppState::new(config);
    server::run(state).await
}

/// Resolve the config path from `--config <path>` (or `--config=<path>`),
/// falling back to the `CONFIG` env var. The CLI flag wins over the env var.
fn resolve_config_path() -> Result<PathBuf> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.iter().any(|a| a == "-h" || a == "--help") {
        eprintln!("usage: gatewaykit --config <path>   (or CONFIG=<path> gatewaykit)");
        std::process::exit(0);
    }

    if let Some(pos) = args.iter().position(|a| a == "--config") {
        let p = args
            .get(pos + 1)
            .context("--config requires a path argument")?;
        return Ok(PathBuf::from(p));
    }
    if let Some(p) = args.iter().find_map(|a| a.strip_prefix("--config=")) {
        return Ok(PathBuf::from(p));
    }
    if let Ok(p) = std::env::var("CONFIG") {
        if !p.is_empty() {
            return Ok(PathBuf::from(p));
        }
    }
    bail!("no config provided: pass --config <path> or set the CONFIG env var")
}
