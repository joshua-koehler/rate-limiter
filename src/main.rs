//! GatewayKit — a lightweight, config-driven API gateway.
//!
//! (Scaffolding step: this entrypoint currently only loads and validates the
//! config, failing fast on any error. The HTTP server is wired up next.)

mod config;

use std::path::PathBuf;

use anyhow::{bail, Context, Result};

fn main() -> Result<()> {
    let config_path = resolve_config_path()?;
    let cfg = config::load(&config_path)
        .with_context(|| format!("loading config from '{}'", config_path.display()))?;
    eprintln!(
        "GatewayKit config OK: port {}, {} route(s)",
        cfg.gateway.port,
        cfg.routes.len()
    );
    Ok(())
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
