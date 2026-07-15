# GatewayKit

A lightweight, config-driven API gateway — reverse-proxies requests to upstreams through an ordered pipeline of policy stages, all driven by a single YAML config.

Built on [`hyper`](https://hyper.rs/) used **only as a low-level HTTP transport** (Rust's std lib has no HTTP stack). All routing, matching, and proxy logic is hand-built — no gateway or reverse-proxy crate.

## Requirements / build

- Rust, stable toolchain, edition 2021.

```sh
cargo build --release
```

## Run

GatewayKit reads its config from a YAML file. The path is supplied via a CLI flag **or** an environment variable — **the CLI flag wins** when both are set:

```sh
# CLI flag
cargo run -- --config gateway.yaml

# environment variable
CONFIG=gateway.yaml cargo run
```

The gateway binds the port from `gateway.port` in the config. A **malformed or invalid config fails fast**: it prints a clear error and exits non-zero rather than starting half-configured. Validation covers durations, enums, and cross-field constraints (see [DECISIONS.md](./DECISIONS.md)).

## Test

The suite is self-contained — it spawns the real `gatewaykit` binary alongside an in-process mock upstream, so **no external services are required**.

```sh
cargo test
```

## Architecture

The request pipeline is an ordered list of composable **stages**. Each stage implements a common `Stage` trait, and the pipeline is assembled **per route** from the parsed config, then iterated in order. Fast rejections (404 / 405 / 401 / 429) short-circuit before any upstream work.

This is the core extensibility decision: **adding a config feature means adding a config struct, writing one stage, and registering it — no change to the core loop.**

### Module map

| Module | Responsibility |
| --- | --- |
| `config` | Parse YAML into typed structs; validate durations, enums, and cross-field constraints at load. |
| `pipeline/` | The `Stage` trait plus one file per stage. |
| `upstream/` | HTTP client, target selection, and proxy hygiene (hop-by-hop stripping, `Host` rewrite, `Content-Length` recompute). |
| `router` | Route matching — longest-prefix, on path-segment boundaries. |
| `server` | `hyper` wiring, request handling, and structured access logging. |
| `health` | The `/health` endpoint, handled independently of routing. |
| `error` | Error-to-status mapping. |

See **[DECISIONS.md](./DECISIONS.md)** for the rationale behind these choices (pipeline order, route-match semantics, rate-limit bucketing, concurrency strategy, and every resolved ambiguity).

## Feature checklist

Organized by priority tier (see [reqs.md](./reqs.md) for the full requirements).

> **Note:** later-tier config blocks already **parse and validate** today, so a complete config loads successfully even where enforcement lands in a later tier. Enforcement is what remains for `[ ]` items.

### P0 — Core (done)

- [x] Config load — CLI flag / env var, fail-fast on invalid config, cross-field validation
- [x] `GET /health` — always `200` with JSON status + uptime, independent of routing
- [x] Prefix proxying — longest-match, segment-boundary route matching; unmatched path → `404`
- [x] Method filtering — disallowed method → `405` with an `Allow` header
- [x] Schema-general — works with any valid config; no hard-coded paths, ports, or keys

### P1 — High value

- [x] Rate limiting — global + per-route; `fixed_window` & `sliding_window`; `per: ip` / `per: global`; over-limit → `429` + `Retry-After`; sharded + concurrency-exact; idle-key eviction
- [x] `strip_prefix` — forward the original or prefix-stripped path (query preserved)
- [x] Timeouts — global + per-route override; upstream timeout → `504`

### P2 — Resilience & policy (planned)

- [ ] Authentication — `api_key` header check; missing/invalid → `401`
- [ ] Retry with backoff — fixed / exponential, on configured statuses and transport errors
- [ ] Circuit breaker — per-target; open → `503` envelope
- [ ] Load balancing — `round_robin` / `weighted_round_robin` across targets
- [ ] Health checks — active probing with passive ejection

### P3 — Transformation & advanced (planned)

- [ ] Request transforms — header add/remove, JSON body mapping, dynamic values
- [ ] Response transforms — header add/remove, body envelope
