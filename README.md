# GatewayKit

A lightweight, config-driven API gateway — reverse-proxies requests to upstreams through an ordered pipeline of policy stages, all driven by a single YAML config.

Built on [`hyper`](https://hyper.rs/) used **only as a low-level HTTP transport** (Rust's std lib has no HTTP stack). All routing, matching, and proxy logic is hand-built — no gateway or reverse-proxy crate.

## Requirements / build

- Rust, stable toolchain, edition 2021.

```sh
cargo build --release
```

## Run

GatewayKit reads its config from a YAML file. A ready-to-run, fully-commented example ships as [`gateway.example.yaml`](./gateway.example.yaml) — it exercises every feature across all tiers. The config path is supplied via a CLI flag **or** an environment variable — **the CLI flag wins** when both are set:

```sh
# CLI flag
cargo run -- --config gateway.example.yaml

# environment variable
CONFIG=gateway.example.yaml cargo run
```

The gateway binds the port from `gateway.port` in the config (`8080` in the example). A **malformed or invalid config fails fast**: it prints a clear error and exits non-zero rather than starting half-configured. Validation covers durations, enums, and cross-field constraints (see [DECISIONS.md](./DECISIONS.md)).

### Try it end to end

```sh
# 1. start the gateway
cargo run -- --config gateway.example.yaml

# 2. in another shell — the health endpoint is always up, independent of routing
curl -s localhost:8080/health
# {"status":"healthy","uptime_seconds":3}

# 3. proxy a request to an upstream (point the config's upstream URLs at any
#    HTTP service, or run one quickly — e.g. `python3 -m http.server 3001`)
curl -i localhost:8080/api/users
```

Prefer no external services? `cargo test` spins up an in-process mock upstream and drives the real binary — see below.

## Test

The suite is self-contained — it spawns the real `gatewaykit` binary alongside an in-process mock upstream, so **no external services are required**.

```sh
cargo test
```

## Architecture

The request pipeline is an ordered list of composable **stages**. Each stage implements a common `Stage` trait, and the pipeline is assembled **per route** from the parsed config, then iterated in order. Fast rejections (404 / 405 / 401 / 429) short-circuit before any upstream work. The response side is symmetric: a `ResponseStage` chain (also assembled per route) transforms genuine upstream responses on the way back — so the same "add a stage" extensibility applies in both directions.

This is the core extensibility decision: **adding a config feature means adding a config struct, writing one stage, and registering it — no change to the core loop.**

### Module map

| Module | Responsibility |
| --- | --- |
| `config` | Parse YAML into typed structs; validate durations, enums, and cross-field constraints at load. |
| `pipeline/` | The request `Stage` trait and the response `ResponseStage` trait, plus one file per stage (`method`, `auth`, `rate_limit`, `request_transform`, `response_transform`) and the shared `transform` helpers (timestamps, value resolution, JSON dot-paths). |
| `upstream/` | The terminal upstream call + the P2 resilience layer: load balancing, per-target circuit breakers, active/passive health, retry+backoff, request-body buffering, and proxy hygiene (hop-by-hop stripping, `Host` rewrite, `Content-Length` recompute). |
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

### P2 — Resilience & policy (done)

- [x] Authentication — `api_key` header check; missing/invalid → `401`; constant-time key compare; runs before rate limiting
- [x] Retry with backoff — fixed / exponential, on configured statuses **and** transport errors/timeouts; per-attempt timeout + overall wall-clock budget; fails over to the next eligible target
- [x] Circuit breaker — per-target, single-probe half-open; open → `503` `{ "error": "service_unavailable", "retry_after": <s> }`
- [x] Load balancing — `round_robin` (weight-ignoring) / `weighted_round_robin` (smooth WRR) across `targets`; skips unhealthy / breaker-open targets
- [x] Health checks — active background probing (eject after `unhealthy_threshold`, recover on first success) + passive breaker ejection between probes; all targets down → `503`
- [x] Request body cap — retries buffer the request body (bounded, 2 MiB); oversize → `413`

### P3 — Transformation & advanced

- [x] Request transforms — header add/remove (`$request_time` / `$literal:` values), JSON body mapping (dot-path `dest <- source`, non-JSON passthrough); `$request_time` computed once so header and body agree
- [x] Response transforms — header add/remove, body envelope (`$body` / `$response_time` / `$route_path`); non-JSON/empty upstream body embedded as a JSON string; applied only to real upstream responses (never to gateway-generated errors)
