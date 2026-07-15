# DECISIONS

## Scope & priority
**Target:** aim for everything, but only advance to the next tier after all tests pass *and* an adversarial architecture review (human-judged) passes. Quality gate per tier, not raw coverage.

Build in this order; a few features done cleanly beats many half-done.
1. **P0** — config load, `/health`, prefix proxying (longest match, 404), method filter (405), schema-general.
2. **P1** — `strip_prefix`, timeouts (global + per-route → 504), rate limiting (global + per-route).
3. **P2** — retry/backoff, circuit breaker, api_key auth, load balancing, health checks.
4. **P3** — request/response transforms (headers, body mapping, envelope).

## Architecture
- Pipeline: `match route → method → auth → rate limit → request transform → circuit-breaker gate → select target → timeout+retry around upstream → response transform → return`. Fast rejections (404/405/401/429) before any upstream work.
- All state in-memory, single instance (spec blesses this). Rate-limit counters, breaker state, LB cursors, health status are shared mutable state — all must be race-free (atomics / per-key locks).
- **HTTP stack:** `hyper` (low-level HTTP lib, not a gateway/proxy framework — closest legit analogue to std-lib HTTP in Rust) for server + client; no gateway/proxy framework.
- Parse all durations and enums (`strategy`, `backoff`, `balance`, `per`, `auth.type`) at load; reject unknown values with non-zero exit.

## Resolved ambiguities (spec silent → our call)
- **Route match:** longest matching prefix.
- **Client IP (`per: ip`):** socket peer; ignore `X-Forwarded-For` (no trusted-proxy config).
- **Rate limit override:** route `rate_limit` fully replaces global (no merge). Over limit → **429 + `Retry-After`**. `sliding_window` = sliding-counter (weighted current+previous window, O(1)/key).
- **Timeout placement:** accept `timeout` at route level *and* under `upstream`; route value wins over global.
- **Retry:** `attempts` = total tries. Retry on listed statuses **and** transport errors/timeouts. Honor config even for non-idempotent methods (at-least-once risk accepted).
- **Circuit breaker:** per-route, single-probe half-open; open → 503 envelope `{ "error": "service_unavailable", "retry_after": <s> }`.
- **Auth:** missing *or* invalid key → 401 (no 403 distinction); constant-time compare.
- **Health checks:** mark unhealthy after `unhealthy_threshold` consecutive failures; recover on first success; all targets unhealthy → 503.
- **Body transform:** only for JSON bodies; non-JSON passes through unchanged.

## Error mapping
404 unmatched · 405 bad method · 401 auth · 429 rate limit · 503 circuit open / all targets unhealthy · 504 upstream timeout · 502 upstream connection error. No panic leaks to clients.

## What's next / partial
_(fill in at submission: which P2/P3 features are stubbed and their state.)_

## AI tooling
_(fill in: how Claude Code was used.)_
