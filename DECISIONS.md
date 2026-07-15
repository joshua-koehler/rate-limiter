# DECISIONS

## Scope & priority
**Target:** aim for everything, but only advance to the next tier after all tests pass *and* an adversarial architecture review (human-judged) passes. Quality gate per tier, not raw coverage.

Build in this order; a few features done cleanly beats many half-done.
1. **P0** — config load, `/health`, prefix proxying (longest match, 404), method filter (405), schema-general.
2. **P1** — rate limiting (global + per-route) **first** (namesake + concurrency proof), then `strip_prefix`, then timeouts (global + per-route → 504).
3. **P2** — api_key auth **first** (cheapest stage, proves the architecture), then retry/backoff, circuit breaker, load balancing, health checks.
4. **P3** — request/response transforms (headers, body mapping, envelope).

**Grade-optimal floor:** P0 + rate limiting (both strategies, concurrency-proven) + one clean resilience feature + auth + flawless DECISIONS/README. Beyond the floor is bonus; stub cleanly, never half-wire. Per-tier review gate must not burn the clock.

## Architecture
- **Pluggable pipeline (extensibility — the 60%-of-grade decision):** the pipeline is an ordered list of composable **stages**, each a `Stage` trait (`async fn apply(&mut ctx) -> Flow { Continue | ShortCircuit(Response) }`), assembled per-route from config. New config feature = config struct + one stage + register; no core-loop change. This is the criteria.md "extend in an afternoon" litmus test.
- **Module map:** `config/` (parse+validate), `pipeline/` (`Stage` trait + one file per stage), `upstream/` (client, target pool, health), `server` (hyper wiring, `RequestCtx`, error→status, access log), `mock_upstream` (tests).
- Pipeline order: `match route → method → auth → rate limit → request transform → circuit-breaker gate → select target → timeout+retry around upstream → response transform → return`. Fast rejections (404/405/401/429) before any upstream work.
- All state in-memory, single instance (spec blesses this). Rate-limit counters, breaker state, LB cursors, health status are shared mutable state — race-free via **sharded maps / atomic per-entry counters** (no map-level lock serializing throughput).
- **HTTP stack:** `hyper` (+ `hyper-util`) used **only as a low-level HTTP transport** — Rust std has no HTTP; closest legit analogue to std-lib HTTP. All routing/matching/proxy logic hand-built; no gateway/reverse-proxy crate.
- **Proxy hygiene:** strip hop-by-hop headers both directions (`Connection`/`Transfer-Encoding`/`Keep-Alive`/`Upgrade`/`Proxy-*`), rewrite `Host` to upstream, recompute `Content-Length` on body change.
- Parse all durations and enums (`strategy`, `backoff`, `balance`, `per`, `auth.type`) at load; **cross-field validation** (weighted RR needs weights; `url` xor `targets`; `health_check` needs `path`); reject unknown values with non-zero exit.
- **Observability:** one structured access-log line per request (route, decision/status, latency, target) + error logging.
- **Body limits:** cap max body size + read timeout even with transforms stubbed; oversize → 413.

## Resolved ambiguities (spec silent → our call)
- **Route match:** **segment-boundary** longest prefix (`/api/users` matches `/api/users` + `/api/users/…`, not `/api/usersXYZ`). Path matched first; method mismatch on the chosen route → 405 (no fallback to a shorter route).
- **Port:** bind `gateway.port` (literal 8080 in spec is an example, not hard-coded).
- **Client IP (`per: ip`):** socket peer; ignore `X-Forwarded-For` (no trusted-proxy config).
- **Rate-limit bucket identity:** key = `(route_id, per_key)`. Global default instantiated **per-route** (routes don't share one gateway-wide budget). `per: ip` → `(route_id, ip)`; `per: global` → `(route_id)` — one bucket **per route**, not gateway-wide.
- **Rate limit override:** route `rate_limit` fully replaces global (no merge). Over limit → **429 + `Retry-After`** (int seconds to capacity). `sliding_window` = sliding-counter (weighted current+previous window, O(1)/key). Sharded map + idle-key eviction (unbounded per-IP growth).
- **Timeout placement/scope:** accept `timeout` under `upstream` *and* at route level; route/upstream value beats global. Timeout is **per attempt**; separate overall wall-clock budget caps retries+backoff. (Note: in the example config, both `/api/orders` and `/api/products` place `timeout` under `upstream`.)
- **Retry:** `attempts` = total tries. Retry on listed statuses **and** transport errors/timeouts. Honor config even for non-idempotent methods (at-least-once risk accepted). On retry, advance to **next healthy target**. A fully-failed retried request = **one** breaker failure (per-request, not per-attempt); retry sits inside the breaker gate.
- **Circuit breaker:** **per-target** (per-upstream), single-probe half-open; open → 503 envelope `{ "error": "service_unavailable", "retry_after": <s> }`. Counts 5xx/timeouts/connection errors. Single-`url` upstream = one breaker.
- **Auth:** missing *or* invalid key → 401 (no 403 distinction); constant-time compare. Runs **before** rate limiting (protects key compare; `per` is ip/global so no identity needed to bucket). Tradeoff: rate-limit-first would shed bad-key floods cheaper — accepted.
- **Health checks:** active probe; mark unhealthy after `unhealthy_threshold` consecutive failures; recover on first success; all targets unhealthy → 503. **Passive ejection:** live-traffic failures feed the per-target breaker so a dead target is ejected before its next probe. `health_check` applies to single-`url` upstreams too.
- **Load balancing:** `round_robin` ignores `weight`; `weighted_round_robin` = smooth weighted RR; atomic cursor; skip unhealthy/Open targets.
- **Request/response body transform:** only for JSON bodies; non-JSON request body passes through unchanged. Response envelope: non-JSON/empty upstream body embedded as a JSON string under `$body`; envelope applies only to real upstream responses, **not** gateway-generated errors.
- **Dynamic values:** `$request_time`/`$response_time` = RFC-3339 UTC, computed once per request/response (header + body-mapping uses agree). `$route_path` = matched route path.
- **405** carries an `Allow` header; **413** for oversize request bodies.

## Error mapping
404 unmatched · 405 bad method (+`Allow`) · 401 auth · 429 rate limit (+`Retry-After`) · 413 oversize body · 503 circuit open / all targets unhealthy · 504 upstream timeout · 502 upstream connection error. No panic leaks to clients.

## Deliverables (Communication)
`DECISIONS.md` (this) · `README.md` (setup, run, one-command test, feature checklist) · atomic commit-per-tier history narrating build order · self-contained test suite incl. mock upstream (slow + flaky) and the P0.5 alternate-config boot test.

## What's next / partial
_(fill in at submission: which P2/P3 features are stubbed and their state.)_

## AI tooling
Used claude code for the following:
- parse yaml spec into feature taxonomy
- spec out solution, built off my opionated guidelines, using me as human in the loop to make decisions
- log the decisions I make for me in DECISIONS.md to maximize human input
- agent teams implement in parallel where possible, restricted to within a single tier.  
- adversarial review on each tier before moving onto the next tier
