# GatewayKit — Requirements

Derived from `spec.md`. The `gateway.yaml` in the spec **is** the specification: every
field is a requirement. Graders re-run the gateway against a *different* config using the
same schema, so requirements are written against the **schema**, not the example values.

**Scope decisions (confirmed):**
- Track the `gateway.yaml` config schema only. Redis/etcd/TLS/OAuth/response-caching from
  `plan.md` are explicitly **out of scope** for these requirements.
- All state (rate-limit counters, circuit-breaker state, upstream health) is **in-memory**,
  single-instance. The spec blesses this.
- No off-the-shelf gateway / reverse-proxy / HTTP-proxy library. Std-lib HTTP server/client
  and a YAML parser only.

**Priority legend:**
- **P0 — Core / non-negotiable.** Spec's explicit baseline. Nothing else counts if these fail.
- **P1 — High value, build next.** Cheap and/or central; strong ROI under time pressure.
- **P2 — Resilience & policy.** Meaningful features with moderate cost.
- **P3 — Transformation & advanced.** Highest cost / lowest incremental grade per hour.

Ordering reflects a proposed build sequence, not just importance. Rationale for the order
is at the end.

---

## P0 — Core Requirements (non-negotiable)

The spec lists these as the baseline. Implement and test these before anything else.

### P0.1 — Startup & config loading
- Read config from a YAML file path passed as a **CLI argument or environment variable**.
- Listen on the port from `gateway.port` (example: 8080).
- On **malformed or invalid config**, fail fast with a clear error and non-zero exit — do not
  start half-configured. (Production-thinking: graders will feed a different config.)
- **Acceptance:** `gateway --config path.yaml` (and/or `CONFIG=path.yaml gateway`) boots and
  binds the configured port; a broken YAML exits with a readable message.

### P0.2 — Health endpoint
- `GET /health` **always** returns `200` with JSON `{ "status": "healthy",
  "uptime_seconds": <int> }`, regardless of config and regardless of any configured route.
- Not rate-limited, not authed, not routed to an upstream.
- **Acceptance:** works before/independent of any route; `uptime_seconds` is a monotonic int.

### P0.3 — Basic proxying
- A request matching a configured route is forwarded to the route's upstream, and the
  upstream response (status, headers, body) is returned to the client.
- **Unmatched path → `404`.**
- **Route matching:** path is treated as a **prefix** (the `strip_prefix` examples —
  `/api/products/123`, `/api/legacy/v1/data` — only make sense as prefix matches). On overlap,
  match the **longest** matching prefix. *(Decision — spec is silent; documented assumption.)*
- **Acceptance:** request to a configured path reaches a mock upstream and its response is
  relayed byte-faithfully; unknown path returns 404.

### P0.4 — Method filtering
- Requests whose method is not in the route's `methods` list → **`405` Method Not Allowed**.
- **Acceptance:** `POST` to a `["GET"]`-only route returns 405; `GET` passes.

### P0.5 — Schema-general, not example-specific
- Works with **any** valid config following the schema: any number of routes, any paths,
  any subset of optional blocks present or absent. No hard-coded paths, ports, or keys.
- **Acceptance:** a second config with different routes/values behaves correctly with no code
  changes.

---

## P1 — High value, build next

### P1.1 — `strip_prefix`
- `strip_prefix: false` → forward the original request path unchanged.
- `strip_prefix: true` → remove the matched route prefix before forwarding
  (`/api/products/123` → `/123`; `/api/legacy/v1/data` → `/v1/data`).
- Edge: stripping the whole path yields `/`. Preserve query string in both modes.
- **Acceptance:** upstream receives the correctly transformed path in both modes.

### P1.2 — Timeouts (global + per-route override)
- `gateway.global_timeout` applies to all upstream requests by default.
- A route's `upstream.timeout` (note: `/api/orders` puts it under `upstream`,
  `/api/products` under the route — *support both placements*) overrides the global for that
  route. *(Decision: accept `timeout` at route level and under `upstream`; route-specific value
  wins over global.)*
- On timeout, return **`504` Gateway Timeout** (unless retry/circuit-breaker intervenes).
- Parse duration strings (`"30s"`, `"5s"`, `"1s"`) into durations; reject unparseable values
  at config load.
- **Acceptance:** a slow mock upstream exceeding the timeout yields 504 within the bound.

### P1.3 — Rate limiting (global + per-route)
The gateway's namesake feature; central and high-value.
- `gateway.global_rate_limit` applies to every route **unless** the route defines its own
  `rate_limit`, which **fully overrides** (not merges with) the global. *(Decision: override,
  not merge.)*
- Fields: `requests`, `window` (duration), `strategy`, `per`.
- **Strategies:**
  - `fixed_window` — count per discrete window; reset at window boundary.
  - `sliding_window` — smooth window (sliding log or sliding counter) so bursts across a
    boundary are still limited.
- **Bucket key (`per`):**
  - `ip` — per client IP. *(Decision: derive IP from the socket peer; do not trust
    `X-Forwarded-For` unless we add explicit trusted-proxy config. Document this.)*
  - `global` — one shared bucket for the route/gateway.
- **Over limit → `429` Too Many Requests.** Include a `Retry-After` header
  (seconds until capacity). *(Decision: 429 + `Retry-After`; spec doesn't name the code.)*
- **Concurrency:** counters must be correct under simultaneous requests (the spec calls out
  "50 requests hit a rate-limited route simultaneously"). Guard with atomic ops / per-key locks.
- **Acceptance:** N+1 requests in a window → the (N+1)th gets 429; parallel load does not
  over-admit; per-IP isolation verified; both strategies tested at a window boundary.

---

## P2 — Resilience & policy

### P2.1 — Retry with backoff
- Config: `retry.attempts`, `retry.backoff` (`fixed` | `exponential`),
  `retry.initial_delay`, `retry.on` (list of HTTP status codes).
- Retry when the upstream responds with a status in `on`, **or** on connection failure/timeout.
  *(Decision: also retry transport errors/timeouts, not just listed statuses.)*
- `attempts` = total attempts (or additional retries?) — *(Decision: treat `attempts` as total
  tries; document.)* Between attempts wait: `fixed` → `initial_delay` each time; `exponential`
  → `initial_delay * 2^(n-1)`.
- **Safety:** only retry idempotent methods by default? Spec's `/api/orders` retries on
  `POST/PUT`. *(Decision: honor config as written — retry regardless of method, since the
  config author opted in; note the at-least-once risk in DECISIONS.md.)*
- **Acceptance:** a flaky mock returning 503 then 200 succeeds within `attempts`; backoff
  delays observed; exhausted retries return the last upstream error.

### P2.2 — Circuit breaker
- Config: `threshold` (failures), `window`, `cooldown`.
- **Closed** → count failures (5xx / timeouts) in a rolling `window`; at `threshold`, trip to
  **Open**.
- **Open** → immediately return **`503`** with body
  `{ "error": "service_unavailable", "retry_after": <seconds_remaining> }` without contacting
  upstream, until `cooldown` elapses.
- **Half-Open** → after cooldown, allow a trial request; success → Closed, failure → Open again.
  *(Decision: single-probe half-open; document.)*
- Per-route (per-upstream) breaker state, in-memory, concurrency-safe.
- **Acceptance:** `threshold` induced failures trip the breaker; subsequent calls get the 503
  envelope with a decreasing `retry_after`; recovery after cooldown.

### P2.3 — Authentication (`api_key`)
- Config: `auth.type: api_key`, `auth.header`, `auth.keys` (list).
- Request must present the named header with a value in `keys`; otherwise **`401`
  Unauthorized**. *(Decision: missing/invalid both → 401; do not distinguish 403.)*
- Applies only to routes that declare `auth`. Constant-time compare where practical.
- **Acceptance:** valid key passes; missing/invalid key → 401; other routes unaffected.

### P2.4 — Load balancing across targets
- Config: `upstream.targets[]` each with `url` and `weight`; `upstream.balance`
  (`round_robin` | `weighted_round_robin`).
- `round_robin` — even rotation across targets.
- `weighted_round_robin` — distribute in proportion to `weight` (3:1 in the example).
- Concurrency-safe target selection (atomic cursor).
- Interacts with health checks (P2.5): skip targets currently marked unhealthy.
- **Acceptance:** over many requests, distribution matches the configured weights; single-`url`
  upstreams (no `targets`) still work.

### P2.5 — Passive/active health checks
- Config: `health_check.path`, `health_check.interval`, `health_check.unhealthy_threshold`.
- Periodically (`interval`) probe each target's `path`; after `unhealthy_threshold` consecutive
  failures, mark the target **unhealthy** and remove it from the LB pool; restore after a
  success. *(Decision: recover on first successful probe; document.)*
- If **all** targets are unhealthy → return `503`. *(Decision.)*
- Background probing must not block request handling.
- **Acceptance:** a target whose `/healthz` fails `threshold` times stops receiving traffic;
  traffic resumes after it recovers.

---

## P3 — Transformation & advanced

Highest implementation cost; smallest baseline-grade impact. Do these last / partially.

### P3.1 — Request transform: headers
- `request_transform.headers.add` — add/overwrite headers on the forwarded request.
- `request_transform.headers.remove` — strip listed headers before forwarding.
- **Dynamic values:** `$request_time` → inject request timestamp;
  `$literal:<value>` → literal string.
- **Acceptance:** upstream sees added headers (incl. resolved `$request_time`) and does not see
  removed ones.

### P3.2 — Request transform: body mapping
- `request_transform.body.mapping` maps `destination.path <- "source"` using **dot notation**,
  building a new JSON body from the incoming one.
- Sources: a dot-path into the incoming body; `$literal:<value>`; `$request_time`.
- Only meaningful for JSON bodies. *(Decision: if body isn't JSON, pass through unchanged and
  note it; document.)*
- **Acceptance:** given the example mapping, upstream receives the restructured body
  (`user.id`, `user.name`, `meta.source`, `meta.timestamp`).

### P3.3 — Response transform: headers
- `response_transform.headers.add` / `remove` applied to the response before returning to the
  client. Same `$literal` / dynamic-value rules where applicable.
- **Acceptance:** client sees added/removed response headers.

### P3.4 — Response transform: body envelope
- `response_transform.body.envelope` wraps the upstream response body in a new structure.
- Placeholders: `$body` (original response body), `$response_time`, `$route_path` (matched
  route path).
- **Acceptance:** client receives the enveloped JSON with `data` = original body and
  `gateway_metadata` populated.

---

## Cross-cutting requirements

- **Request pipeline order** *(Decision — document in DECISIONS.md):*
  `match route → method filter → auth → rate limit → request transform → circuit-breaker gate
  → load-balance/select target → timeout+retry around upstream call → response transform →
  return`. Fast rejections (404/405/401/429) happen before any upstream work.
- **Concurrency correctness:** rate-limit counters, breaker state, LB cursors, and health
  status are shared mutable state accessed by many in-flight requests; all must be race-free.
- **Failure modes / error mapping:** unmatched 404, bad method 405, auth 401, rate limit 429,
  circuit open 503 (envelope), upstream timeout 504, upstream connection error 502,
  all targets unhealthy 503. No unhandled panics leak to clients.
- **Duration & config parsing:** parse all duration strings and enums (`strategy`, `backoff`,
  `balance`, `per`, `auth.type`) up front; reject unknown values at load.
- **Testing (spec deliverable):** self-contained test suite runnable with one command,
  including a mock upstream server (with a slow and a flaky endpoint) so tests need no external
  services.

---

## Prioritization rationale (for DECISIONS.md)

1. **P0 first** because they're the graded baseline and everything depends on the proxy core.
2. **`strip_prefix` + timeouts** are cheap, touch every proxied request, and are easy to get
   subtly wrong — high ROI.
3. **Rate limiting** next: it's the repo's namesake, central to the "50 simultaneous requests"
   production-thinking prompt, and demonstrates concurrency competence.
4. **Resilience (retry, circuit breaker, auth, LB, health)** shows production maturity; each is
   independently testable and degrades gracefully if partially done.
5. **Transforms last:** the most code for the least baseline grade, and the easiest to leave
   cleanly stubbed with a clear "next steps" note — exactly the "well-architected 3 features
   beats brittle 6" trade-off the spec rewards.

## Open questions / assumptions to confirm

Marked *(Decision)* inline above where the spec is silent. The load-bearing ones:
- Client IP source for `per: ip` (socket peer vs. `X-Forwarded-For`).
- `retry.attempts` = total tries vs. extra retries; retrying non-idempotent methods.
- Half-open circuit breaker = single probe; health recovery = first success.
- Rate-limit override (route replaces global) rather than merge.
- 429 as the rate-limit status (spec names it only for the circuit breaker's 503).
