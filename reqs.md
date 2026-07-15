# GatewayKit — Requirements

Derived from `spec.md`. The `gateway.yaml` in the spec **is** the specification: every
field is a requirement. Graders re-run the gateway against a *different* config using the
same schema, so requirements are written against the **schema**, not the example values.

**Scope decisions (confirmed):**
- Track the `gateway.yaml` config schema only. Redis/etcd/TLS/OAuth/response-caching from
  `plan.md` are explicitly **out of scope** for these requirements.
- All state (rate-limit counters, circuit-breaker state, upstream health) is **in-memory**,
  single-instance. The spec blesses this.
- No off-the-shelf gateway / reverse-proxy / HTTP-proxy library. `hyper` (+ `hyper-util`) is
  used **only as a low-level HTTP transport** (server + client): Rust's std lib has no HTTP
  stack, so hyper is the closest legitimate analogue to "std-lib HTTP." All routing, matching,
  and proxy logic is **hand-built**; no gateway/reverse-proxy crate is used. *(Defend this
  boundary explicitly in the walkthrough.)*

**Priority legend:**
- **P0 — Core / non-negotiable.** Spec's explicit baseline. Nothing else counts if these fail.
- **P1 — High value, build next.** Cheap and/or central; strong ROI under time pressure.
- **P2 — Resilience & policy.** Meaningful features with moderate cost.
- **P3 — Transformation & advanced.** Highest cost / lowest incremental grade per hour.

Ordering reflects a proposed build sequence, not just importance. Rationale for the order
is at the end.

**Grade-optimal floor (target this before anything speculative):** P0 complete · rate limiting
done immaculately (both strategies, concurrency-proven) · **one** clean resilience feature ·
api_key auth · flawless DECISIONS.md + README. Everything beyond the floor is bonus — the
rubric rewards "3 clean features over 6 brittle" (spec), so partially-done features are cleanly
stubbed with a "next steps" note, never half-wired. The per-tier human-review gate must not
consume build time.

---

## Architecture & extensibility (grade-critical — 60% of the rubric)

`criteria.md` weights Architectural Judgment (35%) and Readability/extensibility (25%). Its
literal litmus test is "another engineer could extend with a **new config feature** in an
afternoon." The design must make that true, not just implement features.

- **Pluggable pipeline, not a switch statement.** The request pipeline is an ordered list of
  composable **stages**, each implementing a common trait
  (`async fn apply(&self, ctx: &mut RequestCtx) -> Flow`, where `Flow = Continue |
  ShortCircuit(Response)`). Stages are assembled **per route** from the parsed config and
  iterated in order. Adding a config feature = add a config struct + one stage + register it —
  no changes to the core loop. *This is the single highest-leverage design decision; call it
  out in DECISIONS.md with a worked "to add feature X…" example.*
- **Module map (separation of concerns):**
  - `config/` — parse YAML → typed structs; durations, enums, and **cross-field** validation
    at load (see Cross-cutting).
  - `pipeline/` — the `Stage` trait + one file per stage (`method`, `auth`, `rate_limit`,
    `request_transform`, `circuit_breaker`, `load_balance`, `response_transform`).
  - `upstream/` — HTTP client, per-target pool, health status, target selection.
  - `server` — hyper wiring, `RequestCtx`, error→status mapping, access logging.
  - `mock_upstream` — canned / slow / flaky endpoints so tests are self-contained.

---

## P0 — Core Requirements (non-negotiable)

The spec lists these as the baseline. Implement and test these before anything else.

### P0.1 — Startup & config loading
- Read config from a YAML file path passed as a **CLI argument or environment variable**.
- Listen on the port from `gateway.port` (example: 8080). *(Core Req #1 says "port 8080"; we
  bind `gateway.port` because the graders' config is schema-general — the literal 8080 is an
  example value, not a hard-coded requirement.)*
- On **malformed or invalid config**, fail fast with a clear error and non-zero exit — do not
  start half-configured. Validation covers durations, enums, **and cross-field constraints**
  (see Cross-cutting). (Production-thinking: graders will feed a different config.)
- **Acceptance:** `gateway --config path.yaml` (and/or `CONFIG=path.yaml gateway`) boots and
  binds the configured port; a broken YAML exits with a readable message.

### P0.2 — Health endpoint
- `GET /health` **always** returns `200` with JSON `{ "status": "healthy",
  "uptime_seconds": <int> }`, regardless of config and regardless of any configured route.
- Not rate-limited, not authed, not routed to an upstream. Handled before pipeline assembly.
- **Acceptance:** works before/independent of any route; `uptime_seconds` is a monotonic int.

### P0.3 — Basic proxying
- A request matching a configured route is forwarded to the route's upstream, and the
  upstream response (status, headers, body) is returned to the client.
- **Unmatched path → `404`.**
- **Route matching:** path is a **prefix**, matched on **path-segment boundaries** — `/api/users`
  matches `/api/users` and `/api/users/…` but **not** `/api/usersXYZ`. On overlap, match the
  **longest** matching prefix. Path is matched first; method filtering (P0.4) is applied to the
  chosen route (we do not fall back to a shorter route to satisfy the method). *(Decision — spec
  is silent; segment-boundary + longest-match + path-first documented in DECISIONS.md.)*
- **Header hygiene (real-proxy correctness, see Cross-cutting):** strip hop-by-hop headers,
  rewrite `Host` to the upstream authority, recompute `Content-Length` when the body changes.
- **Acceptance:** request to a configured path reaches a mock upstream and its response is
  relayed faithfully; `/api/usersXYZ` does **not** match `/api/users`; unknown path returns 404.

### P0.4 — Method filtering
- Requests whose method is not in the route's `methods` list → **`405` Method Not Allowed**,
  with an **`Allow` header** listing the route's permitted methods.
- **Acceptance:** `POST` to a `["GET"]`-only route returns 405 with `Allow: GET`; `GET` passes.

### P0.5 — Schema-general, not example-specific
- Works with **any** valid config following the schema: any number of routes, any paths,
  any subset of optional blocks present or absent. No hard-coded paths, ports, or keys.
- **Acceptance:** a **second config fixture** with different routes/values boots and behaves
  correctly with no code changes (this test is a P0 deliverable, not an afterthought).

---

## P1 — High value, build next

*(Rate limiting is sequenced first: it is the gateway's namesake, the "50 simultaneous
requests" concurrency demonstration the rubric calls out, and the item most at risk if rushed.)*

### P1.1 — Rate limiting (global + per-route)
The gateway's namesake feature; central and high-value; the marquee concurrency proof.
- `gateway.global_rate_limit` applies to every route **unless** the route defines its own
  `rate_limit`, which **fully overrides** (not merges with) the global. *(Decision: override,
  not merge.)*
- Fields: `requests`, `window` (duration), `strategy`, `per`.
- **Strategies:**
  - `fixed_window` — count per discrete window; reset at window boundary.
  - `sliding_window` — sliding-counter (weighted current + previous window, O(1)/key) so bursts
    across a boundary are still limited.
- **Bucket identity (`per`):** the counter key is `(route_id, per_key)`. The global default is
  instantiated **per route** (each route lacking its own `rate_limit` gets its own bucket from
  the global config — routes do not share one gateway-wide budget).
  - `ip` — key is `(route_id, client_ip)`. *(Decision: derive IP from the socket peer; do not
    trust `X-Forwarded-For` unless we add explicit trusted-proxy config. Document this.)*
  - `global` — key is `(route_id)` — one shared bucket **per route** (not gateway-wide).
- **Over limit → `429` Too Many Requests** with a `Retry-After` header (integer seconds until
  capacity; for sliding-counter, seconds until the weighted count falls below `requests`).
  *(Decision: 429 + `Retry-After`; spec names 429 only for the breaker's 503.)*
- **Concurrency & memory (production-thinking):** counters must be correct under simultaneous
  requests (spec: "50 requests hit a rate-limited route simultaneously"). Use a **sharded map
  / atomic per-entry counters** so the map lock does not serialize the very concurrency it
  guards. Per-IP keys grow unbounded → **evict idle keys** (windowed cleanup / lazy expiry).
- **Acceptance:** N+1 requests in a window → the (N+1)th gets 429; 50 parallel requests do not
  over-admit (exact admitted count); per-IP isolation verified; both strategies tested at a
  window boundary; idle keys are reclaimed.

### P1.2 — `strip_prefix`
- `strip_prefix: false` → forward the original request path unchanged.
- `strip_prefix: true` → remove the matched route prefix before forwarding
  (`/api/products/123` → `/123`; `/api/legacy/v1/data` → `/v1/data`).
- Edge: stripping the whole path yields `/`. Preserve query string in both modes.
- **Acceptance:** upstream receives the correctly transformed path in both modes.

### P1.3 — Timeouts (global + per-route override)
- `gateway.global_timeout` applies to all upstream requests by default.
- A route's upstream timeout overrides the global for that route. In the example config, both
  `/api/orders` and `/api/products` place `timeout` **under `upstream`**; we parse `timeout`
  **both under `upstream` and at the route level** (tolerant), route-specific value wins over
  global. *(Decision: accept either placement; upstream/route value beats global.)*
- On timeout, return **`504` Gateway Timeout** (unless retry/circuit-breaker intervenes).
- **Timeout scope (see P2.1):** the configured timeout is **per upstream attempt**; a separate
  overall wall-clock budget caps total time across retries + backoff.
- Parse duration strings (`"30s"`, `"5s"`, `"1s"`) into durations; reject unparseable values
  at config load.
- **Acceptance:** a slow mock upstream exceeding the timeout yields 504 within the bound.

---

## P2 — Resilience & policy

*(Auth is the cheapest, highest-signal stage here — a ~20-line fast rejection that proves the
pluggable-stage architecture — so build it first within P2.)*

### P2.1 — Authentication (`api_key`)
- Config: `auth.type: api_key`, `auth.header`, `auth.keys` (list).
- Request must present the named header with a value in `keys`; otherwise **`401`
  Unauthorized**. *(Decision: missing/invalid both → 401; do not distinguish 403.)*
- Applies only to routes that declare `auth`. Constant-time compare where practical.
- **Pipeline placement:** auth runs **before** rate limiting (protects the key compare;
  `per` is ip/global, not per-key, so identity is not needed to bucket). *Tradeoff noted:*
  rate-limit-first would shed bad-key floods more cheaply — documented in DECISIONS.md.
- **Acceptance:** valid key passes; missing/invalid key → 401; other routes unaffected.

### P2.2 — Retry with backoff
- Config: `retry.attempts`, `retry.backoff` (`fixed` | `exponential`),
  `retry.initial_delay`, `retry.on` (list of HTTP status codes).
- Retry when the upstream responds with a status in `on`, **or** on connection failure/timeout.
  *(Decision: also retry transport errors/timeouts, not just listed statuses.)*
- `attempts` = **total tries** (not additional retries). *(Decision; document.)* Between
  attempts wait: `fixed` → `initial_delay` each time; `exponential` → `initial_delay * 2^(n-1)`.
- **Timeout interaction:** each attempt gets the per-attempt timeout (P1.3); an overall budget
  caps total wall-clock across attempts + backoff. On exhaustion, return the last upstream
  error (mapped: timeout→504, connection→502).
- **Load-balancer interaction:** on retry, select the **next healthy target** (P2.4) rather
  than hammering the same one.
- **Circuit-breaker interaction:** a fully-failed request counts as **one** failure toward the
  breaker threshold (per-request outcome, not per-attempt); retry sits **inside** the breaker
  gate. *(Decision.)*
- **Safety:** the spec's `/api/orders` retries on `POST/PUT`. *(Decision: honor config as
  written — retry regardless of method, since the config author opted in; note the at-least-once
  risk in DECISIONS.md.)*
- **Acceptance:** a flaky mock returning 503 then 200 succeeds within `attempts`; backoff
  delays observed; exhausted retries return the last upstream error; one failed retried request
  increments the breaker by one.

### P2.3 — Circuit breaker
- Config: `threshold` (failures), `window`, `cooldown`.
- **Closed** → count failures (5xx / timeouts / connection errors) in a rolling `window`; at
  `threshold`, trip to **Open**.
- **Open** → immediately return **`503`** with body
  `{ "error": "service_unavailable", "retry_after": <seconds_remaining> }` without contacting
  upstream, until `cooldown` elapses.
- **Half-Open** → after cooldown, allow a trial request; success → Closed, failure → Open again.
  *(Decision: single-probe half-open; document.)*
- **Granularity:** breaker state is **per target** (per-upstream), composed with per-target
  health (P2.4/P2.5): a request fails over to the next target whose breaker is Closed and whose
  health is good. For single-`url` upstreams this reduces to one breaker. *(Decision: per-target,
  not per-route; document.)* In-memory, concurrency-safe.
- **Acceptance:** `threshold` induced failures trip the breaker; subsequent calls get the 503
  envelope with a decreasing `retry_after`; recovery after cooldown; multi-target routes trip
  independently per target.

### P2.4 — Load balancing across targets
- Config: `upstream.targets[]` each with `url` and `weight`; `upstream.balance`
  (`round_robin` | `weighted_round_robin`).
- `round_robin` — even rotation across targets; `weight` is **ignored** in this mode.
- `weighted_round_robin` — distribute in proportion to `weight` (3:1 in the example); use
  smooth weighted round-robin.
- Concurrency-safe target selection (atomic cursor).
- Interacts with health checks (P2.5) and breakers (P2.3): skip targets currently marked
  unhealthy or whose breaker is Open.
- **Acceptance:** over many requests, distribution matches the configured weights; `round_robin`
  ignores weights; single-`url` upstreams (no `targets`) still work.

### P2.5 — Health checks (active, with passive ejection)
- Config: `health_check.path`, `health_check.interval`, `health_check.unhealthy_threshold`.
- **Active:** periodically (`interval`) probe each target's `path`; after `unhealthy_threshold`
  consecutive failures, mark the target **unhealthy** and remove it from the LB pool; restore
  after a success. *(Decision: recover on first successful probe; document.)*
- **Passive:** live-traffic failures also feed per-target breaker state (P2.3) so a target that
  dies mid-interval is ejected before the next probe — the `interval` gap is not a blind spot.
- `health_check` applies to any upstream, including single-`url` (probe that one url).
- If **all** targets are unhealthy → return `503`. *(Decision.)*
- Background probing must not block request handling.
- **Acceptance:** a target whose `/healthz` fails `threshold` times stops receiving traffic;
  traffic resumes after it recovers; a target failing live requests is ejected before its next
  scheduled probe.

---

## P3 — Transformation & advanced

Highest implementation cost; smallest baseline-grade impact. Do these last / partially.

### P3.1 — Request transform: headers
- `request_transform.headers.add` — add/overwrite headers on the forwarded request.
- `request_transform.headers.remove` — strip listed headers before forwarding.
- **Dynamic values:** `$request_time` → inject request timestamp;
  `$literal:<value>` → literal string.
- **Timestamp format:** `$request_time` is a single value computed **once per request** (so the
  header and body-mapping uses agree), formatted as **RFC 3339 / ISO-8601 UTC**. `$response_time`
  likewise once per response. *(Decision; document.)*
- **Acceptance:** upstream sees added headers (incl. resolved `$request_time`) and does not see
  removed ones; the same request's `$request_time` is identical across header and body.

### P3.2 — Request transform: body mapping
- `request_transform.body.mapping` maps `destination.path <- "source"` using **dot notation**,
  building a new JSON body from the incoming one.
- Sources: a dot-path into the incoming body; `$literal:<value>`; `$request_time`.
- Only meaningful for JSON bodies. *(Decision: if body isn't JSON, pass through unchanged and
  note it; document.)* Recompute `Content-Length` after rebuilding the body.
- **Body buffering:** transforms and POST/PUT retries require buffering the request body; enforce
  the cross-cutting max-body-size + read-timeout caps (reject oversize with 413).
- **Acceptance:** given the example mapping, upstream receives the restructured body
  (`user.id`, `user.name`, `meta.source`, `meta.timestamp`); non-JSON passes through.

### P3.3 — Response transform: headers
- `response_transform.headers.add` / `remove` applied to the response before returning to the
  client. Same `$literal` / dynamic-value rules where applicable.
- **Acceptance:** client sees added/removed response headers.

### P3.4 — Response transform: body envelope
- `response_transform.body.envelope` wraps the upstream response body in a new structure.
- Placeholders: `$body` (original response body), `$response_time`, `$route_path` (matched
  route path).
- **Non-JSON / empty upstream body:** embed the raw body as a JSON string under `$body` (do not
  500); recompute `Content-Length`. Envelope applies only to real upstream responses, **not** to
  gateway-generated errors (404/401/429/502/504/breaker-503). *(Decision; document.)*
- **Acceptance:** client receives the enveloped JSON with `data` = original body and
  `gateway_metadata` populated; a non-JSON upstream body is embedded as a string, not an error.

---

## Cross-cutting requirements

- **Request pipeline order** *(Decision — document in DECISIONS.md):*
  `match route → method filter → auth → rate limit → request transform → circuit-breaker gate
  → load-balance/select target → timeout+retry around upstream call → response transform →
  return`. Fast rejections (404/405/401/429) happen before any upstream work. *(Auth-before-
  rate-limit tradeoff noted in P2.1.)*
- **Proxy correctness (real-gateway hygiene):** strip **hop-by-hop** headers in both directions
  (`Connection`, `Transfer-Encoding`, `Keep-Alive`, `Upgrade`, `Proxy-*`, plus anything named in
  `Connection`); rewrite the forwarded `Host` to the upstream authority; recompute
  `Content-Length` whenever the body is transformed.
- **Body limits (production-thinking / DoS):** cap max request body size and apply a read
  timeout even when transforms are stubbed; oversize → `413`. Buffer bodies only as needed for
  retry/transform.
- **Concurrency correctness:** rate-limit counters, breaker state, LB cursors, and health
  status are shared mutable state accessed by many in-flight requests; all must be race-free
  (sharded maps / atomics / per-key locks), with no map-level lock serializing throughput.
- **Failure modes / error mapping:** unmatched 404, bad method 405 (+`Allow`), auth 401, rate
  limit 429 (+`Retry-After`), circuit open 503 (envelope), all targets unhealthy 503, upstream
  timeout 504, upstream connection error 502, oversize body 413. No unhandled panics leak to
  clients.
- **Config parsing & validation:** parse all durations and enums (`strategy`, `backoff`,
  `balance`, `per`, `auth.type`) up front and reject unknown values. **Cross-field validation:**
  `weighted_round_robin` requires `weight`s; a route has either `upstream.url` **or**
  `upstream.targets`, not both; `health_check` requires a `path`; unknown/duplicate routes
  rejected. Fail fast with non-zero exit.
- **Observability:** one structured access-log line per request (matched route, decision/status,
  latency, chosen target) plus error logging — makes "upstream down" and rate-limit rejections
  observable.
- **Testing (spec deliverable):** self-contained test suite runnable with **one command**,
  including a mock upstream server (with a slow and a flaky endpoint) so tests need no external
  services. Include the P0.5 alternate-config boot test.
- **Deliverables (Communication, 15%):** `DECISIONS.md` (prioritization, architecture/pipeline,
  what's next, partial features, AI usage), `README.md` (setup, run, test command, **feature
  checklist**), and an atomic **commit-per-tier history that narrates the build order**.

---

## Prioritization rationale (for DECISIONS.md)

1. **P0 first** because they're the graded baseline and everything depends on the proxy core.
2. **Rate limiting first in P1:** it's the repo's namesake, the "50 simultaneous requests"
   production-thinking prompt, and the clearest concurrency demonstration — highest signal, and
   the item most damaged if rushed, so it gets built while time and focus are fresh.
3. **`strip_prefix` + timeouts** next: cheap, touch every proxied request, easy to get subtly
   wrong — high ROI once the namesake feature is solid.
4. **Resilience (auth, retry, circuit breaker, LB, health)** shows production maturity; each is
   independently testable and degrades gracefully if partially done. Auth first within P2 — it's
   the cheapest stage and proves the pluggable-stage architecture.
5. **Transforms last:** the most code for the least baseline grade, and the easiest to leave
   cleanly stubbed with a clear "next steps" note — exactly the "well-architected 3 features
   beats brittle 6" trade-off the spec rewards.

## Open questions / assumptions to confirm

Marked *(Decision)* inline above where the spec is silent. The load-bearing ones (all resolved
in DECISIONS.md):
- Route match = **segment-boundary longest prefix**, path-first-then-405.
- Rate-limit bucket key = `(route_id, per_key)`; global default is **per-route**; `per: global`
  is **per-route**, not gateway-wide.
- Client IP source for `per: ip` (socket peer vs. `X-Forwarded-For`).
- `retry.attempts` = total tries; retry non-idempotent methods; per-attempt timeout + overall
  budget; one failed retried request = one breaker failure; retry advances to next target.
- Circuit breaker granularity = **per target**; single-probe half-open; health recovery = first
  success; passive ejection between probes.
- Rate-limit override (route replaces global) rather than merge.
- 429 (+`Retry-After`) as the rate-limit status; 405 carries `Allow`; 413 for oversize bodies.
- `$request_time`/`$response_time` = RFC-3339 UTC, computed once per request/response.
- Response envelope skips gateway-generated errors; non-JSON body embedded as a string.
