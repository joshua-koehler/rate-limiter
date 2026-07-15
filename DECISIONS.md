# DECISIONS
Exhaustive but terse. Principles live in convictions.md; details live in code.

## Scope & priority
**Gate:** advance a tier only after tests pass *and* a human-judged adversarial review passes. Quality per tier, not raw coverage. A few features done cleanly beats many half-done.

Build order:
1. **P0** — config load, `/health`, prefix proxying (longest match, 404), method filter (405), general schema.
2. **P1** — rate limiting (global + per-route) first, then `strip_prefix`, then timeouts (→504).
3. **P2** — auth first, then retry/backoff, circuit breaker, load balancing, health checks.
4. **P3** — request/response transforms.

**Floor:** P0 + rate limiting (both strategies, concurrency-proven) + one resilience feature + auth + clean docs. Beyond is bonus — stub cleanly, never half-wire.

## Architecture
- **Pluggable pipeline (the extensibility bet):** ordered list of `Stage`s (`async fn apply(&mut ctx) -> Continue | ShortCircuit(Response)`), assembled per-route from config. New feature = config struct + one stage + register; no core-loop change.
- **Modules:** `config/` (parse+validate), `pipeline/` (`Stage` trait, one file per stage), `upstream/` (client, pool, health), `server` (hyper wiring, `RequestCtx`, errors, access log), `mock_upstream` (tests).
- **Order:** match route → method → auth → rate limit → request transform → breaker gate → select target → timeout+retry → response transform → return. Fast rejects (404/405/401/429) before upstream work.
- **State:** all in-memory, single instance. Counters/breakers/cursors/health are race-free via sharded maps + atomic per-entry counters (no map-level lock).
- **HTTP:** `hyper`/`hyper-util` as low-level transport only (std has no HTTP). All routing/proxy hand-built; no gateway crate.
- **Proxy hygiene:** strip hop-by-hop headers both ways, rewrite `Host`, recompute `Content-Length` on body change.
- **Validation:** parse durations + enums at load; cross-field checks (weighted RR needs weights; `url` xor `targets`; `health_check` needs `path`); reject unknowns with non-zero exit.
- **Observability:** one structured access-log line per request + error logging.
- **Body:** cap size + read timeout; oversize → 413.

## Resolved ambiguities (spec silent → our call)
- **Route match:** segment-boundary longest prefix (`/api/users` ≠ `/api/usersXYZ`). Path first; wrong method on chosen route → 405, no fallback.
- **Port:** bind `gateway.port`, don't hardcode.
- **Client IP:** use socket peer, not `X-Forwarded-For`.
- **Rate-limit bucket:** per route. Route `rate_limit` fully replaces global (no merge). Over limit → 429 + `Retry-After`. `sliding_window` = weighted current+previous window, O(1)/key. Sharded map + idle-key eviction.
- **Timeouts:** accept under `upstream` and at route level; route/upstream beats global. Per-attempt; a separate wall-clock budget caps retries+backoff.
- **Retry:** `attempts` = total tries. Retry on listed statuses + transport errors/timeouts, even for non-idempotent methods (at-least-once accepted). Advance to next healthy target. A fully-failed request = one breaker failure; retry sits inside the breaker gate.
- **Circuit breaker:** per-target, single-probe half-open; open → 503 `{error, retry_after}`. Counts 5xx/timeout/connection errors. Single `url` = one breaker.
- **Auth:** missing or invalid key → 401 (no 403); constant-time compare. Runs before rate limiting (protects key compare; `per` is ip/global so no identity needed).
- **Health checks:** active probe; unhealthy after `unhealthy_threshold` consecutive fails; recover on first success; all unhealthy → 503. Passive ejection: live failures feed the breaker so dead targets drop before the next probe. Applies to single `url` too.
- **Load balancing:** `round_robin` ignores weight; `weighted_round_robin` = smooth weighted RR; atomic cursor; skip unhealthy/Open.
- **Transforms:** JSON bodies only; non-JSON request body passes through. Response envelope embeds non-JSON/empty upstream body as a JSON string under `$body`; applies only to real upstream responses, never gateway errors.
- **Dynamic values:** `$request_time`/`$response_time` = RFC-3339 UTC, computed once per request/response. `$route_path` = matched route.
- **Headers:** 405 carries `Allow`; 413 for oversize bodies.

## Error mapping
404 unmatched · 405 bad method (+`Allow`) · 401 auth · 429 rate limit (+`Retry-After`) · 413 oversize · 503 breaker open / all unhealthy · 504 upstream timeout · 502 upstream connection error. No panic leaks to clients.

## Deliverables
`DECISIONS.md` · `README.md` (setup, run, one-command test, checklist) · atomic commit-per-tier history · self-contained tests incl. mock upstream (slow + flaky) + alternate-config boot test.

## Status
All four tiers complete, reviewed, green.
- **P0:** config + cross-field validation, `/health`, longest-prefix proxying + 404, method filter (405 + `Allow`), pluggable `Stage` pipeline, access logging.
- **P1:** rate limiting (fixed + sliding, `per: ip`/`global`, sharded + atomic check-and-increment proven exact under 50 concurrent requests, idle-key eviction, 429 + `Retry-After`); `strip_prefix`; per-attempt timeouts (route → upstream → global, 504).
- **P2:** auth (before rate-limit, constant-time, 401); retry+backoff (fixed/exp, per-attempt timeout + overall budget, failover to next target); per-target breaker (single-probe half-open, 503 envelope); load balancing (RR + smooth weighted RR, skips unhealthy/Open); active health checks + passive ejection. All target state in one per-route `UpstreamRegistry`. Body cap + 413 landed here.
- **P3:** request transform (header add/remove + JSON dot-path body mapping, non-JSON passthrough); response transform (header add/remove + `$body`/`$response_time`/`$route_path` envelope, upstream-only). `Content-Length` recompute falls out free.

## As-built notes (where impl concretized the plan)
- **Module layout:** `pipeline/` and `upstream/` are dirs (the extensibility surface); `config`/`server`/`health`/`router`/`error` stay single-file.
- **Route match before the chain** (it picks the chain); `MethodStage` is the first stage.
- **Unified upstream runtime:** breaker/balancer/health collapsed into one `UpstreamRegistry` — all three key on the same per-target identity and are consulted together. Single `url` = a one-target pool. Selection is one scan: skip unhealthy, skip Open, take first eligible.
- **Two 503 flavours:** nothing eligible + an Open breaker → `CircuitOpen{retry_after}` (soonest cooldown); otherwise all health-ejected → `AllTargetsUnhealthy`.
- **Breaker accounting:** at most one failure per target per request (`failed_targets` set). Single `url` retried 3× = one failure; with failover each distinct failed target = one. A `<500` response = success even if its status is in `retry.on`.
- **Retry ⟂ breaker classification:** whether to retry (status ∈ `retry.on` / timeout / transport) is separate from whether it's a breaker failure (5xx / timeout / transport). A 500 not in `retry.on` relays to client but still counts against the breaker.
- **Limits:** body cap = 2 MiB (`MAX_BODY_BYTES`), enforced while reading. Retry budget = `attempts * per_attempt_timeout + Σ backoffs`. Slow body → 30s read timeout (408) + connection `header_read_timeout` (slow-loris guard).
- **Defaults:** multiple `targets` no `balance` → round-robin; lone target → no balancing. `breaker.threshold: 0` clamped to 1.
- **Extensibility scope (honest):** `Stage`/`ResponseStage` chains cover request/response-phase policies (method, auth, rate limit, transforms) — genuinely "config struct + one stage + register." Upstream-call concerns (retry, breaker, LB, health) are *not* stages — they loop, select targets, wrap the terminal call. Their surface is the `upstream` module: new balancer/breaker policy = enum variant + match arm (an afternoon); a wholly new upstream concern means editing `upstream::proxy`'s retry loop. The litmus test holds per-surface, not as one universal mechanism.
- **Unconditional body buffering (tradeoff):** `upstream::proxy` buffers every request body once (retry needs it re-sendable; body mapping reuses it). Cost vs P0 streaming: even a plain route buffers up to `MAX_BODY_BYTES` (413 above) and a slow body is bounded (408). Accepted for uniform DoS posture + one body path.
- **Transforms extend the same pipeline both ways:** request headers = `RequestTransformStage` (`Stage`); response headers+envelope = `ResponseTransformStage` (`ResponseStage`). Neither request nor response loop changed.
- **Where body transforms run:** request-body mapping runs at the buffer boundary in `upstream::proxy` (the retry loop already buffers once — a second buffer of `Incoming` is impossible inside `&mut RequestCtx`). Both header and body read the one `RequestCtx.request_time`. Cost: request-transform logic in two files, accepted.
- **Envelope only on real upstream responses:** runs on the `Ok(resp)` arm only; gateway errors take `Err` and skip it. Enveloping buffers the response; header-only transforms leave it streamed. Non-JSON/empty body embeds as a JSON string, never 500 (presentation must not fail a good response).
- **Hand-rolled RFC-3339 UTC:** ~15-line civil-from-days routine (`pipeline::transform`), keeping the from-scratch boundary crisp. Unit-tested incl. a leap day. (P3 review later swapped this for the `time` crate.)
- **`$literal:` + missing sources:** `$literal:<v>` taken verbatim (lets a value start with `$`). A body-mapping source absent in the input is skipped, not written as null.

## Process notes
- **P1/P2 built by two parallel agent teams** on disjoint files, with shared error variants pre-added to avoid conflicts; compilation deferred to tier end (speed tradeoff). P2 single integration fix: the alternate-config auth route now enforces the key (200 with, 401 without). 92 tests green before the P2 gate.
- **P2 review** (3 reviewers: correctness/architecture/spec; human-judged) → pass with iteration. **Fixed:** (1) breaker reset its count on every non-5xx, so a partially-degrading upstream never tripped — failures now age out by time, only a half-open success closes; (2) passive ejection required a configured breaker, leaving `health_check`-only routes blind — live failures now feed a per-target passive-health streak too; (3) added 30s body-read timeout (408) + connection `header_read_timeout`. **Deferred:** the scope + buffering notes above, plus access-log target, `Connection`-token stripping, lock-poison recovery, some acceptance-test gaps.
- **P3 built by three parallel agent teams** on disjoint files; orchestrator held the conflict-sensitive wiring (`RequestCtx.request_time`, the two `assemble*` registrations, the body-mapping hook). Single fix: the alternate-config response-transform route returns the live envelope. 119 tests green before the P3 gate. Mid-tier, a P1-review agent landed the symmetric response-phase seam; P3 adopted it over the planned free function.
- **Mid-P1 refactor (kept, human's call):** an agent refactored P0's free-functions into the promised `Stage`-trait module, briefly regressing `strip_prefix`/timeouts (re-applied). Kept — it's the extensibility contract, and rate limiting already builds on it. Commits + this note kept over rewriting history.

## AI tooling
Claude Code used to: parse the spec into a feature taxonomy; spec out the solution off my guidelines with me deciding; log my decisions here; run parallel agent teams within a tier; run adversarial review per tier.
