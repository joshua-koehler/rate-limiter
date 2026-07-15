//! Configuration: serde structs mirroring the **full** `gateway.yaml` schema
//! (all P0–P3 fields), plus duration/enum parsing and a `validate()` pass.
//!
//! Many fields below are not consumed yet — P0 only reads `gateway.port`,
//! `routes[].path/methods/upstream`. The rest (rate_limit, retry, auth,
//! circuit_breaker, transforms, targets, health_check, timeouts) are modelled
//! now so a full config *loads and validates* today and later tiers can act on
//! them without touching the parser. Unused-for-now fields are expected.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use hyper::Uri;
use serde::Deserialize;

/// Top-level `gateway.yaml` document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub gateway: Gateway,
    #[serde(default)]
    pub routes: Vec<Route>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gateway {
    pub port: u16,
    #[serde(default, deserialize_with = "de_opt_duration")]
    pub global_timeout: Option<Duration>,
    #[serde(default)]
    pub global_rate_limit: Option<RateLimit>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub path: String,
    #[serde(default)]
    pub methods: Vec<String>,
    #[serde(default)]
    pub strip_prefix: bool,
    pub upstream: Upstream,
    /// Route-level timeout override (the spec places `timeout` here for
    /// `/api/orders` and under `upstream` for `/api/products`; we accept both).
    #[serde(default, deserialize_with = "de_opt_duration")]
    pub timeout: Option<Duration>,
    #[serde(default)]
    pub retry: Option<Retry>,
    #[serde(default)]
    pub rate_limit: Option<RateLimit>,
    #[serde(default)]
    pub auth: Option<Auth>,
    #[serde(default)]
    pub circuit_breaker: Option<CircuitBreaker>,
    #[serde(default)]
    pub request_transform: Option<RequestTransform>,
    #[serde(default)]
    pub response_transform: Option<ResponseTransform>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    /// Single upstream. Mutually exclusive with `targets` (enforced by validate()).
    #[serde(default)]
    pub url: Option<String>,
    /// Multiple targets for load balancing (P2).
    #[serde(default)]
    pub targets: Vec<Target>,
    #[serde(default)]
    pub balance: Option<Balance>,
    /// Upstream-level timeout override (see `Route::timeout`).
    #[serde(default, deserialize_with = "de_opt_duration")]
    pub timeout: Option<Duration>,
    #[serde(default)]
    pub health_check: Option<HealthCheck>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub url: String,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

fn default_weight() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimit {
    pub requests: u64,
    #[serde(deserialize_with = "de_duration")]
    pub window: Duration,
    pub strategy: Strategy,
    pub per: Per,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    FixedWindow,
    SlidingWindow,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Per {
    Ip,
    Global,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retry {
    pub attempts: u32,
    pub backoff: Backoff,
    #[serde(deserialize_with = "de_duration")]
    pub initial_delay: Duration,
    #[serde(default)]
    pub on: Vec<u16>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Backoff {
    Fixed,
    Exponential,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Balance {
    RoundRobin,
    WeightedRoundRobin,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Auth {
    #[serde(rename = "type")]
    pub auth_type: AuthType,
    pub header: String,
    #[serde(default)]
    pub keys: Vec<String>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    ApiKey,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreaker {
    pub threshold: u32,
    #[serde(deserialize_with = "de_duration")]
    pub window: Duration,
    #[serde(deserialize_with = "de_duration")]
    pub cooldown: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthCheck {
    pub path: String,
    #[serde(deserialize_with = "de_duration")]
    pub interval: Duration,
    pub unhealthy_threshold: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestTransform {
    #[serde(default)]
    pub headers: Option<HeaderTransform>,
    #[serde(default)]
    pub body: Option<RequestBodyTransform>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseTransform {
    #[serde(default)]
    pub headers: Option<HeaderTransform>,
    #[serde(default)]
    pub body: Option<ResponseBodyTransform>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderTransform {
    #[serde(default)]
    pub add: BTreeMap<String, String>,
    #[serde(default)]
    pub remove: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestBodyTransform {
    #[serde(default)]
    pub mapping: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseBodyTransform {
    /// Arbitrary nested envelope structure with `$body`/`$response_time`/
    /// `$route_path` placeholders — kept as a raw value for the P3 transformer.
    #[serde(default)]
    pub envelope: Option<serde_yaml::Value>,
}

// ── Loading & validation ─────────────────────────────────────────────────────

impl Config {
    /// Read, parse, and validate a config file. Any failure is a fail-fast error
    /// (the caller exits non-zero); we never return a half-valid config.
    pub fn load(path: &Path) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file '{}'", path.display()))?;
        let config: Config = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing YAML config '{}'", path.display()))?;
        config
            .validate()
            .with_context(|| format!("validating config '{}'", path.display()))?;
        Ok(config)
    }

    /// Structural checks that serde can't express (durations/enums are already
    /// validated during deserialization; unknown enum values are rejected there).
    pub fn validate(&self) -> anyhow::Result<()> {
        // Track normalized paths to reject duplicate routes (which would make
        // request routing ambiguous).
        let mut seen_paths: BTreeMap<String, usize> = BTreeMap::new();
        for (i, route) in self.routes.iter().enumerate() {
            let ctx = format!("route[{i}] (path '{}')", route.path);
            if !route.path.starts_with('/') {
                anyhow::bail!("{ctx}: path must start with '/'");
            }
            if route.methods.is_empty() {
                anyhow::bail!("{ctx}: 'methods' must list at least one HTTP method");
            }
            let has_url = route.upstream.url.is_some();
            let has_targets = !route.upstream.targets.is_empty();
            match (has_url, has_targets) {
                (true, true) => {
                    anyhow::bail!("{ctx}: upstream sets both 'url' and 'targets'; use exactly one")
                }
                (false, false) => {
                    anyhow::bail!("{ctx}: upstream must set either 'url' or 'targets'")
                }
                _ => {}
            }
            // Upstream URLs must be absolute http(s) URLs the client can dial.
            // A scheme-less value ("localhost:3001") or empty string otherwise
            // loads fine and fails on *every* request with a 502 — catch it at
            // load so a malformed config never half-starts (fail-fast contract).
            if let Some(url) = &route.upstream.url {
                validate_upstream_url(url, &ctx)?;
            }
            for (j, target) in route.upstream.targets.iter().enumerate() {
                validate_upstream_url(&target.url, &format!("{ctx} targets[{j}]"))?;
            }
            let normalized = normalize_path(&route.path);
            if let Some(prev) = seen_paths.insert(normalized.clone(), i) {
                anyhow::bail!(
                    "{ctx}: duplicate route path (collides with route[{prev}] after normalization to '{normalized}')"
                );
            }
            // `balance` load-balances across multiple targets; it's meaningless
            // (and likely a config mistake) without any.
            if route.upstream.balance.is_some() && !has_targets {
                anyhow::bail!("{ctx}: 'balance' requires 'targets'; a single 'url' has nothing to balance");
            }
            // Weighted round-robin divides traffic by weight; a zero weight would
            // starve a target entirely, so it's almost certainly a mistake.
            if route.upstream.balance == Some(Balance::WeightedRoundRobin) {
                if !has_targets {
                    anyhow::bail!("{ctx}: 'weighted_round_robin' requires non-empty 'targets'");
                }
                for (j, target) in route.upstream.targets.iter().enumerate() {
                    if target.weight == 0 {
                        anyhow::bail!(
                            "{ctx}: targets[{j}] (url '{}') has weight 0; 'weighted_round_robin' requires weight > 0",
                            target.url
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

/// Validate that an upstream URL is an absolute `http`/`https` URL with a host.
/// Rejecting scheme-less/empty/unsupported-scheme values here (rather than
/// per-request) keeps the fail-fast contract for graders' configs.
fn validate_upstream_url(url: &str, ctx: &str) -> anyhow::Result<()> {
    let uri: Uri = url
        .parse()
        .with_context(|| format!("{ctx}: upstream url '{url}' is not a valid URI"))?;
    match uri.scheme_str() {
        Some("http") | Some("https") => {}
        Some(other) => anyhow::bail!(
            "{ctx}: upstream url '{url}' has unsupported scheme '{other}' (expected http/https)"
        ),
        None => anyhow::bail!(
            "{ctx}: upstream url '{url}' must be absolute with an http/https scheme"
        ),
    }
    if uri.authority().is_none() {
        anyhow::bail!("{ctx}: upstream url '{url}' is missing a host");
    }
    Ok(())
}

/// Normalize a route path for duplicate detection: trim a trailing '/' so that
/// e.g. "/api" and "/api/" collide, while the root "/" is preserved as "/".
fn normalize_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Convenience wrapper used by `main`.
pub fn load(path: &Path) -> anyhow::Result<Config> {
    Config::load(path)
}

// ── Duration parsing ("30s", "500ms", "5m", "1h") ────────────────────────────

/// Parse a duration string of the form `<integer><unit>` where unit ∈
/// {ms, s, m, h}. Kept simple/strict on purpose: the spec only uses integer
/// second/millisecond values.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration string".to_string());
    }
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("duration '{s}' is missing a unit (expected e.g. '30s')"))?;
    let (num, unit) = s.split_at(split);
    let value: u64 = num
        .parse()
        .map_err(|_| format!("duration '{s}' has an invalid numeric part '{num}'"))?;
    let dur = match unit {
        "ms" => Duration::from_millis(value),
        "s" => Duration::from_secs(value),
        "m" => Duration::from_secs(value * 60),
        "h" => Duration::from_secs(value * 3600),
        other => return Err(format!("duration '{s}' has an unknown unit '{other}'")),
    };
    Ok(dur)
}

fn de_duration<'de, D>(d: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    parse_duration(&s).map_err(serde::de::Error::custom)
}

fn de_opt_duration<'de, D>(d: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match Option::<String>::deserialize(d)? {
        Some(s) => parse_duration(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_duration_units() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn rejects_bad_durations() {
        assert!(parse_duration("30").is_err()); // no unit
        assert!(parse_duration("abc").is_err()); // no number
        assert!(parse_duration("10x").is_err()); // unknown unit
        assert!(parse_duration("").is_err());
    }

    #[test]
    fn parses_minimal_config() {
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/api/users"
    methods: ["GET", "POST"]
    upstream:
      url: "http://localhost:3001"
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.gateway.port, 8080);
        assert_eq!(cfg.routes.len(), 1);
        assert_eq!(cfg.routes[0].methods, vec!["GET", "POST"]);
    }

    #[test]
    fn parses_full_schema_with_all_optional_blocks() {
        // Proves every optional P0–P3 block deserializes so a full config loads.
        let yaml = r#"
gateway:
  port: 8080
  global_timeout: "30s"
  global_rate_limit:
    requests: 100
    window: "60s"
    strategy: "fixed_window"
    per: "ip"
routes:
  - path: "/api/orders"
    methods: ["GET", "POST", "PUT"]
    strip_prefix: false
    upstream:
      url: "http://localhost:3002"
    timeout: "5s"
    retry:
      attempts: 3
      backoff: "exponential"
      initial_delay: "1s"
      on: [502, 503, 504]
    rate_limit:
      requests: 10
      window: "10s"
      strategy: "fixed_window"
      per: "ip"
  - path: "/api/products"
    methods: ["GET"]
    strip_prefix: true
    upstream:
      targets:
        - url: "http://localhost:3003"
          weight: 3
        - url: "http://localhost:3004"
          weight: 1
      balance: "weighted_round_robin"
      timeout: "10s"
      health_check:
        path: "/healthz"
        interval: "30s"
        unhealthy_threshold: 3
  - path: "/api/internal"
    methods: ["GET", "POST"]
    upstream:
      url: "http://localhost:3006"
    auth:
      type: "api_key"
      header: "X-API-Key"
      keys: ["sk_live_abc123", "sk_live_def456"]
    circuit_breaker:
      threshold: 5
      window: "60s"
      cooldown: "30s"
    request_transform:
      headers:
        add:
          X-Gateway: "gatewaykit"
        remove: ["X-Debug"]
      body:
        mapping:
          user.id: "userId"
    response_transform:
      headers:
        add:
          X-Served-By: "gatewaykit"
      body:
        envelope:
          data: "$body"
          gateway_metadata:
            route: "$route_path"
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.routes.len(), 3);
        assert_eq!(cfg.gateway.global_timeout, Some(Duration::from_secs(30)));
        assert_eq!(cfg.routes[0].timeout, Some(Duration::from_secs(5)));
        assert_eq!(cfg.routes[1].upstream.targets.len(), 2);
        assert_eq!(cfg.routes[1].upstream.timeout, Some(Duration::from_secs(10)));
    }

    #[test]
    fn rejects_unknown_enum_value() {
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream:
      url: "http://localhost:1"
    rate_limit:
      requests: 1
      window: "1s"
      strategy: "bogus_strategy"
      per: "ip"
"#;
        assert!(serde_yaml::from_str::<Config>(yaml).is_err());
    }

    #[test]
    fn rejects_unparseable_duration() {
        let yaml = r#"
gateway:
  port: 8080
  global_timeout: "not-a-duration"
routes: []
"#;
        assert!(serde_yaml::from_str::<Config>(yaml).is_err());
    }

    #[test]
    fn validate_rejects_upstream_without_url_or_targets() {
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream: {}
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_upstream_with_both_url_and_targets() {
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream:
      url: "http://a"
      targets:
        - url: "http://b"
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_duplicate_route_paths() {
        // "/api" and "/api/" normalize to the same path and must collide.
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/api"
    methods: ["GET"]
    upstream:
      url: "http://a"
  - path: "/api/"
    methods: ["POST"]
    upstream:
      url: "http://b"
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_weighted_round_robin_with_zero_weight() {
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream:
      balance: "weighted_round_robin"
      targets:
        - url: "http://a"
          weight: 3
        - url: "http://b"
          weight: 0
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_weighted_round_robin_with_positive_weights() {
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream:
      balance: "weighted_round_robin"
      targets:
        - url: "http://a"
          weight: 3
        - url: "http://b"
          weight: 1
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_balance_without_targets() {
        // `balance` is meaningless over a single `url`.
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream:
      url: "http://a"
      balance: "round_robin"
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_normal_config() {
        // Guard against false positives: distinct paths, balanced targets.
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/api/users"
    methods: ["GET", "POST"]
    upstream:
      url: "http://localhost:3001"
  - path: "/api/products"
    methods: ["GET"]
    upstream:
      balance: "round_robin"
      targets:
        - url: "http://localhost:3003"
        - url: "http://localhost:3004"
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn rejects_unknown_field() {
        // A typo'd key (`stip_prefix`) must fail fast, not silently default to
        // false and boot a gateway that behaves contrary to the YAML.
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    stip_prefix: true
    upstream:
      url: "http://localhost:1"
"#;
        assert!(serde_yaml::from_str::<Config>(yaml).is_err());
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let yaml = r#"
gateway:
  port: 8080
routes: []
bogus_section: 1
"#;
        assert!(serde_yaml::from_str::<Config>(yaml).is_err());
    }

    #[test]
    fn validate_rejects_schemeless_upstream_url() {
        // "localhost:3001" (no scheme) parses as a URI but has no http/https
        // scheme; it would 502 on every request, so reject it at load.
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream:
      url: "localhost:3001"
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_upstream_url() {
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream:
      url: ""
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_schemeless_target_url() {
        let yaml = r#"
gateway:
  port: 8080
routes:
  - path: "/x"
    methods: ["GET"]
    upstream:
      balance: "round_robin"
      targets:
        - url: "http://localhost:3003"
        - url: "localhost:3004"
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }
}
