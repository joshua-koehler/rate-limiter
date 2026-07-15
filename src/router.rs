//! Route matching: longest matching prefix (a documented decision — the spec is
//! silent, but `strip_prefix` examples like `/api/products/123 -> /123` only
//! make sense as prefix matches).

use crate::config::Config;

/// Precomputed prefix table, ordered longest-first for longest-prefix wins.
pub struct Router {
    /// `(normalized_prefix, route_index)`, sorted by prefix length descending.
    entries: Vec<(String, usize)>,
}

/// A successful match: which route, and the path remainder after the prefix.
pub struct Match {
    pub route_index: usize,
    /// Path after the matched prefix (e.g. prefix `/api/products`, path
    /// `/api/products/123` -> tail `/123`). P0 forwards the original path; P1's
    /// `strip_prefix` consumes this tail.
    pub tail: String,
}

impl Router {
    pub fn build(config: &Config) -> Self {
        let mut entries: Vec<(String, usize)> = config
            .routes
            .iter()
            .enumerate()
            .map(|(i, r)| (normalize(&r.path), i))
            .collect();
        // Longest prefix first so the first match wins.
        entries.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
        Router { entries }
    }

    /// Return the longest-prefix match for `path` (path only, no query string).
    pub fn match_route(&self, path: &str) -> Option<Match> {
        self.entries.iter().find_map(|(prefix, idx)| {
            match_prefix(prefix, path).map(|tail| Match {
                route_index: *idx,
                tail,
            })
        })
    }
}

/// Normalize a route prefix: drop a trailing slash (so `/api/` == `/api`),
/// keeping root `/` as-is.
fn normalize(prefix: &str) -> String {
    let trimmed = prefix.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Prefix match on path-segment boundaries (so `/api/users` matches
/// `/api/users` and `/api/users/1` but not `/api/usersX`). Returns the tail.
fn match_prefix(prefix: &str, path: &str) -> Option<String> {
    if prefix == "/" {
        // Root matches everything; tail is the full path.
        return Some(path.to_string());
    }
    if path == prefix {
        return Some("/".to_string());
    }
    let rest = path.strip_prefix(prefix)?;
    rest.starts_with('/').then(|| rest.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(paths: &[&str]) -> Config {
        let routes: String = paths
            .iter()
            .map(|p| {
                format!(
                    "  - path: \"{p}\"\n    methods: [\"GET\"]\n    upstream:\n      url: \"http://x\"\n"
                )
            })
            .collect();
        let yaml = format!("gateway:\n  port: 8080\nroutes:\n{routes}");
        serde_yaml::from_str(&yaml).unwrap()
    }

    #[test]
    fn longest_prefix_wins_regardless_of_declaration_order() {
        let config = cfg(&["/api", "/api/special"]);
        let router = Router::build(&config);
        // `/api/special/x` must pick the more specific route, not `/api`.
        let m = router.match_route("/api/special/x").unwrap();
        assert_eq!(config.routes[m.route_index].path, "/api/special");
        assert_eq!(m.tail, "/x");

        let m = router.match_route("/api/other").unwrap();
        assert_eq!(config.routes[m.route_index].path, "/api");
    }

    #[test]
    fn respects_segment_boundaries_and_misses() {
        let config = cfg(&["/api/users"]);
        let router = Router::build(&config);
        assert!(router.match_route("/api/users").is_some());
        assert!(router.match_route("/api/users/42").is_some());
        assert!(router.match_route("/api/usersX").is_none());
        assert!(router.match_route("/nope").is_none());
    }

    #[test]
    fn exact_match_has_root_tail() {
        let config = cfg(&["/api/users"]);
        let router = Router::build(&config);
        assert_eq!(router.match_route("/api/users").unwrap().tail, "/");
    }
}
