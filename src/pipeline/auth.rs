//! api_key authentication stage [P2.1]: a request must present a configured
//! header whose value matches one of the route's allowed keys, else it is
//! rejected with `401 Unauthorized`. Missing header and wrong value are treated
//! identically (no `403` distinction — see DECISIONS.md), so we never reveal
//! whether a key was *recognized* versus *absent*.
//!
//! Pipeline placement: assembled only for routes that declare `auth`, and slotted
//! **after method filtering but BEFORE rate limiting**. Auth-before-rate-limit is
//! deliberate (DECISIONS.md): the key compare is the sensitive operation we want
//! guarded first, and rate-limit bucketing (`per: ip`/`global`) needs no client
//! identity, so nothing is lost by authing ahead of it.

use async_trait::async_trait;

use crate::config::Auth;
use crate::error::GatewayError;

use super::{Flow, RequestCtx, Stage};

/// Holds the configured header name and the allowed keys (owned so the stage
/// outlives the config borrow used to build it, mirroring `MethodStage`).
pub struct AuthStage {
    header: String,
    keys: Vec<String>,
}

impl AuthStage {
    pub fn new(auth: &Auth) -> Self {
        AuthStage {
            header: auth.header.clone(),
            keys: auth.keys.clone(),
        }
    }

    /// Does `presented` match any configured key?
    ///
    /// Constant-time-where-practical (DECISIONS.md): we defend the key compare
    /// against timing side-channels by (a) folding a byte-wise XOR accumulator
    /// over the full length rather than short-circuiting on the first differing
    /// byte, and (b) iterating **every** key, OR-ing the per-key result into
    /// `matched` without breaking early — so the time taken does not reveal
    /// which key matched (or how far into a key a near-miss diverged). This
    /// compare running before rate limiting is exactly why auth precedes the
    /// rate-limit stage. Length is not treated as secret: unequal lengths are a
    /// non-match without a byte scan.
    fn matches(&self, presented: &[u8]) -> bool {
        let mut matched = false;
        for key in &self.keys {
            let key = key.as_bytes();
            if key.len() == presented.len() {
                let mut diff: u8 = 0;
                for (a, b) in key.iter().zip(presented.iter()) {
                    diff |= a ^ b;
                }
                matched |= diff == 0;
            }
        }
        matched
    }
}

#[async_trait]
impl Stage for AuthStage {
    async fn apply(&self, ctx: &mut RequestCtx) -> Flow {
        // `HeaderMap::get` is case-insensitive per HTTP header semantics, so the
        // configured header name matches regardless of the client's casing.
        let presented = ctx.req.headers().get(&self.header);
        let ok = match presented {
            Some(value) => self.matches(value.as_bytes()),
            None => false,
        };
        if ok {
            Flow::Continue
        } else {
            // Missing header and wrong key both land here → 401, no 403.
            Flow::ShortCircuit(GatewayError::Unauthorized.into_response())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthType;

    fn stage(header: &str, keys: &[&str]) -> AuthStage {
        AuthStage::new(&Auth {
            auth_type: AuthType::ApiKey,
            header: header.to_string(),
            keys: keys.iter().map(|k| k.to_string()).collect(),
        })
    }

    #[test]
    fn accepts_a_configured_key() {
        let s = stage("X-API-Key", &["sk_live_abc123", "sk_live_def456"]);
        assert!(s.matches(b"sk_live_abc123"));
        assert!(s.matches(b"sk_live_def456"));
    }

    #[test]
    fn rejects_a_wrong_key() {
        let s = stage("X-API-Key", &["sk_live_abc123"]);
        assert!(!s.matches(b"sk_live_wrong"));
        // A prefix / different-length near-miss is also a non-match.
        assert!(!s.matches(b"sk_live_abc"));
        assert!(!s.matches(b"sk_live_abc1234"));
    }

    #[test]
    fn rejects_when_no_keys_configured() {
        let s = stage("X-API-Key", &[]);
        assert!(!s.matches(b"anything"));
        assert!(!s.matches(b""));
    }

    #[test]
    fn header_lookup_is_case_insensitive() {
        // hyper's HeaderMap::get is case-insensitive; a client may send any
        // casing of the configured header name and still be matched.
        use hyper::header::HeaderMap;
        let s = stage("X-API-Key", &["sk_live_abc123"]);
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "sk_live_abc123".parse().unwrap());
        let value = headers.get(&s.header).expect("case-insensitive lookup");
        assert!(s.matches(value.as_bytes()));
    }
}
