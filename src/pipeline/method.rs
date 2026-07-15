//! Method-filtering stage [P0.4]: a request whose method is not in the route's
//! `methods` list is rejected with `405 Method Not Allowed` carrying an `Allow`
//! header of the permitted methods. This is the simplest [`Stage`] and doubles
//! as the worked example of the pluggable-pipeline pattern.

use async_trait::async_trait;

use crate::error::GatewayError;

use super::{Flow, RequestCtx, Stage};

/// Holds the route's allowed methods (owned so the stage outlives the config
/// borrow used to build it).
pub struct MethodStage {
    allow: Vec<String>,
}

impl MethodStage {
    pub fn new(methods: &[String]) -> Self {
        MethodStage {
            allow: methods.to_vec(),
        }
    }
}

#[async_trait]
impl Stage for MethodStage {
    async fn apply(&self, ctx: &mut RequestCtx) -> Flow {
        let method = ctx.req.method().as_str();
        if self.allow.iter().any(|m| m.eq_ignore_ascii_case(method)) {
            Flow::Continue
        } else {
            Flow::ShortCircuit(GatewayError::method_not_allowed(&self.allow).into_response())
        }
    }
}
