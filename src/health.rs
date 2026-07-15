//! `GET /health` — always 200, never routed/authed/rate-limited. Handled at the
//! very top of the pipeline so no config can shadow or gate it.

use hyper::{header, Response, StatusCode};

use crate::error::{full, BoxBody};
use crate::state::AppState;

pub fn health_response(state: &AppState) -> Response<BoxBody> {
    let uptime_seconds = state.start.elapsed().as_secs();
    let body = format!("{{\"status\":\"healthy\",\"uptime_seconds\":{uptime_seconds}}}");
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(full(body))
        .expect("health response is always well-formed")
}
