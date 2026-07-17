pub mod logs;
pub mod metrics;
pub mod traces;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestResponse {
    pub success: bool,
    pub ingested_count: usize,
    pub message: String,
}

/// Durable write failed after DuckLake's own conflict retries — ask exporters to retry.
pub(crate) fn ingest_write_failed(message: String) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(IngestResponse {
            success: false,
            ingested_count: 0,
            message,
        }),
    )
        .into_response()
}
