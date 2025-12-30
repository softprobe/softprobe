pub mod ingestion;
pub mod query;
pub mod health;

use axum::{routing::{get, post}, Router};
use crate::storage::{Storage, span_buffer::SimpleSpanBuffer};
use crate::query::QueryEngine;
use std::sync::Arc;

// Unified application state for Axum router
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Storage>,
    pub query_engine: Arc<QueryEngine>,
    pub span_buffer: Option<Arc<SimpleSpanBuffer>>,
}

pub async fn create_router(
    storage: Storage,
    query_engine: QueryEngine,
    span_buffer: Option<SimpleSpanBuffer>,
) -> anyhow::Result<Router> {
    let state = AppState {
        storage: Arc::new(storage),
        query_engine: Arc::new(query_engine),
        span_buffer: span_buffer.map(Arc::new),
    };
    
    // Only OTLP standard endpoints - no legacy /v1/recordings
    let router: Router = Router::new()
        .route("/health", get(health::health_check))
        .route("/ready", get(health::ready_check))
        .route("/v1/traces", post(ingestion::ingest_otlp_traces))
        .route("/query", post(query::query_recordings))
        .route("/retrieve", post(query::retrieve_payloads))
        .route("/v1/query/session/{session_id}", get(query::query_session_by_id))
        .route("/v1/query/debug/all", get(query::query_all_spans_debug))
        .with_state(state);

    Ok(router)
}
