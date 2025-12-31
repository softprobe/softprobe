use axum::{extract::{State, Path}, Json, http::StatusCode};
use serde::{Deserialize, Serialize};
use crate::api::AppState;

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub app_id: Option<String>,
    pub record_ids: Option<Vec<String>>,
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    pub category_type: Option<String>,
    pub operation_name: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub records: Vec<RecordingMetadata>,
    pub total: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordingMetadata {
    pub record_id: String,
    pub replay_id: Option<String>,
    pub session_id: Option<String>,
    pub app_id: String,
    pub operation_name: String,
    pub category_type: String,
    pub creation_time: chrono::DateTime<chrono::Utc>,
    pub payload_file_uri: String,
    pub payload_file_offset: i32,
    pub payload_row_group_index: i32,
    pub request_size: i64,
    pub response_size: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RetrieveRequest {
    pub record_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RetrieveResponse {
    pub payloads: Vec<RecordingPayload>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordingPayload {
    pub record_id: String,
    pub request: RequestPayload,
    pub response: ResponsePayload,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestPayload {
    pub body: Vec<u8>,
    pub headers: Option<serde_json::Value>,
    pub attributes: Option<serde_json::Value>,
    pub content_type: Option<String>,
    pub encoding: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponsePayload {
    pub body: Vec<u8>,
    pub headers: Option<serde_json::Value>,
    pub status: i32,
    pub content_type: Option<String>,
    pub encoding: Option<String>,
}

pub async fn query_recordings(
    State(_state): State<AppState>,
    Json(_request): Json<QueryRequest>,
) -> Json<QueryResponse> {
    // TODO: Implement query logic using DuckDB (Phase 1.2)
    // - Build SQL query with partition pruning (app_id, record_date, category_type)
    // - Execute query via state.query_engine.duckdb.query_metadata()
    // - Return metadata records with payload_file_uri, payload_file_offset, payload_row_group_index
    // See: docs/migration-to-iceberg-design.md lines 993-1004 for query pattern
    
    Json(QueryResponse {
        records: vec![],
        total: 0,
    })
}

pub async fn retrieve_payloads(
    State(_state): State<AppState>,
    Json(_request): Json<RetrieveRequest>,
) -> Json<RetrieveResponse> {
    // TODO: Implement payload retrieval from S3 (Phase 1.2)
    // - Query metadata for record_ids to get payload_file_uri, payload_file_offset, payload_row_group_index
    // - Group by payload_file_uri to minimize S3 GETs
    // - Fetch each unique Parquet file once
    // - Extract multiple recordings from each file using row_group_index and offset
    // - Cache frequently accessed files in memory
    // See: docs/migration-to-iceberg-design.md lines 1068-1150 for retrieval strategy

    Json(RetrieveResponse {
        payloads: vec![],
    })
}

/// Response for session query
#[derive(Debug, Serialize, Deserialize)]
pub struct SessionQueryResponse {
    pub session_id: String,
    pub spans: Vec<SpanData>,
    pub total_count: usize,
}

/// Span data returned from query
#[derive(Debug, Serialize, Deserialize)]
pub struct SpanData {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub app_id: String,
    pub organization_id: Option<String>,
    pub tenant_id: Option<String>,
    pub message_type: String,
    pub span_kind: Option<String>,
    pub timestamp: String,
    pub end_timestamp: Option<String>,
    pub attributes: serde_json::Value,
    pub events: Vec<serde_json::Value>,
    pub status_code: Option<String>,
    pub status_message: Option<String>,
}

/// Query all spans for a given session_id
/// GET /v1/query/session/{session_id}
pub async fn query_session_by_id(
    State(_state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionQueryResponse>, (StatusCode, String)> {
    tracing::info!("Querying spans for session_id: {}", session_id);

    // TODO: Re-implement query methods in refactored Iceberg module
    tracing::warn!("Query methods not yet implemented after refactoring");
    Err((
        StatusCode::NOT_IMPLEMENTED,
        "Query functionality temporarily disabled during refactoring".to_string(),
    ))
}

/// Debug endpoint: Query all spans (no filter) to test table visibility
/// GET /v1/query/debug/all
pub async fn query_all_spans_debug(
    State(_state): State<AppState>,
) -> Result<Json<SessionQueryResponse>, (StatusCode, String)> {
    tracing::info!("DEBUG: Querying all spans (no predicate) with limit 100");

    // TODO: Re-implement query methods in refactored Iceberg module
    tracing::warn!("Query methods not yet implemented after refactoring");
    Err((
        StatusCode::NOT_IMPLEMENTED,
        "Query functionality temporarily disabled during refactoring".to_string(),
    ))
}
