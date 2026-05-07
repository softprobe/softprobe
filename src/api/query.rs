use crate::api::AppState;
use crate::authn::TenantInfo;
use axum::extract::Extension;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Serialize, Deserialize)]
pub struct SqlQueryRequest {
    pub sql: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SqlQueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
}

/// Execute a raw SQL query via DuckDB and return results as JSON.
/// Internal/debug surface only; AI agents use local DuckDB setup material from
/// `GET /v1/data/ducklake-connection` instead of this runtime SQL endpoint.
pub async fn execute_sql(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Json(request): Json<SqlQueryRequest>,
) -> Result<Json<SqlQueryResponse>, (StatusCode, Json<serde_json::Value>)> {
    if request.sql.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "SQL query cannot be empty"
            })),
        ));
    }

    match state
        .execute_tenant_scoped_sql(tenant.as_ref().map(|e| &e.0), &request.sql)
        .await
    {
        Ok(result) => Ok(Json(SqlQueryResponse {
            columns: result.columns,
            rows: result.rows,
            row_count: result.row_count,
        })),
        Err(err) => {
            warn!("SQL query execution failed: {}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Query execution failed: {}", err)
                })),
            ))
        }
    }
}
