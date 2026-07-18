use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::models::{Score, ScoreDataType, ScoreSource};
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateScoreRequest {
    pub score_id: String,
    pub timestamp: DateTime<Utc>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub session_id: Option<String>,
    pub name: String,
    pub data_type: ScoreDataType,
    pub numeric_value: Option<f64>,
    pub string_value: Option<String>,
    pub boolean_value: Option<bool>,
    pub source: ScoreSource,
    pub comment: Option<String>,
    pub config_id: Option<String>,
    pub author_id: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl From<CreateScoreRequest> for Score {
    fn from(request: CreateScoreRequest) -> Self {
        Self {
            score_id: request.score_id,
            timestamp: request.timestamp,
            trace_id: request.trace_id,
            span_id: request.span_id,
            session_id: request.session_id,
            name: request.name,
            data_type: request.data_type,
            numeric_value: request.numeric_value,
            string_value: request.string_value,
            boolean_value: request.boolean_value,
            source: request.source,
            comment: request.comment,
            config_id: request.config_id,
            author_id: request.author_id,
            metadata: request.metadata,
            record_date: request.timestamp.date_naive(),
        }
    }
}

type ApiError = (StatusCode, Json<serde_json::Value>);

pub async fn create_score(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Json(request): Json<CreateScoreRequest>,
) -> Result<(StatusCode, Json<Score>), ApiError> {
    let score = Score::from(request);
    if let Err(message) = score.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": message })),
        ));
    }

    let tenant_info = tenant.as_ref().map(|extension| &extension.0);
    let engine = match tenant_info {
        Some(info) => state.engine_for_tenant(info).await,
        None => state.engine_for_id("").await,
    }
    .map_err(|error| {
        warn!("failed to resolve tenant runtime for score: {}", error);
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "tenant runtime unavailable" })),
        )
    })?;

    if engine
        .storage
        .writer
        .score_exists(&score.score_id)
        .await
        .map_err(|error| {
            warn!("score idempotency lookup failed: {}", error);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "score lookup failed" })),
            )
        })?
    {
        return Ok((StatusCode::OK, Json(score)));
    }

    engine
        .storage
        .writer
        .write_score_batches(vec![vec![score.clone()]])
        .await
        .map_err(|error| {
            warn!("score write failed: {}", error);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "score write failed" })),
            )
        })?;

    Ok((StatusCode::CREATED, Json(score)))
}
