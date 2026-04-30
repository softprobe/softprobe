use axum::Json;
use serde_json::json;

pub async fn health_check() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "specVersion": "http-control-api@v1",
        "schemaVersion": "1"
    }))
}

pub async fn ready_check() -> Json<serde_json::Value> {
    // TODO: Check if storage and query engine are ready
    Json(json!({
        "status": "ready",
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}
