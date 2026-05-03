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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_check_shape() {
        let j = health_check().await.0;
        assert_eq!(j["status"], "ok");
        assert_eq!(j["specVersion"], "http-control-api@v1");
    }

    #[tokio::test]
    async fn ready_check_shape() {
        let j = ready_check().await.0;
        assert_eq!(j["status"], "ready");
        assert!(j["timestamp"].as_str().unwrap().contains('T'));
    }
}
