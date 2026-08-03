use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde_json::json;

use crate::api::AppState;
use crate::query::duckdb;

/// Liveness gives up only when self-heal has failed this many times in a row.
/// A one-off rebuild failure (Postgres blip mid-rebuild) must not get the pod
/// killed; a rebuild that keeps failing never recovers without a restart.
const MAX_CONSECUTIVE_REBUILD_FAILURES: u64 = 3;

/// Liveness. Deliberately does not probe dependencies -- k8s must not restart
/// the pod because Postgres or the object store blinked. What it does surface
/// is the one state a restart genuinely fixes: worker self-heal failing over
/// and over. The previous version returned a hardcoded "ok", which is how the
/// 2026-08-03 outage served 503s for half an hour behind a green health check.
pub async fn health_check() -> (StatusCode, Json<serde_json::Value>) {
    let heal = duckdb::self_heal_snapshot();
    if heal.consecutive_failures >= MAX_CONSECUTIVE_REBUILD_FAILURES {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "unhealthy",
                "reason": "query worker connection rebuild keeps failing",
                "consecutiveRebuildFailures": heal.consecutive_failures,
                "specVersion": "http-control-api@v1",
                "schemaVersion": "1"
            })),
        );
    }
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "specVersion": "http-control-api@v1",
            "schemaVersion": "1"
        })),
    )
}

/// Readiness. Runs a real statement through a pool worker: it fails while
/// workers are dead or poisoned (and heals a poisoned one as a side effect).
/// The probe stops at `SELECT 1` on purpose -- catalog and object-store
/// reachability are enforced when connections attach (startup and rebuild),
/// and scanning cold DuckLake tables here would make readiness flap on slow
/// storage rather than on actual brokenness.
///
/// The first probe also constructs the default tenant engine (extensions,
/// catalog attach), which is legitimate readiness work and can take seconds.
/// Two consequences: the probe runs in a spawned task so a dropped probe
/// connection (k8s timing out its attempt) cannot discard construction
/// progress -- the engine still lands in the manager's cache for the next
/// attempt -- and the in-handler budget is generous; the k8s probe's own
/// `timeoutSeconds` bounds each attempt.
pub async fn ready_check(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let probe = tokio::spawn(async move {
        state.execute_tenant_scoped_sql(None, "SELECT 1").await
    });
    let reason = match tokio::time::timeout(std::time::Duration::from_secs(10), probe).await {
        Ok(Ok(Ok(_))) => {
            return (
                StatusCode::OK,
                Json(json!({
                    "status": "ready",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                })),
            );
        }
        // Raw engine errors can carry the catalog DSN (credentials): log the
        // detail, return a generic reason. Same policy as the LLM query API.
        Ok(Ok(Err(err))) => {
            tracing::warn!("readiness probe failed: {err}");
            "query engine probe failed"
        }
        Ok(Err(join_err)) => {
            tracing::warn!("readiness probe task failed: {join_err}");
            "query engine probe failed"
        }
        Err(_) => {
            tracing::warn!("readiness probe timed out");
            "query engine probe timed out"
        }
    };
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "status": "unready",
            "reason": reason,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_check_shape() {
        let (status, j) = health_check().await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(j.0["status"], "ok");
        assert_eq!(j.0["specVersion"], "http-control-api@v1");
    }

    // ready_check needs an AppState with a live engine; it is covered by the
    // router-level test (`unit_health_ready_traces_logs_metrics_query`),
    // which asserts /ready returns 200 against a real local engine.
}
