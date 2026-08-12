//! Phase 0 stub HTTP handlers for declared compatibility routes.
//!
//! Auth is enforced by [`crate::runtime_api::runtime_auth_middleware`].
//! Scope-header mismatch is checked here after `TenantInfo` is available.

use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::compat::errors::CompatError;
use crate::compat::tenant::{
    scope_header_value, ProtocolScope, QueryLimits, TenantContext, LOKI_SCOPE_HEADER,
};
use axum::extract::Extension;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

async fn stub_prometheus(tenant: Extension<TenantInfo>) -> Result<Json<Value>, CompatError> {
    let _ctx = TenantContext::from_authenticated(
        tenant.0.clone(),
        ProtocolScope::Prometheus,
        None,
        QueryLimits::default(),
    )?;
    Err(CompatError::unsupported("prometheus_api"))
}

async fn stub_loki(
    tenant: Extension<TenantInfo>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, CompatError> {
    let scope = scope_header_value(&headers, LOKI_SCOPE_HEADER);
    let _ctx = TenantContext::from_authenticated(
        tenant.0.clone(),
        ProtocolScope::Loki,
        scope,
        QueryLimits::default(),
    )?;
    Err(CompatError::unsupported("loki_api"))
}

async fn stub_tempo(
    tenant: Extension<TenantInfo>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, CompatError> {
    let scope = scope_header_value(&headers, LOKI_SCOPE_HEADER);
    let _ctx = TenantContext::from_authenticated(
        tenant.0.clone(),
        ProtocolScope::Tempo,
        scope,
        QueryLimits::default(),
    )?;
    Err(CompatError::unsupported("tempo_api"))
}

/// Routes that return `501 unsupported_feature` after auth + scope checks.
pub fn compat_stub_routes() -> Router<AppState> {
    Router::new()
        .route("/api/v1/query", get(stub_prometheus).post(stub_prometheus))
        .route(
            "/api/v1/query_range",
            get(stub_prometheus).post(stub_prometheus),
        )
        .route("/api/v1/labels", get(stub_prometheus))
        .route("/api/v1/label/{name}/values", get(stub_prometheus))
        .route("/api/v1/series", get(stub_prometheus))
        .route("/api/v1/metadata", get(stub_prometheus))
        .route("/loki/api/v1/query", get(stub_loki))
        .route("/loki/api/v1/query_range", get(stub_loki))
        .route("/loki/api/v1/labels", get(stub_loki))
        .route("/loki/api/v1/label/{name}/values", get(stub_loki))
        .route("/loki/api/v1/series", get(stub_loki))
        .route("/api/traces/{trace_id}", get(stub_tempo))
        .route("/api/v2/traces/{trace_id}", get(stub_tempo))
        .route("/api/search", get(stub_tempo))
        .route("/api/search/tags", get(stub_tempo))
        .route("/api/search/tag/{tag}/values", get(stub_tempo))
}

/// Isolation fixture: every declared Phase 0 matrix route path pattern.
pub fn declared_compat_route_templates() -> &'static [&'static str] {
    &[
        "/api/v1/query",
        "/api/v1/query_range",
        "/api/v1/labels",
        "/api/v1/label/{name}/values",
        "/api/v1/series",
        "/api/v1/metadata",
        "/loki/api/v1/query",
        "/loki/api/v1/query_range",
        "/loki/api/v1/labels",
        "/loki/api/v1/label/{name}/values",
        "/loki/api/v1/series",
        "/api/traces/{trace_id}",
        "/api/v2/traces/{trace_id}",
        "/api/search",
        "/api/search/tags",
        "/api/search/tag/{tag}/values",
    ]
}

/// Example concrete paths used by isolation tests (path params filled).
pub fn declared_compat_probe_paths() -> &'static [(&'static str, &'static str)] {
    &[
        ("GET", "/api/v1/query"),
        ("POST", "/api/v1/query"),
        ("GET", "/api/v1/query_range"),
        ("GET", "/api/v1/labels"),
        ("GET", "/api/v1/label/job/values"),
        ("GET", "/api/v1/series"),
        ("GET", "/api/v1/metadata"),
        ("GET", "/loki/api/v1/query"),
        ("GET", "/loki/api/v1/query_range"),
        ("GET", "/loki/api/v1/labels"),
        ("GET", "/loki/api/v1/label/service_name/values"),
        ("GET", "/loki/api/v1/series"),
        ("GET", "/api/traces/abc123"),
        ("GET", "/api/v2/traces/abc123"),
        ("GET", "/api/search"),
        ("GET", "/api/search/tags"),
        ("GET", "/api/search/tag/http.method/values"),
    ]
}

pub fn unsupported_json_body() -> Value {
    json!({
        "status": "error",
        "error": {
            "code": "unsupported_feature",
            "message": "unsupported feature"
        }
    })
}
