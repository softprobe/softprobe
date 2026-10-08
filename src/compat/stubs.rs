//! Phase 0 stub HTTP handlers for declared Loki/Tempo compatibility routes.
//!
//! Prometheus routes live in [`crate::compat::prometheus`].
//! Auth is enforced by [`crate::runtime_api::runtime_auth_middleware`].
//! Scope-header mismatch is checked here after `TenantInfo` is available.
//! Error bodies use protocol-native envelopes (see [`crate::compat::envelopes`]).

use crate::api::AppState;
use crate::compat::envelopes::error_envelope;
use crate::compat::errors::CompatError;
use crate::compat::tenant::ProtocolScope;
use axum::Router;
use serde_json::Value;

/// Compatibility routes that remain declared but unsupported.
pub fn compat_stub_routes() -> Router<AppState> {
    Router::new()
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
        ("POST", "/api/v1/query_range"),
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
    error_envelope(
        ProtocolScope::Prometheus,
        &CompatError::unsupported("prometheus_api"),
    )
}
