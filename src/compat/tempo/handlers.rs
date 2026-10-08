use super::encode::{
    search_response, tag_names_response, tag_values_response, trace_v1_response, trace_v2_response,
};
use super::params::{
    parse_tempo_search_params, parse_tempo_tag_params, parse_tempo_trace_lookup_params,
};
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::compat::backends::ducklake_traces::DuckLakeTraceBackend;
use crate::compat::backends::traces::{TraceLookupBounds, TraceQueryBackend, TraceSearchRequest};
use crate::compat::envelopes::error_response;
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::tenant::{
    scope_header_value, ProtocolScope, QueryLimits, TenantContext, TEMPO_SCOPE_HEADER,
};
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde_json::json;
use std::sync::Arc;

const PROTOCOL: ProtocolScope = ProtocolScope::Tempo;

async fn backend_for(
    state: &AppState,
    ctx: &TenantContext,
) -> Result<DuckLakeTraceBackend, CompatError> {
    let engine = state.engine_for_tenant(&ctx.tenant).await.map_err(|err| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("tenant engine unavailable: {err}"),
        )
    })?;
    Ok(DuckLakeTraceBackend::new(Arc::clone(&engine.query)))
}

fn tenant_context(tenant: TenantInfo, headers: &HeaderMap) -> Result<TenantContext, CompatError> {
    TenantContext::from_authenticated(
        tenant,
        PROTOCOL,
        scope_header_value(headers, TEMPO_SCOPE_HEADER),
        QueryLimits::default(),
    )
}

fn pairs(uri: &Uri) -> Vec<(String, String)> {
    uri.query()
        .map(crate::compat::prometheus::pairs_from_query)
        .unwrap_or_default()
}

async fn trace_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(trace_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    v2: bool,
) -> Response {
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    if !is_trace_id(&trace_id) {
        return error_response(
            PROTOCOL,
            bad("trace id must be exactly 32 hexadecimal characters"),
        );
    }
    let lookup = match parse_tempo_trace_lookup_params(&pairs(&uri), &ctx.limits) {
        Ok(params) => params,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let bounds = TraceLookupBounds {
        start_ns: lookup.start_ns,
        end_ns: lookup.end_ns,
    };
    let backend = match backend_for(&state, &ctx).await {
        Ok(backend) => backend,
        Err(err) => return error_response(PROTOCOL, err),
    };
    match backend.get_trace(&ctx, &trace_id, bounds).await {
        Ok(Some(data)) if v2 => trace_v2_response(&data, ctx.limits.max_response_bytes)
            .unwrap_or_else(|err| error_response(PROTOCOL, err)),
        Ok(Some(data)) => trace_v1_response(&data, ctx.limits.max_response_bytes)
            .unwrap_or_else(|err| error_response(PROTOCOL, err)),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"message": "trace not found"})),
        )
            .into_response(),
        Err(err) => error_response(PROTOCOL, err),
    }
}

async fn trace_v1_handler(
    state: State<AppState>,
    tenant: Extension<TenantInfo>,
    path: Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    trace_handler(state, tenant, path, headers, uri, false).await
}

async fn trace_v2_handler(
    state: State<AppState>,
    tenant: Extension<TenantInfo>,
    path: Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    trace_handler(state, tenant, path, headers, uri, true).await
}

async fn search_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let params = match parse_tempo_search_params(&pairs(&uri), &ctx.limits) {
        Ok(params) => params,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let request = TraceSearchRequest {
        tags: params.tags,
        selector: params.selector,
        min_duration_ns: params.min_duration_ns,
        max_duration_ns: params.max_duration_ns,
        start_ns: params.start_ns,
        end_ns: params.end_ns,
        limit: params.limit,
    };
    let backend = match backend_for(&state, &ctx).await {
        Ok(backend) => backend,
        Err(err) => return error_response(PROTOCOL, err),
    };
    match backend.search(&ctx, request).await {
        Ok(hits) => search_response(&hits, ctx.limits.max_response_bytes)
            .unwrap_or_else(|err| error_response(PROTOCOL, err)),
        Err(err) => error_response(PROTOCOL, err),
    }
}

async fn tags_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Err(err) = parse_tempo_tag_params(&pairs(&uri)) {
        return error_response(PROTOCOL, err);
    }
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let backend = match backend_for(&state, &ctx).await {
        Ok(backend) => backend,
        Err(err) => return error_response(PROTOCOL, err),
    };
    match backend.search_tags(&ctx).await {
        Ok(values) => tag_names_response(&values, ctx.limits.max_response_bytes)
            .unwrap_or_else(|err| error_response(PROTOCOL, err)),
        Err(err) => error_response(PROTOCOL, err),
    }
}

async fn tag_values_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(tag): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Err(err) = parse_tempo_tag_params(&pairs(&uri)) {
        return error_response(PROTOCOL, err);
    }
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    if tag.trim().is_empty() {
        return error_response(PROTOCOL, bad("tag must not be empty"));
    }
    let backend = match backend_for(&state, &ctx).await {
        Ok(backend) => backend,
        Err(err) => return error_response(PROTOCOL, err),
    };
    match backend.search_tag_values(&ctx, &tag).await {
        Ok(values) => tag_values_response(&values, ctx.limits.max_response_bytes)
            .unwrap_or_else(|err| error_response(PROTOCOL, err)),
        Err(err) => error_response(PROTOCOL, err),
    }
}

fn bad(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

fn is_trace_id(trace_id: &str) -> bool {
    trace_id.len() == 32 && trace_id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn tempo_routes() -> Router<AppState> {
    Router::new()
        .route("/api/traces/{trace_id}", get(trace_v1_handler))
        .route("/api/v2/traces/{trace_id}", get(trace_v2_handler))
        .route("/api/search", get(search_handler))
        .route("/api/search/tags", get(tags_handler))
        .route("/api/search/tag/{tag}/values", get(tag_values_handler))
}

#[cfg(test)]
mod tests {
    use super::is_trace_id;

    #[test]
    fn accepts_only_canonical_hex_trace_ids() {
        assert!(is_trace_id("000102030405060708090a0b0c0d0e0f"));
        assert!(!is_trace_id("00010203"));
        assert!(!is_trace_id("000102030405060708090a0b0c0d0e0g"));
    }
}
