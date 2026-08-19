//! Prometheus HTTP API route handlers.

use super::encode::{
    encode_eval_result, label_values_data, labels_data, metadata_data, series_data,
    success_response_limited,
};
use super::params::{
    pairs_from_form, pairs_from_query, parse_discovery_params, parse_metadata_params,
    parse_query_params, DiscoveryParams,
};
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::compat::backends::metrics::{MetricsDiscoveryRequest, MetricsQueryBackend};
use crate::compat::backends::DuckLakeMetricsBackend;
use crate::compat::envelopes::error_response;
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::promql::{eval_instant, eval_range, parse_promql};
use crate::compat::tenant::{ProtocolScope, QueryLimits, TenantContext};
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, Method};
use axum::response::Response;
use axum::Extension;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

const PROTO: ProtocolScope = ProtocolScope::Prometheus;

fn tenant_ctx(tenant: TenantInfo) -> Result<TenantContext, CompatError> {
    TenantContext::from_authenticated(tenant, PROTO, None, QueryLimits::default())
}

async fn backend_for(
    state: &AppState,
    ctx: &TenantContext,
) -> Result<DuckLakeMetricsBackend, CompatError> {
    let engine = state.engine_for_tenant(&ctx.tenant).await.map_err(|e| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("tenant engine unavailable: {e}"),
        )
    })?;
    Ok(DuckLakeMetricsBackend::new(Arc::clone(&engine.query)))
}

fn collect_pairs(
    method: &Method,
    uri_query: Option<&str>,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Vec<(String, String)>, CompatError> {
    let mut pairs = Vec::new();
    if let Some(q) = uri_query {
        pairs.extend(pairs_from_query(q));
    }
    if *method == Method::POST {
        let ct = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if ct.starts_with("application/x-www-form-urlencoded") {
            let text = std::str::from_utf8(body).map_err(|_| {
                CompatError::new(
                    CompatErrorCode::BadRequest,
                    "POST form body is not valid UTF-8",
                )
            })?;
            pairs.extend(pairs_from_form(text));
        }
    }
    Ok(pairs)
}

fn map_err(err: CompatError) -> Response {
    error_response(PROTO, err)
}

fn respond_data(ctx: &TenantContext, data: Value) -> Response {
    match success_response_limited(data, ctx.limits.max_response_bytes) {
        Ok(resp) => resp,
        Err(e) => map_err(e),
    }
}

/// Shared auth → pairs → backend setup for discovery/query handlers.
async fn prepare(
    state: &AppState,
    tenant: TenantInfo,
    method: &Method,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<(TenantContext, Vec<(String, String)>, DuckLakeMetricsBackend), CompatError> {
    let ctx = tenant_ctx(tenant)?;
    let pairs = collect_pairs(method, uri.query(), headers, body)?;
    let backend = backend_for(state, &ctx).await?;
    Ok((ctx, pairs, backend))
}

fn discovery_request(params: DiscoveryParams) -> MetricsDiscoveryRequest {
    MetricsDiscoveryRequest {
        start_ms: params.start_ms,
        end_ms: params.end_ms,
        matchers: params.matchers,
    }
}

async fn labels_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (ctx, pairs, backend) = match prepare(&state, tenant, &method, &uri, &headers, &body).await
    {
        Ok(v) => v,
        Err(e) => return map_err(e),
    };
    let params = match parse_discovery_params(&pairs, &ctx.limits) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    match backend.label_names(&ctx, &discovery_request(params)).await {
        Ok(names) => respond_data(&ctx, labels_data(&names)),
        Err(e) => map_err(e),
    }
}

async fn label_values_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(name): Path<String>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (ctx, pairs, backend) = match prepare(&state, tenant, &method, &uri, &headers, &body).await
    {
        Ok(v) => v,
        Err(e) => return map_err(e),
    };
    let params = match parse_discovery_params(&pairs, &ctx.limits) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    match backend
        .label_values(&ctx, &name, &discovery_request(params))
        .await
    {
        Ok(values) => respond_data(&ctx, label_values_data(&values)),
        Err(e) => map_err(e),
    }
}

async fn series_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (ctx, pairs, backend) = match prepare(&state, tenant, &method, &uri, &headers, &body).await
    {
        Ok(v) => v,
        Err(e) => return map_err(e),
    };
    let params = match parse_discovery_params(&pairs, &ctx.limits) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    match backend.series(&ctx, &discovery_request(params)).await {
        Ok(series) => respond_data(&ctx, series_data(&series)),
        Err(e) => map_err(e),
    }
}

async fn metadata_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let ctx = match tenant_ctx(tenant) {
        Ok(c) => c,
        Err(e) => return map_err(e),
    };
    let pairs: Vec<(String, String)> = query.into_iter().collect();
    let params = match parse_metadata_params(&pairs, &ctx.limits) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    let backend = match backend_for(&state, &ctx).await {
        Ok(b) => b,
        Err(e) => return map_err(e),
    };
    match backend
        .metadata(
            &ctx,
            params.metric.as_deref(),
            params.limit,
            params.start_ms,
            params.end_ms,
        )
        .await
    {
        Ok(items) => respond_data(&ctx, metadata_data(&items)),
        Err(e) => map_err(e),
    }
}

async fn query_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (ctx, pairs, backend) = match prepare(&state, tenant, &method, &uri, &headers, &body).await
    {
        Ok(v) => v,
        Err(e) => return map_err(e),
    };
    let params = match parse_query_params(&pairs, &ctx.limits, false) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    let expr = match parse_promql(&params.query) {
        Ok(e) => e,
        Err(e) => return map_err(e),
    };
    let eval_ms = params
        .time_ms
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    match eval_instant(&backend, &ctx, &expr, eval_ms).await {
        Ok(result) => respond_data(&ctx, encode_eval_result(result)),
        Err(e) => map_err(e),
    }
}

async fn query_range_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (ctx, pairs, backend) = match prepare(&state, tenant, &method, &uri, &headers, &body).await
    {
        Ok(v) => v,
        Err(e) => return map_err(e),
    };
    let params = match parse_query_params(&pairs, &ctx.limits, true) {
        Ok(p) => p,
        Err(e) => return map_err(e),
    };
    let start_ms = params.start_ms.unwrap();
    let end_ms = params.end_ms.unwrap();
    let step_ms = params.step_ms.unwrap();
    let expr = match parse_promql(&params.query) {
        Ok(e) => e,
        Err(e) => return map_err(e),
    };
    match eval_range(&backend, &ctx, &expr, start_ms, end_ms, step_ms).await {
        Ok(result) => respond_data(&ctx, encode_eval_result(result)),
        Err(e) => map_err(e),
    }
}

pub fn prometheus_routes() -> axum::Router<AppState> {
    use axum::routing::get;
    axum::Router::new()
        .route("/api/v1/query", get(query_handler).post(query_handler))
        .route(
            "/api/v1/query_range",
            get(query_range_handler).post(query_range_handler),
        )
        .route("/api/v1/labels", get(labels_handler))
        .route("/api/v1/label/{name}/values", get(label_values_handler))
        .route("/api/v1/series", get(series_handler))
        .route("/api/v1/metadata", get(metadata_handler))
}
