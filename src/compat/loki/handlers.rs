use super::encode::compute_index_stats;
use super::encode::{
    instant_metric_vector_response, labels_response, matrix_response, series_response,
    streams_response,
};
use super::logql::{parse_logql, parse_metric_expression, parse_selector};
use super::params::{
    parse_index_stats_params, parse_loki_params_with_limits, parse_tail_params, LokiParams,
};
use super::stats::{parse_stats_query, stats_query_request};
use super::tail;
use super::volume::{
    default_step_ns, eval_logs_volume, parse_logs_volume_query, volume_query_request,
};
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::compat::backends::ducklake_logs::DuckLakeLogsBackend;
use crate::compat::backends::logs::{LogsDiscoveryRequest, LogsQueryBackend};
use crate::compat::envelopes::error_response;
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::tenant::{
    scope_header_value, ProtocolScope, QueryLimits, TenantContext, LOKI_SCOPE_HEADER,
};
use axum::extract::{Extension, Path, State, WebSocketUpgrade};
use axum::http::{HeaderMap, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Json;
use axum::Router;
use std::sync::Arc;

const PROTOCOL: ProtocolScope = ProtocolScope::Loki;
const INSTANT_QUERY_LOOKBACK_NS: i64 = 30_000_000_000;

pub(crate) async fn backend_for(
    state: &AppState,
    ctx: &TenantContext,
) -> Result<DuckLakeLogsBackend, CompatError> {
    let engine = state.engine_for_tenant(&ctx.tenant).await.map_err(|err| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("tenant engine unavailable: {err}"),
        )
    })?;
    Ok(DuckLakeLogsBackend::new(Arc::clone(&engine.query)))
}

fn tenant_context(tenant: TenantInfo, headers: &HeaderMap) -> Result<TenantContext, CompatError> {
    TenantContext::from_authenticated(
        tenant,
        PROTOCOL,
        scope_header_value(headers, LOKI_SCOPE_HEADER),
        QueryLimits::default(),
    )
}

fn pairs(uri: &Uri) -> Vec<(String, String)> {
    uri.query()
        .map(crate::compat::prometheus::pairs_from_query)
        .unwrap_or_default()
}

async fn query_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    run_query(&state, tenant, headers, uri, false).await
}

async fn query_range_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    run_query(&state, tenant, headers, uri, true).await
}

async fn run_query(
    state: &AppState,
    tenant: TenantInfo,
    headers: HeaderMap,
    uri: Uri,
    range: bool,
) -> Response {
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let params = match parse_loki_params_with_limits(&pairs(&uri), range, &ctx.limits) {
        Ok(params) => params,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let query = match params.query.as_deref() {
        Some(query) => query,
        None => return error_response(PROTOCOL, bad("missing query parameter")),
    };
    // Grafana probes Loki datasources with literal metric expressions such as
    // `vector(1) + vector(1)`; evaluate them without touching log storage.
    if let Ok(Some(expr)) = parse_metric_expression(query) {
        let value = expr.eval();
        let timestamp_ns = params
            .time_ns
            .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
        return instant_metric_vector_response(value, timestamp_ns, ctx.limits.max_response_bytes)
            .unwrap_or_else(|err| error_response(PROTOCOL, err));
    }
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let (start_ns, end_ns) = effective_window(&params, range, now_ns);
    if let Ok(Some(volume)) = parse_logs_volume_query(query) {
        let start_ns = start_ns.unwrap_or(now_ns);
        let end_ns = end_ns.unwrap_or(now_ns.saturating_add(1));
        let step_ns = params
            .step_ns
            .or(params.interval_ns)
            .unwrap_or_else(|| default_step_ns(start_ns, end_ns));
        let range_ns = if volume.range_ns <= 1 {
            step_ns
        } else {
            volume.range_ns
        };
        let cap = ctx.limits.max_series.saturating_mul(100).max(10_000);
        let request = volume_query_request(volume.request, start_ns, end_ns, cap);
        let backend = match backend_for(state, &ctx).await {
            Ok(backend) => backend,
            Err(err) => return error_response(PROTOCOL, err),
        };
        return match backend.query_range(&ctx, request).await {
            Ok(hits) => {
                let series =
                    eval_logs_volume(&hits, &volume.group_by, start_ns, end_ns, step_ns, range_ns);
                matrix_response(&series, ctx.limits.max_response_bytes)
                    .unwrap_or_else(|err| error_response(PROTOCOL, err))
            }
            Err(err) => error_response(PROTOCOL, err),
        };
    }
    let mut request = match parse_logql(query) {
        Ok(request) => request,
        Err(err) => return error_response(PROTOCOL, err),
    };
    request.start_ns = start_ns;
    request.end_ns = end_ns;
    request.limit = params.limit;
    request.direction = params.direction;
    let backend = match backend_for(state, &ctx).await {
        Ok(backend) => backend,
        Err(err) => return error_response(PROTOCOL, err),
    };
    match backend.query_range(&ctx, request).await {
        Ok(hits) => match streams_response(&hits, ctx.limits.max_response_bytes) {
            Ok(response) => response,
            Err(err) => error_response(PROTOCOL, err),
        },
        Err(err) => error_response(PROTOCOL, err),
    }
}

async fn labels_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let request = match discovery_request(&pairs(&uri), &ctx.limits) {
        Ok(request) => request,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let backend = match backend_for(&state, &ctx).await {
        Ok(backend) => backend,
        Err(err) => return error_response(PROTOCOL, err),
    };
    match backend.label_names(&ctx, request).await {
        Ok(values) => labels_response(&values, ctx.limits.max_response_bytes)
            .unwrap_or_else(|err| error_response(PROTOCOL, err)),
        Err(err) => error_response(PROTOCOL, err),
    }
}

async fn label_values_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let name = crate::compat::projection::prometheus::sanitize_label_name(&name);
    let request = match discovery_request(&pairs(&uri), &ctx.limits) {
        Ok(request) => request,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let backend = match backend_for(&state, &ctx).await {
        Ok(backend) => backend,
        Err(err) => return error_response(PROTOCOL, err),
    };
    match backend.label_values(&ctx, &name, request).await {
        Ok(values) => labels_response(&values, ctx.limits.max_response_bytes)
            .unwrap_or_else(|err| error_response(PROTOCOL, err)),
        Err(err) => error_response(PROTOCOL, err),
    }
}

async fn series_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let request = match discovery_request(&pairs(&uri), &ctx.limits) {
        Ok(request) => request,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let backend = match backend_for(&state, &ctx).await {
        Ok(backend) => backend,
        Err(err) => return error_response(PROTOCOL, err),
    };
    match backend.series(&ctx, request).await {
        Ok(series) => series_response(&series, ctx.limits.max_response_bytes)
            .unwrap_or_else(|err| error_response(PROTOCOL, err)),
        Err(err) => error_response(PROTOCOL, err),
    }
}

async fn index_stats_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let params = match parse_index_stats_params(&pairs(&uri), &ctx.limits) {
        Ok(params) => params,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let request = match parse_stats_query(&params.query) {
        Ok(request) => request,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let cap = ctx.limits.max_series.saturating_mul(100).max(10_000);
    let request = stats_query_request(request, params.start_ns, params.end_ns, cap);
    let backend = match backend_for(&state, &ctx).await {
        Ok(backend) => backend,
        Err(err) => return error_response(PROTOCOL, err),
    };
    match backend.query_range(&ctx, request).await {
        Ok(hits) => Json(compute_index_stats(&hits)).into_response(),
        Err(err) => error_response(PROTOCOL, err),
    }
}

async fn tail_handler(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    headers: HeaderMap,
    uri: Uri,
    ws: WebSocketUpgrade,
) -> Response {
    let ctx = match tenant_context(tenant, &headers) {
        Ok(ctx) => ctx,
        Err(err) => return error_response(PROTOCOL, err),
    };
    let params = match parse_tail_params(&pairs(&uri), &ctx.limits) {
        Ok(params) => params,
        Err(err) => return error_response(PROTOCOL, err),
    };
    if let Err(err) = parse_logql(&params.query) {
        return error_response(PROTOCOL, err);
    }
    ws.on_upgrade(move |socket| tail::run(state, ctx, socket, params))
}

fn discovery_request(
    pairs: &[(String, String)],
    limits: &QueryLimits,
) -> Result<LogsDiscoveryRequest, CompatError> {
    let params = parse_loki_params_with_limits(pairs, false, limits)?;
    let mut matchers = Vec::new();
    for (key, value) in pairs {
        if key == "match[]" {
            matchers.push(parse_selector(value)?);
        }
    }
    Ok(LogsDiscoveryRequest {
        start_ns: params.start_ns,
        end_ns: params.end_ns,
        matchers,
    })
}

fn effective_window(params: &LokiParams, range: bool, now_ns: i64) -> (Option<i64>, Option<i64>) {
    if let Some(since) = params.since_ns {
        return (
            Some(params.end_ns.unwrap_or(now_ns).saturating_sub(since)),
            Some(params.end_ns.unwrap_or(now_ns)),
        );
    }
    if !range {
        if let Some(time) = params.time_ns {
            return (
                Some(time.saturating_sub(INSTANT_QUERY_LOOKBACK_NS)),
                Some(time.saturating_add(1)),
            );
        }
        if params.start_ns.is_some() || params.end_ns.is_some() {
            return (params.start_ns, params.end_ns);
        }
        return (Some(now_ns), Some(now_ns.saturating_add(1)));
    }
    (params.start_ns, params.end_ns)
}

fn bad(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

pub fn loki_routes() -> Router<AppState> {
    Router::new()
        .route("/loki/api/v1/query", get(query_handler))
        .route("/loki/api/v1/query_range", get(query_range_handler))
        .route("/loki/api/v1/labels", get(labels_handler))
        .route(
            "/loki/api/v1/label/{name}/values",
            get(label_values_handler),
        )
        .route("/loki/api/v1/series", get(series_handler))
        .route("/loki/api/v1/index/stats", get(index_stats_handler))
        .route("/loki/api/v1/tail", get(tail_handler))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::backends::logs::LogDirection;

    #[test]
    fn instant_query_without_time_defaults_to_bounded_now_window() {
        assert_eq!(
            effective_window(
                &LokiParams {
                    query: Some("{}".into()),
                    start_ns: None,
                    end_ns: None,
                    time_ns: None,
                    limit: 1000,
                    direction: LogDirection::Backward,
                    interval_ns: None,
                    step_ns: None,
                    since_ns: None,
                },
                false,
                42
            ),
            (Some(42), Some(43))
        );
    }

    #[test]
    fn instant_query_preserves_explicit_bounds() {
        let params = LokiParams {
            query: Some("{}".into()),
            start_ns: Some(10),
            end_ns: Some(20),
            time_ns: None,
            limit: 1000,
            direction: LogDirection::Backward,
            interval_ns: None,
            step_ns: None,
            since_ns: None,
        };
        assert_eq!(effective_window(&params, false, 42), (Some(10), Some(20)));
    }

    #[test]
    fn instant_query_with_time_uses_thirty_second_lookback_window() {
        let params = LokiParams {
            query: Some("{}".into()),
            start_ns: None,
            end_ns: None,
            time_ns: Some(60_000_000_000),
            limit: 1000,
            direction: LogDirection::Backward,
            interval_ns: None,
            step_ns: None,
            since_ns: None,
        };
        assert_eq!(
            effective_window(&params, false, 42),
            (Some(30_000_000_000), Some(60_000_000_001))
        );
    }
}
