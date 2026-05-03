//! Hosted control API + tenant-scoped OTLP trace ingest.

use crate::api::ingestion::traces::process_traces;
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::capture_export::{build_capture_json, capture_query_sql};
use crate::inject::{
    build_error_response, build_mock_response, case_embedded_rules, encode_inject_response_proto,
    is_strict_external_http_policy, normalize_otlp_body, parse_inject_lookup,
    parse_inject_rules_document, select_inject_rule,
};
use axum::{
    body::Bytes,
    extract::{Extension, Path, Query, Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::Message;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

/// Require `Authorization: Bearer` for `/v1/*`, resolve tenant, store [`TenantInfo`] in extensions.
pub async fn hosted_auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let path = req.uri().path();
    if !path.starts_with("/v1/") {
        return Ok(next.run(req).await);
    }

    if state.hosted.is_none() {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let auth = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token = parse_bearer(auth).ok_or(StatusCode::UNAUTHORIZED)?;

    let hosted = state.hosted.as_ref().unwrap();
    let info = hosted
        .resolver
        .resolve(&token)
        .await
        .map_err(|_| StatusCode::FORBIDDEN)?;

    req.extensions_mut().insert(info);
    Ok(next.run(req).await)
}

pub(crate) fn parse_bearer(h: &str) -> Option<String> {
    let h = h.trim();
    let rest = h.strip_prefix("Bearer ")?;
    let t = rest.trim();
    if t.is_empty() {
        return None;
    }
    Some(t.to_string())
}

async fn v1_meta() -> impl IntoResponse {
    Json(json!({
        "runtimeVersion": env!("CARGO_PKG_VERSION"),
        "specVersion": "http-control-api@v1",
        "schemaVersion": "1"
    }))
}

pub fn hosted_routes() -> axum::Router<AppState> {
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/v1/meta", get(v1_meta))
        .route(
            "/v1/sessions",
            post(v1_create_session).get(v1_list_sessions),
        )
        .route("/v1/sessions/{id}/close", post(v1_close_session))
        .route("/v1/sessions/{id}/load-case", post(v1_load_case))
        .route("/v1/sessions/{id}/rules", post(v1_apply_rules))
        .route("/v1/sessions/{id}/policy", post(v1_apply_policy))
        .route("/v1/sessions/{id}/fixtures/auth", post(v1_fixtures_auth))
        .route("/v1/sessions/{id}/stats", get(v1_session_stats))
        .route("/v1/sessions/{id}/state", get(v1_session_state))
        .route("/v1/inject", post(v1_inject))
        .route("/v1/captures/{capture_id}", get(v1_get_capture))
        .route("/v1/catalog/entity-types", get(v1_catalog_entity_types))
        .route("/v1/catalog/values", get(v1_catalog_values))
}

#[derive(Debug, Deserialize)]
pub struct CatalogValuesQuery {
    #[serde(rename = "entityType")]
    pub entity_type: String,
    #[serde(default = "default_catalog_limit")]
    pub limit: i64,
}

fn default_catalog_limit() -> i64 {
    500
}

async fn v1_catalog_entity_types(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let Some(cat) = state.dropdown_catalog.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "dropdown_catalog_unavailable"})),
        ));
    };
    let days = cat.active_values_days();
    match cat.list_entity_types(&tenant.tenant_id, days).await {
        Ok(types) => Ok(Json(json!({ "entityTypes": types }))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{}", e)})),
        )),
    }
}

async fn v1_catalog_values(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Query(q): Query<CatalogValuesQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if q.entity_type.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "entityType is required"})),
        ));
    }
    let Some(cat) = state.dropdown_catalog.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "dropdown_catalog_unavailable"})),
        ));
    };
    let limit = q.limit.clamp(1, 10_000);
    let days = cat.active_values_days();
    match cat
        .list_entity_values(&tenant.tenant_id, &q.entity_type, days, limit)
        .await
    {
        Ok(values) => Ok(Json(json!({
            "entityType": q.entity_type,
            "values": values
        }))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{}", e)})),
        )),
    }
}

async fn v1_create_session(
    State(state): State<AppState>,
    Extension(_tenant): Extension<TenantInfo>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let mode = body.get("mode").and_then(|m| m.as_str()).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid create session request"})),
        )
    })?;
    let hosted = state.hosted.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"no hosted"})),
    ))?;
    let mut store = hosted.session_store.lock().await;
    let s = store.create(mode).await;
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": 0
    })))
}

async fn v1_list_sessions(
    State(state): State<AppState>,
    Extension(_tenant): Extension<TenantInfo>,
) -> Result<impl IntoResponse, StatusCode> {
    let hosted = state
        .hosted
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let mut store = hosted.session_store.lock().await;
    let list = store.list().await;
    let sessions: Vec<_> = list
        .into_iter()
        .map(|s| {
            json!({
                "sessionId": s.id,
                "mode": s.mode,
                "sessionRevision": s.revision
            })
        })
        .collect();
    Ok(Json(json!({ "sessions": sessions })))
}

async fn v1_close_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let hosted = state.hosted.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"no hosted"})),
    ))?;
    let session = {
        let mut store = hosted.session_store.lock().await;
        store.get(&id).await
    };
    let Some(session) = session else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "unknown_session", "message": "unknown session"}})),
        ));
    };
    // Capture export reads `committed_*` views; spans may still be in RAM until flush.
    if session.mode.eq_ignore_ascii_case("capture") {
        if let Err(e) = flush_buffers_for_capture_export(&state).await {
            tracing::warn!("capture session close: buffer flush failed: {e}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": {"code": "flush_failed", "message": e.to_string()}})),
            ));
        }
    }
    let closed = {
        let mut store = hosted.session_store.lock().await;
        store.close(&id).await
    };
    if !closed {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "unknown_session", "message": "unknown session"}})),
        ));
    }
    Ok(Json(json!({"sessionId": id, "closed": true})))
}

/// Ensure buffered telemetry is persisted so capture SQL over `committed_*` sees recent data.
async fn flush_buffers_for_capture_export(state: &AppState) -> anyhow::Result<()> {
    if let Some(buf) = state.span_buffer.as_ref() {
        buf.force_flush().await?;
    }
    if let Some(buf) = state.log_buffer.as_ref() {
        buf.force_flush().await?;
    }
    if let Some(buf) = state.metric_buffer.as_ref() {
        buf.force_flush().await?;
    }
    Ok(())
}

async fn v1_load_case(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid load-case request"})),
        ));
    }
    let hosted = state.hosted.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"no hosted"})),
    ))?;
    let mut store = hosted.session_store.lock().await;
    let Some(s) = store.load_case(&id, body.to_vec()).await else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "unknown_session", "message": "unknown session"}})),
        ));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": s.revision
    })))
}

async fn v1_apply_rules(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid control payload"})),
        ));
    }
    if let Err(e) = parse_inject_rules_document(&body) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "code": "invalid_rules",
                    "message": format!("rules document must be valid JSON and match the runtime inject rule shape: {e}")
                }
            })),
        ));
    }
    let hosted = state.hosted.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"no hosted"})),
    ))?;
    let mut store = hosted.session_store.lock().await;
    let Some(s) = store.apply_rules(&id, body.to_vec()).await else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "unknown_session", "message": "unknown session"}})),
        ));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": s.revision
    })))
}

async fn v1_apply_policy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid control payload"})),
        ));
    }
    let hosted = state.hosted.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"no hosted"})),
    ))?;
    let mut store = hosted.session_store.lock().await;
    let Some(s) = store.apply_policy(&id, body.to_vec()).await else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "unknown_session", "message": "unknown session"}})),
        ));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": s.revision
    })))
}

async fn v1_fixtures_auth(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid control payload"})),
        ));
    }
    let hosted = state.hosted.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"no hosted"})),
    ))?;
    let mut store = hosted.session_store.lock().await;
    let Some(s) = store.apply_fixtures_auth(&id, body.to_vec()).await else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "unknown_session", "message": "unknown session"}})),
        ));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": s.revision
    })))
}

async fn v1_session_stats(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let hosted = state.hosted.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"no hosted"})),
    ))?;
    let mut store = hosted.session_store.lock().await;
    let Some(s) = store.get(&id).await else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "unknown_session", "message": "unknown session"}})),
        ));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": s.revision,
        "mode": s.mode,
        "stats": {
            "injectedSpans": s.stats.injected_spans,
            "extractedSpans": s.stats.extracted_spans,
            "strictMisses": s.stats.strict_misses
        }
    })))
}

async fn v1_session_state(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let hosted = state.hosted.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"no hosted"})),
    ))?;
    let mut store = hosted.session_store.lock().await;
    let Some(s) = store.get(&id).await else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "unknown_session", "message": "unknown session"}})),
        ));
    };
    let out = json!({
        "sessionId": s.id,
        "sessionRevision": s.revision,
        "mode": s.mode,
        "caseSummary": {"traceCount": 0},
        "stats": {
            "injectedSpans": s.stats.injected_spans,
            "extractedSpans": s.stats.extracted_spans,
            "strictMisses": s.stats.strict_misses
        }
    });
    Ok(Json(out))
}

/// POST /v1/traces (hosted): annotate tenant + capture id, then ingest.
/// Hosted OTLP trace export (gRPC): same processing as [`hosted_post_v1_traces`].
pub async fn hosted_export_trace_request(
    state: AppState,
    tenant: &TenantInfo,
    req: ExportTraceServiceRequest,
) -> anyhow::Result<()> {
    let (capture_id, _) = parse_extract_meta(&req).map_err(|e| anyhow::anyhow!(e))?;
    let capture_id = if capture_id.is_empty() {
        format!("cap_{}", Uuid::new_v4())
    } else {
        capture_id
    };
    let annotated = annotate_export_request(req, &capture_id, &tenant.tenant_id);
    let body_size = annotated.encoded_len();
    process_traces(state, annotated, body_size).await?;
    Ok(())
}

pub async fn hosted_post_v1_traces(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let req = normalize_otlp_body(&body).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let (capture_id, _) = parse_extract_meta(&req).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let capture_id = if capture_id.is_empty() {
        format!("cap_{}", Uuid::new_v4())
    } else {
        capture_id
    };
    let annotated = annotate_export_request(req, &capture_id, &tenant.tenant_id);
    let body_size = annotated.encoded_len();
    process_traces(state.clone(), annotated, body_size)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((
        StatusCode::OK,
        Json(json!({ "captureId": capture_id, "accepted": true })),
    ))
}

/// Derives optional capture correlation from the first `sp.session.id` on any span (preferred)
/// or resource, and counts spans. Accepts standard OTLP traces (no `sp.span.type=extract` required).
fn parse_extract_meta(req: &ExportTraceServiceRequest) -> Result<(String, usize), String> {
    let mut session_hint = String::new();
    let mut span_count = 0usize;

    for rs in &req.resource_spans {
        for ss in &rs.scope_spans {
            for sp in &ss.spans {
                span_count += 1;
                if session_hint.is_empty() {
                    session_hint = span_attr(sp, "sp.session.id");
                }
            }
        }
    }

    if session_hint.is_empty() {
        for rs in &req.resource_spans {
            if let Some(res) = &rs.resource {
                session_hint = resource_attr_str(res, "sp.session.id");
                if !session_hint.is_empty() {
                    break;
                }
            }
        }
    }

    if span_count == 0 {
        return Err("no spans in OTLP export".into());
    }
    Ok((session_hint, span_count))
}

fn span_attr(sp: &Span, key: &str) -> String {
    for kv in &sp.attributes {
        if kv.key == key {
            if let Some(v) = &kv.value {
                if let Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                ) = &v.value
                {
                    return s.clone();
                }
            }
        }
    }
    String::new()
}

fn resource_attr_str(res: &Resource, key: &str) -> String {
    for kv in &res.attributes {
        if kv.key == key {
            if let Some(v) = &kv.value {
                if let Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                ) = &v.value
                {
                    return s.clone();
                }
            }
        }
    }
    String::new()
}

fn annotate_export_request(
    mut req: ExportTraceServiceRequest,
    capture_id: &str,
    tenant_id: &str,
) -> ExportTraceServiceRequest {
    for rs in &mut req.resource_spans {
        append_resource_kv(&mut rs.resource, "sp.capture.id", capture_id);
        append_resource_kv(&mut rs.resource, "sp.tenant.id", tenant_id);
        for ss in &mut rs.scope_spans {
            for sp in &mut ss.spans {
                append_span_kv(sp, "sp.capture.id", capture_id);
                append_span_kv(sp, "sp.tenant.id", tenant_id);
            }
        }
    }
    req
}

fn append_resource_kv(res: &mut Option<Resource>, key: &str, val: &str) {
    let r = res.get_or_insert_with(|| Resource {
        attributes: vec![],
        dropped_attributes_count: 0,
    });
    r.attributes.push(kv_str(key, val));
}

fn append_span_kv(sp: &mut Span, key: &str, val: &str) {
    sp.attributes.push(kv_str(key, val));
}

fn kv_str(key: &str, val: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(
                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                    val.to_string(),
                ),
            ),
        }),
    }
}

async fn v1_inject(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let hosted = state
        .hosted
        .as_ref()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no hosted".into()))?;
    let payload =
        normalize_otlp_body(&body).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let lookup =
        parse_inject_lookup(&payload).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    if lookup.session_id.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "missing session id".into()));
    }
    let mut store = hosted.session_store.lock().await;
    let Some(sess) = store.get(&lookup.session_id).await else {
        return Err((StatusCode::NOT_FOUND, "unknown session".into()));
    };
    // Rules are validated on `POST …/rules`. If this fails, the stored blob was
    // not written through that path, data was corrupted, or binary versions skewed.
    let session_rules = parse_inject_rules_document(&sess.rules).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("stored session rules failed to parse: {e}"),
        )
    })?;
    let case_rules = case_embedded_rules(&sess.loaded_case);
    let strict = is_strict_external_http_policy(&sess.policy);
    let m = select_inject_rule(&lookup, strict, &case_rules, &session_rules);
    let Some(m) = m else {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no inject match"})),
        )
            .into_response());
    };
    match m.rule.then.action.as_str() {
        "mock" => {
            let resp = build_mock_response(&m.rule).ok_or((
                StatusCode::INTERNAL_SERVER_ERROR,
                "mock rule missing response".into(),
            ))?;
            let _ = store.record_injected_spans(&lookup.session_id, 1).await;
            drop(store);
            let body = encode_inject_response_proto(&resp)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/x-protobuf")
                .body(axum::body::Body::from(body))
                .unwrap())
        }
        "error" => {
            let (st, msg) = build_error_response(&m.rule);
            if m.source == "policy" {
                let _ = store.record_strict_miss(&lookup.session_id, 1).await;
            }
            drop(store);
            let code = StatusCode::from_u16(st as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Ok((code, Json(json!({"error": msg}))).into_response())
        }
        "passthrough" | "capture_only" => {
            drop(store);
            Ok((
                StatusCode::NOT_FOUND,
                Json(json!({"error": "no inject match"})),
            )
                .into_response())
        }
        _ => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "unsupported rule action".into(),
        )),
    }
}

async fn v1_get_capture(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(capture_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let sql = capture_query_sql(&tenant.tenant_id, &capture_id);
    match state.query_engine.execute_query(&sql).await {
        Ok(result) => {
            if result.row_count == 0 {
                return Err((
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": {"code": "not_found", "message": "capture not found"}})),
                ));
            }
            let out = build_capture_json(&capture_id, &result.columns, &result.rows).map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": {"code": "internal_error", "message": "failed to build capture"}})),
                )
            })?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(out))
                .unwrap())
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"code": "storage_error", "message": e.to_string()}})),
        )),
    }
}

#[cfg(test)]
mod bearer_tests {
    use super::parse_bearer;

    #[test]
    fn parses_valid_bearer_token() {
        assert_eq!(parse_bearer("Bearer abc").as_deref(), Some("abc"));
        assert_eq!(parse_bearer("Bearer  ").as_deref(), None);
    }

    #[test]
    fn trims_and_extracts_after_prefix() {
        assert_eq!(
            parse_bearer("  Bearer   tok  ").as_deref(),
            Some("tok")
        );
    }

    #[test]
    fn rejects_missing_or_empty_token() {
        assert!(parse_bearer("Bearer").is_none());
        assert!(parse_bearer("Bearer ").is_none());
        assert!(parse_bearer("").is_none());
        assert!(parse_bearer("Basic x").is_none());
    }
}
