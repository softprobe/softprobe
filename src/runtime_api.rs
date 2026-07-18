//! Runtime control API + OTLP trace ingest for the configured DuckLake scope.

use crate::api::ingestion::traces::process_traces;
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::capture_export::{build_capture_json, capture_query_sql};
use crate::config::Config;
use crate::inject::{
    build_error_response, build_mock_response, case_embedded_rules, encode_inject_response_proto,
    is_strict_external_http_policy, normalize_otlp_body, parse_inject_lookup,
    parse_inject_rules_document, select_inject_rule,
};
use crate::promotion::{
    business_current_view_name, business_physical_table_name, parse_promotion_manifest,
    validate_business_table_compatible, BusinessTableManifest, PromotionDataType,
    PromotionManifest, TelemetryColumnsManifest, TelemetryTable,
};
use crate::runtime_engine::{DuckLakeScope, ScopeProvisioningRequest};
use crate::runtime_engine::{RuntimeEngine, TenantSessionStore};
use axum::{
    body::Bytes,
    extract::{Extension, Path, Query, Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

async fn sessions_or_fail(
    state: &AppState,
    tenant: &TenantInfo,
) -> Result<Arc<TenantSessionStore>, (StatusCode, Json<serde_json::Value>)> {
    let engine = state.engine_for_tenant(tenant).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"code": "engine_unavailable", "message": e.to_string()}})),
        )
    })?;
    engine.sessions.clone().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(
            json!({"error": {"code": "session_store_unavailable", "message": "sessions require Redis control-plane"}}),
        ),
    ))
}

async fn flush_engine_capture_buffers(engine: &RuntimeEngine) -> anyhow::Result<()> {
    engine.ingest.force_flush_spans().await?;
    engine.ingest.force_flush_logs().await?;
    engine.ingest.force_flush_metrics().await?;
    Ok(())
}

/// Require `Authorization: Bearer` for `/v1/*`, resolve tenant, store [`TenantInfo`] in extensions.
pub async fn runtime_auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let path = req.uri().path();
    if !requires_runtime_auth(req.method(), path) {
        return Ok(next.run(req).await);
    }

    let auth = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token = parse_bearer(auth).ok_or(StatusCode::UNAUTHORIZED)?;

    let control_plane = state
        .engines
        .control_plane()
        .expect("runtime auth middleware requires control-plane state");
    let info = control_plane
        .resolver
        .resolve(&token)
        .await
        .map_err(|_| StatusCode::FORBIDDEN)?;

    req.extensions_mut().insert(info);
    Ok(next.run(req).await)
}

fn requires_runtime_auth(method: &Method, path: &str) -> bool {
    if path == "/v1/tenants" && *method == Method::POST {
        return false;
    }
    path.starts_with("/v1/")
}

fn admin_provision_token_matches(token: &str) -> bool {
    let Ok(want) = std::env::var("SOFTPROBE_ADMIN_API_KEY") else {
        return false;
    };
    let want = want.trim();
    !want.is_empty() && want == token.trim()
}

/// JSON body for HTTP 404 when a session id is missing from this process's session store
/// ([`spec/schemas/session-error.response.schema.json`]).
fn json_unknown_session(session_id: &str) -> serde_json::Value {
    json!({
        "error": {
            "code": "unknown_session",
            "message": format!(
                "no session '{session_id}' in this runtime's session store; confirm SOFTPROBE_RUNTIME_URL points at the deployment that created the session, check session TTL or store reset, and create a new session if needed"
            )
        }
    })
}

/// Plain-text body for `POST /v1/inject` when the session is missing (proxy contract uses HTTP 404 for misses).
fn text_unknown_session_for_inject(session_id: &str) -> String {
    format!(
        "unknown session '{session_id}': not in this runtime's session store; confirm the proxy/SDK and session control use the same runtime base URL and that the session was created on this deployment (TTL or Redis flush also clears state)"
    )
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct DuckLakeConnectionMaterial {
    version: u8,
    tenant_id: String,
    ducklake_pg_uri: String,
    ducklake_metadata_schema: String,
    ducklake_data_path: String,
    gcs_bucket: String,
    /// Object-store access key (`GCS_HMAC_*` for `gs://`, `AWS_ACCESS_KEY_ID` for `s3://`).
    gcs_hmac_access_key_id: String,
    /// Object-store secret (`GCS_HMAC_SECRET` / `AWS_SECRET_ACCESS_KEY`).
    gcs_hmac_secret: String,
    /// Optional STS / EC2 role session token for `s3://` (`AWS_SESSION_TOKEN`).
    /// Empty when unused (HMAC / static keys). Clients must `SET s3_session_token` when set.
    session_token: String,
    schema_version: String,
}

fn ducklake_connection_material(
    tenant: &TenantInfo,
    scope: &DuckLakeScope,
) -> Result<DuckLakeConnectionMaterial, String> {
    let config = Config::load().map_err(|e| format!("runtime config load failed: {e}"))?;
    let ducklake = &config.ducklake;
    let ducklake_pg_uri = postgres_ducklake_metadata_path(ducklake);
    // DuckLake schema and data path come from the tenant scope resolved for this process.
    let ducklake_data_path = scope.data_path.clone();
    let ducklake_metadata_schema = scope.metadata_schema.clone();
    let creds = config.resolve_object_store_credentials(&ducklake_data_path);

    if ducklake.catalog_type != "postgres" {
        return Err(format!(
            "ducklake.catalog_type must be postgres for agent setup, got {}",
            ducklake.catalog_type
        ));
    }
    if ducklake_pg_uri.trim().is_empty() {
        return Err("DuckLake Postgres metadata path is required".to_string());
    }
    if ducklake_data_path.trim().is_empty() {
        return Err("DuckLake data path is required".to_string());
    }
    if path_requires_hmac(&ducklake_data_path) && !creds.is_complete() {
        return Err(
            "GCS/S3 DuckLake data path requires object-store credentials in the environment \
             (GCS_HMAC_ACCESS_KEY_ID/GCS_HMAC_SECRET for gs://, or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY for s3://)"
                .to_string(),
        );
    }
    if tenant.bucket_name.trim().is_empty() {
        return Err("tenant is missing bucket_name".to_string());
    }

    Ok(DuckLakeConnectionMaterial {
        version: 1,
        tenant_id: tenant.tenant_id.clone(),
        ducklake_pg_uri,
        ducklake_metadata_schema,
        ducklake_data_path,
        gcs_bucket: tenant.bucket_name.clone(),
        gcs_hmac_access_key_id: creds.access_key_id.unwrap_or_default(),
        gcs_hmac_secret: creds.secret_access_key.unwrap_or_default(),
        session_token: creds.session_token.unwrap_or_default(),
        schema_version: "1".to_string(),
    })
}

fn postgres_ducklake_metadata_path(ducklake: &crate::config::DuckLakeConfig) -> String {
    let metadata_path = ducklake.metadata_path.trim();
    if metadata_path.starts_with("postgres:") {
        metadata_path.trim_start_matches("postgres:").to_string()
    } else {
        metadata_path.to_string()
    }
}

fn path_requires_hmac(data_path: &str) -> bool {
    let p = data_path.trim();
    p.starts_with("gs://") || p.starts_with("s3://")
}

#[cfg(test)]
mod data_connection_tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn ducklake_connection_material_uses_runtime_config_and_tenant_scope() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("runtime.yaml");
        let mut config = Config::default();
        config.ducklake.catalog_type = "postgres".to_string();
        config.ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();
        config.ducklake.data_path = "./warehouse/ducklake/data/".to_string();
        config.ducklake.metadata_schema = "tenant_meta".to_string();
        std::fs::write(&config_path, serde_yaml::to_string(&config).expect("yaml"))
            .expect("write config");
        std::env::set_var("CONFIG_FILE", config_path.to_string_lossy().to_string());
        std::env::remove_var("DUCKLAKE_PG_URI");
        std::env::remove_var("DUCKLAKE_DATA_PATH");
        std::env::remove_var("DUCKLAKE_METADATA_SCHEMA");
        std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCS_HMAC_SECRET");

        let tenant = TenantInfo {
            tenant_id: "tenant-123".to_string(),
            bucket_name: "softprobe-tenant-bucket".to_string(),
            dataset_id: "ignored".to_string(),
        };
        let scope = DuckLakeScope {
            metadata_schema: "tenant_tenant_123".to_string(),
            data_path: "./warehouse/ducklake/data/".to_string(),
        };

        let material = ducklake_connection_material(&tenant, &scope).expect("connection material");
        assert_eq!(material.version, 1);
        assert_eq!(material.tenant_id, "tenant-123");
        assert_eq!(
            material.ducklake_pg_uri,
            "host=pg port=5432 dbname=ducklake user=reader password=secret"
        );
        assert_eq!(material.ducklake_metadata_schema, "tenant_tenant_123");
        assert_eq!(material.ducklake_data_path, "./warehouse/ducklake/data/");
        assert_eq!(material.gcs_bucket, "softprobe-tenant-bucket");
        assert_eq!(material.gcs_hmac_access_key_id, "");
        assert_eq!(material.gcs_hmac_secret, "");
        assert_eq!(material.session_token, "");
        assert_eq!(material.schema_version, "1");

        std::env::remove_var("CONFIG_FILE");
    }

    #[test]
    fn ducklake_connection_material_reads_hmac_from_environment() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("runtime.yaml");
        let mut config = Config::default();
        config.ducklake.catalog_type = "postgres".to_string();
        config.ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();
        config.ducklake.data_path = "gs://bucket/ducklake/data/".to_string();
        config.ducklake.metadata_schema = "config_schema".to_string();
        std::fs::write(&config_path, serde_yaml::to_string(&config).expect("yaml"))
            .expect("write config");
        std::env::set_var("CONFIG_FILE", config_path.to_string_lossy().to_string());
        std::env::set_var("DUCKLAKE_PG_URI", "host=override port=5432 dbname=ducklake");
        std::env::set_var("DUCKLAKE_METADATA_SCHEMA", "override_schema");
        std::env::set_var("DUCKLAKE_DATA_PATH", "gs://override/ducklake/data/");
        std::env::set_var("GCS_HMAC_ACCESS_KEY_ID", "access-id");
        std::env::set_var("GCS_HMAC_SECRET", "secret-value");

        let tenant = TenantInfo {
            tenant_id: "tenant-123".to_string(),
            bucket_name: "softprobe-tenant-bucket".to_string(),
            dataset_id: "ignored".to_string(),
        };
        let scope = DuckLakeScope {
            metadata_schema: "tenant_tenant_123".to_string(),
            data_path: "gs://bucket/ducklake/data/".to_string(),
        };

        let material = ducklake_connection_material(&tenant, &scope).expect("connection material");
        assert_eq!(
            material.ducklake_pg_uri,
            "host=pg port=5432 dbname=ducklake user=reader password=secret"
        );
        assert_eq!(material.ducklake_metadata_schema, "tenant_tenant_123");
        assert_eq!(material.ducklake_data_path, "gs://bucket/ducklake/data/");
        assert_eq!(material.gcs_hmac_access_key_id, "access-id");
        assert_eq!(material.gcs_hmac_secret, "secret-value");
        assert_eq!(material.session_token, "");

        std::env::remove_var("CONFIG_FILE");
        std::env::remove_var("DUCKLAKE_PG_URI");
        std::env::remove_var("DUCKLAKE_DATA_PATH");
        std::env::remove_var("DUCKLAKE_METADATA_SCHEMA");
        std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCS_HMAC_SECRET");
    }

    #[test]
    fn ducklake_connection_material_includes_s3_session_token() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("runtime.yaml");
        let mut config = Config::default();
        config.ducklake.catalog_type = "postgres".to_string();
        config.ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();
        config.ducklake.data_path = "s3://bucket/ducklake/data/".to_string();
        config.object_store.region = "us-west-2".to_string();
        std::fs::write(&config_path, serde_yaml::to_string(&config).expect("yaml"))
            .expect("write config");
        std::env::set_var("CONFIG_FILE", config_path.to_string_lossy().to_string());
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIATEST");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "secret-test");
        std::env::set_var("AWS_SESSION_TOKEN", "session-test-token");
        std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCS_HMAC_SECRET");

        let tenant = TenantInfo {
            tenant_id: "tenant-123".to_string(),
            bucket_name: "softprobe-tenant-bucket".to_string(),
            dataset_id: "ignored".to_string(),
        };
        let scope = DuckLakeScope {
            metadata_schema: "tenant_tenant_123".to_string(),
            data_path: "s3://bucket/ducklake/data/".to_string(),
        };

        let material = ducklake_connection_material(&tenant, &scope).expect("connection material");
        assert_eq!(material.gcs_hmac_access_key_id, "AKIATEST");
        assert_eq!(material.gcs_hmac_secret, "secret-test");
        assert_eq!(material.session_token, "session-test-token");

        std::env::remove_var("CONFIG_FILE");
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_SESSION_TOKEN");
    }

    #[test]
    fn ducklake_connection_material_requires_hmac_for_gcs_path() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("runtime.yaml");
        let mut config = Config::default();
        config.ducklake.catalog_type = "postgres".to_string();
        config.ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();
        config.ducklake.data_path = "gs://bucket/ducklake/data/".to_string();
        std::fs::write(&config_path, serde_yaml::to_string(&config).expect("yaml"))
            .expect("write config");
        std::env::set_var("CONFIG_FILE", config_path.to_string_lossy().to_string());
        std::env::remove_var("DUCKLAKE_PG_URI");
        std::env::remove_var("DUCKLAKE_DATA_PATH");
        std::env::remove_var("DUCKLAKE_METADATA_SCHEMA");
        std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCS_HMAC_SECRET");
        std::env::remove_var("GCP_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCP_HMAC_SECRET");

        let tenant = TenantInfo {
            tenant_id: "tenant-123".to_string(),
            bucket_name: "softprobe-tenant-bucket".to_string(),
            dataset_id: "ignored".to_string(),
        };
        let scope = DuckLakeScope {
            metadata_schema: "tenant_tenant_123".to_string(),
            data_path: "gs://bucket/ducklake/data/".to_string(),
        };

        let err =
            ducklake_connection_material(&tenant, &scope).expect_err("missing hmac should fail");
        assert!(err.contains("GCS_HMAC_ACCESS_KEY_ID") || err.contains("object-store credentials"));

        std::env::remove_var("CONFIG_FILE");
    }
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TenantProvisionHttpRequest {
    tenant_id: String,
    #[serde(default)]
    storage_hints: Option<TenantStorageHintsBody>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TenantStorageHintsBody {
    ducklake_metadata_schema: Option<String>,
    ducklake_data_path: Option<String>,
    gcs_bucket: Option<String>,
}

/// `POST /v1/tenants` — admin-only tenant provisioning ([`spec/protocol/http-control-api.md`]).
async fn v1_provision_scope(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<TenantProvisionHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": {"code": "unauthorized", "message": "Authorization header required"}})),
        ))?;
    let token = parse_bearer(auth).ok_or((
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": {"code": "unauthorized", "message": "Bearer token required"}})),
    ))?;
    if !admin_provision_token_matches(&token) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(
                json!({"error": {"code": "admin_required", "message": "admin API key required for tenant provisioning"}}),
            ),
        ));
    }

    let tenant_id = body.tenant_id.trim().to_string();
    if tenant_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": {"code": "invalid_request", "message": "tenantId is required"}})),
        ));
    }

    let Some(tenant_ducklake) = state.engines.scope_registry() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(
                json!({"error": {"code": "tenant_provisioning_unavailable", "message": "tenant registry is not configured"}}),
            ),
        ));
    };

    let hints = body.storage_hints.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": {"code": "invalid_request", "message": "storageHints is required"}})),
        )
    })?;
    let metadata_schema = hints.ducklake_metadata_schema.clone().unwrap_or_default();
    let data_path = hints.ducklake_data_path.clone().unwrap_or_default();
    if metadata_schema.trim().is_empty() || data_path.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error": {"code": "invalid_request", "message": "storageHints.ducklakeMetadataSchema and ducklakeDataPath are required"}}),
            ),
        ));
    }

    if let Ok(existing) = tenant_ducklake.resolve_scope(&tenant_id).await {
        if existing.metadata_schema == metadata_schema && existing.data_path == data_path {
            let mut scope = json!({
                "ducklakeMetadataSchema": existing.metadata_schema,
                "ducklakeDataPath": existing.data_path,
            });
            if let Some(b) = hints.gcs_bucket.as_ref().filter(|s| !s.trim().is_empty()) {
                scope["gcsBucket"] = json!(b);
            }
            return Ok(Json(json!({
                "version": 1,
                "tenantId": tenant_id,
                "status": "exists",
                "scope": scope
            })));
        }
        return Err((
            StatusCode::CONFLICT,
            Json(
                json!({"error": {"code": "tenant_scope_conflict", "message": "tenant exists with different storage scope"}}),
            ),
        ));
    }

    let scope = tenant_ducklake
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_id.clone(),
            metadata_schema: metadata_schema.clone(),
            data_path: data_path.clone(),
        })
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": {"code": "provision_failed", "message": e.to_string()}})),
            )
        })?;

    let mut scope_json = json!({
        "ducklakeMetadataSchema": scope.metadata_schema,
        "ducklakeDataPath": scope.data_path,
    });
    if let Some(b) = hints.gcs_bucket.as_ref().filter(|s| !s.trim().is_empty()) {
        scope_json["gcsBucket"] = json!(b);
    }

    state.engines.invalidate(&tenant_id);

    Ok(Json(json!({
        "version": 1,
        "tenantId": tenant_id,
        "status": "created",
        "scope": scope_json
    })))
}

async fn v1_meta() -> impl IntoResponse {
    Json(json!({
        "runtimeVersion": env!("CARGO_PKG_VERSION"),
        "specVersion": "http-control-api@v1",
        "schemaVersion": "1"
    }))
}

pub fn runtime_control_routes() -> axum::Router<AppState> {
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/v1/tenants", post(v1_provision_scope))
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
        .route("/v1/data/ducklake-connection", get(v1_ducklake_connection))
        .route("/v1/promotions/apply", post(v1_promotions_apply))
        .route("/v1/captures/{capture_id}", get(v1_get_capture))
        .route("/v1/catalog/entity-types", get(v1_catalog_entity_types))
        .route("/v1/catalog/values", get(v1_catalog_values))
}

async fn v1_ducklake_connection(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let Some(tenant_ducklake) = state.engines.scope_registry() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "ducklake_connection_unavailable",
                    "message": "tenant DuckLake resolver is unavailable"
                }
            })),
        ));
    };
    let scope = tenant_ducklake
        .resolve_scope(&tenant.tenant_id)
        .await
        .map_err(|err| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": {
                        "code": "ducklake_connection_unavailable",
                        "message": err.to_string()
                    }
                })),
            )
        })?;
    match ducklake_connection_material(&tenant, &scope) {
        Ok(material) => Ok(Json(material)),
        Err(err) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "ducklake_connection_unavailable",
                    "message": err
                }
            })),
        )),
    }
}

#[derive(Debug, Deserialize)]
struct PromotionApplyRequest {
    #[serde(rename = "manifestYaml")]
    manifest_yaml: String,
}

async fn v1_promotions_apply(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Json(req): Json<PromotionApplyRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let manifest = parse_promotion_manifest(&req.manifest_yaml).map_err(|err| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": {
                    "code": err.code(),
                    "message": err.to_string()
                }
            })),
        )
    })?;
    match manifest {
        PromotionManifest::TelemetryColumns(spec) => {
            apply_telemetry_promotion(state, tenant, req.manifest_yaml, spec).await
        }
        PromotionManifest::BusinessTable(spec) => {
            apply_business_table_promotion(state, tenant, req.manifest_yaml, spec).await
        }
    }
}

async fn apply_telemetry_promotion(
    state: AppState,
    tenant: TenantInfo,
    manifest_yaml: String,
    spec: TelemetryColumnsManifest,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let Some(tenant_ducklake) = state.engines.scope_registry() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "ducklake_connection_unavailable",
                    "message": "tenant DuckLake resolver is unavailable"
                }
            })),
        ));
    };
    let engine = state
        .engine_for_tenant(&tenant)
        .await
        .map_err(|err| promotion_apply_error("ducklake_scope_unavailable", err))?;
    engine
        .storage
        .writer
        .apply_telemetry_column_promotion(&engine.scope, &spec)
        .await
        .map_err(|err| promotion_apply_error("promotion_schema_apply_failed", err))?;
    tenant_ducklake
        .record_active_telemetry_promotion_spec(
            &engine.scope,
            &manifest_yaml,
            &telemetry_table_names(&spec.target.tables),
        )
        .await
        .map_err(|err| promotion_apply_error("promotion_spec_record_failed", err))?;
    Ok(Json(json!({
        "specVersion": "softprobe.promotion.apply.v1",
        "applied": true,
        "target": {
            "kind": "telemetry_columns",
            "tables": telemetry_table_names(&spec.target.tables)
        },
        "schemaChanges": telemetry_schema_changes(&spec)
    })))
}

async fn apply_business_table_promotion(
    state: AppState,
    tenant: TenantInfo,
    manifest_yaml: String,
    spec: BusinessTableManifest,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let Some(tenant_ducklake) = state.engines.scope_registry() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "ducklake_connection_unavailable",
                    "message": "tenant DuckLake resolver is unavailable"
                }
            })),
        ));
    };
    let engine = state
        .engine_for_tenant(&tenant)
        .await
        .map_err(|err| promotion_apply_error("ducklake_scope_unavailable", err))?;
    let current = tenant_ducklake
        .load_active_business_table_manifest_for_scope(&engine.scope, &spec.target.table)
        .await
        .map_err(|err| promotion_apply_error("promotion_spec_load_failed", err))?;
    if let Some(current) = current.as_ref() {
        validate_business_table_compatible(current, &spec).map_err(|err| {
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": {
                        "code": err.code(),
                        "message": err.to_string(),
                        "path": err.path()
                    }
                })),
            )
        })?;
    }
    engine
        .storage
        .writer
        .apply_business_table_promotion(&engine.scope, &spec)
        .await
        .map_err(|err| promotion_apply_error("promotion_schema_apply_failed", err))?;
    tenant_ducklake
        .record_active_business_promotion_spec(&engine.scope, &manifest_yaml, &spec.target.table)
        .await
        .map_err(|err| promotion_apply_error("promotion_spec_record_failed", err))?;
    Ok(Json(json!({
        "specVersion": "softprobe.promotion.apply.v1",
        "applied": true,
        "target": {
            "kind": "business_table",
            "table": spec.target.table,
            "version": spec.target.version
        },
        "schemaChanges": business_schema_changes(&spec)
    })))
}

fn promotion_apply_error(
    code: &'static str,
    err: anyhow::Error,
) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": {
                "code": code,
                "message": err.to_string()
            }
        })),
    )
}

fn telemetry_table_names(tables: &[TelemetryTable]) -> Vec<String> {
    tables
        .iter()
        .map(|table| match table {
            TelemetryTable::Traces => "traces",
            TelemetryTable::Logs => "logs",
            TelemetryTable::Metrics => "metrics",
        })
        .map(str::to_string)
        .collect()
}

fn telemetry_schema_changes(spec: &TelemetryColumnsManifest) -> Vec<serde_json::Value> {
    let mut changes = Vec::new();
    for table in telemetry_table_names(&spec.target.tables) {
        for col in &spec.columns {
            changes.push(json!({
                "table": table,
                "action": "add_column",
                "column": col.name,
                "type": promotion_type_name(&col.data_type),
                "nullable": col.nullable
            }));
        }
    }
    changes
}

fn business_schema_changes(spec: &BusinessTableManifest) -> Vec<serde_json::Value> {
    let table = business_physical_table_name(spec);
    let view = business_current_view_name(spec);
    vec![
        json!({
            "action": "create_table",
            "table": table
        }),
        json!({
            "action": "create_or_replace_view",
            "view": view,
            "sourceTable": table
        }),
    ]
}

fn promotion_type_name(data_type: &PromotionDataType) -> &'static str {
    match data_type {
        PromotionDataType::String => "string",
        PromotionDataType::Bool => "bool",
        PromotionDataType::Int64 => "int64",
        PromotionDataType::Double => "double",
        PromotionDataType::Decimal => "decimal",
        PromotionDataType::Timestamp => "timestamp",
        PromotionDataType::Json => "json",
    }
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
    let engine = state.engine_for_tenant(&tenant).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"code": "engine_unavailable", "message": e.to_string()}})),
        )
    })?;
    let Some(cat) = engine.dropdown_catalog.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "dropdown_catalog_unavailable"})),
        ));
    };
    let days = cat.active_values_days();
    match cat.list_entity_types(days).await {
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
    let engine = state.engine_for_tenant(&tenant).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"code": "engine_unavailable", "message": e.to_string()}})),
        )
    })?;
    let Some(cat) = engine.dropdown_catalog.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "dropdown_catalog_unavailable"})),
        ));
    };
    let limit = q.limit.clamp(1, 10_000);
    let days = cat.active_values_days();
    match cat.list_entity_values(&q.entity_type, days, limit).await {
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
    Extension(tenant): Extension<TenantInfo>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let mode = body.get("mode").and_then(|m| m.as_str()).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid create session request"})),
        )
    })?;
    let sessions = sessions_or_fail(&state, &tenant).await?;
    let Some(s) = sessions.create(mode).await else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "session_store_failed"})),
        ));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": 0
    })))
}

async fn v1_list_sessions(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
) -> Result<impl IntoResponse, StatusCode> {
    let sessions = match sessions_or_fail(&state, &tenant).await {
        Ok(s) => s,
        Err(_) => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };
    let list = sessions.list().await;
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
    Extension(tenant): Extension<TenantInfo>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let engine = state.engine_for_tenant(&tenant).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"code": "engine_unavailable", "message": e.to_string()}})),
        )
    })?;
    let sessions = sessions_or_fail(&state, &tenant).await?;
    let session = sessions.get(&id).await;
    let Some(session) = session else {
        return Err((StatusCode::NOT_FOUND, Json(json_unknown_session(&id))));
    };
    // Capture export reads `committed_*` views; spans may still be in RAM until flush.
    if session.mode.eq_ignore_ascii_case("capture") {
        if let Err(e) = flush_engine_capture_buffers(&engine).await {
            tracing::warn!("capture session close: buffer flush failed: {e}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": {"code": "flush_failed", "message": e.to_string()}})),
            ));
        }
    }
    let closed = sessions.close(&id).await;
    if !closed {
        return Err((StatusCode::NOT_FOUND, Json(json_unknown_session(&id))));
    }
    Ok(Json(json!({"sessionId": id, "closed": true})))
}

async fn v1_load_case(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid load-case request"})),
        ));
    }
    let sessions = sessions_or_fail(&state, &tenant).await?;
    let Some(s) = sessions.load_case(&id, body.to_vec()).await else {
        return Err((StatusCode::NOT_FOUND, Json(json_unknown_session(&id))));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": s.revision
    })))
}

async fn v1_apply_rules(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
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
    let sessions = sessions_or_fail(&state, &tenant).await?;
    let Some(s) = sessions.apply_rules(&id, body.to_vec()).await else {
        return Err((StatusCode::NOT_FOUND, Json(json_unknown_session(&id))));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": s.revision
    })))
}

async fn v1_apply_policy(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid control payload"})),
        ));
    }
    let sessions = sessions_or_fail(&state, &tenant).await?;
    let Some(s) = sessions.apply_policy(&id, body.to_vec()).await else {
        return Err((StatusCode::NOT_FOUND, Json(json_unknown_session(&id))));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": s.revision
    })))
}

async fn v1_fixtures_auth(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid control payload"})),
        ));
    }
    let sessions = sessions_or_fail(&state, &tenant).await?;
    let Some(s) = sessions.apply_fixtures_auth(&id, body.to_vec()).await else {
        return Err((StatusCode::NOT_FOUND, Json(json_unknown_session(&id))));
    };
    Ok(Json(json!({
        "sessionId": s.id,
        "sessionRevision": s.revision
    })))
}

async fn v1_session_stats(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let sessions = sessions_or_fail(&state, &tenant).await?;
    let Some(s) = sessions.get(&id).await else {
        return Err((StatusCode::NOT_FOUND, Json(json_unknown_session(&id))));
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
    Extension(tenant): Extension<TenantInfo>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let sessions = sessions_or_fail(&state, &tenant).await?;
    let Some(s) = sessions.get(&id).await else {
        return Err((StatusCode::NOT_FOUND, Json(json_unknown_session(&id))));
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

/// POST /v1/traces (runtime control): annotate tenant + capture id, then ingest.
/// Runtime OTLP trace export (gRPC): same processing as [`runtime_post_v1_traces`].
pub async fn runtime_export_trace_request(
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
    let annotated = annotate_export_request(req, &capture_id);
    let body_size = annotated.encoded_len();
    process_traces(state, annotated, body_size, Some(tenant.tenant_id.clone())).await?;
    Ok(())
}

pub async fn runtime_post_v1_traces(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let req = normalize_otlp_body(&body).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let (capture_id, _) = parse_extract_meta(&req).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let capture_id = if capture_id.is_empty() {
        format!("cap_{}", Uuid::new_v4())
    } else {
        capture_id
    };
    let annotated = annotate_export_request(req, &capture_id);
    let body_size = annotated.encoded_len();
    process_traces(
        state.clone(),
        annotated,
        body_size,
        tenant.map(|t| t.tenant_id.clone()),
    )
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
) -> ExportTraceServiceRequest {
    for rs in &mut req.resource_spans {
        append_resource_kv(&mut rs.resource, "sp.capture.id", capture_id);
        for ss in &mut rs.scope_spans {
            for sp in &mut ss.spans {
                append_span_kv(sp, "sp.capture.id", capture_id);
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

#[cfg(test)]
mod unknown_session_error_tests {
    use super::{json_unknown_session, text_unknown_session_for_inject};

    #[test]
    fn json_unknown_session_includes_id_and_runtime_url_hint() {
        let v = json_unknown_session("sess_it_123");
        assert_eq!(v["error"]["code"], "unknown_session");
        let m = v["error"]["message"].as_str().expect("message");
        assert!(
            m.contains("sess_it_123"),
            "message should cite session id: {m}"
        );
        assert!(
            m.contains("SOFTPROBE_RUNTIME_URL"),
            "message should hint runtime URL: {m}"
        );
    }

    #[test]
    fn inject_miss_body_is_plain_text_with_session_id() {
        let t = text_unknown_session_for_inject("sess_proxy_9");
        assert!(t.contains("sess_proxy_9"), "{t}");
        assert!(t.to_ascii_lowercase().contains("runtime"), "{t}");
    }
}

#[cfg(test)]
mod annotate_export_tests {
    use super::annotate_export_request;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    #[test]
    fn annotate_export_request_only_adds_capture_id() {
        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    spans: vec![Span::default()],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let out = annotate_export_request(req, "cap-123");
        let resource_attrs = &out.resource_spans[0]
            .resource
            .as_ref()
            .expect("resource")
            .attributes;
        let span_attrs = &out.resource_spans[0].scope_spans[0].spans[0].attributes;

        assert!(contains_kv(resource_attrs, "sp.capture.id", "cap-123"));
        assert!(contains_kv(span_attrs, "sp.capture.id", "cap-123"));
        assert!(
            !contains_key(resource_attrs, "sp.tenant.id")
                && !contains_key(span_attrs, "sp.tenant.id"),
            "runtime annotation must not inject tenant routing attributes"
        );
    }

    fn contains_key(attrs: &[KeyValue], key: &str) -> bool {
        attrs.iter().any(|kv| kv.key == key)
    }

    fn contains_kv(attrs: &[KeyValue], key: &str, want: &str) -> bool {
        attrs.iter().any(|kv| {
            kv.key == key
                && kv
                    .value
                    .as_ref()
                    .and_then(|v| v.value.as_ref())
                    .map(|value| matches!(value, Value::StringValue(s) if s == want))
                    .unwrap_or(false)
        })
    }
}

async fn v1_inject(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantInfo>,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let sessions = sessions_or_fail(&state, &tenant).await.map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "session store unavailable".into(),
        )
    })?;
    let payload =
        normalize_otlp_body(&body).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let lookup =
        parse_inject_lookup(&payload).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    if lookup.session_id.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "missing session id".into()));
    }
    let Some(sess) = sessions.get(&lookup.session_id).await else {
        return Err((
            StatusCode::NOT_FOUND,
            text_unknown_session_for_inject(&lookup.session_id),
        ));
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
            let _ = sessions.record_injected_spans(&lookup.session_id, 1).await;
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
                let _ = sessions.record_strict_miss(&lookup.session_id, 1).await;
            }
            let code = StatusCode::from_u16(st as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Ok((code, Json(json!({"error": msg}))).into_response())
        }
        "passthrough" | "capture_only" => Ok((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no inject match"})),
        )
            .into_response()),
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
    let sql = capture_query_sql(&capture_id, &tenant.tenant_id);
    let engine = state.engine_for_tenant(&tenant).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"code": "storage_error", "message": e.to_string()}})),
        )
    })?;
    let result = engine.query.execute_query(&sql).await;

    match result {
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
        Err(e) => {
            let message = e.to_string();
            if message.contains("Table with name traces does not exist") {
                return Err((
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": {"code": "not_found", "message": "capture not found"}})),
                ));
            }
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": {"code": "storage_error", "message": message}})),
            ))
        }
    }
}

#[cfg(test)]
mod bearer_tests {
    use super::{parse_bearer, requires_runtime_auth};
    use axum::http::Method;

    #[test]
    fn parses_valid_bearer_token() {
        assert_eq!(parse_bearer("Bearer abc").as_deref(), Some("abc"));
        assert_eq!(parse_bearer("Bearer  ").as_deref(), None);
    }

    #[test]
    fn trims_and_extracts_after_prefix() {
        assert_eq!(parse_bearer("  Bearer   tok  ").as_deref(), Some("tok"));
    }

    #[test]
    fn rejects_missing_or_empty_token() {
        assert!(parse_bearer("Bearer").is_none());
        assert!(parse_bearer("Bearer ").is_none());
        assert!(parse_bearer("").is_none());
        assert!(parse_bearer("Basic x").is_none());
    }

    #[test]
    fn requires_auth_for_v1_options_preflight() {
        assert!(requires_runtime_auth(
            &Method::OPTIONS,
            "/v1/telemetry/search"
        ));
        assert!(requires_runtime_auth(&Method::POST, "/v1/telemetry/search"));
    }

    #[test]
    fn exempts_only_documented_non_v1_routes() {
        for path in ["/health", "/ready", "/openapi.json", "/swagger"] {
            assert!(
                !requires_runtime_auth(&Method::GET, path),
                "{path} must remain unauthenticated"
            );
        }

        for path in ["/v1/traces", "/v1/sessions", "/v1/inject", "/v1/meta"] {
            assert!(
                requires_runtime_auth(&Method::GET, path),
                "{path} must require auth"
            );
        }

        assert!(
            !requires_runtime_auth(&Method::POST, "/v1/tenants"),
            "POST /v1/tenants uses admin Bearer validated in-handler, not tenant middleware"
        );
    }
}
