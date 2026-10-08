//! Runtime control API for the configured DuckLake scope (tenant provisioning, meta,
//! DuckLake connection material, schema promotions, and catalog lookups).

use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::config::Config;
use crate::promotion::{
    business_current_view_name, business_physical_table_name, parse_promotion_manifest,
    BusinessApplyError, BusinessTableManifest, PromotionDataType, PromotionManifest,
    TelemetryColumnsManifest, TelemetryTable,
};
use crate::runtime_engine::{DuckLakeScope, ScopeProvisioningRequest};
use axum::{
    extract::{Extension, Query, Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Require `Authorization: Bearer` for `/v1/*` (except CORS `OPTIONS` preflight and
/// admin `POST /v1/tenants`), resolve tenant, store [`TenantInfo`] in extensions.
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
    // Defense in depth with outermost CorsLayer: browser CORS preflight is OPTIONS
    // without Authorization. Auth here would 401 and block SPA OTLP
    // (e.g. @softprobe/web-record → POST /v1/traces).
    if *method == Method::OPTIONS && is_authenticated_api_prefix(path) {
        return false;
    }
    if path == "/v1/tenants" && *method == Method::POST {
        return false;
    }
    is_authenticated_api_prefix(path)
}

/// Paths that require Bearer → tenant resolution (OTLP/control + compatibility stubs).
fn is_authenticated_api_prefix(path: &str) -> bool {
    path.starts_with("/v1/")
        || path.starts_with("/api/v1/")
        || path.starts_with("/loki/api/v1/")
        || path.starts_with("/api/traces")
        || path.starts_with("/api/v2/traces")
        || path.starts_with("/api/search")
}

fn admin_provision_token_matches(token: &str) -> bool {
    let Ok(want) = std::env::var("SOFTPROBE_ADMIN_API_KEY") else {
        return false;
    };
    let want = want.trim();
    !want.is_empty() && want == token.trim()
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
        .route("/v1/data/ducklake-connection", get(v1_ducklake_connection))
        .route("/v1/promotions/apply", post(v1_promotions_apply))
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
    let engine = state
        .engine_for_tenant(&tenant)
        .await
        .map_err(|err| promotion_apply_error("ducklake_scope_unavailable", err))?;
    let tables = telemetry_table_names(&spec.target.tables);
    // Writer facade serializes DDL + spec activation (Postgres advisory lock / SQLite mutex).
    engine
        .storage
        .writer
        .apply_and_record_telemetry_promotion(&engine.scope, &manifest_yaml, &spec, &tables)
        .await
        .map_err(|err| promotion_apply_error("promotion_schema_apply_failed", err))?;
    Ok(Json(json!({
        "specVersion": "softprobe.promotion.apply.v1",
        "applied": true,
        "target": {
            "kind": "telemetry_columns",
            "tables": tables
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
    let engine = state
        .engine_for_tenant(&tenant)
        .await
        .map_err(|err| promotion_apply_error("ducklake_scope_unavailable", err))?;
    // Writer facade dispatches: Postgres uses pg advisory lock; SQLite uses a process-global mutex.
    // Both serialize load -> compatibility check -> DDL -> record.
    engine
        .storage
        .writer
        .apply_business_promotion_guarded(&engine.scope, &manifest_yaml, &spec)
        .await
        .map_err(|err| match err {
            BusinessApplyError::Incompatible(e) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": {
                        "code": e.code(),
                        "message": e.to_string(),
                        "path": e.path()
                    }
                })),
            ),
            BusinessApplyError::Other(e) => {
                promotion_apply_error("promotion_schema_apply_failed", e)
            }
        })?;
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
            TelemetryTable::Metrics => "metric_samples",
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
    fn skips_auth_for_v1_options_preflight() {
        assert!(!requires_runtime_auth(
            &Method::OPTIONS,
            "/v1/telemetry/search"
        ));
        assert!(!requires_runtime_auth(&Method::OPTIONS, "/v1/traces"));
        assert!(!requires_runtime_auth(&Method::OPTIONS, "/api/v1/query"));
        assert!(!requires_runtime_auth(
            &Method::OPTIONS,
            "/loki/api/v1/labels"
        ));
        assert!(requires_runtime_auth(&Method::POST, "/v1/telemetry/search"));
        assert!(requires_runtime_auth(&Method::POST, "/v1/traces"));
        assert!(requires_runtime_auth(&Method::GET, "/api/v1/query"));
        assert!(requires_runtime_auth(&Method::GET, "/loki/api/v1/query"));
        assert!(requires_runtime_auth(&Method::GET, "/api/traces/abc"));
        assert!(requires_runtime_auth(&Method::GET, "/api/search"));
    }

    #[test]
    fn exempts_only_documented_non_v1_routes() {
        for path in ["/health", "/ready", "/openapi.json", "/swagger"] {
            assert!(
                !requires_runtime_auth(&Method::GET, path),
                "{path} must remain unauthenticated"
            );
        }

        for path in [
            "/v1/traces",
            "/v1/meta",
            "/v1/promotions/apply",
            "/api/v1/labels",
            "/loki/api/v1/labels",
            "/api/search/tags",
        ] {
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
