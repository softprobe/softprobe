//! Runtime control API + tenant-scoped OTLP trace ingest.

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
    BusinessTableManifest, PromotionDataType, PromotionManifest, TelemetryColumnsManifest,
    TelemetryTable,
};
use crate::tenant_ducklake::TenantDuckLakeScope;
use axum::{
    body::Bytes,
    extract::{Extension, Path, Query, Request, State},
    http::{header, Method, StatusCode},
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
use uuid::Uuid;

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
        .control_plane
        .as_ref()
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
    path.starts_with("/v1/") && method != Method::OPTIONS
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
    gcs_hmac_access_key_id: String,
    gcs_hmac_secret: String,
    schema_version: String,
}

fn ducklake_connection_material(
    tenant: &TenantInfo,
    scope: &TenantDuckLakeScope,
) -> Result<DuckLakeConnectionMaterial, String> {
    let config = Config::load().map_err(|e| format!("runtime config load failed: {e}"))?;
    let ducklake = config.ducklake_or_default();
    let ducklake_pg_uri = postgres_ducklake_metadata_path(&ducklake);
    // Tenant DuckLake schema is control-plane state, not YAML config. The resolver owns the
    // Postgres mapping from tenant_id to DuckLake schema and data path.
    let ducklake_data_path = scope.data_path.clone();
    let ducklake_metadata_schema = scope.metadata_schema.clone();
    let gcs_hmac_access_key_id = config.s3.access_key_id.clone();
    let gcs_hmac_secret = config.s3.secret_access_key.clone();

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
    if path_requires_hmac(&ducklake_data_path)
        && (gcs_hmac_access_key_id
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
            || gcs_hmac_secret.as_deref().unwrap_or("").trim().is_empty())
    {
        return Err(
            "GCS/S3 DuckLake data path requires config.s3.access_key_id and config.s3.secret_access_key"
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
        gcs_hmac_access_key_id: gcs_hmac_access_key_id.unwrap_or_default(),
        gcs_hmac_secret: gcs_hmac_secret.unwrap_or_default(),
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
        let mut ducklake = config.ducklake_or_default();
        ducklake.catalog_type = "postgres".to_string();
        ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();
        ducklake.data_path = "./warehouse/ducklake/data/".to_string();
        ducklake.metadata_schema = "tenant_meta".to_string();
        config.ducklake = Some(ducklake);
        config.s3.access_key_id = None;
        config.s3.secret_access_key = None;
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
        let scope = TenantDuckLakeScope {
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
        assert_eq!(material.schema_version, "1");

        std::env::remove_var("CONFIG_FILE");
    }

    #[test]
    fn ducklake_connection_material_ignores_env_overrides_and_uses_config() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("runtime.yaml");
        let mut config = Config::default();
        let mut ducklake = config.ducklake_or_default();
        ducklake.catalog_type = "postgres".to_string();
        ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();
        ducklake.data_path = "gs://bucket/ducklake/data/".to_string();
        ducklake.metadata_schema = "config_schema".to_string();
        config.ducklake = Some(ducklake);
        config.s3.access_key_id = Some("config-access-id".to_string());
        config.s3.secret_access_key = Some("config-secret".to_string());
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
        let scope = TenantDuckLakeScope {
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
        assert_eq!(material.gcs_hmac_access_key_id, "config-access-id");
        assert_eq!(material.gcs_hmac_secret, "config-secret");

        std::env::remove_var("CONFIG_FILE");
        std::env::remove_var("DUCKLAKE_PG_URI");
        std::env::remove_var("DUCKLAKE_DATA_PATH");
        std::env::remove_var("DUCKLAKE_METADATA_SCHEMA");
        std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCS_HMAC_SECRET");
    }

    #[test]
    fn ducklake_connection_material_requires_hmac_for_gcs_path() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("runtime.yaml");
        let mut config = Config::default();
        let mut ducklake = config.ducklake_or_default();
        ducklake.catalog_type = "postgres".to_string();
        ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();
        ducklake.data_path = "gs://bucket/ducklake/data/".to_string();
        config.ducklake = Some(ducklake);
        config.s3.access_key_id = None;
        config.s3.secret_access_key = None;
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
        let scope = TenantDuckLakeScope {
            metadata_schema: "tenant_tenant_123".to_string(),
            data_path: "gs://bucket/ducklake/data/".to_string(),
        };

        let err =
            ducklake_connection_material(&tenant, &scope).expect_err("missing hmac should fail");
        assert!(err.contains("config.s3.access_key_id"));

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
    let Some(tenant_ducklake) = state
        .control_plane
        .as_ref()
        .and_then(|cp| cp.tenant_ducklake.as_ref())
    else {
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
        .resolve_or_create(&tenant.tenant_id)
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
    let Some(tenant_ducklake) = state
        .control_plane
        .as_ref()
        .and_then(|cp| cp.tenant_ducklake.as_ref())
    else {
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
        .resolve_or_create(&tenant.tenant_id)
        .await
        .map_err(|err| promotion_apply_error("ducklake_scope_unavailable", err))?;
    state
        .storage
        .writer
        .apply_telemetry_column_promotion(&scope, &spec)
        .await
        .map_err(|err| promotion_apply_error("promotion_schema_apply_failed", err))?;
    tenant_ducklake
        .record_active_telemetry_promotion_spec(
            &scope,
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
    let Some(tenant_ducklake) = state
        .control_plane
        .as_ref()
        .and_then(|cp| cp.tenant_ducklake.as_ref())
    else {
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
        .resolve_or_create(&tenant.tenant_id)
        .await
        .map_err(|err| promotion_apply_error("ducklake_scope_unavailable", err))?;
    state
        .storage
        .writer
        .apply_business_table_promotion(&scope, &spec)
        .await
        .map_err(|err| promotion_apply_error("promotion_schema_apply_failed", err))?;
    tenant_ducklake
        .record_active_business_promotion_spec(&scope, &manifest_yaml, &spec.target.table)
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let mut store = control_plane.session_store.lock().await;
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let mut store = control_plane.session_store.lock().await;
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let session = {
        let mut store = control_plane.session_store.lock().await;
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
        let mut store = control_plane.session_store.lock().await;
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let mut store = control_plane.session_store.lock().await;
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let mut store = control_plane.session_store.lock().await;
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let mut store = control_plane.session_store.lock().await;
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let mut store = control_plane.session_store.lock().await;
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let mut store = control_plane.session_store.lock().await;
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let mut store = control_plane.session_store.lock().await;
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
    let annotated = annotate_export_request(req, &capture_id, &tenant.tenant_id);
    let body_size = annotated.encoded_len();
    process_traces(state, annotated, body_size).await?;
    Ok(())
}

pub async fn runtime_post_v1_traces(
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
    let control_plane = state
        .control_plane
        .as_ref()
        .expect("runtime control routes require control-plane state");
    let payload =
        normalize_otlp_body(&body).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let lookup =
        parse_inject_lookup(&payload).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    if lookup.session_id.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "missing session id".into()));
    }
    let mut store = control_plane.session_store.lock().await;
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
    let result = if let Some(tenant_ducklake) = state
        .control_plane
        .as_ref()
        .and_then(|cp| cp.tenant_ducklake.as_ref())
    {
        let scope = tenant_ducklake
            .resolve_or_create(&tenant.tenant_id)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": {"code": "storage_error", "message": e.to_string()}})),
                )
            })?;
        state
            .query_engine
            .execute_query_in_ducklake_scope(&sql, &scope)
            .await
    } else {
        state.query_engine.execute_query(&sql).await
    };

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
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"code": "storage_error", "message": e.to_string()}})),
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
        assert!(requires_runtime_auth(&Method::POST, "/v1/telemetry/search"));
    }
}
