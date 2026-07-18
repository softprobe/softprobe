//! Single-scope SQLite promotion lifecycle (local/dev; no Postgres/Redis infra).

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::{from_fn, Next};
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::runtime_api::{runtime_control_routes, runtime_post_v1_traces};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;

const MANIFEST_V1: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: service_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: service.name
"#;

const MANIFEST_V2: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: service_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: service.name
  - name: division_name
    type: string
    nullable: true
    source:
      from: attribute
      key: division.name
"#;

const BUSINESS_V1: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#;

const BUSINESS_V1_INCOMPATIBLE: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: total_cents
    type: string
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#;

const BUSINESS_V1_ADDITIVE: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
  - name: coupon_code
    type: string
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.coupon
"#;

struct SqlitePromoFixture {
    _temp: TempDir,
    router: Router,
    metadata_path: String,
    data_path: String,
}

async fn inject_tenant(
    mut req: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    req.extensions_mut().insert(TenantInfo {
        tenant_id: "local-sqlite-tenant".to_string(),
        bucket_name: String::new(),
        dataset_id: String::new(),
    });
    next.run(req).await
}

async fn build_router_from_config(config: Config) -> (Router, softprobe_runtime::api::AppState) {
    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let query_engine = softprobe_runtime::query::create_query_engine(
        &config,
        Arc::new(pipeline.storage.clone()),
    )
    .await
    .expect("query engine");
    let config = Arc::new(config);
    let (router, state) = softprobe_runtime::api::create_router(
        config,
        pipeline.storage,
        query_engine,
        post(runtime_post_v1_traces),
        None,
        None,
    )
    .await
    .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn(inject_tenant));
    (router, state)
}

async fn setup_sqlite_promo() -> SqlitePromoFixture {
    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let (router, _state) = build_router_from_config(config).await;
    SqlitePromoFixture {
        _temp: temp,
        router,
        metadata_path,
        data_path,
    }
}

async fn apply_manifest(router: &Router, manifest_yaml: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/promotions/apply")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({ "manifestYaml": manifest_yaml })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .expect("apply");
    let status = resp.status();
    let body = resp.into_body().collect().await.expect("body").to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap_or_else(|_| {
        json!({ "raw": String::from_utf8_lossy(&body) })
    });
    (status, json)
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

async fn ingest_otlp(
    router: &Router,
    session_id: &str,
    service_name: &str,
    division_name: Option<&str>,
) -> StatusCode {
    let mut span_attrs = vec![string_kv("sp.session.id", session_id)];
    if let Some(division) = division_name {
        span_attrs.push(string_kv("division.name", division));
    }
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", service_name),
                    string_kv("sp.app.id", "sqlite-promo"),
                ],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "sqlite.promo".to_string(),
                    version: "1.0.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: vec![
                        0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                        0x99, 0xaa, 0xbb, 0xcc,
                    ],
                    span_id: vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
                    name: "sqlite_promo_span".to_string(),
                    kind: span::SpanKind::Internal as i32,
                    start_time_unix_nano: 1_640_995_200_000_000_000,
                    end_time_unix_nano: 1_640_995_260_000_000_000,
                    attributes: span_attrs,
                    status: Some(Status {
                        code: 1,
                        message: String::new(),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let mut body = Vec::new();
    req.encode(&mut body).expect("encode");
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("ingest");
    resp.status()
}

fn attach(metadata_path: &str, data_path: &str) -> duckdb::Connection {
    let conn = duckdb::Connection::open_in_memory().expect("duckdb");
    conn.execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
        .expect("extensions");
    let attach = format!(
        "ATTACH 'ducklake:sqlite:{}' AS softprobe (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, DATA_INLINING_ROW_LIMIT 0);",
        metadata_path.replace('\'', "''"),
        data_path.replace('\'', "''"),
    );
    conn.execute_batch(&attach).expect("attach");
    conn
}

fn query_promoted(
    metadata_path: &str,
    data_path: &str,
    session_id: &str,
    columns: &[&str],
) -> Vec<Option<String>> {
    let conn = attach(metadata_path, data_path);
    let cols = columns.join(", ");
    let sql = format!(
        "SELECT {cols} FROM softprobe.traces WHERE session_id = '{}'",
        session_id.replace('\'', "''")
    );
    conn.query_row(&sql, [], |row| {
        let mut out = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            out.push(row.get::<_, Option<String>>(i)?);
        }
        Ok(out)
    })
    .expect("query")
}

fn active_telemetry_count(metadata_path: &str, data_path: &str) -> i64 {
    let conn = attach(metadata_path, data_path);
    conn.query_row(
        "SELECT count(*) FROM softprobe.promotion_specs \
WHERE status = 'active' AND target_kind = 'telemetry_columns'",
        [],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

fn inactive_telemetry_count(metadata_path: &str, data_path: &str) -> i64 {
    let conn = attach(metadata_path, data_path);
    conn.query_row(
        "SELECT count(*) FROM softprobe.promotion_specs \
WHERE status = 'inactive' AND target_kind = 'telemetry_columns'",
        [],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

fn column_exists(metadata_path: &str, data_path: &str, table: &str, column: &str) -> bool {
    let conn = attach(metadata_path, data_path);
    let sql = format!(
        "SELECT count(*) FROM information_schema.columns \
WHERE table_catalog = 'softprobe' AND table_name = '{table}' AND column_name = '{column}'"
    );
    let n: i64 = conn.query_row(&sql, [], |row| row.get(0)).unwrap_or(0);
    n > 0
}

#[tokio::test]
async fn sqlite_apply_then_otlp_ingest_populates_promoted_columns() {
    let fx = setup_sqlite_promo().await;
    let (status, body) = apply_manifest(&fx.router, MANIFEST_V1).await;
    assert_eq!(status, StatusCode::OK, "apply failed: {body}");
    assert_eq!(body["applied"], true);

    let session_id = "sess-sqlite-apply";
    let ingest = ingest_otlp(&fx.router, session_id, "checkout-api", None).await;
    assert_eq!(ingest, StatusCode::OK);

    let values = query_promoted(
        &fx.metadata_path,
        &fx.data_path,
        session_id,
        &["service_name"],
    );
    assert_eq!(values[0].as_deref(), Some("checkout-api"));
    assert_eq!(
        active_telemetry_count(&fx.metadata_path, &fx.data_path),
        1
    );
}

#[tokio::test]
async fn sqlite_apply_v1_then_v2_and_idempotent_reapply() {
    let fx = setup_sqlite_promo().await;
    let (s1, b1) = apply_manifest(&fx.router, MANIFEST_V1).await;
    assert_eq!(s1, StatusCode::OK, "{b1}");
    let (s2, b2) = apply_manifest(&fx.router, MANIFEST_V2).await;
    assert_eq!(s2, StatusCode::OK, "{b2}");
    assert_eq!(
        active_telemetry_count(&fx.metadata_path, &fx.data_path),
        1
    );
    assert_eq!(
        inactive_telemetry_count(&fx.metadata_path, &fx.data_path),
        1
    );

    let session_id = "sess-sqlite-v2";
    assert_eq!(
        ingest_otlp(&fx.router, session_id, "checkout-api", Some("payments")).await,
        StatusCode::OK
    );
    let values = query_promoted(
        &fx.metadata_path,
        &fx.data_path,
        session_id,
        &["service_name", "division_name"],
    );
    assert_eq!(values[0].as_deref(), Some("checkout-api"));
    assert_eq!(values[1].as_deref(), Some("payments"));

    let (s3, b3) = apply_manifest(&fx.router, MANIFEST_V2).await;
    assert_eq!(s3, StatusCode::OK, "idempotent re-apply: {b3}");
    assert_eq!(
        active_telemetry_count(&fx.metadata_path, &fx.data_path),
        1
    );
}

#[tokio::test]
async fn sqlite_shrunken_manifest_keeps_ingest_with_stale_null() {
    let fx = setup_sqlite_promo().await;
    assert_eq!(
        apply_manifest(&fx.router, MANIFEST_V2).await.0,
        StatusCode::OK
    );
    assert_eq!(
        ingest_otlp(&fx.router, "sess-wide", "checkout-api", Some("payments")).await,
        StatusCode::OK
    );
    assert_eq!(
        apply_manifest(&fx.router, MANIFEST_V1).await.0,
        StatusCode::OK
    );
    assert_eq!(
        ingest_otlp(&fx.router, "sess-narrow", "checkout-api", Some("payments")).await,
        StatusCode::OK
    );
    let values = query_promoted(
        &fx.metadata_path,
        &fx.data_path,
        "sess-narrow",
        &["service_name", "division_name"],
    );
    assert_eq!(values[0].as_deref(), Some("checkout-api"));
    assert_eq!(values[1], None);
}

#[tokio::test]
async fn sqlite_business_compat_and_additive() {
    let fx = setup_sqlite_promo().await;
    let (s1, b1) = apply_manifest(&fx.router, BUSINESS_V1).await;
    assert_eq!(s1, StatusCode::OK, "{b1}");
    assert!(column_exists(
        &fx.metadata_path,
        &fx.data_path,
        "checkout_orders_v1",
        "total_cents"
    ));

    let (bad, body) = apply_manifest(&fx.router, BUSINESS_V1_INCOMPATIBLE).await;
    assert_eq!(bad, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "business_column_type_changed");

    let (add, add_body) = apply_manifest(&fx.router, BUSINESS_V1_ADDITIVE).await;
    assert_eq!(add, StatusCode::OK, "{add_body}");
    assert!(column_exists(
        &fx.metadata_path,
        &fx.data_path,
        "checkout_orders_v1",
        "coupon_code"
    ));
}

#[tokio::test]
async fn sqlite_promotion_specs_persist_across_writer_rebuild() {
    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let duck_dir = PathBuf::from(metadata_path.clone())
        .parent()
        .expect("parent")
        .to_path_buf();

    {
        let (router, _state) = build_router_from_config(config).await;
        let (status, body) = apply_manifest(&router, MANIFEST_V1).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(active_telemetry_count(&metadata_path, &data_path), 1);
    }

    // Rebuild against the same on-disk catalog paths.
    let mut config2 = Config::default();
    config2.maintenance.enabled = false;
    config2.maintenance.metadata_enabled = false;
    config2.query.cache_dir = Some(temp.path().join("cache2").to_string_lossy().into());
    config2.ducklake.catalog_type = "sqlite".to_string();
    config2.ducklake.metadata_path = duck_dir
        .join("metadata.sqlite")
        .to_string_lossy()
        .into();
    config2.ducklake.data_path = duck_dir.join("data").to_string_lossy().into_owned() + "/";
    let (router2, _state2) = build_router_from_config(config2).await;
    assert_eq!(
        active_telemetry_count(&metadata_path, &data_path),
        1,
        "specs must survive writer rebuild"
    );
    let session_id = "sess-persist";
    assert_eq!(
        ingest_otlp(&router2, session_id, "checkout-api", None).await,
        StatusCode::OK
    );
    let values = query_promoted(&metadata_path, &data_path, session_id, &["service_name"]);
    assert_eq!(values[0].as_deref(), Some("checkout-api"));
}
