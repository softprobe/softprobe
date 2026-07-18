//! HTTP apply → DuckLake schema → OTLP ingest → query promotion lifecycle (issue #5).

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use softprobe_runtime::api::{create_router, ControlPlaneRuntime};
use softprobe_runtime::authn::Resolver;
use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::runtime_api::{runtime_auth_middleware, runtime_control_routes, runtime_post_v1_traces};
use softprobe_runtime::runtime_engine::{DuckLakeScopeResolver, ScopeProvisioningRequest};
use softprobe_runtime::session_redis::RedisStore;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

struct PromoFixture {
    _temp: TempDir,
    _mock: MockServer,
    router: Router,
    metadata_path: String,
    data_path: String,
    metadata_schema: String,
    api_key: String,
}

async fn setup_promo_fixture() -> PromoFixture {
    let mock = MockServer::start().await;
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let short = &suffix[..8.min(suffix.len())];

    let mut config = Config::default();
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().into());
    config.ducklake.catalog_type = "postgres".to_string();
    config.ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    config.ducklake.catalog_alias = "softprobe".to_string();
    config.ducklake.metadata_schema = format!("sp_promo_reg_{short}");
    let data_path = temp.path().join("tenant-data").to_string_lossy().to_string();
    config.ducklake.data_path = data_path.clone();
    config.ducklake.data_inlining_row_limit = Some(0);

    let tenant_id = format!("tenant-promo-{short}");
    let metadata_schema = format!("sp_promo_data_{short}");

    let resolver = DuckLakeScopeResolver::connect(&config)
        .await
        .expect("resolver")
        .expect("postgres resolver");
    resolver
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_id.clone(),
            metadata_schema: metadata_schema.clone(),
            data_path: data_path.clone(),
        })
        .await
        .expect("provision tenant");

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "success": true,
            "data": {
                "tenantId": tenant_id.clone(),
                "resources": []
            }
        })))
        .mount(&mock)
        .await;

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let query_engine =
        softprobe_runtime::query::create_query_engine(&config, Arc::new(pipeline.storage.clone()))
            .await
            .expect("query engine");

    let redis = RedisStore::connect_host_port("127.0.0.1", 6379, None, Duration::from_secs(3600))
        .await
        .expect("redis on 127.0.0.1:6379 (make setup-local)");

    let control = ControlPlaneRuntime {
        resolver: Resolver::new(format!("{}/", mock.uri()), Duration::from_secs(60)),
        session_store: Arc::new(tokio::sync::Mutex::new(redis)),
    };

    let metadata_path = config.ducklake.metadata_path.clone();
    let config = Arc::new(config);
    let (router, state) = create_router(
        config,
        pipeline.storage,
        query_engine,
        post(runtime_post_v1_traces),
        Some(control),
        None,
    )
    .await
    .expect("router");

    let router = router
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn_with_state(state.clone(), runtime_auth_middleware));

    PromoFixture {
        _temp: temp,
        _mock: mock,
        router,
        metadata_path,
        data_path,
        metadata_schema,
        api_key: "promo-e2e-api-key".to_string(),
    }
}

async fn apply_manifest(router: &Router, api_key: &str, manifest_yaml: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/promotions/apply")
                .header(header::AUTHORIZATION, format!("Bearer {api_key}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({ "manifestYaml": manifest_yaml })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .expect("apply request");
    let status = resp.status();
    let body = resp.into_body().collect().await.expect("body").to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap_or_else(|_| {
        json!({ "raw": String::from_utf8_lossy(&body) })
    });
    (status, json)
}

async fn ingest_otlp_trace(
    router: &Router,
    api_key: &str,
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
                    string_kv("sp.app.id", "promo-e2e"),
                ],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "promo.e2e".to_string(),
                    version: "1.0.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: vec![
                        0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                        0x99, 0xaa, 0xbb, 0xcc,
                    ],
                    span_id: vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
                    name: "promo_e2e_span".to_string(),
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
    req.encode(&mut body).expect("encode otlp");
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::AUTHORIZATION, format!("Bearer {api_key}"))
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("ingest request");
    resp.status()
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn attach_and_query_promoted(
    metadata_path: &str,
    data_path: &str,
    metadata_schema: &str,
    session_id: &str,
    columns: &[&str],
) -> Vec<Option<String>> {
    let conn = duckdb::Connection::open_in_memory().expect("duckdb");
    conn.execute_batch("INSTALL ducklake; INSTALL postgres; LOAD postgres;")
        .expect("extensions");
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:postgres:{}' AS softprobe (DATA_PATH '{}', METADATA_SCHEMA '{}', META_SCHEMA '{}', DATA_INLINING_ROW_LIMIT 0);",
        metadata_path.replace('\'', "''"),
        data_path.replace('\'', "''"),
        metadata_schema.replace('\'', "''"),
        metadata_schema.replace('\'', "''"),
    ))
    .expect("attach");
    let cols = columns.join(", ");
    let sql = format!(
        r#"SELECT {cols} FROM softprobe.{metadata_schema}.traces WHERE session_id = '{}'"#,
        session_id.replace('\'', "''")
    );
    conn.query_row(&sql, [], |row| {
        let mut out = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            out.push(row.get::<_, Option<String>>(i)?);
        }
        Ok(out)
    })
    .expect("query promoted columns")
}

fn ducklake_column_exists(
    metadata_path: &str,
    data_path: &str,
    metadata_schema: &str,
    column: &str,
) -> bool {
    let conn = duckdb::Connection::open_in_memory().expect("duckdb");
    conn.execute_batch("INSTALL ducklake; INSTALL postgres; LOAD postgres;")
        .expect("extensions");
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:postgres:{}' AS softprobe (DATA_PATH '{}', METADATA_SCHEMA '{}', META_SCHEMA '{}', DATA_INLINING_ROW_LIMIT 0);",
        metadata_path.replace('\'', "''"),
        data_path.replace('\'', "''"),
        metadata_schema.replace('\'', "''"),
        metadata_schema.replace('\'', "''"),
    ))
    .expect("attach");
    let sql = format!(
        r#"SELECT count(*) FROM information_schema.columns
WHERE table_catalog = 'softprobe'
  AND table_schema = '{metadata_schema}'
  AND table_name = 'traces'
  AND column_name = '{column}'"#
    );
    let n: i64 = conn
        .query_row(&sql, [], |row| row.get(0))
        .unwrap_or(0);
    n > 0
}

async fn active_telemetry_spec_count(schema: &str) -> i64 {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
        NoTls,
    )
    .await
    .expect("postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            &format!(
                r#"SELECT count(*)::bigint FROM "{}".promotion_specs
WHERE status = 'active' AND target_kind = 'telemetry_columns'"#,
                schema
            ),
            &[],
        )
        .await
        .expect("count active");
    row.get(0)
}

async fn inactive_telemetry_spec_count(schema: &str) -> i64 {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
        NoTls,
    )
    .await
    .expect("postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            &format!(
                r#"SELECT count(*)::bigint FROM "{}".promotion_specs
WHERE status = 'inactive' AND target_kind = 'telemetry_columns'"#,
                schema
            ),
            &[],
        )
        .await
        .expect("count inactive");
    row.get(0)
}

#[tokio::test]
async fn http_apply_then_otlp_ingest_populates_promoted_columns() {
    let fx = setup_promo_fixture().await;

    let (status, body) = apply_manifest(&fx.router, &fx.api_key, MANIFEST_V1).await;
    assert_eq!(status, StatusCode::OK, "apply failed: {body}");
    assert_eq!(body["applied"], true);
    assert!(
        ducklake_column_exists(
            &fx.metadata_path,
            &fx.data_path,
            &fx.metadata_schema,
            "service_name"
        ),
        "service_name column must exist after apply"
    );

    let session_id = format!("sess-apply-{}", Uuid::new_v4());
    let ingest_status = ingest_otlp_trace(
        &fx.router,
        &fx.api_key,
        &session_id,
        "checkout-api",
        None,
    )
    .await;
    assert_eq!(ingest_status, StatusCode::OK);

    let values = attach_and_query_promoted(
        &fx.metadata_path,
        &fx.data_path,
        &fx.metadata_schema,
        &session_id,
        &["service_name"],
    );
    assert_eq!(values[0].as_deref(), Some("checkout-api"));
    assert_eq!(active_telemetry_spec_count(&fx.metadata_schema).await, 1);
}

#[tokio::test]
async fn apply_v1_then_v2_adds_column_and_deactivates_old_spec() {
    let fx = setup_promo_fixture().await;

    let (status1, body1) = apply_manifest(&fx.router, &fx.api_key, MANIFEST_V1).await;
    assert_eq!(status1, StatusCode::OK, "v1 apply failed: {body1}");

    let (status2, body2) = apply_manifest(&fx.router, &fx.api_key, MANIFEST_V2).await;
    assert_eq!(status2, StatusCode::OK, "v2 apply failed: {body2}");

    assert!(ducklake_column_exists(
        &fx.metadata_path,
        &fx.data_path,
        &fx.metadata_schema,
        "service_name"
    ));
    assert!(ducklake_column_exists(
        &fx.metadata_path,
        &fx.data_path,
        &fx.metadata_schema,
        "division_name"
    ));
    assert_eq!(active_telemetry_spec_count(&fx.metadata_schema).await, 1);
    assert_eq!(inactive_telemetry_spec_count(&fx.metadata_schema).await, 1);

    let session_id = format!("sess-v2-{}", Uuid::new_v4());
    let ingest_status = ingest_otlp_trace(
        &fx.router,
        &fx.api_key,
        &session_id,
        "checkout-api",
        Some("payments"),
    )
    .await;
    assert_eq!(ingest_status, StatusCode::OK);

    let values = attach_and_query_promoted(
        &fx.metadata_path,
        &fx.data_path,
        &fx.metadata_schema,
        &session_id,
        &["service_name", "division_name"],
    );
    assert_eq!(values[0].as_deref(), Some("checkout-api"));
    assert_eq!(values[1].as_deref(), Some("payments"));
}

#[tokio::test]
async fn reapplying_same_telemetry_manifest_is_idempotent() {
    let fx = setup_promo_fixture().await;

    let (status1, body1) = apply_manifest(&fx.router, &fx.api_key, MANIFEST_V1).await;
    assert_eq!(status1, StatusCode::OK, "first apply failed: {body1}");
    let (status2, body2) = apply_manifest(&fx.router, &fx.api_key, MANIFEST_V1).await;
    assert_eq!(status2, StatusCode::OK, "second apply failed: {body2}");
    assert_eq!(body2["applied"], true);
    assert_eq!(active_telemetry_spec_count(&fx.metadata_schema).await, 1);
    assert_eq!(inactive_telemetry_spec_count(&fx.metadata_schema).await, 0);
}
