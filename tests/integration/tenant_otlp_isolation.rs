//! Tenant-scoped OTLP ingest isolation (**I2**) and gRPC OTLP parity (**G1** / **G2**) per
//! [`docs/tenant-isolation-implementation-plan.md`](../../../docs/tenant-isolation-implementation-plan.md) §6.

use axum::routing::post;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{span, Span};
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::{create_router, ControlPlaneRuntime};
use softprobe_runtime::authn::{Resolver, TenantInfo};
use softprobe_runtime::config::Config;
use softprobe_runtime::grpc_otlp::GrpcTraceService;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::models::Span as ModelSpan;
use softprobe_runtime::runtime_api::runtime_export_trace_request;
use softprobe_runtime::session_redis::RedisStore;
use softprobe_runtime::runtime_engine::{DuckLakeScopeResolver, ScopeProvisioningRequest};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tonic::Request;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn postgres_registry_config(temp: &TempDir, registry_schema: String) -> Config {
    let mut config = Config::default();
    config.compaction.enabled = false;
    config.compaction.metadata_maintenance_enabled = false;
    config.ingest_engine.cache_dir = Some(temp.path().join("cache").to_string_lossy().into());
    config.ingest_engine.wal_dir = Some(temp.path().join("wal").to_string_lossy().into());
    config.ingest_engine.optimizer_interval_seconds = 3600;

    let mut ducklake = config.ducklake_or_default();
    ducklake.catalog_type = "postgres".to_string();
    ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    ducklake.catalog_alias = "softprobe".to_string();
    ducklake.metadata_schema = registry_schema;
    ducklake.data_path = temp.path().join("default_data").to_string_lossy().into();
    ducklake.data_inlining_row_limit = Some(0);
    config.ducklake = Some(ducklake);
    config
}

fn isolation_span(tenant_id: &str, session_id: &str, trace_id: &str) -> ModelSpan {
    ModelSpan {
        session_id: session_id.to_string(),
        trace_id: trace_id.to_string(),
        span_id: format!("span-{trace_id}"),
        parent_span_id: None,
        app_id: "it-app".to_string(),
        organization_id: None,
        tenant_id: Some(tenant_id.to_string()),
        message_type: "op".to_string(),
        span_kind: Some("SPAN_KIND_INTERNAL".to_string()),
        timestamp: chrono::Utc::now(),
        end_timestamp: None,
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        status_code: None,
        status_message: None,
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
    }
}

fn trace_count_for_session(
    conn: &duckdb::Connection,
    metadata_path: &str,
    data_path: &str,
    meta_schema: &str,
    session_id: &str,
) -> i64 {
    // File-backed tenant DATA_PATH: keep extension load minimal (httpfs + path-style S3 breaks attach).
    // s3:// tenant paths (gRPC parity test): match tests/fixtures/legacy_verify_session.sql so MinIO
    // does not return HTTP 301 (virtual-host style) on parquet reads.
    let needs_object_store = data_path.starts_with("s3://") || data_path.starts_with("gs://");
    if needs_object_store {
        conn.execute_batch(
            "INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;",
        )
        .expect("ducklake extensions");
        conn.execute_batch(
            "SET s3_endpoint = 'localhost:9000';
             SET s3_url_style = 'path';
             SET s3_use_ssl = false;
             SET s3_access_key_id = 'minioadmin';
             SET s3_secret_access_key = 'minioadmin';
             SET s3_region = 'us-east-1';",
        )
        .expect("minio httpfs for ducklake data_path");
    } else {
        conn.execute_batch("INSTALL ducklake; INSTALL postgres; LOAD postgres;")
            .expect("ducklake extensions");
    }
    let attach = format!(
        "ATTACH 'ducklake:postgres:{}' AS q (DATA_PATH '{}', METADATA_SCHEMA '{}', META_SCHEMA '{}', DATA_INLINING_ROW_LIMIT 0);",
        metadata_path.replace('\'', "''"),
        data_path.replace('\'', "''"),
        meta_schema.replace('\'', "''"),
        meta_schema.replace('\'', "''"),
    );
    conn.execute_batch(&attach).expect("attach");
    let sql = format!(
        "SELECT count(*) FROM q.{}.traces WHERE session_id = '{}';",
        meta_schema,
        session_id.replace('\'', "''")
    );
    conn.query_row(&sql, [], |row| row.get(0))
        .expect("count traces")
}

#[tokio::test]
async fn tenant_scoped_ingest_is_isolated_between_two_registry_tenants() {
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let registry_schema = format!("softprobe_ingest_iso_{suffix}");
    let config = postgres_registry_config(&temp, registry_schema.clone());

    let tenant_a = format!("tenant_ingest_a_{suffix}");
    let tenant_b = format!("tenant_ingest_b_{suffix}");
    let meta_a = format!("softprobe_ingest_a_data_{suffix}");
    let meta_b = format!("softprobe_ingest_b_data_{suffix}");
    let path_a = temp.path().join("data_a").to_string_lossy().to_string();
    let path_b = temp.path().join("data_b").to_string_lossy().to_string();

    let resolver = DuckLakeScopeResolver::connect(&config)
        .await
        .expect("connect resolver")
        .expect("postgres resolver");

    resolver
        .provision_scope(ScopeProvisioningRequest {
            tenant_id: tenant_a.clone(),
            metadata_schema: meta_a.clone(),
            data_path: path_a.clone(),
        })
        .await
        .expect("provision A");
    resolver
        .provision_scope(ScopeProvisioningRequest {
            tenant_id: tenant_b.clone(),
            metadata_schema: meta_b.clone(),
            data_path: path_b.clone(),
        })
        .await
        .expect("provision B");

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let session_id = format!("sess-iso-{suffix}");
    let trace_id = format!("trace-iso-{suffix}");
    // Provision does not create telemetry Iceberg tables; a tenant with no ingest has no `traces`
    // table yet. Materialize B's table with a decoy session so we can COUNT tenant A's session_id.
    pipeline
        .write_span_batches(vec![vec![isolation_span(
            &tenant_b,
            &format!("sess-bootstrap-{suffix}"),
            &format!("trace-bootstrap-{suffix}"),
        )]])
        .await
        .expect("bootstrap traces table for tenant B");
    pipeline
        .write_span_batches(vec![vec![isolation_span(
            &tenant_a,
            &session_id,
            &trace_id,
        )]])
        .await
        .expect("write spans for tenant A");

    let metadata_path = config.ducklake.as_ref().unwrap().metadata_path.clone();

    let conn = duckdb::Connection::open_in_memory().expect("duckdb");
    let n_b = trace_count_for_session(&conn, &metadata_path, &path_b, &meta_b, &session_id);
    assert_eq!(
        n_b, 0,
        "tenant B DuckLake must not contain tenant A's session"
    );

    let conn2 = duckdb::Connection::open_in_memory().expect("duckdb");
    let n_a = trace_count_for_session(&conn2, &metadata_path, &path_a, &meta_a, &session_id);
    assert_eq!(n_a, 1, "tenant A scope must contain ingested span");
}

fn otlp_export_with_session(session: &str) -> ExportTraceServiceRequest {
    let span = Span {
        trace_id: vec![
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ],
        span_id: vec![0x21, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
        parent_span_id: vec![],
        name: "grpc_otlp_export".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: 1_640_995_200_000_000_000,
        end_time_unix_nano: 1_640_995_260_000_000_000,
        attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
            key: "sp.session.id".to_string(),
            value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        session.to_string(),
                    ),
                ),
            }),
        }],
        events: vec![],
        status: Some(opentelemetry_proto::tonic::trace::v1::Status {
            code: 1,
            message: String::new(),
        }),
        ..Default::default()
    };
    let scope = opentelemetry_proto::tonic::trace::v1::ScopeSpans {
        scope: Some(
            opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                name: "softprobe.grpc_it".to_string(),
                version: "1.0.0".to_string(),
                ..Default::default()
            },
        ),
        spans: vec![span],
        schema_url: String::new(),
    };
    let resource = opentelemetry_proto::tonic::resource::v1::Resource {
        attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
            key: "sp.app.id".to_string(),
            value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "grpc_it_app".to_string(),
                    ),
                ),
            }),
        }],
        dropped_attributes_count: 0,
    };
    ExportTraceServiceRequest {
        resource_spans: vec![opentelemetry_proto::tonic::trace::v1::ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![scope],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn grpc_otlp_and_http_export_share_bearer_resolved_tenant_ducklake_scope() {
    let mock = MockServer::start().await;
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let registry_schema = format!("softprobe_grpc_it_{suffix}");
    let mut config = postgres_registry_config(&temp, registry_schema.clone());

    std::env::set_var("S3_ENDPOINT", "http://localhost:9000");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");
    config.s3.endpoint = Some("http://localhost:9000".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();

    let tenant_id = format!("tenant_grpc_it_{suffix}");
    let tenant_schema = format!("softprobe_grpc_it_data_{suffix}");
    let tenant_data_path = format!("s3://warehouse/grpc_it/{}/", suffix);

    let resolver_reg = DuckLakeScopeResolver::connect(&config)
        .await
        .expect("resolver")
        .expect("postgres resolver");
    resolver_reg
        .provision_scope(ScopeProvisioningRequest {
            tenant_id: tenant_id.clone(),
            metadata_schema: tenant_schema.clone(),
            data_path: tenant_data_path.clone(),
        })
        .await
        .expect("provision tenant");

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
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
        .expect("redis on 127.0.0.1:6379 (e2e compose publishes this port for integration-e2e)");

    let control = ControlPlaneRuntime {
        resolver: Resolver::new(format!("{}/", mock.uri()), Duration::from_secs(60)),
        session_store: Arc::new(tokio::sync::Mutex::new(redis)),
    };

    let config = std::sync::Arc::new(config);
    let (_router, state) = create_router(
        config.clone(),
        pipeline.storage.clone(),
        query_engine,
        Some(pipeline.storage.span_buffer.clone()),
        Some(pipeline.storage.log_buffer.clone()),
        Some(pipeline.storage.metric_buffer.clone()),
        post(ingest_traces),
        Some(control),
        None,
    )
    .await
    .expect("router");

    let api_key = "grpc-integration-test-key";
    let svc = GrpcTraceService {
        state: state.clone(),
    };

    let mut grpc_req = Request::new(otlp_export_with_session(&format!("grpc-sess-{suffix}")));
    grpc_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {api_key}").parse().unwrap(),
    );
    TraceService::export(&svc, grpc_req)
        .await
        .expect("gRPC export should accept bearer metadata and ingest");

    let tenant_info = TenantInfo {
        tenant_id,
        bucket_name: String::new(),
        dataset_id: String::new(),
    };
    runtime_export_trace_request(
        state,
        &tenant_info,
        otlp_export_with_session(&format!("http-sess-{suffix}")),
    )
    .await
    .expect("HTTP-path export should write to the same tenant scope");

    pipeline.force_flush_spans().await.expect("flush spans");

    let metadata_path = config.ducklake.as_ref().unwrap().metadata_path.clone();
    let conn = duckdb::Connection::open_in_memory().expect("duckdb");
    let n: i64 = trace_count_for_session(
        &conn,
        &metadata_path,
        &tenant_data_path,
        &tenant_schema,
        &format!("grpc-sess-{suffix}"),
    );
    assert_eq!(n, 1, "gRPC export must land in tenant-scoped traces table");
    let conn2 = duckdb::Connection::open_in_memory().expect("duckdb");
    let n2: i64 = trace_count_for_session(
        &conn2,
        &metadata_path,
        &tenant_data_path,
        &tenant_schema,
        &format!("http-sess-{suffix}"),
    );
    assert_eq!(
        n2, 1,
        "HTTP-path export must share the same DuckLake scope as gRPC"
    );
}
