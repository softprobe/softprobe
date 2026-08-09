//! Verify the canonical softprobe/sp-llm `manifests/llm-v1.yaml` promotion profile.
//!
//! The manifest is loaded from the sibling sp-llm checkout and is not duplicated here.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::post;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::json;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

use crate::util::config::file_backed_test_config;
use crate::util::otlp::{double_kv, int_kv, string_kv};
use crate::util::sp_llm_manifests::sp_llm_manifest_path;
use crate::util::tenant::inject_local_sqlite_tenant as inject_tenant;

fn llm_v1_manifest_path() -> PathBuf {
    if let Ok(path) = std::env::var("SP_LLM_MANIFEST") {
        return PathBuf::from(path);
    }
    sp_llm_manifest_path("llm-v1.yaml")
}

fn load_llm_v1_manifest() -> Option<String> {
    let path = llm_v1_manifest_path();
    std::fs::read_to_string(&path).ok()
}

fn generation_request(session_id: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "llm-gateway"),
                    string_kv("deployment.environment.name", "staging"),
                    string_kv("service.version", "1.2.3"),
                ],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.llm".to_string(),
                    version: "0.1.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: Uuid::new_v4().as_bytes().to_vec(),
                    span_id: Uuid::new_v4().as_bytes()[..8].to_vec(),
                    name: "chat.completions".to_string(),
                    kind: span::SpanKind::Client as i32,
                    start_time_unix_nano: 1_721_349_720_000_000_000,
                    end_time_unix_nano: 1_721_349_721_000_000_000,
                    attributes: vec![
                        string_kv("sp.session.id", session_id),
                        string_kv("sp.observation.type", "generation"),
                        string_kv("sp.user.id", "user-promo-1"),
                        string_kv("gen_ai.provider.name", "openai"),
                        string_kv("gen_ai.request.model", "gpt-4o"),
                        string_kv("gen_ai.response.model", "gpt-4o-2024-08-06"),
                        string_kv("gen_ai.operation.name", "chat"),
                        int_kv("gen_ai.usage.input_tokens", 11),
                        int_kv("gen_ai.usage.output_tokens", 22),
                        int_kv("gen_ai.usage.total_tokens", 33),
                        double_kv("sp.cost.input", 0.001),
                        double_kv("sp.cost.output", 0.002),
                        double_kv("sp.cost.total", 0.003),
                    ],
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
    }
}

#[tokio::test]
async fn canonical_llm_v1_manifest_promotes_generation_fields() {
    let Some(manifest_yaml) = load_llm_v1_manifest() else {
        eprintln!(
            "skipping: canonical manifest not found at {}",
            llm_v1_manifest_path().display()
        );
        return;
    };
    assert!(
        manifest_yaml.contains("observation_type"),
        "unexpected llm-v1 manifest contents"
    );

    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();

    let (router, state) = softprobe_runtime::api::create_router(
        Arc::new(config),
        post(ingest_traces),
        None,
    )
    .await
    .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_tenant));

    let apply = router
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
    let apply_status = apply.status();
    let apply_body = apply
        .into_body()
        .collect()
        .await
        .expect("apply body")
        .to_bytes();
    assert_eq!(
        apply_status,
        StatusCode::OK,
        "apply failed: {}",
        String::from_utf8_lossy(&apply_body)
    );

    let session_id = format!("sess-llm-v1-{}", Uuid::new_v4());
    let mut body = Vec::new();
    generation_request(&session_id)
        .encode(&mut body)
        .expect("encode");
    let ingest = router
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
    assert_eq!(ingest.status(), StatusCode::OK);

    let connection = duckdb::Connection::open_in_memory().expect("duckdb");
    connection
        .execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
        .expect("extensions");
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS softprobe \
             (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, \
              DATA_INLINING_ROW_LIMIT 0);",
            metadata_path.replace('\'', "''"),
            data_path.replace('\'', "''"),
        ))
        .expect("attach");

    for column in [
        "observation_type",
        "model_name",
        "model_provider",
        "user_id",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "total_cost",
        "environment",
        "release",
    ] {
        let count: i64 = connection
            .query_row(
                &format!(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_catalog = 'softprobe' AND table_name = 'traces' \
                     AND column_name = '{column}'"
                ),
                [],
                |row| row.get(0),
            )
            .expect("column exists query");
        assert!(count > 0, "expected promoted column {column}");
    }

    let sql = format!(
        "SELECT observation_type, model_name, model_provider, user_id, \
                input_tokens, output_tokens, total_tokens, total_cost, \
                environment, release \
         FROM softprobe.traces WHERE session_id = '{}'",
        session_id.replace('\'', "''")
    );
    let row = connection
        .query_row(&sql, [], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<f64>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
            ))
        })
        .expect("query promoted generation");

    assert_eq!(row.0.as_deref(), Some("generation"));
    assert_eq!(row.1.as_deref(), Some("gpt-4o"));
    assert_eq!(row.2.as_deref(), Some("openai"));
    assert_eq!(row.3.as_deref(), Some("user-promo-1"));
    assert_eq!(row.4, Some(11));
    assert_eq!(row.5, Some(22));
    assert_eq!(row.6, Some(33));
    assert_eq!(row.7, Some(0.003));
    assert_eq!(row.8.as_deref(), Some("staging"));
    assert_eq!(row.9.as_deref(), Some("1.2.3"));
}
