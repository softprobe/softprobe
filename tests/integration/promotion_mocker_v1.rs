//! Verify `sp-llm/manifests/mocker-v1.yaml` merges with the canonical `llm-v1.yaml` promotion
//! profile into the single platform `telemetry_columns` manifest a tenant may have active
//! (`thelake/docs/promotion.md`) and applies without colliding with llm-v1 or base `traces`
//! columns.
//!
//! Phase 0 of `backend/docs/thelake-telemetry-mocker-migration-plan.md`. Both manifests are
//! loaded from the sibling sp-llm checkout and are not duplicated here.

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
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::promotion::{
    merge_telemetry_columns_manifests, parse_promotion_manifest,
    telemetry_columns_manifest_to_yaml, PromotionManifest, TelemetryColumnsManifest,
};
use softprobe_runtime::runtime_api::{runtime_control_routes, runtime_post_v1_traces};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

use crate::util::config::file_backed_test_config;
use crate::util::otlp::{bool_kv, int_kv, string_kv};
use crate::util::sp_llm_manifests::{load_sp_llm_manifest, sp_llm_manifest_path};
use crate::util::tenant::inject_local_sqlite_tenant as inject_tenant;

fn load_telemetry_manifest(name: &str) -> Option<TelemetryColumnsManifest> {
    let yaml = load_sp_llm_manifest(name)?;
    match parse_promotion_manifest(&yaml).unwrap_or_else(|err| {
        panic!(
            "{name} at {} failed to parse: {err}",
            sp_llm_manifest_path(name).display()
        )
    }) {
        PromotionManifest::TelemetryColumns(manifest) => Some(manifest),
        PromotionManifest::BusinessTable(_) => {
            panic!("{name} is a business_table manifest, expected telemetry_columns")
        }
    }
}

/// One span carrying both llm-v1 (`gen_ai.*` / `sp.*`) and mocker-v1 (`sp_*`) source attributes.
fn combined_request(session_id: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "mocker-gateway"),
                    string_kv("deployment.environment.name", "staging"),
                    string_kv("service.version", "4.5.6"),
                ],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.mocker".to_string(),
                    version: "0.1.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: Uuid::new_v4().as_bytes().to_vec(),
                    span_id: Uuid::new_v4().as_bytes()[..8].to_vec(),
                    name: "mocker.replay".to_string(),
                    kind: span::SpanKind::Server as i32,
                    start_time_unix_nano: 1_721_349_720_000_000_000,
                    end_time_unix_nano: 1_721_349_721_000_000_000,
                    attributes: vec![
                        string_kv("sp.session.id", session_id),
                        // llm-v1 source attributes.
                        string_kv("sp.observation.type", "generation"),
                        string_kv("gen_ai.operation.name", "chat"),
                        // mocker-v1 source attributes (underscore wire keys).
                        string_kv("sp_operation_name", "GET /checkout"),
                        int_kv("sp_record_environment", 2),
                        string_kv("sp_record_version", "v3"),
                        string_kv("sp_category_type", "http"),
                        string_kv("sp_record_id", "record-abc-123"),
                        string_kv("sp_mocker_id", "mocker-xyz-789"),
                        string_kv("sp_expiration_time", "2026-08-01T00:00:00Z"),
                        string_kv("sp_update_time", "2026-07-26T07:00:00Z"),
                        bool_kv("sp_record_deleted", false),
                        bool_kv("sp_record_ghost", false),
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
async fn merged_llm_and_mocker_manifest_applies_without_collisions() {
    let Some(llm) = load_telemetry_manifest("llm-v1.yaml") else {
        eprintln!(
            "skipping: llm-v1.yaml not found at {}",
            sp_llm_manifest_path("llm-v1.yaml").display()
        );
        return;
    };
    let Some(mocker) = load_telemetry_manifest("mocker-v1.yaml") else {
        eprintln!(
            "skipping: mocker-v1.yaml not found at {}",
            sp_llm_manifest_path("mocker-v1.yaml").display()
        );
        return;
    };

    let merged = merge_telemetry_columns_manifests(&[llm, mocker])
        .expect("llm-v1 and mocker-v1 must merge without conflicts");
    let manifest_yaml = telemetry_columns_manifest_to_yaml(&merged);

    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let query_engine =
        softprobe_runtime::query::create_query_engine(&config, Arc::new(pipeline.storage.clone()))
            .await
            .expect("query engine");
    let (router, state) = softprobe_runtime::api::create_router(
        Arc::new(config),
        pipeline.storage,
        query_engine,
        post(runtime_post_v1_traces),
        None,
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
        "merged manifest apply failed: {}",
        String::from_utf8_lossy(&apply_body)
    );

    let session_id = format!("sess-merged-v1-{}", Uuid::new_v4());
    let mut body = Vec::new();
    combined_request(&session_id)
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

    // All llm-v1 + mocker-v1 columns must exist on one merged spec, with no name collisions.
    for column in [
        "observation_type",
        "operation_name",
        "environment",
        "record_operation",
        "record_environment",
        "record_version",
        "record_category",
        "record_id",
        "mocker_id",
        "expiration_time",
        "update_time",
        "record_deleted",
        "record_ghost",
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
        assert!(count > 0, "expected merged promoted column {column}");
    }

    let sql = format!(
        "SELECT observation_type, operation_name, environment, \
                record_operation, record_environment, record_version, record_category, \
                record_id, mocker_id, CAST(expiration_time AS VARCHAR), \
                CAST(update_time AS VARCHAR), record_deleted, record_ghost \
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
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<bool>>(11)?,
                row.get::<_, Option<bool>>(12)?,
            ))
        })
        .expect("query merged promoted columns");

    // llm-v1 fields populate under their original names.
    assert_eq!(row.0.as_deref(), Some("generation"), "observation_type");
    assert_eq!(row.1.as_deref(), Some("chat"), "operation_name (llm-v1)");
    assert_eq!(row.2.as_deref(), Some("staging"), "environment (llm-v1)");

    // mocker-v1 fields populate under record_*, distinct from the llm-v1 columns above.
    assert_eq!(
        row.3.as_deref(),
        Some("GET /checkout"),
        "record_operation (mocker-v1)"
    );
    assert_eq!(row.4, Some(2), "record_environment (mocker-v1)");
    assert_eq!(row.5.as_deref(), Some("v3"), "record_version");
    assert_eq!(row.6.as_deref(), Some("http"), "record_category");
    assert_eq!(row.7.as_deref(), Some("record-abc-123"), "record_id");
    assert_eq!(row.8.as_deref(), Some("mocker-xyz-789"), "mocker_id");
    assert_eq!(
        row.9.as_deref(),
        Some("2026-08-01 00:00:00+00"),
        "expiration_time"
    );
    assert_eq!(
        row.10.as_deref(),
        Some("2026-07-26 07:00:00+00"),
        "update_time"
    );
    assert_eq!(row.11, Some(false), "record_deleted");
    assert_eq!(row.12, Some(false), "record_ghost");
}
