//! Phase 1 (T013–T020): mocker-shaped OTLP ingest → merged promotion → count/list/fetch SQL.
//!
//! Proves thelake can accept mocker wire attrs (underscore keys + HTTP body columns) and answer
//! the aggregation/list recipes in `api::mocker::query` with no backend API cutover.
//! See `backend/docs/thelake-telemetry-mocker-migration-plan.md` Phase 1.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use chrono::{DateTime, Utc};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::json;
use softprobe_runtime::api::mocker::query::{
    compile_count_by_operation_sql, compile_count_by_range_sql, compile_entry_point_by_range_sql,
    compile_fetch_by_span_sql, compile_fetch_by_trace_sql, MockerRangeFilter,
};
use softprobe_runtime::api::AppState;
use softprobe_runtime::authn::TenantInfo;
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
use crate::util::tenant::{inject_local_sqlite_tenant, LOCAL_SQLITE_TENANT_ID};

/// Fixed span timestamp used by fixtures (2024-07-18T22:22:00Z).
const SPAN_START_NS: u64 = 1_721_349_720_000_000_000;
const SPAN_END_NS: u64 = 1_721_349_721_000_000_000;

const REQ_BODY: &str = r#"{"orderId":"ord-42","qty":2}"#;
const RESP_BODY: &str = r#"{"ok":true,"orderId":"ord-42"}"#;

struct MockerHarness {
    _temp: TempDir,
    router: Router,
    state: AppState,
}

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

fn merged_manifest_yaml() -> Option<String> {
    let llm = load_telemetry_manifest("llm-v1.yaml")?;
    let mocker = load_telemetry_manifest("mocker-v1.yaml")?;
    let merged = merge_telemetry_columns_manifests(&[llm, mocker])
        .expect("llm-v1 ∪ mocker-v1 must merge without conflicts");
    Some(telemetry_columns_manifest_to_yaml(&merged))
}

fn range_bounds() -> (DateTime<Utc>, DateTime<Utc>, DateTime<Utc>) {
    // Fixture SPAN_START_NS = 1_721_349_720e9 → 2024-07-19T00:42:00Z.
    let from = DateTime::parse_from_rfc3339("2024-07-18T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let to = DateTime::parse_from_rfc3339("2024-07-20T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    // After span timestamps, before fixture expiration_time (2026-08-01).
    let now = DateTime::parse_from_rfc3339("2024-07-20T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    (from, to, now)
}

fn mocker_span(
    app_id: &str,
    session_id: &str,
    trace_id: &str,
    span_id: &str,
    operation: &str,
    category: &str,
    parent_span_id: Option<&str>,
    deleted: bool,
) -> Span {
    let mut attributes = vec![
        string_kv("sp_app_id", app_id),
        string_kv("sp_session_id", session_id),
        string_kv("sp_trace_id", trace_id),
        string_kv("sp_span_id", span_id),
        string_kv("sp_operation_name", operation),
        string_kv("sp_category_type", category),
        int_kv("sp_record_environment", 2),
        string_kv("sp_record_version", "v3"),
        string_kv("sp_record_id", &format!("record-{span_id}")),
        string_kv("sp_mocker_id", &format!("mocker-{span_id}")),
        string_kv("sp_expiration_time", "2026-08-01T00:00:00Z"),
        string_kv("sp_update_time", "2024-07-18T22:22:00Z"),
        bool_kv("sp_record_deleted", deleted),
        bool_kv("sp_record_ghost", false),
        string_kv("http.request.method", "POST"),
        string_kv("http.request.path", "/api/checkout"),
        string_kv("http.request.body", REQ_BODY),
        string_kv("http.response.status_code", "200"),
        string_kv("http.response.body", RESP_BODY),
    ];
    if let Some(parent) = parent_span_id {
        attributes.push(string_kv("sp_parent_span_id", parent));
    }

    Span {
        // Binary IDs are placeholders; underscore wire attrs above are authoritative.
        trace_id: Uuid::new_v4().as_bytes().to_vec(),
        span_id: Uuid::new_v4().as_bytes()[..8].to_vec(),
        parent_span_id: vec![],
        name: operation.to_string(),
        kind: span::SpanKind::Server as i32,
        start_time_unix_nano: SPAN_START_NS,
        end_time_unix_nano: SPAN_END_NS,
        attributes,
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        ..Default::default()
    }
}

fn llm_generation_span(session_id: &str) -> Span {
    Span {
        trace_id: Uuid::new_v4().as_bytes().to_vec(),
        span_id: Uuid::new_v4().as_bytes()[..8].to_vec(),
        name: "chat.completions".to_string(),
        kind: span::SpanKind::Client as i32,
        start_time_unix_nano: SPAN_START_NS,
        end_time_unix_nano: SPAN_END_NS,
        attributes: vec![
            string_kv("sp.session.id", session_id),
            string_kv("sp.observation.type", "generation"),
            string_kv("sp.user.id", "user-phase1"),
            string_kv("gen_ai.provider.name", "openai"),
            string_kv("gen_ai.request.model", "gpt-4o"),
            string_kv("gen_ai.operation.name", "chat"),
            int_kv("gen_ai.usage.input_tokens", 11),
            int_kv("gen_ai.usage.output_tokens", 22),
            int_kv("gen_ai.usage.total_tokens", 33),
        ],
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        ..Default::default()
    }
}

fn export_request(spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "mocker-gateway")],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.mocker".to_string(),
                    version: "0.1.0".to_string(),
                    ..Default::default()
                }),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

async fn setup_with_merged_manifest() -> Option<MockerHarness> {
    let Some(manifest_yaml) = merged_manifest_yaml() else {
        eprintln!(
            "skipping: llm-v1/mocker-v1 not found under {}",
            sp_llm_manifest_path("").display()
        );
        return None;
    };

    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
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
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn(inject_local_sqlite_tenant));

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

    Some(MockerHarness {
        _temp: temp,
        router,
        state,
    })
}

async fn ingest(router: &Router, request: ExportTraceServiceRequest) {
    let mut body = Vec::new();
    request.encode(&mut body).expect("encode");
    // Body is well under server.max_body_size (default 5 MiB / config 100 MiB).
    assert!(
        body.len() < 1024 * 1024,
        "fixture body unexpectedly large: {} bytes",
        body.len()
    );
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
    assert_eq!(resp.status(), StatusCode::OK);
}

async fn flush(state: &AppState) {
    let engine = state
        .engine_for_id(LOCAL_SQLITE_TENANT_ID)
        .await
        .expect("engine");
    engine.ingest.force_flush_spans().await.expect("flush");
}

fn tenant() -> TenantInfo {
    TenantInfo {
        tenant_id: LOCAL_SQLITE_TENANT_ID.to_string(),
        bucket_name: String::new(),
        dataset_id: String::new(),
    }
}

async fn exec_sql(state: &AppState, sql: &str) -> softprobe_runtime::query::duckdb::QueryResult {
    state
        .execute_tenant_scoped_sql(Some(&tenant()), sql)
        .await
        .unwrap_or_else(|err| panic!("sql failed: {err}\n{sql}"))
}

fn col_index(columns: &[String], name: &str) -> usize {
    columns
        .iter()
        .position(|c| c == name)
        .unwrap_or_else(|| panic!("missing column {name} in {columns:?}"))
}

fn cell_str(result: &softprobe_runtime::query::duckdb::QueryResult, row: usize, col: &str) -> String {
    let idx = col_index(&result.columns, col);
    match &result.rows[row][idx] {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn cell_i64(result: &softprobe_runtime::query::duckdb::QueryResult, row: usize, col: &str) -> i64 {
    let idx = col_index(&result.columns, col);
    match &result.rows[row][idx] {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// T013: ingest mocker-shaped span → promote via merged manifest → queryable by
/// record_operation / record_category / trace_id / span_id.
#[tokio::test]
async fn mocker_shaped_span_queryable_after_merged_promote() {
    let Some(h) = setup_with_merged_manifest().await else {
        return;
    };
    let session = format!("sess-t013-{}", Uuid::new_v4());
    let trace_id = format!("trace-t013-{}", Uuid::new_v4());
    let span_id = format!("span-t013-{}", Uuid::new_v4());

    ingest(
        &h.router,
        export_request(vec![mocker_span(
            "checkout-api",
            &session,
            &trace_id,
            &span_id,
            "POST /checkout",
            "http",
            None,
            false,
        )]),
    )
    .await;
    flush(&h.state).await;

    let sql = format!(
        "SELECT record_operation, record_category, trace_id, span_id, app_id \
         FROM union_spans \
         WHERE record_operation = 'POST /checkout' \
           AND record_category = 'http' \
           AND trace_id = '{trace}' \
           AND span_id = '{span}'",
        trace = trace_id.replace('\'', "''"),
        span = span_id.replace('\'', "''"),
    );
    let result = exec_sql(&h.state, &sql).await;
    assert_eq!(result.row_count, 1, "expected one promoted mocker row");
    assert_eq!(cell_str(&result, 0, "record_operation"), "POST /checkout");
    assert_eq!(cell_str(&result, 0, "record_category"), "http");
    assert_eq!(cell_str(&result, 0, "trace_id"), trace_id);
    assert_eq!(cell_str(&result, 0, "span_id"), span_id);
    assert_eq!(cell_str(&result, 0, "app_id"), "checkout-api");
}

/// T014: plaintext HTTP body columns survive ingest → promote → read unchanged.
#[tokio::test]
async fn mocker_http_body_payload_round_trips() {
    let Some(h) = setup_with_merged_manifest().await else {
        return;
    };
    let session = format!("sess-t014-{}", Uuid::new_v4());
    let trace_id = format!("trace-t014-{}", Uuid::new_v4());
    let span_id = format!("span-t014-{}", Uuid::new_v4());

    ingest(
        &h.router,
        export_request(vec![mocker_span(
            "checkout-api",
            &session,
            &trace_id,
            &span_id,
            "POST /checkout",
            "http",
            None,
            false,
        )]),
    )
    .await;
    flush(&h.state).await;

    let sql = format!(
        "SELECT http_request_body, http_response_body, http_request_method, http_request_path \
         FROM union_spans WHERE span_id = '{}'",
        span_id.replace('\'', "''")
    );
    let result = exec_sql(&h.state, &sql).await;
    assert_eq!(result.row_count, 1);
    assert_eq!(cell_str(&result, 0, "http_request_body"), REQ_BODY);
    assert_eq!(cell_str(&result, 0, "http_response_body"), RESP_BODY);
    assert_eq!(cell_str(&result, 0, "http_request_method"), "POST");
    assert_eq!(cell_str(&result, 0, "http_request_path"), "/api/checkout");
}

/// T015: auth tenant binding still stamps mocker spans (OTLP tenant claim ignored).
#[tokio::test]
async fn mocker_span_binds_auth_tenant_not_otlp_claim() {
    let Some(h) = setup_with_merged_manifest().await else {
        return;
    };
    let session = format!("sess-t015-{}", Uuid::new_v4());
    let trace_id = format!("trace-t015-{}", Uuid::new_v4());
    let span_id = format!("span-t015-{}", Uuid::new_v4());

    let mut span = mocker_span(
        "checkout-api",
        &session,
        &trace_id,
        &span_id,
        "POST /checkout",
        "http",
        None,
        false,
    );
    // Spurious claim must not override Bearer/auth-injected tenant binding.
    span.attributes
        .push(string_kv("sp.tenant.id", "attacker-tenant"));

    ingest(&h.router, export_request(vec![span])).await;
    flush(&h.state).await;

    let sql = format!(
        "SELECT tenant_id FROM union_spans WHERE span_id = '{}'",
        span_id.replace('\'', "''")
    );
    let result = exec_sql(&h.state, &sql).await;
    assert_eq!(result.row_count, 1);
    assert_eq!(
        cell_str(&result, 0, "tenant_id"),
        LOCAL_SQLITE_TENANT_ID,
        "auth tenant must win over OTLP claim"
    );
}

/// T017–T019: count-by-operation / bounded-range / entry-point / fetch-by-trace|span.
#[tokio::test]
async fn mocker_count_list_and_fetch_sql_return_expected_counts() {
    let Some(h) = setup_with_merged_manifest().await else {
        return;
    };
    let session = format!("sess-t019-{}", Uuid::new_v4());
    let trace_a = format!("trace-a-{}", Uuid::new_v4());
    let root_a = format!("span-root-a-{}", Uuid::new_v4());
    let child_a = format!("span-child-a-{}", Uuid::new_v4());
    let trace_b = format!("trace-b-{}", Uuid::new_v4());
    let root_b = format!("span-root-b-{}", Uuid::new_v4());
    let deleted = format!("span-del-{}", Uuid::new_v4());

    ingest(
        &h.router,
        export_request(vec![
            mocker_span(
                "checkout-api",
                &session,
                &trace_a,
                &root_a,
                "POST /checkout",
                "http",
                None,
                false,
            ),
            mocker_span(
                "checkout-api",
                &session,
                &trace_a,
                &child_a,
                "POST /checkout",
                "http",
                Some(&root_a),
                false,
            ),
            mocker_span(
                "payments-api",
                &session,
                &trace_b,
                &root_b,
                "GET /pay",
                "http",
                None,
                false,
            ),
            // Soft-deleted: must be excluded from count/list helpers.
            mocker_span(
                "checkout-api",
                &session,
                &format!("trace-del-{}", Uuid::new_v4()),
                &deleted,
                "POST /checkout",
                "http",
                None,
                true,
            ),
        ]),
    )
    .await;
    flush(&h.state).await;

    let (from, to, now) = range_bounds();
    let filter = MockerRangeFilter {
        record_category: Some("http".to_string()),
        ..Default::default()
    };

    let count_sql = compile_count_by_range_sql(&filter, from, to, now).expect("count range sql");
    let count_result = exec_sql(&h.state, &count_sql).await;
    assert_eq!(count_result.row_count, 1);
    assert_eq!(
        cell_i64(&count_result, 0, "count"),
        3,
        "deleted row must be excluded from countByRange"
    );

    let by_op_sql =
        compile_count_by_operation_sql(&filter, from, to, now).expect("count by op sql");
    let by_op = exec_sql(&h.state, &by_op_sql).await;
    let mut checkout = 0i64;
    let mut pay = 0i64;
    for row in 0..by_op.row_count {
        let app = cell_str(&by_op, row, "app_id");
        let op = cell_str(&by_op, row, "record_operation");
        let cat = cell_str(&by_op, row, "record_category");
        let n = cell_i64(&by_op, row, "count");
        assert_eq!(cat, "http");
        if app == "checkout-api" && op == "POST /checkout" {
            checkout = n;
        }
        if app == "payments-api" && op == "GET /pay" {
            pay = n;
        }
    }
    assert_eq!(checkout, 2, "checkout group: root + child");
    assert_eq!(pay, 1, "payments group: one root");

    let entry_sql =
        compile_entry_point_by_range_sql(&filter, from, to, now, Some(50)).expect("entry sql");
    let entries = exec_sql(&h.state, &entry_sql).await;
    let entry_ids: Vec<_> = (0..entries.row_count)
        .map(|i| cell_str(&entries, i, "span_id"))
        .collect();
    assert!(entry_ids.contains(&root_a), "root A is an entry point");
    assert!(entry_ids.contains(&root_b), "root B is an entry point");
    assert!(!entry_ids.contains(&child_a), "child must not be an entry point");
    assert!(!entry_ids.contains(&deleted), "deleted must not list");

    let fetch_trace = exec_sql(&h.state, &compile_fetch_by_trace_sql(&trace_a)).await;
    assert_eq!(fetch_trace.row_count, 2, "preload returns both spans on trace_a");
    let fetch_span = exec_sql(&h.state, &compile_fetch_by_span_sql(&root_b)).await;
    assert_eq!(fetch_span.row_count, 1);
    assert_eq!(cell_str(&fetch_span, 0, "span_id"), root_b);

    let deleted_fetch = exec_sql(&h.state, &compile_fetch_by_span_sql(&deleted)).await;
    assert_eq!(
        deleted_fetch.row_count, 0,
        "fetch-by-span must exclude soft-deleted"
    );
}

/// T020: mocker ingest suite + llm-v1 promotion coexist on one thelake instance (no interference).
#[tokio::test]
async fn mocker_and_llm_v1_coexist_on_same_instance() {
    let Some(h) = setup_with_merged_manifest().await else {
        return;
    };
    let mocker_session = format!("sess-mocker-{}", Uuid::new_v4());
    let llm_session = format!("sess-llm-{}", Uuid::new_v4());
    let trace_id = format!("trace-coexist-{}", Uuid::new_v4());
    let span_id = format!("span-coexist-{}", Uuid::new_v4());

    ingest(
        &h.router,
        export_request(vec![
            mocker_span(
                "checkout-api",
                &mocker_session,
                &trace_id,
                &span_id,
                "POST /checkout",
                "http",
                None,
                false,
            ),
            llm_generation_span(&llm_session),
        ]),
    )
    .await;
    flush(&h.state).await;

    let mocker_sql = format!(
        "SELECT record_operation, record_category FROM union_spans WHERE session_id = '{}'",
        mocker_session.replace('\'', "''")
    );
    let mocker = exec_sql(&h.state, &mocker_sql).await;
    assert_eq!(mocker.row_count, 1);
    assert_eq!(cell_str(&mocker, 0, "record_operation"), "POST /checkout");

    let llm_sql = format!(
        "SELECT observation_type, operation_name, model_name FROM union_spans WHERE session_id = '{}'",
        llm_session.replace('\'', "''")
    );
    let llm = exec_sql(&h.state, &llm_sql).await;
    assert_eq!(llm.row_count, 1);
    assert_eq!(cell_str(&llm, 0, "observation_type"), "generation");
    assert_eq!(cell_str(&llm, 0, "operation_name"), "chat");
    assert_eq!(cell_str(&llm, 0, "model_name"), "gpt-4o");
}

/// Phase 4 (T037–T040 store-level): execute Phase 1 `compile_*` SQL against dual-written-shaped
/// OTLP ingest and assert counts/lists match the Mongo-direct expectations documented by
/// `backend/.../MockerReadShadowFixtures` / `MockerReadShadowParityTest` (Servlet + HttpClient).
///
/// This is the DuckLake leg of read-shadow evidence — not a Java predicate mimic.
#[tokio::test]
async fn phase4_dual_written_fixture_matches_mongo_expected_via_compile_sql() {
    let Some(h) = setup_with_merged_manifest().await else {
        return;
    };
    let session = format!("sess-p4-{}", Uuid::new_v4());
    let trace_a = format!("trace-a-{}", Uuid::new_v4());
    let root_a = format!("span-root-a-{}", Uuid::new_v4());
    let child_a = format!("span-child-a-{}", Uuid::new_v4());
    let trace_b = format!("trace-b-{}", Uuid::new_v4());
    let root_b = format!("span-root-b-{}", Uuid::new_v4());
    let deleted = format!("span-del-{}", Uuid::new_v4());

    // Same dual-written shape as Java MockerReadShadowFixtures.dualWrittenActive (+ tombstone):
    // two Servlet roots (checkout-api), one HttpClient child on trace_a, one soft-deleted Servlet.
    ingest(
        &h.router,
        export_request(vec![
            mocker_span(
                "checkout-api",
                &session,
                &trace_a,
                &root_a,
                "/api/checkout",
                "Servlet",
                None,
                false,
            ),
            mocker_span(
                "checkout-api",
                &session,
                &trace_a,
                &child_a,
                "POST /payments",
                "HttpClient",
                Some(&root_a),
                false,
            ),
            mocker_span(
                "checkout-api",
                &session,
                &trace_b,
                &root_b,
                "/api/pay",
                "Servlet",
                None,
                false,
            ),
            mocker_span(
                "checkout-api",
                &session,
                &format!("trace-del-{}", Uuid::new_v4()),
                &deleted,
                "/api/checkout",
                "Servlet",
                None,
                true,
            ),
        ]),
    )
    .await;
    flush(&h.state).await;

    let (from, to, now) = range_bounds();
    let servlet_filter = MockerRangeFilter {
        app_id: Some("checkout-api".to_string()),
        record_category: Some("Servlet".to_string()),
        record_environment: Some(2),
        ..Default::default()
    };

    // Mongo-direct expected for Servlet+checkout-api (tombstone absent on Mongo; thelake excludes):
    // countByRange = 2; countByOperationName = {/api/checkout:1, /api/pay:1}.
    let count_sql =
        compile_count_by_range_sql(&servlet_filter, from, to, now).expect("count range sql");
    let count_result = exec_sql(&h.state, &count_sql).await;
    assert_eq!(
        cell_i64(&count_result, 0, "count"),
        2,
        "T038 store-level: countByRange matches Mongo-direct expected"
    );

    let by_op_sql =
        compile_count_by_operation_sql(&servlet_filter, from, to, now).expect("count by op sql");
    let by_op = exec_sql(&h.state, &by_op_sql).await;
    let mut by_operation = std::collections::BTreeMap::<String, i64>::new();
    for row in 0..by_op.row_count {
        let op = cell_str(&by_op, row, "record_operation");
        by_operation.insert(op, cell_i64(&by_op, row, "count"));
    }
    assert_eq!(
        by_operation.get("/api/checkout").copied().unwrap_or(0),
        1,
        "T037 store-level"
    );
    assert_eq!(
        by_operation.get("/api/pay").copied().unwrap_or(0),
        1,
        "T037 store-level"
    );

    let entry_sql =
        compile_entry_point_by_range_sql(&servlet_filter, from, to, now, Some(50)).expect("entry");
    let entries = exec_sql(&h.state, &entry_sql).await;
    let entry_ids: Vec<_> = (0..entries.row_count)
        .map(|i| cell_str(&entries, i, "span_id"))
        .collect();
    assert!(entry_ids.contains(&root_a), "T039 root A");
    assert!(entry_ids.contains(&root_b), "T039 root B");
    assert!(!entry_ids.contains(&child_a), "T039 HttpClient child not entry");
    assert!(!entry_ids.contains(&deleted), "T039 tombstone excluded");

    let fetch_trace = exec_sql(&h.state, &compile_fetch_by_trace_sql(&trace_a)).await;
    assert_eq!(
        fetch_trace.row_count, 2,
        "T040 full-trace preload: Servlet root + HttpClient child"
    );
    let fetch_span = exec_sql(&h.state, &compile_fetch_by_span_sql(&root_b)).await;
    assert_eq!(fetch_span.row_count, 1);
    assert_eq!(cell_str(&fetch_span, 0, "span_id"), root_b);
    let deleted_fetch = exec_sql(&h.state, &compile_fetch_by_span_sql(&deleted)).await;
    assert_eq!(deleted_fetch.row_count, 0, "T040 soft-deleted hidden");
}
