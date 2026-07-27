//! Integration tests for the OTLP HTTP surface (`AppPipeline` router — same handlers as production without `main` middleware layers).

use axum::body::Body;
use axum::http::{header, Request, Response, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use softprobe_runtime::api::{AppPipeline, AppState};
use std::sync::Arc;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;
use crate::util::otlp::{double_kv, int_kv, string_kv};

async fn build_router() -> (Router, tempfile::TempDir) {
    let (router, _state, temp) = build_router_and_state().await;
    (router, temp)
}

async fn build_router_and_state() -> (Router, AppState, tempfile::TempDir) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let config = Arc::new(config);
    let app = AppPipeline::new(config.as_ref())
        .await
        .expect("app pipeline");
    let (router, state) = softprobe_runtime::api::create_router(
        config.clone(),
        app.storage,
        app.query_engine,
        axum::routing::post(softprobe_runtime::api::ingestion::traces::ingest_traces),
        None,
        None,
    )
    .await
    .expect("router");
    (router, state, temp)
}

async fn response_json(resp: Response<Body>) -> Value {
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    serde_json::from_slice(&body).expect("json body")
}

fn llm_generation_request(
    session_id: &str,
    trace_id: [u8; 16],
    span_id: [u8; 8],
) -> ExportTraceServiceRequest {
    let generation = Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: vec![],
        name: "chat.completions".to_string(),
        kind: span::SpanKind::Client as i32,
        start_time_unix_nano: 1_721_349_720_000_000_000,
        end_time_unix_nano: 1_721_349_721_500_000_000,
        attributes: vec![
            string_kv("sp.session.id", session_id),
            string_kv("sp.observation.type", "generation"),
            string_kv("sp.user.id", "user-llm-1"),
            string_kv("gen_ai.provider.name", "openai"),
            string_kv("gen_ai.request.model", "gpt-4o"),
            string_kv("gen_ai.operation.name", "chat"),
            int_kv("gen_ai.usage.input_tokens", 12),
            int_kv("gen_ai.usage.output_tokens", 34),
            int_kv("gen_ai.usage.total_tokens", 46),
            double_kv("sp.cost.total", 0.0123),
        ],
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        ..Default::default()
    };

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "llm-gateway")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.llm".to_string(),
                    ..Default::default()
                }),
                spans: vec![generation],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn telemetry_trace_request(session_id: &str, trace_id: [u8; 16]) -> ExportTraceServiceRequest {
    let root = Span {
        trace_id: trace_id.to_vec(),
        span_id: vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
        parent_span_id: vec![],
        name: "POST /api/checkout".to_string(),
        kind: span::SpanKind::Server as i32,
        start_time_unix_nano: 1_777_802_600_000_000_000,
        end_time_unix_nano: 1_777_802_601_500_000_000,
        attributes: vec![
            string_kv("sp.session.id", session_id),
            string_kv("http.request.method", "POST"),
            string_kv("http.request.path", "/api/checkout"),
            int_kv("http.response.status_code", 503),
        ],
        status: Some(Status {
            code: 2,
            message: "payment provider timeout".to_string(),
        }),
        ..Default::default()
    };

    let resource_spans = ResourceSpans {
        resource: Some(Resource {
            attributes: vec![string_kv("service.name", "checkout-api")],
            ..Default::default()
        }),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "softprobe.e2e".to_string(),
                ..Default::default()
            }),
            spans: vec![root],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    };

    ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    }
}

fn telemetry_logs_request(session_id: &str, trace_id: [u8; 16]) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout-api"),
                    string_kv("sp.session.id", session_id),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_777_802_601_000_000_000,
                    severity_number: 17,
                    severity_text: "ERROR".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "payment provider timeout".to_string(),
                        )),
                    }),
                    attributes: vec![string_kv("sp.session.id", session_id)],
                    trace_id: trace_id.to_vec(),
                    span_id: vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn telemetry_metrics_request(session_id: &str, trace_id: &str) -> ExportMetricsServiceRequest {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point;

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout-api"),
                    string_kv("sp.session.id", session_id),
                    string_kv("trace_id", trace_id),
                ],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "http.server.duration".to_string(),
                    description: "HTTP server duration".to_string(),
                    unit: "ms".to_string(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![
                                string_kv("sp.session.id", session_id),
                                string_kv("trace_id", trace_id),
                            ],
                            time_unix_nano: 1_777_802_601_000_000_000,
                            value: Some(number_data_point::Value::AsDouble(1500.0)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn health_returns_ok_envelope() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;
    assert_eq!(v["status"], "ok");
    assert_eq!(v["specVersion"], "http-control-api@v1");
}

#[tokio::test]
async fn ready_returns_ready() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .uri("/ready")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;
    assert_eq!(v["status"], "ready");
}

#[tokio::test]
async fn openapi_and_swagger_endpoints_are_served() {
    let (router, _t) = build_router().await;

    let req = Request::builder()
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/json")
    );

    let req = Request::builder()
        .uri("/swagger")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    let html = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(html.contains("SwaggerUIBundle"));
    assert!(html.contains("/openapi.json"));
}

#[tokio::test]
async fn traces_json_empty_batch_succeeds() {
    let (router, _t) = build_router().await;
    let body = json!({ "resourceSpans": [] }).to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;
    assert!(v["success"].as_bool().unwrap());
    assert_eq!(v["ingested_count"], 0);
}

#[tokio::test]
async fn traces_json_invalid_returns_400() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("not-json"))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn traces_protobuf_empty_roundtrip() {
    let (router, _t) = build_router().await;
    let mut buf = Vec::new();
    ExportTraceServiceRequest::default()
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn traces_protobuf_garbage_returns_400() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(vec![0xffu8, 0xfe]))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn logs_json_empty_batch() {
    let (router, _t) = build_router().await;
    let body = json!({ "resourceLogs": [] }).to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn logs_json_invalid_returns_400() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{"))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn metrics_json_empty_batch() {
    let (router, _t) = build_router().await;
    let body = json!({ "resourceMetrics": [] }).to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn query_sql_empty_returns_400() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "sql": "   " }).to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn query_sql_select_literal() {
    let (router, _t) = build_router().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "sql": "SELECT 1 AS n" }).to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;
    assert!(v["columns"].is_array());
    assert!(v["rows"].is_array());
}

#[tokio::test]
async fn logs_protobuf_empty() {
    let (router, _t) = build_router().await;
    let mut buf = Vec::new();
    ExportLogsServiceRequest::default()
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn metrics_protobuf_empty() {
    let (router, _t) = build_router().await;
    let mut buf = Vec::new();
    ExportMetricsServiceRequest::default()
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn telemetry_search_sessions_returns_summary_rows() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-search-e2e";
    let trace_id = [
        0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99,
    ];

    let trace_body = serde_json::to_string(&telemetry_trace_request(session_id, trace_id)).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(trace_body))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("trace ingest");
    assert_eq!(resp.status(), StatusCode::OK);

    let engine = state.engine_for_id("").await.expect("engine");
    engine
        .ingest
        .force_flush_spans()
        .await
        .expect("flush spans");

    let body = json!({
        "version": 1,
        "scope": "sessions",
        "timeRange": {
            "from": "2026-05-03T10:00:00Z",
            "to": "2026-05-03T11:00:00Z"
        },
        "filter": {
            "and": [
                { "field": "service.name", "op": "eq", "value": "checkout-api" },
                { "field": "http_request_path", "op": "prefix", "value": "/api/checkout" }
            ]
        },
        "columns": ["session_id", "trace_count", "error_count"],
        "sort": [{ "field": "timestamp", "direction": "desc" }],
        "limit": 25
    });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/telemetry/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.expect("search");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;

    assert_eq!(v["version"], 1);
    assert_eq!(v["scope"], "sessions");
    assert_eq!(v["rows"][0]["id"], session_id);
    assert_eq!(v["rows"][0]["summary"]["traceCount"], 1);
    assert_eq!(v["rows"][0]["summary"]["spanCount"], 1);
    assert_eq!(v["rows"][0]["summary"]["errorCount"], 1);
}

#[tokio::test]
async fn telemetry_session_details_returns_spans_logs_and_metrics() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-details-e2e";
    let trace_bytes = [
        0xba, 0xad, 0xf0, 0x0d, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99,
    ];
    let trace_hex = hex::encode(trace_bytes);

    for (uri, body) in [
        (
            "/v1/traces",
            serde_json::to_string(&telemetry_trace_request(session_id, trace_bytes)).unwrap(),
        ),
        (
            "/v1/logs",
            serde_json::to_string(&telemetry_logs_request(session_id, trace_bytes)).unwrap(),
        ),
        (
            "/v1/metrics",
            serde_json::to_string(&telemetry_metrics_request(session_id, &trace_hex)).unwrap(),
        ),
    ] {
        let req = Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();
        let resp = router.clone().oneshot(req).await.expect("ingest");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let engine = state.engine_for_id("").await.expect("engine");
    engine
        .ingest
        .force_flush_spans()
        .await
        .expect("flush spans");
    engine.ingest.force_flush_logs().await.expect("flush logs");
    engine
        .ingest
        .force_flush_metrics()
        .await
        .expect("flush metrics");

    let req = Request::builder()
        .uri(format!(
            "/v1/telemetry/sessions/{}?from=2026-05-03T10:00:00Z&to=2026-05-03T11:00:00Z",
            session_id
        ))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("details");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;

    assert_eq!(v["version"], 1);
    assert_eq!(v["kind"], "session");
    assert_eq!(v["id"], session_id);
    assert_eq!(v["summary"]["spanCount"], 1);
    assert_eq!(v["summary"]["logCount"], 1);
    assert_eq!(v["summary"]["metricCount"], 1);
    assert_eq!(v["spans"][0]["trace_id"], trace_hex);
    assert_eq!(v["logs"][0]["body"], "payment provider timeout");
    assert_eq!(v["metrics"][0]["metric_name"], "http.server.duration");
}

#[tokio::test]
async fn llm_query_endpoints_return_observations_traces_sessions_and_scores() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-llm-query-e2e";
    let trace_bytes = [
        0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47,
        0x36,
    ];
    let span_bytes = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];
    let trace_hex = hex::encode(trace_bytes);
    let span_hex = hex::encode(span_bytes);

    let ingest = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_string(&llm_generation_request(session_id, trace_bytes, span_bytes))
                .unwrap(),
        ))
        .unwrap();
    let ingest_resp = router.clone().oneshot(ingest).await.expect("ingest");
    assert_eq!(ingest_resp.status(), StatusCode::OK);

    let engine = state.engine_for_id("").await.expect("engine");
    engine
        .ingest
        .force_flush_spans()
        .await
        .expect("flush spans");

    let score_body = json!({
        "score_id": "score-llm-query-1",
        "timestamp": "2024-07-19T00:02:00Z",
        "trace_id": trace_hex,
        "span_id": span_hex,
        "session_id": session_id,
        "name": "correctness",
        "data_type": "numeric",
        "numeric_value": 0.91,
        "source": "evaluator",
        "metadata": { "suite": "integration" }
    });
    let score_req = Request::builder()
        .method("POST")
        .uri("/v1/llm/scores")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(score_body.to_string()))
        .unwrap();
    let score_resp = router.clone().oneshot(score_req).await.expect("score");
    assert_eq!(score_resp.status(), StatusCode::CREATED);

    let search_req = Request::builder()
        .method("POST")
        .uri("/v1/llm/observations/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "from": "2024-07-18T00:00:00Z",
                "to": "2024-07-20T00:00:00Z",
                "observation_types": ["generation"],
                "model_name": "gpt-4o",
                "user_id": "user-llm-1",
                "session_id": session_id,
                "limit": 50
            })
            .to_string(),
        ))
        .unwrap();
    let search_resp = router.clone().oneshot(search_req).await.expect("search");
    assert_eq!(search_resp.status(), StatusCode::OK);
    let search = response_json(search_resp).await;
    assert_eq!(search["items"].as_array().unwrap().len(), 1);
    assert_eq!(search["items"][0]["span_id"], span_hex);
    assert_eq!(search["items"][0]["observation_type"], "generation");
    assert_eq!(search["items"][0]["model_name"], "gpt-4o");
    assert_eq!(search["items"][0]["model_provider"], "openai");
    assert_eq!(search["items"][0]["user_id"], "user-llm-1");
    assert_eq!(search["items"][0]["input_tokens"], 12);
    assert_eq!(search["items"][0]["output_tokens"], 34);
    assert_eq!(search["items"][0]["total_tokens"], 46);
    assert!(
        (search["items"][0]["total_cost"].as_f64().unwrap_or(0.0) - 0.0123).abs() < 1e-9,
        "total_cost={}",
        search["items"][0]["total_cost"]
    );
    assert!(search["items"][0].get("attributes").is_none());

    // Variant key negative filter: wrong user_id must not match.
    let miss_req = Request::builder()
        .method("POST")
        .uri("/v1/llm/observations/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "from": "2024-07-18T00:00:00Z",
                "to": "2024-07-20T00:00:00Z",
                "observation_types": ["generation"],
                "user_id": "no-such-user",
                "session_id": session_id,
                "limit": 50
            })
            .to_string(),
        ))
        .unwrap();
    let miss_resp = router.clone().oneshot(miss_req).await.expect("miss search");
    assert_eq!(miss_resp.status(), StatusCode::OK);
    let miss = response_json(miss_resp).await;
    assert_eq!(miss["items"].as_array().unwrap().len(), 0);

    let obs_req = Request::builder()
        .uri(format!("/v1/llm/observations/{span_hex}"))
        .body(Body::empty())
        .unwrap();
    let obs_resp = router.clone().oneshot(obs_req).await.expect("observation");
    assert_eq!(obs_resp.status(), StatusCode::OK);
    let obs = response_json(obs_resp).await;
    assert_eq!(obs["span_id"], span_hex);
    assert_eq!(obs["attributes"]["sp.observation.type"], "generation");
    assert_eq!(obs["scores"].as_array().unwrap().len(), 1);
    assert_eq!(obs["scores"][0]["score_id"], "score-llm-query-1");

    let trace_req = Request::builder()
        .uri(format!("/v1/llm/traces/{trace_hex}"))
        .body(Body::empty())
        .unwrap();
    let trace_resp = router.clone().oneshot(trace_req).await.expect("trace");
    assert_eq!(trace_resp.status(), StatusCode::OK);
    let trace = response_json(trace_resp).await;
    assert_eq!(trace["trace"]["trace_id"], trace_hex);
    assert_eq!(trace["trace"]["observation_count"], 1);
    assert_eq!(trace["observations"].as_array().unwrap().len(), 1);
    assert_eq!(trace["scores"].as_array().unwrap().len(), 1);

    let session_req = Request::builder()
        .uri(format!(
            "/v1/llm/sessions/{session_id}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z"
        ))
        .body(Body::empty())
        .unwrap();
    let session_resp = router.clone().oneshot(session_req).await.expect("session");
    assert_eq!(session_resp.status(), StatusCode::OK);
    let session = response_json(session_resp).await;
    assert_eq!(session["session_id"], session_id);
    assert_eq!(session["trace_count"], 1);
    assert_eq!(session["observation_count"], 1);
    assert_eq!(session["traces"][0]["trace_id"], trace_hex);
    assert_eq!(session["scores"].as_array().unwrap().len(), 1);

    let missing = Request::builder()
        .uri("/v1/llm/observations/does-not-exist")
        .body(Body::empty())
        .unwrap();
    let missing_resp = router.oneshot(missing).await.expect("missing");
    assert_eq!(missing_resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn logs_promote_scope_name_to_logger_name_attribute() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-logger-name-promote";

    let payload = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout-api"),
                    string_kv("sp.session.id", session_id),
                ],
                ..Default::default()
            }),
            scope_logs: vec![
                ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "agent.transform.success".to_string(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_777_802_601_000_000_000,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "promoted-from-scope".to_string(),
                            )),
                        }),
                        attributes: vec![string_kv("sp.session.id", session_id)],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                },
                ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope.should.not.win".to_string(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_777_802_602_000_000_000,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "explicit-attribute-wins".to_string(),
                            )),
                        }),
                        attributes: vec![
                            string_kv("sp.session.id", session_id),
                            string_kv("logger_name", "explicit.logger"),
                        ],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                },
            ],
            schema_url: String::new(),
        }],
    };

    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_string(&payload).unwrap()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("ingest");
    assert_eq!(resp.status(), StatusCode::OK);

    state
        .engine_for_id("")
        .await
        .expect("engine")
        .ingest
        .force_flush_logs()
        .await
        .expect("flush logs");

    // CAST keeps this green under both MAP and VARIANT attribute storage.
    let sql = format!(
        "SELECT body, CAST(attributes['logger_name'] AS VARCHAR) AS logger_name \
         FROM union_logs WHERE session_id = '{session_id}' ORDER BY timestamp ASC"
    );
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "sql": sql }).to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.expect("query");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = response_json(resp).await;
    let rows = v["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 2, "{v}");
    assert_eq!(rows[0][0], "promoted-from-scope");
    assert_eq!(rows[0][1], "agent.transform.success");
    assert_eq!(rows[1][0], "explicit-attribute-wins");
    assert_eq!(rows[1][1], "explicit.logger");
}
