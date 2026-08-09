//! Integration tests for the OTLP HTTP surface (in-process router — same handlers as
//! production without `main` middleware layers). Engines are lazy via RuntimeEngineManager.

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
use softprobe_runtime::api::AppState;
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
    let config = Arc::new(file_backed_test_config(&temp));
    let (router, state) = softprobe_runtime::api::create_router(
        config,
        axum::routing::post(softprobe_runtime::api::ingestion::traces::ingest_traces),
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

/// Full keyset pagination of `/v1/llm/sessions/search`, asserting every session
/// is seen exactly once.
///
/// The existing coverage for this endpoint asserts on generated SQL strings,
/// which cannot catch the failure this pins: cursor literals were rendered at
/// millisecond precision while `start_time` is a microsecond `MIN(timestamp)`,
/// so the predicate came out below the true value and silently dropped every
/// session sharing that millisecond. The last page returned zero rows with a
/// null `next_cursor` and no error -- data loss that looks exactly like
/// "reached the end". Sessions here deliberately sit at sub-millisecond
/// offsets, including two in the same millisecond to exercise the session_id
/// tiebreak.
#[tokio::test]
async fn llm_sessions_search_pages_without_dropping_rows() {
    let (router, state, _t) = build_router_and_state().await;

    // Microsecond offsets within a single millisecond window.
    let offsets_us: [u64; 6] = [0, 400, 456, 1_000, 1_000, 1_500];
    let base_ns: u64 = 1_721_349_720_000_000_000;
    for (i, off) in offsets_us.iter().enumerate() {
        let mut request = llm_generation_request(
            &format!("sess-page-{i}"),
            [0x70 + i as u8; 16],
            [0x80 + i as u8; 8],
        );
        let span = &mut request.resource_spans[0].scope_spans[0].spans[0];
        span.start_time_unix_nano = base_ns + off * 1_000;
        span.end_time_unix_nano = span.start_time_unix_nano + 1_000_000;
        let mut buf = Vec::new();
        request.encode(&mut buf).expect("encode");
        let req = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(buf))
            .unwrap();
        let resp = router.clone().oneshot(req).await.expect("ingest");
        assert_eq!(resp.status(), StatusCode::OK);
    }
    state
        .engine_for_id("")
        .await
        .expect("engine")
        .ingest
        .force_flush_spans()
        .await
        .expect("flush spans");

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    // Bounded so a cursor that fails to advance fails the test instead of
    // looping forever.
    for page in 0..10 {
        let mut body = json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "order_by": "start_time",
            "order": "desc",
            "limit": 2
        });
        if let Some(c) = &cursor {
            body["cursor"] = json!(c);
        }
        let req = Request::builder()
            .method("POST")
            .uri("/v1/llm/sessions/search")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = router.clone().oneshot(req).await.expect("search");
        assert_eq!(resp.status(), StatusCode::OK, "page {page}");
        let v = response_json(resp).await;

        for item in v["items"].as_array().expect("items") {
            seen.push(item["session_id"].as_str().expect("session_id").to_string());
        }
        match v["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
    }

    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        seen.len(),
        "pagination returned duplicates: {seen:?}"
    );
    assert_eq!(
        unique.len(),
        offsets_us.len(),
        "pagination dropped sessions: saw {seen:?}"
    );
}

/// Guards the DuckDB floor set in Cargo.toml.
///
/// DuckDB 1.5.2 crashes with "INTERNAL Error: Attempted to access index 0
/// within vector of size 0" when the ducklake reader hits an empty-array
/// VARIANT value, and then invalidates the whole database so every later
/// query on that connection fails until the process restarts. Production ran
/// 1.5.2 with 84% of the `events` column equal to `[]`; on 2026-08-03 one
/// detail request took the entire query layer down for hours.
///
/// Spans without events serialize to exactly that shape, so this reads one
/// back end-to-end. It passes on 1.5.5 and fails on 1.5.2 -- which is the
/// point: it is the executable form of the version floor.
#[tokio::test]
async fn spans_without_events_are_readable() {
    let (router, state, _t) = build_router_and_state().await;
    let session_id = "sess-empty-events";
    let span_hex = hex::encode([0x91u8; 8]);

    // llm_generation_request leaves `events` empty -> `[]` once stored.
    let mut buf = Vec::new();
    llm_generation_request(session_id, [0x90; 16], [0x91; 8])
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("ingest");
    assert_eq!(resp.status(), StatusCode::OK);
    state
        .engine_for_id("")
        .await
        .expect("engine")
        .ingest
        .force_flush_spans()
        .await
        .expect("flush spans");

    // The detail endpoint projects events; on 1.5.2 this is where it died.
    let req = Request::builder()
        .uri(format!("/v1/llm/observations/{span_hex}"))
        .body(Body::empty())
        .unwrap();
    let resp = router
        .clone()
        .oneshot(req)
        .await
        .expect("observation detail");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "reading a span whose events are an empty array must not fail"
    );
    let obs = response_json(resp).await;
    assert_eq!(obs["span_id"], span_hex);
    assert!(
        obs["events"].as_array().is_none_or(|e| e.is_empty()),
        "expected no events, got: {}",
        obs["events"]
    );

    // And again through search, which projects a different column set.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/llm/observations/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "from": "2024-07-18T00:00:00Z",
                "to": "2024-07-20T00:00:00Z",
                "session_id": session_id,
                "limit": 10
            })
            .to_string(),
        ))
        .unwrap();
    let resp = router.oneshot(req).await.expect("search");
    assert_eq!(resp.status(), StatusCode::OK);
    let found = response_json(resp).await;
    assert_eq!(
        found["items"].as_array().expect("items").len(),
        1,
        "{found}"
    );
}

/// Pins DuckLake data-inlining behavior across a maintenance pass -- the
/// 2026-08-03 production outage shape. Collector-sized batches are meant to
/// be inlined into the catalog (`data_inlining_row_limit`, default 10_000),
/// and reads of inlined rows go through the ducklake extension's inlined-data
/// reader; in production (postgres catalog) that reader crashed with
/// "INTERNAL Error: Attempted to access index 0 within vector of size 0" and
/// DuckDB invalidated the whole database. Until this test existed no CI path
/// ever read inlined data back, let alone after maintenance ran over it.
///
/// Two facts are pinned, discovered while writing this test:
/// - Tables with a VARIANT column (traces/logs/metrics since the VARIANT
///   attribute migration) are NOT inlined at all -- tiny span batches write
///   Parquet despite the config. If a ducklake upgrade starts inlining
///   VARIANT tables, the first assertion fails and forces a conscious look.
/// - Tables without VARIANT (scores: MAP metadata) DO inline, so scores are
///   the live inlined read/write path this test exercises across maintenance.
#[tokio::test]
async fn inlined_data_stays_readable_across_maintenance() {
    use softprobe_runtime::compaction::executor::MaintenanceExecutor;

    fn parquet_count(dir: &std::path::Path) -> usize {
        let mut n = 0;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    n += parquet_count(&path);
                } else if path.extension().is_some_and(|e| e == "parquet") {
                    n += 1;
                }
            }
        }
        n
    }

    let temp = tempfile::TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    // Explicit, not inherited from Config::default(): the inlined path is the
    // point, and this test must keep covering it even if the default flips
    // to 0 (the post-outage production setting).
    config.ducklake.data_inlining_row_limit = Some(10_000);
    // file_backed_test_config turns maintenance OFF for quiet tests. Without
    // these two the maintenance step below is a no-op loop that reports
    // Skipped for every table, and the "across a maintenance pass" claim in
    // this test's name would be theatre.
    config.maintenance.enabled = true;
    config.maintenance.metadata_enabled = true;
    let config = Arc::new(config);
    let (router, state) = softprobe_runtime::api::create_router(
        config.clone(),
        axum::routing::post(softprobe_runtime::api::ingestion::traces::ingest_traces),
        None,
    )
    .await
    .expect("router");

    let session_id = "sess-inline-maintenance";
    let trace_hex = hex::encode([0x51u8; 16]);
    let span_hex = hex::encode([0x61u8; 8]);
    let data_dir = temp.path().join("ducklake").join("data");

    // 1. One collector-sized span batch. VARIANT attribute columns opt the
    //    traces table out of inlining entirely, so this must land as Parquet.
    let mut buf = Vec::new();
    llm_generation_request(session_id, [0x51; 16], [0x61; 8])
        .encode(&mut buf)
        .expect("encode");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("ingest");
    assert_eq!(resp.status(), StatusCode::OK);
    state
        .engine_for_id("")
        .await
        .expect("engine")
        .ingest
        .force_flush_spans()
        .await
        .expect("flush spans");
    assert!(
        parquet_count(&data_dir.join("main").join("traces")) >= 1,
        "traces (VARIANT attributes) were inlined -- ducklake behavior changed, \
         re-evaluate inlining coverage and the inlined-reader risk for spans"
    );

    // 2. One score -> the scores table has no VARIANT column, so this row
    //    must be inlined into the catalog, not written as Parquet.
    let score_body = json!({
        "score_id": "score-inline-1",
        "timestamp": "2024-07-19T00:02:00Z",
        "trace_id": trace_hex,
        "span_id": span_hex,
        "session_id": session_id,
        "name": "correctness",
        "data_type": "numeric",
        "numeric_value": 0.91,
        "source": "evaluator",
        "metadata": { "suite": "inline" }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/llm/scores")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(score_body.to_string()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("score");
    assert_eq!(resp.status(), StatusCode::CREATED);
    // `parquet_count` returns 0 for a missing directory, so "scores has no
    // Parquet" alone would also pass if the layout assumption
    // (<data_path>/<metadata_schema>/<table>/) ever broke. Anchor on the
    // traces directory existing to tell the two apart.
    let scores_dir = data_dir.join("main").join("scores");
    assert!(
        data_dir.join("main").join("traces").is_dir(),
        "expected <data_path>/main/<table>/ layout; found: {:?}",
        std::fs::read_dir(&data_dir).map(|e| e.flatten().map(|x| x.path()).collect::<Vec<_>>())
    );
    assert_eq!(
        parquet_count(&scores_dir),
        0,
        "score batch wrote Parquet -- inlining is not active, this test no \
         longer covers the inlined read path"
    );

    // 3. Inlined read #1: observation detail joins scores through the query
    //    engine's ducklake attachment.
    let req = Request::builder()
        .uri(format!("/v1/llm/observations/{span_hex}"))
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("observation");
    assert_eq!(resp.status(), StatusCode::OK);
    let obs = response_json(resp).await;
    assert_eq!(obs["scores"].as_array().expect("scores").len(), 1, "{obs}");
    assert_eq!(obs["scores"][0]["score_id"], "score-inline-1");

    // 4. A maintenance pass over the same catalog (production runs this
    //    hourly; the outage query came 23 minutes after one).
    //    run_once_ducklake funnels every failure into warn! + Skipped, so
    //    `.expect()` can never fire -- assert on the summary instead, or a
    //    pass that did nothing at all would look like success.
    let maintenance = MaintenanceExecutor::new(config.as_ref(), None, None)
        .await
        .expect("maintenance executor");
    let summary = maintenance.run_once().await.expect("maintenance run");
    // `table` is `<metadata_schema>.<table>` (executor.rs builds `table_ident`).
    let scores_result = summary
        .tables
        .iter()
        .find(|t| t.table.ends_with(".scores"))
        .unwrap_or_else(|| panic!("no scores entry in maintenance summary: {summary:?}"));
    assert!(
        !scores_result.metadata.skipped,
        "maintenance skipped metadata for scores -- the pass did nothing, so \
         the assertions below prove nothing: {summary:?}"
    );

    // 5. Inlined read #2, after maintenance -- the production crash site.
    let req = Request::builder()
        .uri(format!("/v1/llm/observations/{span_hex}"))
        .body(Body::empty())
        .unwrap();
    let resp = router
        .clone()
        .oneshot(req)
        .await
        .expect("observation after maintenance");
    assert_eq!(resp.status(), StatusCode::OK);
    let obs = response_json(resp).await;
    assert_eq!(
        obs["scores"].as_array().expect("scores").len(),
        1,
        "inlined score disappeared after maintenance: {obs}"
    );

    // 6. Inlined write + read after maintenance.
    let score_body = json!({
        "score_id": "score-inline-2",
        "timestamp": "2024-07-19T00:03:00Z",
        "trace_id": trace_hex,
        "span_id": span_hex,
        "session_id": session_id,
        "name": "helpfulness",
        "data_type": "numeric",
        "numeric_value": 0.8,
        "source": "evaluator",
        "metadata": { "suite": "inline" }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/llm/scores")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(score_body.to_string()))
        .unwrap();
    let resp = router
        .clone()
        .oneshot(req)
        .await
        .expect("score after maintenance");
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .uri(format!("/v1/llm/observations/{span_hex}"))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("final observation");
    assert_eq!(resp.status(), StatusCode::OK);
    let obs = response_json(resp).await;
    assert_eq!(
        obs["scores"].as_array().expect("scores").len(),
        2,
        "post-maintenance inlined write not visible: {obs}"
    );
}
