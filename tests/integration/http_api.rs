//! Integration tests for the OTLP HTTP surface (`AppPipeline` router — same handlers as production without `main` middleware layers).

use axum::body::Body;
use axum::http::{header, Request, Response, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use softprobe_runtime::api::{AppPipeline, AppState};
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;

async fn build_router() -> (Router, tempfile::TempDir) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let app = AppPipeline::new(&config).await.expect("app pipeline");
    let router = app.into_router().await.expect("router");
    (router, temp)
}

async fn build_router_and_state() -> (Router, AppState, tempfile::TempDir) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let app = AppPipeline::new(&config).await.expect("app pipeline");
    let (router, state) = softprobe_runtime::api::create_router(
        app.storage,
        app.query_engine,
        Some(app.span_buffer),
        Some(app.log_buffer),
        Some(app.metric_buffer),
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

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn int_kv(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
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

    state
        .span_buffer
        .as_ref()
        .expect("span buffer")
        .force_flush()
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

    state
        .span_buffer
        .as_ref()
        .expect("span buffer")
        .force_flush()
        .await
        .expect("flush spans");
    state
        .log_buffer
        .as_ref()
        .expect("log buffer")
        .force_flush()
        .await
        .expect("flush logs");
    state
        .metric_buffer
        .as_ref()
        .expect("metric buffer")
        .force_flush()
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
