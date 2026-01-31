use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{Span, Status};
use prost::Message;
use reqwest::Client;
use reqwest::StatusCode;
use serde_json;
use uuid::Uuid;
mod util;
use util::http::start_test_server;
use util::otlp::create_test_otlp_request;

// Note: `create_test_otlp_request` and `start_test_server` live under `tests/util/`.

#[tokio::test]
async fn test_health_endpoint() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let response = client
        .get(&format!("{}/health", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_ready_endpoint() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let response = client
        .get(&format!("{}/ready", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_traces_json_endpoint_success() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();
    let sid = format!("it-{}", Uuid::new_v4());
    let mut otlp_request = create_test_otlp_request();
    {
        let span = &mut otlp_request.resource_spans[0].scope_spans[0].spans[0];
        if let Some(kv) = span
            .attributes
            .iter_mut()
            .find(|kv| kv.key == "sp.session.id")
        {
            kv.value = Some(AnyValue {
                value: Some(any_value::Value::StringValue(sid.clone())),
            });
        }
    }

    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Parse response
    let response_json: serde_json::Value = response.json().await.unwrap();

    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 1);
    assert!(response_json["message"]
        .as_str()
        .unwrap()
        .contains("Successfully ingested"));
}

#[tokio::test]
async fn test_traces_protobuf_endpoint_success() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();
    let otlp_request = create_test_otlp_request();

    let body = otlp_request.encode_to_vec();

    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Parse response as JSON body
    let response_json: serde_json::Value = response.json().await.unwrap();
    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 1);
}

#[tokio::test]
async fn test_traces_json_endpoint_multiple_spans() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    // Create request with multiple spans
    let mut otlp_request = create_test_otlp_request();

    // Add another span to the same resource
    let span2 = Span {
        trace_id: vec![
            0x22, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ],
        span_id: vec![0x21, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
        parent_span_id: vec![],
        name: "another_operation".to_string(),
        kind: 1, // SPAN_KIND_INTERNAL
        start_time_unix_nano: 1640995200000000000,
        end_time_unix_nano: 1640995260000000000,
        attributes: vec![KeyValue {
            key: "sp.session.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "test_session_123".to_string(),
                )),
            }),
        }],
        events: vec![],
        status: Some(Status {
            code: 1,
            message: "".to_string(),
        }),
        ..Default::default()
    };

    otlp_request.resource_spans[0].scope_spans[0]
        .spans
        .push(span2);
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let response_json: serde_json::Value = response.json().await.unwrap();

    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 2);
}

#[tokio::test]
async fn test_traces_json_endpoint_empty_request() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let empty_request = ExportTraceServiceRequest {
        resource_spans: vec![],
    };

    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&empty_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let response_json: serde_json::Value = response.json().await.unwrap();

    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 0);
}

#[tokio::test]
async fn test_traces_json_endpoint_invalid_json() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .body("invalid json")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_traces_with_missing_app_id() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    // Create request without sp.app.id
    let mut otlp_request = create_test_otlp_request();
    otlp_request.resource_spans[0].resource = Some(Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "fallback_service".to_string(),
                )),
            }),
        }],
        ..Default::default()
    });
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let response_json: serde_json::Value = response.json().await.unwrap();

    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 1);
}

#[tokio::test]
async fn test_traces_session_grouping() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    // Create multiple spans with the same session ID
    let mut otlp_request = create_test_otlp_request();

    // Modify the existing span and add more spans with same session
    for i in 2..=5 {
        let span = Span {
            trace_id: vec![
                i, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                0x77, 0x88,
            ],
            span_id: vec![i, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
            parent_span_id: vec![],
            name: format!("operation_{}", i),
            kind: 1,
            start_time_unix_nano: 1640995200000000000 + (i as u64 * 1000000000),
            end_time_unix_nano: 1640995260000000000 + (i as u64 * 1000000000),
            attributes: vec![KeyValue {
                key: "sp.session.id".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(
                        "test_session_123".to_string(),
                    )),
                }),
            }],
            events: vec![],
            status: Some(Status {
                code: 1,
                message: "".to_string(),
            }),
            ..Default::default()
        };

        otlp_request.resource_spans[0].scope_spans[0]
            .spans
            .push(span);
    }
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let response_json: serde_json::Value = response.json().await.unwrap();

    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 5); // All 5 spans should be processed
}

#[tokio::test]
async fn test_endpoint_not_found() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let response = client
        .get(&format!("{}/nonexistent", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
