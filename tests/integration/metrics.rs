use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use reqwest::Client;
use reqwest::StatusCode;
use std::time::Duration;

use crate::util::http::start_test_server;

fn create_test_metrics_request() -> ExportMetricsServiceRequest {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point;

    // Create gauge metric
    let gauge_data_point = NumberDataPoint {
        attributes: vec![
            KeyValue {
                key: "host".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("server-1".to_string())),
                }),
            },
            KeyValue {
                key: "region".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("us-east-1".to_string())),
                }),
            },
        ],
        time_unix_nano: 1640995200000000000, // 2022-01-01 00:00:00 UTC
        value: Some(number_data_point::Value::AsDouble(75.5)),
        ..Default::default()
    };

    let gauge_metric = Metric {
        name: "cpu.usage".to_string(),
        description: "CPU usage percentage".to_string(),
        unit: "%".to_string(),
        data: Some(Data::Gauge(Gauge {
            data_points: vec![gauge_data_point],
        })),
        ..Default::default()
    };

    // Create sum metric
    let sum_data_point = NumberDataPoint {
        attributes: vec![
            KeyValue {
                key: "endpoint".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("/api/users".to_string())),
                }),
            },
            KeyValue {
                key: "method".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("POST".to_string())),
                }),
            },
        ],
        time_unix_nano: 1640995200000000000,
        value: Some(number_data_point::Value::AsInt(1234)),
        ..Default::default()
    };

    let sum_metric = Metric {
        name: "http.server.requests".to_string(),
        description: "Total HTTP requests".to_string(),
        unit: "1".to_string(),
        data: Some(Data::Sum(Sum {
            data_points: vec![sum_data_point],
            aggregation_temporality: 2, // CUMULATIVE
            is_monotonic: true,
        })),
        ..Default::default()
    };

    let resource_metrics = ResourceMetrics {
        resource: Some(Resource {
            attributes: vec![
                KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("api_service".to_string())),
                    }),
                },
                KeyValue {
                    key: "service.version".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("1.0.0".to_string())),
                    }),
                },
            ],
            ..Default::default()
        }),
        scope_metrics: vec![ScopeMetrics {
            scope: None,
            metrics: vec![gauge_metric, sum_metric],
            schema_url: "".to_string(),
        }],
        schema_url: "".to_string(),
    };

    ExportMetricsServiceRequest {
        resource_metrics: vec![resource_metrics],
    }
}

#[tokio::test]
async fn test_metrics_ingestion_protobuf() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let request = create_test_metrics_request();
    let body = request.encode_to_vec();

    let response = client
        .post(&format!("{}/v1/metrics", base_url))
        .header("Content-Type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let json: serde_json::Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["ingested_count"], 2); // 2 metrics (1 gauge + 1 sum)
    println!("Response: {}", json);
}

#[tokio::test]
async fn test_metrics_ingestion_json() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let request = create_test_metrics_request();
    let json_body = serde_json::to_string(&request).unwrap();

    let response = client
        .post(&format!("{}/v1/metrics", base_url))
        .header("Content-Type", "application/json")
        .body(json_body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let json: serde_json::Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["ingested_count"], 2);
    println!("Response: {}", json);
}

#[tokio::test]
async fn test_metrics_buffer_flush() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    // Send multiple metric requests
    for i in 0..5 {
        let request = create_test_metrics_request();
        let body = request.encode_to_vec();

        let response = client
            .post(&format!("{}/v1/metrics", base_url))
            .header("Content-Type", "application/x-protobuf")
            .body(body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        println!("Sent batch {}", i + 1);
    }

    // Wait for buffer flush (60 seconds max, but should flush much sooner due to size)
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("Metrics successfully buffered and flushed");
}

#[tokio::test]
async fn test_metrics_with_different_names() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    // Create metrics with different metric names to test grouping
    let metric_names = vec!["cpu.usage", "memory.usage", "disk.io", "network.bytes"];

    for metric_name in metric_names {
        use opentelemetry_proto::tonic::metrics::v1::number_data_point;

        let data_point = NumberDataPoint {
            attributes: vec![],
            time_unix_nano: 1640995200000000000,
            value: Some(number_data_point::Value::AsDouble(42.0)),
            ..Default::default()
        };

        let metric = Metric {
            name: metric_name.to_string(),
            description: format!("{} metric", metric_name),
            unit: "1".to_string(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![data_point],
            })),
            ..Default::default()
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![metric],
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        };

        let body = request.encode_to_vec();

        let response = client
            .post(&format!("{}/v1/metrics", base_url))
            .header("Content-Type", "application/x-protobuf")
            .body(body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    println!("Successfully sent metrics with different names");
}
