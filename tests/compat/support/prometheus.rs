//! Shared helpers for Prometheus compatibility contract / integration tests.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use tower::ServiceExt;

/// Build a simple gauge OTLP payload for Prometheus discovery/query tests.
pub fn gauge_otlp(metric_name: &str, job: &str, value: f64, time_unix_nano: u64) -> Vec<u8> {
    gauge_series_otlp(metric_name, job, &[(time_unix_nano, value)])
}

/// Build a gauge with multiple timestamped samples (same series).
pub fn gauge_series_otlp(metric_name: &str, job: &str, samples: &[(u64, f64)]) -> Vec<u8> {
    let data_points: Vec<NumberDataPoint> = samples
        .iter()
        .map(|(ts, value)| NumberDataPoint {
            attributes: vec![],
            start_time_unix_nano: 0,
            time_unix_nano: *ts,
            exemplars: vec![],
            flags: 0,
            value: Some(number_data_point::Value::AsDouble(*value)),
        })
        .collect();
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(job.into())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.into(),
                    description: "test gauge".into(),
                    unit: "1".into(),
                    data: Some(Data::Gauge(Gauge { data_points })),
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

/// Build a gauge OTLP payload with arbitrary datapoint labels (no `service.name`).
///
/// Use this for promqltest loads so projected labels match Prometheus series labels.
pub fn gauge_labeled_otlp(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, f64)],
) -> Vec<u8> {
    number_series_otlp(metric_name, labels, samples, false)
}

/// Cumulative sum (counter-like) with arbitrary datapoint labels.
pub fn sum_labeled_otlp(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, f64)],
) -> Vec<u8> {
    number_series_otlp(metric_name, labels, samples, true)
}

fn number_series_otlp(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, f64)],
    is_sum: bool,
) -> Vec<u8> {
    let attrs: Vec<KeyValue> = labels
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.clone())),
            }),
        })
        .collect();
    let data_points: Vec<NumberDataPoint> = samples
        .iter()
        .map(|(ts, value)| NumberDataPoint {
            attributes: attrs.clone(),
            start_time_unix_nano: 0,
            time_unix_nano: *ts,
            exemplars: vec![],
            flags: 0,
            value: Some(number_data_point::Value::AsDouble(*value)),
        })
        .collect();
    let data = if is_sum {
        Data::Sum(Sum {
            data_points,
            aggregation_temporality: 2, // CUMULATIVE
            is_monotonic: true,
        })
    } else {
        Data::Gauge(Gauge { data_points })
    };
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.into(),
                    description: "promqltest".into(),
                    unit: "1".into(),
                    data: Some(data),
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

/// Build a cumulative sum (counter-like) series with `service.name` → job projection.
pub fn sum_series_otlp(metric_name: &str, job: &str, samples: &[(u64, f64)]) -> Vec<u8> {
    let data_points: Vec<NumberDataPoint> = samples
        .iter()
        .map(|(ts, value)| NumberDataPoint {
            attributes: vec![],
            start_time_unix_nano: 0,
            time_unix_nano: *ts,
            exemplars: vec![],
            flags: 0,
            value: Some(number_data_point::Value::AsDouble(*value)),
        })
        .collect();
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(job.into())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.into(),
                    description: "test counter".into(),
                    unit: "1".into(),
                    data: Some(Data::Sum(Sum {
                        data_points,
                        aggregation_temporality: 2, // CUMULATIVE
                        is_monotonic: true,
                    })),
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

pub async fn ingest_metrics(router: &Router, body: Vec<u8>) {
    ingest_metrics_as(router, body, None).await;
}

pub async fn ingest_metrics_as(router: &Router, body: Vec<u8>, tenant_id: Option<&str>) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf");
    if let Some(tid) = tenant_id {
        builder = builder.header("x-test-tenant-id", tid);
    }
    let resp = router
        .clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    if status != StatusCode::OK {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap_or_default();
        panic!(
            "OTLP metrics ingest failed: status={status} body={}",
            String::from_utf8_lossy(&bytes)
        );
    }
}

pub async fn get_json(router: &Router, path: &str) -> (StatusCode, serde_json::Value) {
    get_json_as(router, path, None).await
}

pub async fn get_json_as(
    router: &Router,
    path: &str,
    tenant_id: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder().method("GET").uri(path);
    if let Some(tid) = tenant_id {
        builder = builder.header("x-test-tenant-id", tid);
    }
    let resp = router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Encode `application/x-www-form-urlencoded` query components (Prom-style `+` for space).
pub fn encode_query_pairs(params: &[(&str, &str)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", encode_component(k), encode_component(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Encode owned key/value pairs the same way as [`encode_query_pairs`].
pub fn encode_query_owned(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", encode_component(k), encode_component(v)))
        .collect::<Vec<_>>()
        .join("&")
}

fn encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
