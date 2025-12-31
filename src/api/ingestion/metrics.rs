use axum::{extract::State, Json};
use axum::http::{HeaderMap, header::CONTENT_TYPE, StatusCode};
use axum::response::{IntoResponse, Response};
use crate::api::AppState;
use crate::api::ingestion::IngestResponse;
use crate::models::Metric;
use opentelemetry_proto::tonic::metrics::v1::{
    ResourceMetrics, Metric as OtlpMetric, NumberDataPoint, HistogramDataPoint, SummaryDataPoint,
    metric::Data,
};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use std::collections::HashMap;
use tracing::{error, info, warn};
use anyhow::Result;

/// OTLP /v1/metrics JSON handler
pub async fn ingest_metrics_json(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Json<IngestResponse> {
    let body_size = body.len();
    match serde_json::from_slice::<ExportMetricsServiceRequest>(&body) {
        Ok(request) => match process_metrics(state, request, body_size).await {
            Ok(count) => Json(IngestResponse {
                success: true,
                ingested_count: count,
                message: format!("Successfully ingested {} metric data points", count),
            }),
            Err(e) => {
                error!("Failed to process OTLP metrics: {}", e);
                Json(IngestResponse {
                    success: false,
                    ingested_count: 0,
                    message: format!("Ingestion failed: {}", e),
                })
            }
        },
        Err(e) => {
            error!("Failed to decode JSON: {}", e);
            Json(IngestResponse {
                success: false,
                ingested_count: 0,
                message: format!("JSON decode failed: {}", e),
            })
        }
    }
}

/// OTLP /v1/metrics protobuf handler
pub async fn ingest_metrics_protobuf(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Json<IngestResponse> {
    let body_size = body.len();
    match prost::Message::decode(body.as_ref()) {
        Ok(request) => match process_metrics(state, request, body_size).await {
            Ok(count) => Json(IngestResponse {
                success: true,
                ingested_count: count,
                message: format!("Successfully ingested {} metric data points", count),
            }),
            Err(e) => {
                error!("Failed to process OTLP metrics: {}", e);
                Json(IngestResponse {
                    success: false,
                    ingested_count: 0,
                    message: format!("Ingestion failed: {}", e),
                })
            }
        },
        Err(e) => {
            error!("Failed to decode protobuf: {}", e);
            Json(IngestResponse {
                success: false,
                ingested_count: 0,
                message: format!("Protobuf decode failed: {}", e),
            })
        }
    }
}

/// Unified OTLP /v1/metrics handler that switches on Content-Type
pub async fn ingest_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let body_size = body.len();
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    // Protobuf
    if content_type.contains("protobuf") || content_type.contains("application/x-protobuf") {
        match prost::Message::decode(body.as_ref()) {
            Ok(request) => match process_metrics(state, request, body_size).await {
                Ok(count) => Json(IngestResponse {
                    success: true,
                    ingested_count: count,
                    message: format!("Successfully ingested {} metric data points", count),
                }).into_response(),
                Err(e) => {
                    error!("Failed to process OTLP metrics: {}", e);
                    Json(IngestResponse {
                        success: false,
                        ingested_count: 0,
                        message: format!("Ingestion failed: {}", e),
                    }).into_response()
                }
            },
            Err(e) => {
                error!("Failed to decode protobuf: {}", e);
                (StatusCode::BAD_REQUEST, "Invalid protobuf").into_response()
            }
        }
    } else {
        // JSON (application/json)
        match serde_json::from_slice::<ExportMetricsServiceRequest>(&body) {
            Ok(request) => match process_metrics(state, request, body_size).await {
                Ok(count) => Json(IngestResponse {
                    success: true,
                    ingested_count: count,
                    message: format!("Successfully ingested {} metric data points", count),
                }).into_response(),
                Err(e) => {
                    error!("Failed to process OTLP metrics: {}", e);
                    Json(IngestResponse {
                        success: false,
                        ingested_count: 0,
                        message: format!("Ingestion failed: {}", e),
                    }).into_response()
                }
            },
            Err(_) => (StatusCode::BAD_REQUEST, "Invalid JSON").into_response(),
        }
    }
}

/// Core OTLP metrics processing logic
async fn process_metrics(
    state: AppState,
    request: ExportMetricsServiceRequest,
    body_size: usize,
) -> Result<usize> {
    let mut metrics = Vec::new();

    for resource_metrics in request.resource_metrics {
        let resource_attributes = extract_resource_attributes(&resource_metrics);

        for scope_metrics in resource_metrics.scope_metrics {
            for otlp_metric in scope_metrics.metrics {
                let metric_data_points = convert_otlp_metric_to_metrics(
                    &otlp_metric,
                    &resource_attributes,
                )?;
                metrics.extend(metric_data_points);
            }
        }
    }

    let metric_count = metrics.len();

    // Add all metrics to the buffer in one batch with body size tracking
    if let Some(metric_buffer) = &state.metric_buffer {
        metric_buffer.add_items(metrics, body_size).await?;
    } else {
        warn!("Metric buffer not initialized");
    }

    info!("Processed {} metric data points from OTLP request ({} bytes)", metric_count, body_size);
    Ok(metric_count)
}

/// Extract resource attributes from ResourceMetrics
fn extract_resource_attributes(resource_metrics: &ResourceMetrics) -> HashMap<String, String> {
    let mut attributes = HashMap::new();

    if let Some(resource) = &resource_metrics.resource {
        for attr in &resource.attributes {
            if let Some(value) = &attr.value {
                let value_str = match value.value.as_ref() {
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                    _ => continue,
                };
                attributes.insert(attr.key.clone(), value_str);
            }
        }
    }

    attributes
}

/// Convert OTLP Metric to our internal Metric format
/// Returns a Vec because a single OTLP metric can contain multiple data points
fn convert_otlp_metric_to_metrics(
    otlp_metric: &OtlpMetric,
    resource_attributes: &HashMap<String, String>,
) -> Result<Vec<Metric>> {
    let mut metrics = Vec::new();

    let metric_name = otlp_metric.name.clone();
    let description = otlp_metric.description.clone();
    let unit = otlp_metric.unit.clone();

    // Determine metric type and extract data points
    if let Some(data) = &otlp_metric.data {
        match data {
            Data::Gauge(gauge) => {
                for data_point in &gauge.data_points {
                    if let Some(metric) = convert_number_data_point(
                        data_point,
                        &metric_name,
                        &description,
                        &unit,
                        "gauge",
                        resource_attributes,
                    ) {
                        metrics.push(metric);
                    }
                }
            }
            Data::Sum(sum) => {
                for data_point in &sum.data_points {
                    if let Some(metric) = convert_number_data_point(
                        data_point,
                        &metric_name,
                        &description,
                        &unit,
                        "sum",
                        resource_attributes,
                    ) {
                        metrics.push(metric);
                    }
                }
            }
            Data::Histogram(histogram) => {
                for data_point in &histogram.data_points {
                    if let Some(metric) = convert_histogram_data_point(
                        data_point,
                        &metric_name,
                        &description,
                        &unit,
                        resource_attributes,
                    ) {
                        metrics.push(metric);
                    }
                }
            }
            Data::Summary(summary) => {
                for data_point in &summary.data_points {
                    if let Some(metric) = convert_summary_data_point(
                        data_point,
                        &metric_name,
                        &description,
                        &unit,
                        resource_attributes,
                    ) {
                        metrics.push(metric);
                    }
                }
            }
            Data::ExponentialHistogram(_) => {
                // Simplified: treat as histogram with sum value
                warn!("ExponentialHistogram not fully supported, skipping");
            }
        }
    }

    Ok(metrics)
}

/// Convert a NumberDataPoint to our Metric format
fn convert_number_data_point(
    data_point: &NumberDataPoint,
    metric_name: &str,
    description: &str,
    unit: &str,
    metric_type: &str,
    resource_attributes: &HashMap<String, String>,
) -> Option<Metric> {
    // Extract timestamp
    let timestamp = if data_point.time_unix_nano > 0 {
        chrono::DateTime::from_timestamp(
            (data_point.time_unix_nano / 1_000_000_000) as i64,
            (data_point.time_unix_nano % 1_000_000_000) as u32,
        ).unwrap_or_else(|| chrono::Utc::now())
    } else {
        chrono::Utc::now()
    };

    // Extract value
    let value = match &data_point.value {
        Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v)) => *v,
        Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(v)) => *v as f64,
        None => return None,
    };

    // Extract attributes
    let mut attributes = HashMap::new();
    for attr in &data_point.attributes {
        if let Some(attr_value) = &attr.value {
            let value_str = match attr_value.value.as_ref() {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                _ => continue,
            };
            attributes.insert(attr.key.clone(), value_str);
        }
    }

    Some(Metric {
        metric_name: metric_name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metric_type: metric_type.to_string(),
        timestamp,
        value,
        attributes,
        resource_attributes: resource_attributes.clone(),
    })
}

/// Convert a HistogramDataPoint to our Metric format (using sum as value)
fn convert_histogram_data_point(
    data_point: &HistogramDataPoint,
    metric_name: &str,
    description: &str,
    unit: &str,
    resource_attributes: &HashMap<String, String>,
) -> Option<Metric> {
    // Extract timestamp
    let timestamp = if data_point.time_unix_nano > 0 {
        chrono::DateTime::from_timestamp(
            (data_point.time_unix_nano / 1_000_000_000) as i64,
            (data_point.time_unix_nano % 1_000_000_000) as u32,
        ).unwrap_or_else(|| chrono::Utc::now())
    } else {
        chrono::Utc::now()
    };

    // Use sum as the value (default to 0.0 if None)
    let value = data_point.sum.unwrap_or(0.0);

    // Extract attributes
    let mut attributes = HashMap::new();
    for attr in &data_point.attributes {
        if let Some(attr_value) = &attr.value {
            let value_str = match attr_value.value.as_ref() {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                _ => continue,
            };
            attributes.insert(attr.key.clone(), value_str);
        }
    }

    // Add histogram-specific attributes
    attributes.insert("count".to_string(), data_point.count.to_string());

    Some(Metric {
        metric_name: metric_name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metric_type: "histogram".to_string(),
        timestamp,
        value,
        attributes,
        resource_attributes: resource_attributes.clone(),
    })
}

/// Convert a SummaryDataPoint to our Metric format (using sum as value)
fn convert_summary_data_point(
    data_point: &SummaryDataPoint,
    metric_name: &str,
    description: &str,
    unit: &str,
    resource_attributes: &HashMap<String, String>,
) -> Option<Metric> {
    // Extract timestamp
    let timestamp = if data_point.time_unix_nano > 0 {
        chrono::DateTime::from_timestamp(
            (data_point.time_unix_nano / 1_000_000_000) as i64,
            (data_point.time_unix_nano % 1_000_000_000) as u32,
        ).unwrap_or_else(|| chrono::Utc::now())
    } else {
        chrono::Utc::now()
    };

    // Use sum as the value
    let value = data_point.sum;

    // Extract attributes
    let mut attributes = HashMap::new();
    for attr in &data_point.attributes {
        if let Some(attr_value) = &attr.value {
            let value_str = match attr_value.value.as_ref() {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                _ => continue,
            };
            attributes.insert(attr.key.clone(), value_str);
        }
    }

    // Add summary-specific attributes
    attributes.insert("count".to_string(), data_point.count.to_string());

    Some(Metric {
        metric_name: metric_name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metric_type: "summary".to_string(),
        timestamp,
        value,
        attributes,
        resource_attributes: resource_attributes.clone(),
    })
}
