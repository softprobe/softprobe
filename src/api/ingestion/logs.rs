use axum::{extract::State, Json};
use axum::http::{HeaderMap, header::CONTENT_TYPE, StatusCode};
use axum::response::{IntoResponse, Response};
use crate::api::AppState;
use crate::api::ingestion::IngestResponse;
use crate::models::Log as LogData;
use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, LogRecord};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use std::collections::HashMap;
use tracing::{error, info, warn};
use anyhow::Result;

/// Unified OTLP /v1/logs handler that switches on Content-Type
pub async fn ingest_logs(
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
            Ok(request) => match process_logs(state, request, body_size).await {
                Ok(count) => Json(IngestResponse {
                    success: true,
                    ingested_count: count,
                    message: format!("Successfully ingested {} log records", count),
                }).into_response(),
                Err(e) => {
                    error!("Failed to process OTLP logs: {}", e);
                    Json(IngestResponse {
                        success: false,
                        ingested_count: 0,
                        message: format!("Ingestion failed: {}", e),
                    }).into_response()
                }
            },
            Err(e) => {
                error!("Failed to decode protobuf: {}", e);
                (StatusCode::BAD_REQUEST, "Protobuf decode failed").into_response()
            }
        }
    } else {
        // Default to JSON
        match serde_json::from_slice::<ExportLogsServiceRequest>(&body) {
            Ok(request) => match process_logs(state, request, body_size).await {
                Ok(count) => Json(IngestResponse {
                    success: true,
                    ingested_count: count,
                    message: format!("Successfully ingested {} log records", count),
                }).into_response(),
                Err(e) => {
                    error!("Failed to process OTLP logs: {}", e);
                    Json(IngestResponse {
                        success: false,
                        ingested_count: 0,
                        message: format!("Ingestion failed: {}", e),
                    }).into_response()
                }
            },
            Err(e) => {
                error!("Failed to decode JSON: {}", e);
                (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e)).into_response()
            }
        }
    }
}

/// Core OTLP logs processing logic
async fn process_logs(
    state: AppState,
    request: ExportLogsServiceRequest,
    body_size: usize,
) -> Result<usize> {
    let mut logs = Vec::new();

    for resource_logs in request.resource_logs {
        let resource_attributes = extract_resource_attributes(&resource_logs);

        for scope_logs in resource_logs.scope_logs {
            for log_record in scope_logs.log_records {
                // Convert OTLP log record to internal LogData format
                let log_data = convert_otlp_log_to_log_data(
                    log_record,
                    &resource_attributes,
                );
                logs.push(log_data);
            }
        }
    }

    let log_count = logs.len();

    // Add all logs to the buffer in one batch with body size tracking
    if let Some(log_buffer) = &state.log_buffer {
        log_buffer.add_items(logs, body_size).await?;
    } else {
        warn!("Log buffer not initialized");
    }

    info!("Processed {} log records from OTLP request ({} bytes)", log_count, body_size);
    Ok(log_count)
}

/// Extract resource attributes from ResourceLogs
fn extract_resource_attributes(resource_logs: &ResourceLogs) -> HashMap<String, String> {
    let mut attributes = HashMap::new();

    if let Some(resource) = &resource_logs.resource {
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

/// Extract session_id from log record or resource attributes
fn extract_session_id(log_record: &LogRecord, resource_attributes: &HashMap<String, String>) -> Option<String> {
    // Check log record attributes first
    for attr in &log_record.attributes {
        if attr.key == "session.id" || attr.key == "session_id" {
            if let Some(value) = &attr.value {
                if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) = &value.value {
                    return Some(s.clone());
                }
            }
        }
    }

    // Fallback to resource attributes
    resource_attributes.get("session.id")
        .or_else(|| resource_attributes.get("session_id"))
        .cloned()
}

/// Extract log body as string
fn extract_log_body(log_record: &LogRecord) -> Option<String> {
    log_record.body.as_ref().and_then(|body| {
        match body.value.as_ref() {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => Some(s.clone()),
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => Some(i.to_string()),
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => Some(d.to_string()),
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => Some(b.to_string()),
            _ => None,
        }
    })
}

/// Convert OTLP LogRecord to internal LogData format
fn convert_otlp_log_to_log_data(
    log_record: LogRecord,
    resource_attributes: &HashMap<String, String>,
) -> LogData {
    // Extract session_id from log record or resource attributes
    let session_id = extract_session_id(&log_record, resource_attributes);

    // Extract timestamp (nanoseconds since epoch)
    let timestamp = if log_record.time_unix_nano > 0 {
        chrono::DateTime::from_timestamp_nanos(log_record.time_unix_nano as i64)
    } else {
        chrono::Utc::now()
    };

    // Extract observed timestamp
    let observed_timestamp = if log_record.observed_time_unix_nano > 0 {
        Some(chrono::DateTime::from_timestamp_nanos(log_record.observed_time_unix_nano as i64))
    } else {
        None
    };

    // Extract severity
    let severity_text = if !log_record.severity_text.is_empty() {
        log_record.severity_text.clone()
    } else {
        format!("SEVERITY_{:?}", log_record.severity_number())
    };

    // Extract body
    let body = extract_log_body(&log_record).unwrap_or_default();

    // Extract log record attributes
    let mut attributes = HashMap::new();
    for attr in &log_record.attributes {
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

    // Extract trace context
    let trace_id = if !log_record.trace_id.is_empty() {
        Some(hex::encode(&log_record.trace_id))
    } else {
        None
    };

    let span_id = if !log_record.span_id.is_empty() {
        Some(hex::encode(&log_record.span_id))
    } else {
        None
    };

    LogData {
        session_id,
        timestamp,
        observed_timestamp,
        severity_number: log_record.severity_number,
        severity_text,
        body,
        attributes,
        resource_attributes: resource_attributes.clone(),
        trace_id,
        span_id,
    }
}
