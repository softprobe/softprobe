use axum::{extract::State, Json};
use axum::http::{HeaderMap, header::CONTENT_TYPE, StatusCode};
use axum::response::{IntoResponse, Response};
use crate::api::AppState;
use crate::api::ingestion::IngestResponse;
use crate::models::{Span as SpanData, SpanEvent};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, Span};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use std::collections::HashMap;
use tracing::{error, info, warn};
use anyhow::Result;

/// OTLP /v1/traces JSON handler with proper parsing
pub async fn ingest_traces_json(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Json<IngestResponse> {
    let body_size = body.len();
    match serde_json::from_slice::<ExportTraceServiceRequest>(&body) {
        Ok(request) => match process_traces(state, request, body_size).await {
            Ok(count) => Json(IngestResponse {
                success: true,
                ingested_count: count,
                message: format!("Successfully ingested {} spans", count),
            }),
            Err(e) => {
                error!("Failed to process OTLP traces: {}", e);
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

/// OTLP /v1/traces protobuf handler
pub async fn ingest_traces_protobuf(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Json<IngestResponse> {
    let body_size = body.len();
    match prost::Message::decode(body.as_ref()) {
        Ok(request) => match process_traces(state, request, body_size).await {
            Ok(count) => Json(IngestResponse {
                success: true,
                ingested_count: count,
                message: format!("Successfully ingested {} spans", count),
            }),
            Err(e) => {
                error!("Failed to process OTLP traces: {}", e);
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

/// Unified OTLP /v1/traces handler that switches on Content-Type
pub async fn ingest_traces(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
)
-> Response {
    let body_size = body.len();
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    // Protobuf
    if content_type.contains("protobuf") || content_type.contains("application/x-protobuf") {
        match prost::Message::decode(body.as_ref()) {
            Ok(request) => match process_traces(state, request, body_size).await {
                Ok(count) => Json(IngestResponse {
                    success: true,
                    ingested_count: count,
                    message: format!("Successfully ingested {} spans", count),
                }).into_response(),
                Err(e) => {
                    error!("Failed to process OTLP traces: {}", e);
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
        match serde_json::from_slice::<ExportTraceServiceRequest>(&body) {
            Ok(request) => match process_traces(state, request, body_size).await {
                Ok(count) => Json(IngestResponse {
                    success: true,
                    ingested_count: count,
                    message: format!("Successfully ingested {} spans", count),
                }).into_response(),
                Err(e) => {
                    error!("Failed to process OTLP traces: {}", e);
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

/// Core OTLP processing logic
async fn process_traces(
    state: AppState,
    request: ExportTraceServiceRequest,
    body_size: usize,
) -> Result<usize> {
    let mut spans = Vec::new();

    for resource_spans in request.resource_spans {
        let resource_attributes = extract_resource_attributes(&resource_spans);

        for scope_spans in resource_spans.scope_spans {
            for span in scope_spans.spans {
                let span_data = convert_otlp_span_to_span_data(span, &resource_attributes)?;
                spans.push(span_data);
            }
        }
    }

    let span_count = spans.len();

    // Add all spans to the buffer in one batch with body size tracking
    if let Some(span_buffer) = &state.span_buffer {
        span_buffer.add_items(spans, body_size).await?;
    } else {
        warn!("Span buffer not initialized");
    }

    info!("Processed {} spans from OTLP request ({} bytes)", span_count, body_size);
    Ok(span_count)
}

/// Extract resource attributes from ResourceSpans
fn extract_resource_attributes(resource_spans: &ResourceSpans) -> HashMap<String, String> {
    let mut attributes = HashMap::new();

    if let Some(resource) = &resource_spans.resource {
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

/// Convert OTLP Span to our internal SpanData format
fn convert_otlp_span_to_span_data(
    span: Span,
    resource_attributes: &HashMap<String, String>,
) -> Result<SpanData> {
    // Extract span attributes
    let mut attributes = HashMap::new();
    for attr in &span.attributes {
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

    // Extract events (payloads)
    let mut events = Vec::new();
    for event in &span.events {
        let mut event_attributes = HashMap::new();
        for attr in &event.attributes {
            if let Some(value) = &attr.value {
                let value_str = match value.value.as_ref() {
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                    _ => continue,
                };
                event_attributes.insert(attr.key.clone(), value_str);
            }
        }

        let event_timestamp = if event.time_unix_nano > 0 {
            chrono::DateTime::from_timestamp(
                (event.time_unix_nano / 1_000_000_000) as i64,
                (event.time_unix_nano % 1_000_000_000) as u32
            ).unwrap_or_else(|| chrono::Utc::now())
        } else {
            chrono::Utc::now()
        };

        events.push(SpanEvent {
            name: event.name.clone(),
            timestamp: event_timestamp,
            attributes: event_attributes,
        });
    }

    // Convert timestamps
    let timestamp = if span.start_time_unix_nano > 0 {
        chrono::DateTime::from_timestamp(
            (span.start_time_unix_nano / 1_000_000_000) as i64,
            (span.start_time_unix_nano % 1_000_000_000) as u32
        ).unwrap_or_else(|| chrono::Utc::now())
    } else {
        chrono::Utc::now()
    };

    let end_timestamp = if span.end_time_unix_nano > 0 {
        Some(chrono::DateTime::from_timestamp(
            (span.end_time_unix_nano / 1_000_000_000) as i64,
            (span.end_time_unix_nano % 1_000_000_000) as u32
        ).unwrap_or_else(|| chrono::Utc::now()))
    } else {
        None
    };

    // Extract app_id from resource attributes
    let app_id = resource_attributes
        .get("sp.app.id")
        .or_else(|| resource_attributes.get("service.name"))
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());

    Ok(SpanData {
        trace_id: hex::encode(&span.trace_id),
        span_id: hex::encode(&span.span_id),
        parent_span_id: if span.parent_span_id.is_empty() {
            None
        } else {
            Some(hex::encode(&span.parent_span_id))
        },
        app_id,
        organization_id: resource_attributes.get("sp.organization.id").cloned(),
        tenant_id: resource_attributes.get("sp.tenant.id").cloned(),
        message_type: span.name.clone(),
        span_kind: Some(format!("{:?}", span.kind())),
        timestamp,
        end_timestamp,
        attributes,
        events,
        status_code: span.status.as_ref().map(|s| format!("{:?}", s.code())),
        status_message: span.status.as_ref().and_then(|s| {
            if s.message.is_empty() {
                None
            } else {
                Some(s.message.clone())
            }
        }),
    })
}
