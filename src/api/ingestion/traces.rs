use crate::api::ingestion::IngestResponse;
use crate::api::AppState;
use crate::models::Span as SpanData;
use anyhow::Result;
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{extract::State, Json};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tracing::{error, info, warn};

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
            Ok(request) => match process_traces(state, request, body_size).await {
                Ok(count) => Json(IngestResponse {
                    success: true,
                    ingested_count: count,
                    message: format!("Successfully ingested {} spans", count),
                })
                .into_response(),
                Err(e) => {
                    error!("Failed to process OTLP traces: {}", e);
                    Json(IngestResponse {
                        success: false,
                        ingested_count: 0,
                        message: format!("Ingestion failed: {}", e),
                    })
                    .into_response()
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
                })
                .into_response(),
                Err(e) => {
                    error!("Failed to process OTLP traces: {}", e);
                    Json(IngestResponse {
                        success: false,
                        ingested_count: 0,
                        message: format!("Ingestion failed: {}", e),
                    })
                    .into_response()
                }
            },
            Err(_) => (StatusCode::BAD_REQUEST, "Invalid JSON").into_response(),
        }
    }
}

/// Core OTLP processing logic (shared by HTTP and gRPC ingest).
pub async fn process_traces(
    state: AppState,
    request: ExportTraceServiceRequest,
    body_size: usize,
) -> Result<usize> {
    let mut spans = Vec::new();

    for resource_spans in request.resource_spans {
        let resource_attributes = SpanData::extract_resource_attributes(&resource_spans);

        for scope_spans in resource_spans.scope_spans {
            for span in scope_spans.spans {
                let span_data = SpanData::from_otlp(span, &resource_attributes)?;
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

    info!(
        "Processed {} spans from OTLP request ({} bytes)",
        span_count, body_size
    );
    Ok(span_count)
}
