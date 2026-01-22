use crate::api::ingestion::IngestResponse;
use crate::api::AppState;
use crate::models::Log as LogData;
use anyhow::Result;
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{extract::State, Json};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use tracing::{error, info, warn};

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
                })
                .into_response(),
                Err(e) => {
                    error!("Failed to process OTLP logs: {}", e);
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
        match serde_json::from_slice::<ExportLogsServiceRequest>(&body) {
            Ok(request) => match process_logs(state, request, body_size).await {
                Ok(count) => Json(IngestResponse {
                    success: true,
                    ingested_count: count,
                    message: format!("Successfully ingested {} log records", count),
                })
                .into_response(),
                Err(e) => {
                    error!("Failed to process OTLP logs: {}", e);
                    Json(IngestResponse {
                        success: false,
                        ingested_count: 0,
                        message: format!("Ingestion failed: {}", e),
                    })
                    .into_response()
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
        let resource_attributes = LogData::extract_resource_attributes(&resource_logs);

        for scope_logs in resource_logs.scope_logs {
            for log_record in scope_logs.log_records {
                let log_data = LogData::from_otlp(log_record, &resource_attributes)?;
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

    info!(
        "Processed {} log records from OTLP request ({} bytes)",
        log_count, body_size
    );
    Ok(log_count)
}
