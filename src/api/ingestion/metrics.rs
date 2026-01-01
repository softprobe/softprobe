use axum::{extract::State, Json};
use axum::http::{HeaderMap, header::CONTENT_TYPE, StatusCode};
use axum::response::{IntoResponse, Response};
use crate::api::AppState;
use crate::api::ingestion::IngestResponse;
use crate::models::Metric;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
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
        let resource_attributes = Metric::extract_resource_attributes(&resource_metrics);

        for scope_metrics in resource_metrics.scope_metrics {
            for otlp_metric in scope_metrics.metrics {
                let metric_data_points = Metric::from_otlp(&otlp_metric, &resource_attributes)?;
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
