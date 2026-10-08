// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

use crate::api::ingestion::{ingest_write_failed, IngestResponse};
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::models::Metric;
use anyhow::Result;
use axum::extract::Extension;
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{extract::State, Json};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use tracing::{error, info};

/// OTLP /v1/metrics JSON handler
pub async fn ingest_metrics_json(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    body: axum::body::Bytes,
) -> Response {
    let body_size = body.len();
    let tenant_info = tenant.map(|t| t.0);
    match serde_json::from_slice::<ExportMetricsServiceRequest>(&body) {
        Ok(request) => match process_metrics(state, request, body_size, tenant_info).await {
            Ok(count) => Json(IngestResponse {
                success: true,
                ingested_count: count,
                message: format!("Successfully ingested {} metric data points", count),
            })
            .into_response(),
            Err(e) => {
                error!("Failed to process OTLP metrics: {}", e);
                ingest_write_failed(format!("Ingestion failed: {}", e))
            }
        },
        Err(e) => {
            error!("Failed to decode JSON: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(IngestResponse {
                    success: false,
                    ingested_count: 0,
                    message: format!("JSON decode failed: {}", e),
                }),
            )
                .into_response()
        }
    }
}

/// OTLP /v1/metrics protobuf handler
pub async fn ingest_metrics_protobuf(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    body: axum::body::Bytes,
) -> Response {
    let body_size = body.len();
    let tenant_info = tenant.map(|t| t.0);
    match prost::Message::decode(body.as_ref()) {
        Ok(request) => match process_metrics(state, request, body_size, tenant_info).await {
            Ok(count) => Json(IngestResponse {
                success: true,
                ingested_count: count,
                message: format!("Successfully ingested {} metric data points", count),
            })
            .into_response(),
            Err(e) => {
                error!("Failed to process OTLP metrics: {}", e);
                ingest_write_failed(format!("Ingestion failed: {}", e))
            }
        },
        Err(e) => {
            error!("Failed to decode protobuf: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(IngestResponse {
                    success: false,
                    ingested_count: 0,
                    message: format!("Protobuf decode failed: {}", e),
                }),
            )
                .into_response()
        }
    }
}

/// Unified OTLP /v1/metrics handler that switches on Content-Type
pub async fn ingest_metrics(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let body_size = body.len();
    let tenant_info = tenant.map(|t| t.0);
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    // Protobuf
    if content_type.contains("protobuf") || content_type.contains("application/x-protobuf") {
        match prost::Message::decode(body.as_ref()) {
            Ok(request) => {
                match process_metrics(state, request, body_size, tenant_info.clone()).await {
                    Ok(count) => Json(IngestResponse {
                        success: true,
                        ingested_count: count,
                        message: format!("Successfully ingested {} metric data points", count),
                    })
                    .into_response(),
                    Err(e) => {
                        error!("Failed to process OTLP metrics: {}", e);
                        ingest_write_failed(format!("Ingestion failed: {}", e))
                    }
                }
            }
            Err(e) => {
                error!("Failed to decode protobuf: {}", e);
                (StatusCode::BAD_REQUEST, "Invalid protobuf").into_response()
            }
        }
    } else {
        // JSON (application/json)
        match serde_json::from_slice::<ExportMetricsServiceRequest>(&body) {
            Ok(request) => {
                match process_metrics(state, request, body_size, tenant_info.clone()).await {
                    Ok(count) => Json(IngestResponse {
                        success: true,
                        ingested_count: count,
                        message: format!("Successfully ingested {} metric data points", count),
                    })
                    .into_response(),
                    Err(e) => {
                        error!("Failed to process OTLP metrics: {}", e);
                        ingest_write_failed(format!("Ingestion failed: {}", e))
                    }
                }
            }
            Err(_) => (StatusCode::BAD_REQUEST, "Invalid JSON").into_response(),
        }
    }
}

/// Core OTLP metrics processing logic
async fn process_metrics(
    state: AppState,
    request: ExportMetricsServiceRequest,
    body_size: usize,
    tenant: Option<TenantInfo>,
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

    let tenant_id = tenant.map(|t| t.tenant_id).unwrap_or_default();
    let engine = state.engine_for_id(&tenant_id).await?;
    engine.ingest.add_metrics(metrics, body_size).await?;

    info!(
        "Processed {} metric data points from OTLP request ({} bytes)",
        metric_count, body_size
    );
    Ok(metric_count)
}
