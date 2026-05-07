use crate::api::ingestion::IngestResponse;
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::models::Span as SpanData;
use anyhow::Result;
use axum::extract::Extension;
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{extract::State, Json};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tracing::{error, info};

/// OTLP /v1/traces JSON handler with proper parsing
pub async fn ingest_traces_json(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    body: axum::body::Bytes,
) -> Json<IngestResponse> {
    let body_size = body.len();
    let auth_tid = tenant.map(|t| t.0.tenant_id.clone());
    match serde_json::from_slice::<ExportTraceServiceRequest>(&body) {
        Ok(request) => match process_traces(state, request, body_size, auth_tid).await {
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
    tenant: Option<Extension<TenantInfo>>,
    body: axum::body::Bytes,
) -> Json<IngestResponse> {
    let body_size = body.len();
    let auth_tid = tenant.map(|t| t.0.tenant_id.clone());
    match prost::Message::decode(body.as_ref()) {
        Ok(request) => match process_traces(state, request, body_size, auth_tid).await {
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
    tenant: Option<Extension<TenantInfo>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let body_size = body.len();
    let auth_tid = tenant.map(|t| t.0.tenant_id.clone());
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    // Protobuf
    if content_type.contains("protobuf") || content_type.contains("application/x-protobuf") {
        match prost::Message::decode(body.as_ref()) {
            Ok(request) => {
                match process_traces(state, request, body_size, auth_tid.clone()).await {
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
                }
            }
            Err(e) => {
                error!("Failed to decode protobuf: {}", e);
                (StatusCode::BAD_REQUEST, "Protobuf decode failed").into_response()
            }
        }
    } else {
        // Default to JSON
        match serde_json::from_slice::<ExportTraceServiceRequest>(&body) {
            Ok(request) => {
                match process_traces(state, request, body_size, auth_tid.clone()).await {
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
                }
            }
            Err(_) => (StatusCode::BAD_REQUEST, "Invalid JSON").into_response(),
        }
    }
}

/// Core OTLP processing logic (shared by HTTP and gRPC ingest).
///
/// `auth_tenant_id` is the authenticated Softprobe tenant from Bearer validation. When the
/// runtime uses a Postgres tenant registry, this **must** be set so spans flush to the correct
/// DuckLake scope (never inferred from optional OTLP attributes alone).
pub async fn process_traces(
    state: AppState,
    request: ExportTraceServiceRequest,
    body_size: usize,
    auth_tenant_id: Option<String>,
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

    let tid = auth_tenant_id.unwrap_or_default();

    for span in &mut spans {
        span.tenant_id = Some(tid.clone());
    }

    let span_count = spans.len();

    let engine = state.engine_for_id(&tid).await?;
    engine.ingest.add_spans(spans, body_size).await?;

    info!(
        "Processed {} spans from OTLP request ({} bytes)",
        span_count, body_size
    );
    Ok(span_count)
}
