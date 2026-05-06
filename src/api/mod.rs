pub mod health;
pub mod ingestion;
pub mod query;
pub mod telemetry;

use crate::authn;
use crate::catalog::DropdownCatalog;
use crate::config::Config;
use crate::ingest_engine::IngestPipeline;
use crate::query::{self as query_engine, QueryEngine};
use crate::session_redis::RedisStore;
use crate::storage::{LogBuffer, MetricBuffer, SpanBuffer, Storage};
use crate::tenant_ducklake::TenantDuckLakeResolver;
use axum::{
    response::Html,
    routing::{get, post, MethodRouter},
    Json, Router,
};
use serde_json::json;
use std::sync::Arc;

/// Control-plane dependencies (sessions, inject).
#[derive(Clone)]
pub struct ControlPlaneRuntime {
    pub resolver: authn::Resolver,
    pub session_store: Arc<tokio::sync::Mutex<RedisStore>>,
    /// DuckLake Postgres pool + configured metadata schema for this process (runtime-owned scope).
    pub tenant_ducklake: Option<TenantDuckLakeResolver>,
}

// Unified application state for Axum router
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Storage>,
    pub query_engine: Arc<QueryEngine>,
    pub span_buffer: Option<Arc<SpanBuffer>>,
    pub log_buffer: Option<Arc<LogBuffer>>,
    pub metric_buffer: Option<Arc<MetricBuffer>>,
    /// When set, `/v1/sessions`, `/v1/inject`, and control-plane trace ingest are enabled.
    pub control_plane: Option<ControlPlaneRuntime>,
    /// UI dropdown metadata (Postgres EAV); requires `dropdown_catalog.enabled` and DuckLake+Postgres.
    pub dropdown_catalog: Option<Arc<DropdownCatalog>>,
}

pub struct AppPipeline {
    pub storage: Storage,
    pub query_engine: QueryEngine,
    pub span_buffer: SpanBuffer,
    pub log_buffer: LogBuffer,
    pub metric_buffer: MetricBuffer,
}

impl AppPipeline {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let pipeline = IngestPipeline::new(config).await?;
        let storage = pipeline.storage.clone();
        let query_engine =
            query_engine::create_query_engine(config, Arc::new(storage.clone())).await?;
        Ok(Self {
            storage,
            query_engine,
            span_buffer: pipeline.storage.span_buffer,
            log_buffer: pipeline.storage.log_buffer,
            metric_buffer: pipeline.storage.metric_buffer,
        })
    }

    pub async fn into_router(self) -> anyhow::Result<Router> {
        let (r, _) = create_router(
            self.storage,
            self.query_engine,
            Some(self.span_buffer),
            Some(self.log_buffer),
            Some(self.metric_buffer),
            post(ingestion::traces::ingest_traces),
            None,
            None,
        )
        .await?;
        Ok(r)
    }
}

pub async fn create_router(
    storage: Storage,
    query_engine: QueryEngine,
    span_buffer: Option<SpanBuffer>,
    log_buffer: Option<LogBuffer>,
    metric_buffer: Option<MetricBuffer>,
    traces: MethodRouter<AppState>,
    control_plane: Option<ControlPlaneRuntime>,
    dropdown_catalog: Option<Arc<DropdownCatalog>>,
) -> anyhow::Result<(Router, AppState)> {
    let state = AppState {
        storage: Arc::new(storage),
        query_engine: Arc::new(query_engine),
        span_buffer: span_buffer.map(Arc::new),
        log_buffer: log_buffer.map(Arc::new),
        metric_buffer: metric_buffer.map(Arc::new),
        control_plane,
        dropdown_catalog,
    };

    // OTLP standard endpoints (`with_state` closes the state type → `Router` is ready for `axum::serve`)
    let router = Router::new()
        .route("/health", get(health::health_check))
        .route("/ready", get(health::ready_check))
        .route("/openapi.json", get(openapi_spec))
        .route("/swagger", get(swagger_ui))
        .route("/v1/traces", traces)
        .route("/v1/logs", post(ingestion::logs::ingest_logs))
        .route("/v1/metrics", post(ingestion::metrics::ingest_metrics))
        .route("/v1/query/sql", post(query::execute_sql))
        .route("/v1/telemetry/search", post(telemetry::search))
        .route("/v1/telemetry/details", post(telemetry::details_post))
        .route("/v1/telemetry/fields", get(telemetry::fields))
        .route(
            "/v1/telemetry/fields/{field}/values",
            get(telemetry::field_values),
        )
        .route(
            "/v1/telemetry/sessions/{session_id}",
            get(telemetry::session_details),
        )
        .route(
            "/v1/telemetry/traces/{trace_id}",
            get(telemetry::trace_details),
        )
        .with_state(state.clone());

    Ok((router, state))
}

async fn openapi_spec() -> Json<serde_json::Value> {
    Json(json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Softprobe Runtime API",
            "version": env!("CARGO_PKG_VERSION")
        },
        "paths": {
            "/health": { "get": { "summary": "Health check" } },
            "/ready": { "get": { "summary": "Readiness check" } },
            "/v1/traces": { "post": { "summary": "Ingest traces" } },
            "/v1/logs": { "post": { "summary": "Ingest logs" } },
            "/v1/metrics": { "post": { "summary": "Ingest metrics" } },
            "/v1/query/sql": { "post": { "summary": "Execute SQL query" } },
            "/v1/telemetry/search": { "post": { "summary": "Search telemetry evidence" } },
            "/v1/telemetry/details": { "post": { "summary": "Fetch evidence details" } },
            "/v1/data/ducklake-connection": { "get": { "summary": "DuckLake setup material" } },
            "/v1/promotions/apply": { "post": { "summary": "Apply a promotion manifest to this runtime's DuckLake scope" } },
            "/v1/sessions": { "post": { "summary": "Create session" }, "get": { "summary": "List sessions" } }
        }
    }))
}

async fn swagger_ui() -> Html<&'static str> {
    Html(
        r##"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Softprobe Runtime API</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui" });
    </script>
  </body>
</html>"#,
 </html>"##,
    )
}

#[cfg(test)]
mod unit_tests;
