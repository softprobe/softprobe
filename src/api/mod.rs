// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

pub mod health;
pub mod ingestion;
pub mod query;
pub mod telemetry;

use crate::authn::TenantInfo;
use crate::catalog::DropdownCatalog;
use crate::config::Config;
use crate::ingest_engine::IngestPipeline;
use crate::query::{self as query_engine, QueryEngine};
use crate::runtime_engine::DuckLakeScopeResolver;
use crate::storage::Storage;
use axum::{
    response::Html,
    routing::{get, post, MethodRouter},
    Json, Router,
};
use serde_json::json;
use std::sync::Arc;

pub use crate::control_plane::ControlPlaneRuntime;
pub use crate::runtime_engine::{RuntimeEngine, RuntimeEngineManager};

/// Unified application state for Axum router
#[derive(Clone)]
pub struct AppState {
    pub engines: Arc<RuntimeEngineManager>,
}

impl AppState {
    pub async fn engine_for_tenant(
        &self,
        tenant: &TenantInfo,
    ) -> anyhow::Result<Arc<RuntimeEngine>> {
        self.engines.engine_for_tenant(tenant).await
    }

    pub async fn engine_for_id(&self, tenant_id: &str) -> anyhow::Result<Arc<RuntimeEngine>> {
        self.engines.engine_for(tenant_id).await
    }

    /// Execute SQL on the tenant-bound query engine (scope fixed at engine construction).
    pub async fn execute_tenant_scoped_sql(
        &self,
        tenant: Option<&TenantInfo>,
        sql: &str,
    ) -> anyhow::Result<crate::query::duckdb::QueryResult> {
        let tenant_id = tenant.map(|t| t.tenant_id.as_str()).unwrap_or("");
        let engine = self.engines.engine_for(tenant_id).await?;
        match engine.query.execute_query(sql).await {
            Ok(result) => Ok(result),
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("Table with name traces does not exist")
                    || msg.contains("Table with name logs does not exist")
                    || msg.contains("Table with name metrics does not exist")
                {
                    return Ok(crate::query::duckdb::QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        row_count: 0,
                    });
                }
                Err(err)
            }
        }
    }
}

pub struct AppPipeline {
    pub storage: Storage,
    pub query_engine: QueryEngine,
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
        })
    }

    pub async fn into_router(self) -> anyhow::Result<Router> {
        let config = Arc::new(Config::load()?);
        let (r, _) = create_router(
            config,
            self.storage,
            self.query_engine,
            post(ingestion::traces::ingest_traces),
            None,
            None,
        )
        .await?;
        Ok(r)
    }
}

pub async fn create_router(
    config: Arc<Config>,
    _storage: Storage,
    _query_engine: QueryEngine,
    traces: MethodRouter<AppState>,
    control_plane: Option<ControlPlaneRuntime>,
    _dropdown_catalog: Option<Arc<DropdownCatalog>>,
) -> anyhow::Result<(Router, AppState)> {
    let scope_registry = DuckLakeScopeResolver::connect(config.as_ref()).await?;
    let runtime_engine_manager = Arc::new(RuntimeEngineManager::new(
        config,
        control_plane.clone(),
        scope_registry,
    ));
    let state = AppState {
        engines: runtime_engine_manager,
    };

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
</html>"##,
    )
}

#[cfg(test)]
mod unit_tests;
