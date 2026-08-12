// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

pub mod health;
pub mod ingestion;
pub mod llm;
pub mod query;
pub(crate) mod sql_support;
pub mod telemetry;

use crate::authn::TenantInfo;
use crate::compat::stubs::compat_stub_routes;
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
                    || msg.contains("Table with name scores does not exist")
                    || msg.contains("Table with name score_configs does not exist")
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
}

/// HTTP router + [`AppState`]. Per-tenant DuckLake/query engines are created
/// lazily on first request via [`RuntimeEngineManager`] — callers must not
/// pre-build an unused [`AppPipeline`] just to satisfy this API.
pub async fn create_router(
    config: Arc<Config>,
    traces: MethodRouter<AppState>,
    control_plane: Option<ControlPlaneRuntime>,
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
        .route("/v1/llm/scores", post(llm::create_score))
        .route(
            "/v1/llm/score-configs",
            get(llm::list_score_configs).post(llm::create_score_config),
        )
        .route(
            "/v1/llm/observations/search",
            post(llm::query::search_observations),
        )
        .route(
            "/v1/llm/observations/{span_id}",
            get(llm::query::get_observation),
        )
        .route("/v1/llm/traces/{trace_id}", get(llm::query::get_trace))
        .route("/v1/llm/sessions/search", post(llm::query::search_sessions))
        .route(
            "/v1/llm/sessions/{session_id}",
            get(llm::query::get_session),
        )
        .route(
            "/v1/llm/sessions/{session_id}/recording",
            get(llm::query::get_session_recording),
        )
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
        .merge(compat_stub_routes())
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
            "/v1/llm/scores": {
                "post": {
                    "summary": "Create an immutable LLM evaluation score",
                    "description": "Creates a score in the authenticated tenant's DuckLake scope. At least one of trace_id, span_id, or session_id is required. Exactly one value must match data_type. score_id is the tenant-local idempotency key. When config_id is set it must exist and match name/data_type.",
                    "operationId": "createScore",
                    "security": [{ "bearerAuth": [] }],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/CreateScoreRequest" }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Score created",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Score" }
                                }
                            }
                        },
                        "200": {
                            "description": "Idempotent retry; the score ID already exists",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Score" }
                                }
                            }
                        },
                        "400": {
                            "description": "Invalid score target, typed value, or config_id",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        },
                        "401": { "description": "Missing or invalid bearer token" },
                        "403": { "description": "Bearer token could not be resolved to a tenant" },
                        "422": { "description": "Malformed JSON or field type mismatch" },
                        "503": {
                            "description": "Tenant runtime, score lookup, or DuckLake write unavailable",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/llm/score-configs": {
                "get": {
                    "summary": "List score configs (seeds defaults when empty)",
                    "operationId": "listScoreConfigs",
                    "security": [{ "bearerAuth": [] }],
                    "responses": {
                        "200": {
                            "description": "Score config list",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ScoreConfigListResponse" }
                                }
                            }
                        },
                        "401": { "description": "Missing or invalid bearer token" },
                        "403": { "description": "Bearer token could not be resolved to a tenant" },
                        "503": {
                            "description": "Tenant runtime or DuckLake unavailable",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        }
                    }
                },
                "post": {
                    "summary": "Create an append-only score config",
                    "operationId": "createScoreConfig",
                    "security": [{ "bearerAuth": [] }],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/CreateScoreConfigRequest" }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Score config created",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ScoreConfig" }
                                }
                            }
                        },
                        "200": {
                            "description": "Existing config returned after an idempotent retry",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ScoreConfig" }
                                }
                            }
                        },
                        "400": {
                            "description": "Invalid score config",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        },
                        "401": { "description": "Missing or invalid bearer token" },
                        "403": { "description": "Bearer token could not be resolved to a tenant" },
                        "503": {
                            "description": "Tenant runtime or DuckLake write unavailable",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/llm/observations/search": {
                "post": {
                    "summary": "Search lightweight LLM observation projections",
                    "operationId": "searchObservations",
                    "security": [{ "bearerAuth": [] }],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/ObservationSearchRequest" }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Bounded observation page",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ObservationSearchResponse" }
                                }
                            }
                        },
                        "400": {
                            "description": "Missing time range, invalid filter, or malformed cursor",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        },
                        "401": { "description": "Missing or invalid bearer token" },
                        "403": { "description": "Bearer token could not be resolved to a tenant" },
                        "503": {
                            "description": "Tenant runtime or query unavailable",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/llm/observations/{span_id}": {
                "get": {
                    "summary": "Get one observation with full payload and attached scores",
                    "operationId": "getObservation",
                    "security": [{ "bearerAuth": [] }],
                    "parameters": [
                        { "name": "span_id", "in": "path", "required": true, "schema": { "type": "string" } },
                        { "name": "from", "in": "query", "required": false, "schema": { "type": "string", "format": "date-time" } },
                        { "name": "to", "in": "query", "required": false, "schema": { "type": "string", "format": "date-time" } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Observation detail",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ObservationDetail" }
                                }
                            }
                        },
                        "404": {
                            "description": "Observation not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        },
                        "401": { "description": "Missing or invalid bearer token" },
                        "503": {
                            "description": "Tenant runtime or query unavailable",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/llm/traces/{trace_id}": {
                "get": {
                    "summary": "Get a derived trace summary, observations, and scores",
                    "operationId": "getTrace",
                    "security": [{ "bearerAuth": [] }],
                    "parameters": [
                        { "name": "trace_id", "in": "path", "required": true, "schema": { "type": "string" } },
                        { "name": "from", "in": "query", "required": false, "schema": { "type": "string", "format": "date-time" } },
                        { "name": "to", "in": "query", "required": false, "schema": { "type": "string", "format": "date-time" } },
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 100 } },
                        { "name": "cursor", "in": "query", "required": false, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Trace detail",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/TraceDetail" }
                                }
                            }
                        },
                        "400": {
                            "description": "Invalid limit or cursor",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        },
                        "404": {
                            "description": "Trace not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        },
                        "401": { "description": "Missing or invalid bearer token" },
                        "503": {
                            "description": "Tenant runtime or query unavailable",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/llm/sessions/{session_id}": {
                "get": {
                    "summary": "Get a time-bounded session summary",
                    "operationId": "getSession",
                    "security": [{ "bearerAuth": [] }],
                    "parameters": [
                        { "name": "session_id", "in": "path", "required": true, "schema": { "type": "string" } },
                        { "name": "from", "in": "query", "required": true, "schema": { "type": "string", "format": "date-time" } },
                        { "name": "to", "in": "query", "required": true, "schema": { "type": "string", "format": "date-time" } },
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 } },
                        { "name": "cursor", "in": "query", "required": false, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Session summary",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SessionDetail" }
                                }
                            }
                        },
                        "400": {
                            "description": "Missing time range, invalid limit, or malformed cursor",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        },
                        "404": {
                            "description": "No session activity in the requested range",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        },
                        "401": { "description": "Missing or invalid bearer token" },
                        "503": {
                            "description": "Tenant runtime or query unavailable",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/llm/sessions/{session_id}/recording": {
                "get": {
                    "summary": "Get web session recording batches for a session",
                    "operationId": "getSessionRecording",
                    "security": [{ "bearerAuth": [] }],
                    "parameters": [
                        { "name": "session_id", "in": "path", "required": true, "schema": { "type": "string" } },
                        { "name": "from", "in": "query", "required": true, "schema": { "type": "string", "format": "date-time" } },
                        { "name": "to", "in": "query", "required": true, "schema": { "type": "string", "format": "date-time" } },
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Ordered recording batches and flattened rrweb events",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SessionRecording" }
                                }
                            }
                        },
                        "400": {
                            "description": "Missing time range or invalid limit",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        },
                        "401": { "description": "Missing or invalid bearer token" },
                        "503": {
                            "description": "Tenant runtime or query unavailable",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ApiError" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/query/sql": { "post": { "summary": "Execute SQL query" } },
            "/v1/telemetry/search": { "post": { "summary": "Search telemetry evidence" } },
            "/v1/telemetry/details": { "post": { "summary": "Fetch evidence details" } },
            "/v1/data/ducklake-connection": { "get": { "summary": "DuckLake setup material" } },
            "/v1/promotions/apply": { "post": { "summary": "Apply a promotion manifest to this runtime's DuckLake scope" } }
        },
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer"
                }
            },
            "schemas": {
                "CreateScoreRequest": {
                    "type": "object",
                    "required": ["score_id", "timestamp", "name", "data_type", "source"],
                    "description": "At least one target ID and exactly one value matching data_type are required. Tenant identity is intentionally absent.",
                    "properties": {
                        "score_id": { "type": "string", "minLength": 1 },
                        "timestamp": { "type": "string", "format": "date-time" },
                        "trace_id": { "type": "string", "nullable": true },
                        "span_id": { "type": "string", "nullable": true },
                        "session_id": { "type": "string", "nullable": true },
                        "name": { "type": "string", "minLength": 1 },
                        "data_type": {
                            "type": "string",
                            "enum": ["numeric", "categorical", "boolean", "text"]
                        },
                        "numeric_value": { "type": "number", "format": "double", "nullable": true },
                        "string_value": { "type": "string", "nullable": true },
                        "boolean_value": { "type": "boolean", "nullable": true },
                        "source": {
                            "type": "string",
                            "enum": ["api", "user", "evaluator", "annotation"]
                        },
                        "comment": { "type": "string", "nullable": true },
                        "config_id": { "type": "string", "nullable": true },
                        "author_id": { "type": "string", "nullable": true },
                        "metadata": {
                            "type": "object",
                            "additionalProperties": { "type": "string" },
                            "default": {}
                        }
                    }
                },
                "Score": {
                    "allOf": [
                        { "$ref": "#/components/schemas/CreateScoreRequest" },
                        {
                            "type": "object",
                            "required": ["record_date"],
                            "properties": {
                                "record_date": { "type": "string", "format": "date" }
                            }
                        }
                    ]
                },
                "ApiError": {
                    "type": "object",
                    "required": ["error"],
                    "properties": {
                        "error": { "type": "string" }
                    }
                },
                "ObservationSearchRequest": {
                    "type": "object",
                    "required": ["from", "to"],
                    "properties": {
                        "from": { "type": "string", "format": "date-time" },
                        "to": { "type": "string", "format": "date-time" },
                        "observation_types": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["span", "event", "generation", "agent", "tool", "chain", "retriever", "evaluator", "embedding", "guardrail"]
                            }
                        },
                        "model_name": { "type": "string" },
                        "user_id": { "type": "string" },
                        "session_id": { "type": "string" },
                        "trace_id": { "type": "string" },
                        "limit": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 },
                        "cursor": { "type": "string" }
                    }
                },
                "CreateScoreConfigRequest": {
                    "type": "object",
                    "required": ["config_id", "timestamp", "name", "data_type"],
                    "properties": {
                        "config_id": { "type": "string", "minLength": 1 },
                        "timestamp": { "type": "string", "format": "date-time" },
                        "name": { "type": "string", "minLength": 1 },
                        "data_type": {
                            "type": "string",
                            "enum": ["numeric", "categorical", "boolean", "text"]
                        },
                        "description": { "type": "string", "nullable": true },
                        "min_value": { "type": "number", "format": "double", "nullable": true },
                        "max_value": { "type": "number", "format": "double", "nullable": true },
                        "categories": {
                            "type": "array",
                            "items": { "type": "string" },
                            "default": []
                        },
                        "author_id": { "type": "string", "nullable": true },
                        "metadata": {
                            "type": "object",
                            "additionalProperties": { "type": "string" },
                            "default": {}
                        }
                    }
                },
                "ScoreConfig": {
                    "allOf": [
                        { "$ref": "#/components/schemas/CreateScoreConfigRequest" },
                        {
                            "type": "object",
                            "required": ["record_date"],
                            "properties": {
                                "record_date": { "type": "string", "format": "date" }
                            }
                        }
                    ]
                },
                "ScoreConfigListResponse": {
                    "type": "object",
                    "required": ["items"],
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/ScoreConfig" }
                        }
                    }
                },
                "ObservationSummary": {
                    "type": "object",
                    "required": ["trace_id", "span_id", "name", "observation_type", "start_time"],
                    "properties": {
                        "trace_id": { "type": "string" },
                        "span_id": { "type": "string" },
                        "parent_span_id": { "type": "string", "nullable": true },
                        "session_id": { "type": "string", "nullable": true },
                        "name": { "type": "string" },
                        "observation_type": { "type": "string" },
                        "start_time": { "type": "string", "format": "date-time" },
                        "end_time": { "type": "string", "format": "date-time", "nullable": true },
                        "status_code": { "type": "string", "nullable": true },
                        "model_name": { "type": "string", "nullable": true },
                        "model_provider": { "type": "string", "nullable": true },
                        "user_id": { "type": "string", "nullable": true },
                        "input_tokens": { "type": "integer", "nullable": true },
                        "output_tokens": { "type": "integer", "nullable": true },
                        "total_tokens": { "type": "integer", "nullable": true },
                        "total_cost": { "type": "number", "nullable": true }
                    }
                },
                "ObservationSearchResponse": {
                    "type": "object",
                    "required": ["items"],
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/ObservationSummary" }
                        },
                        "next_cursor": { "type": "string", "nullable": true }
                    }
                },
                "ObservationDetail": {
                    "allOf": [
                        { "$ref": "#/components/schemas/ObservationSummary" },
                        {
                            "type": "object",
                            "properties": {
                                "attributes": {
                                    "type": "object",
                                    "additionalProperties": { "type": "string" }
                                },
                                "events": { "type": "array", "items": { "type": "object" } },
                                "scores": {
                                    "type": "array",
                                    "items": { "$ref": "#/components/schemas/Score" }
                                }
                            }
                        }
                    ]
                },
                "TraceSummary": {
                    "type": "object",
                    "required": ["trace_id", "start_time", "end_time", "observation_count"],
                    "properties": {
                        "trace_id": { "type": "string" },
                        "session_id": { "type": "string", "nullable": true },
                        "name": { "type": "string", "nullable": true },
                        "start_time": { "type": "string", "format": "date-time" },
                        "end_time": { "type": "string", "format": "date-time" },
                        "observation_count": { "type": "integer" },
                        "error_count": { "type": "integer" },
                        "input_tokens": { "type": "integer", "nullable": true },
                        "output_tokens": { "type": "integer", "nullable": true },
                        "total_tokens": { "type": "integer", "nullable": true },
                        "total_cost": { "type": "number", "nullable": true },
                        "user_id": { "type": "string", "nullable": true }
                    }
                },
                "TraceDetail": {
                    "type": "object",
                    "required": ["trace", "observations", "scores"],
                    "properties": {
                        "trace": { "$ref": "#/components/schemas/TraceSummary" },
                        "observations": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/ObservationDetail" }
                        },
                        "scores": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/Score" }
                        },
                        "next_cursor": { "type": "string", "nullable": true }
                    }
                },
                "SessionDetail": {
                    "type": "object",
                    "required": ["session_id", "from", "to", "trace_count", "observation_count", "traces", "scores"],
                    "properties": {
                        "session_id": { "type": "string" },
                        "from": { "type": "string", "format": "date-time" },
                        "to": { "type": "string", "format": "date-time" },
                        "trace_count": { "type": "integer" },
                        "observation_count": { "type": "integer" },
                        "user_ids": { "type": "array", "items": { "type": "string" } },
                        "input_tokens": { "type": "integer", "nullable": true },
                        "output_tokens": { "type": "integer", "nullable": true },
                        "total_tokens": { "type": "integer", "nullable": true },
                        "total_cost": { "type": "number", "nullable": true },
                        "traces": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/TraceSummary" }
                        },
                        "scores": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/Score" }
                        },
                        "next_cursor": { "type": "string", "nullable": true }
                    }
                },
                "SessionRecording": {
                    "type": "object",
                    "required": ["session_id", "from", "to", "batches", "events"],
                    "properties": {
                        "session_id": { "type": "string" },
                        "from": { "type": "string", "format": "date-time" },
                        "to": { "type": "string", "format": "date-time" },
                        "batches": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/RecordingBatch" }
                        },
                        "events": {
                            "type": "array",
                            "description": "Flattened rrweb events sorted by timestamp",
                            "items": { "type": "object" }
                        }
                    }
                },
                "RecordingBatch": {
                    "type": "object",
                    "required": ["span_id", "trace_id", "start_time", "events"],
                    "properties": {
                        "span_id": { "type": "string" },
                        "trace_id": { "type": "string" },
                        "start_time": { "type": "string", "format": "date-time" },
                        "batch_index": { "type": "integer", "nullable": true },
                        "attributes": {
                            "type": "object",
                            "additionalProperties": { "type": "string" }
                        },
                        "events": {
                            "type": "array",
                            "items": { "type": "object" }
                        }
                    }
                }
            }
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
