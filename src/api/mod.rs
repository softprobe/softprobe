pub mod health;
pub mod ingestion;
pub mod query;

use crate::config::Config;
use crate::query::{self as query_engine, QueryEngine};
use crate::ingest_engine::IngestPipeline;
use crate::storage::{LogBuffer, MetricBuffer, SpanBuffer, Storage};
use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

// Unified application state for Axum router
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Storage>,
    pub query_engine: Arc<QueryEngine>,
    pub span_buffer: Option<Arc<SpanBuffer>>,
    pub log_buffer: Option<Arc<LogBuffer>>,
    pub metric_buffer: Option<Arc<MetricBuffer>>,
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
        create_router(
            self.storage,
            self.query_engine,
            Some(self.span_buffer),
            Some(self.log_buffer),
            Some(self.metric_buffer),
        )
        .await
    }
}

pub async fn create_router(
    storage: Storage,
    query_engine: QueryEngine,
    span_buffer: Option<SpanBuffer>,
    log_buffer: Option<LogBuffer>,
    metric_buffer: Option<MetricBuffer>,
) -> anyhow::Result<Router> {
    let state = AppState {
        storage: Arc::new(storage),
        query_engine: Arc::new(query_engine),
        span_buffer: span_buffer.map(Arc::new),
        log_buffer: log_buffer.map(Arc::new),
        metric_buffer: metric_buffer.map(Arc::new),
    };

    // OTLP standard endpoints
    let router: Router = Router::new()
        .route("/health", get(health::health_check))
        .route("/ready", get(health::ready_check))
        .route("/v1/traces", post(ingestion::traces::ingest_traces))
        .route("/v1/logs", post(ingestion::logs::ingest_logs))
        .route("/v1/metrics", post(ingestion::metrics::ingest_metrics))
        .route("/v1/query/sql", post(query::execute_sql))
        .with_state(state);

    Ok(router)
}
