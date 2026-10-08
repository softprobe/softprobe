use crate::config::Config;
use crate::runtime_engine::DuckLakeScope;
use crate::storage::TieredStorage;
use std::sync::Arc;

pub mod cache;
pub mod duckdb;

#[derive(Clone)]
pub struct QueryEngine {
    duckdb: Arc<duckdb::DuckDBQueryEngine>,
}

pub async fn create_query_engine(
    config: &Config,
    tiered_storage: Arc<dyn TieredStorage>,
) -> anyhow::Result<QueryEngine> {
    let duckdb = Arc::new(duckdb::DuckDBQueryEngine::new(config, tiered_storage).await?);

    Ok(QueryEngine { duckdb })
}

/// Build a query engine whose DuckLake attachment matches `scope` (same scope as tenant-bound ingest).
pub async fn create_query_engine_for_scope(
    config: &Config,
    tiered_storage: Arc<dyn TieredStorage>,
    scope: &DuckLakeScope,
) -> anyhow::Result<QueryEngine> {
    if scope.metadata_schema.trim().is_empty() && scope.data_path.trim().is_empty() {
        return create_query_engine(config, tiered_storage).await;
    }
    let mut cfg = config.clone();
    cfg.ducklake.metadata_schema = scope.metadata_schema.clone();
    cfg.ducklake.data_path = scope.data_path.clone();
    let duckdb = Arc::new(duckdb::DuckDBQueryEngine::new(&cfg, tiered_storage).await?);
    Ok(QueryEngine { duckdb })
}

impl QueryEngine {
    /// DuckLake catalog alias used by this engine (e.g. `softprobe`).
    pub fn catalog_alias(&self) -> &str {
        self.duckdb.catalog_alias()
    }

    /// Schema-qualified layout prefix (`softprobe` or `softprobe.<tenant_schema>`).
    ///
    /// Must match ingest `layout_catalog_prefix` so Prom postings/sample SQL hits
    /// the same `metric_*` tables the writer populates.
    pub fn layout_catalog_prefix(&self) -> String {
        self.duckdb.layout_catalog_prefix()
    }

    pub async fn execute_query(&self, query: &str) -> anyhow::Result<duckdb::QueryResult> {
        self.duckdb.execute_query(query).await
    }

    /// Execute a DuckLake query against one tenant's resolved metadata schema.
    ///
    /// Runtime control endpoints are tenant-authenticated, so they must read from the same
    /// DuckLake scope that ingest used for that tenant instead of the process-level config schema.
    pub async fn execute_query_in_ducklake_scope(
        &self,
        query: &str,
        scope: &DuckLakeScope,
    ) -> anyhow::Result<duckdb::QueryResult> {
        self.duckdb
            .execute_query_in_ducklake_scope(query, scope)
            .await
    }
}
