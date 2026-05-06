use crate::config::Config;
use crate::storage::TieredStorage;
use crate::tenant_ducklake::TenantDuckLakeScope;
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

impl QueryEngine {
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
        scope: &TenantDuckLakeScope,
    ) -> anyhow::Result<duckdb::QueryResult> {
        self.duckdb
            .execute_query_in_ducklake_scope(query, scope)
            .await
    }
}
