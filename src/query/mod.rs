use crate::config::Config;
use std::sync::Arc;

pub mod cache;
pub mod duckdb;

#[derive(Clone)]
pub struct QueryEngine {
    duckdb: Arc<duckdb::DuckDBQueryEngine>,
}

pub async fn create_query_engine(config: &Config) -> anyhow::Result<QueryEngine> {
    let duckdb = Arc::new(duckdb::DuckDBQueryEngine::new(config).await?);

    Ok(QueryEngine { duckdb })
}

impl QueryEngine {
    pub async fn execute_query(&self, query: &str) -> anyhow::Result<duckdb::QueryResult> {
        self.duckdb.execute_query(query).await
    }
}
