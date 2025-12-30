use std::sync::Arc;
use crate::config::Config;

pub mod duckdb;

pub struct QueryEngine {
    pub duckdb: Arc<duckdb::DuckDBQueryEngine>,
}

pub async fn create_query_engine(config: &Config) -> anyhow::Result<QueryEngine> {
    let duckdb = Arc::new(duckdb::DuckDBQueryEngine::new(config).await?);
    
    Ok(QueryEngine {
        duckdb,
    })
}

