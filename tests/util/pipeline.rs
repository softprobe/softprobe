use softprobe_runtime::config::Config;
use softprobe_runtime::query::{self, QueryEngine};
use softprobe_runtime::ingest_engine::IngestPipeline;
use std::sync::Arc;
use tempfile::TempDir;

pub struct TestPipeline {
    pub config: Config,
    pub cache_dir: TempDir,
    pub pipeline: IngestPipeline,
    query_engine: QueryEngine,
}

impl TestPipeline {
    pub async fn new(mut config: Config) -> Self {
        let cache_dir = TempDir::new().expect("tempdir");
        config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
        config.ingest_engine.wal_dir =
            Some(cache_dir.path().join("wal").to_string_lossy().to_string());
        let pipeline = IngestPipeline::new(&config).await.expect("ingest pipeline");
        
        // Pass tiered storage to query engine so it can access buffer snapshots
        let query_engine = query::create_query_engine(&config, Arc::new(pipeline.storage.clone()))
            .await
            .expect("query engine");
        
        Self {
            config,
            cache_dir,
            pipeline,
            query_engine,
        }
    }

    pub async fn execute_query(
        &self,
        sql: &str,
    ) -> anyhow::Result<softprobe_runtime::query::duckdb::QueryResult> {
        self.query_engine.execute_query(sql).await
    }

    pub fn query_engine(&self) -> &QueryEngine {
        &self.query_engine
    }
}
