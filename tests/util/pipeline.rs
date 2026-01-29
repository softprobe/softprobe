use softprobe_otlp_backend::config::Config;
use softprobe_otlp_backend::query::{self, QueryEngine};
use softprobe_otlp_backend::storage::IngestPipeline;
use std::sync::Arc;
use tempfile::TempDir;

pub struct TestPipeline {
    #[allow(dead_code)]
    pub config: Config,
    pub cache_dir: TempDir,
    pub pipeline: IngestPipeline,
    query_engine: QueryEngine,
}

impl TestPipeline {
    pub async fn new(mut config: Config) -> Self {
        let cache_dir = TempDir::new().expect("tempdir");
        config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
        let pipeline = IngestPipeline::new(&config).await.expect("ingest pipeline");
        
        // Pass pipeline to query engine so it can access buffer snapshots
        let query_engine = query::create_query_engine(&config, Some(Arc::new(pipeline.clone())))
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
    ) -> anyhow::Result<softprobe_otlp_backend::query::duckdb::QueryResult> {
        self.query_engine.execute_query(sql).await
    }

    pub fn query_engine(&self) -> &QueryEngine {
        &self.query_engine
    }
}
