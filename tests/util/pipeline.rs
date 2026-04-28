use splake::config::Config;
use splake::query::{self, QueryEngine};
use splake::ingest_engine::IngestPipeline;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

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
        // Avoid background optimizer races in integration tests that assert staged-file state.
        config.ingest_engine.optimizer_interval_seconds = 3600;
        if config.ducklake.is_none() {
            config.ducklake = Some(config.ducklake_or_default());
        }
        if let Some(ducklake) = config.ducklake.as_mut() {
            let dl_dir = cache_dir.path().join("ducklake");
            std::fs::create_dir_all(&dl_dir).expect("ducklake dir");
            ducklake.metadata_path = dl_dir
                .join(format!("metadata-{}.ducklake", Uuid::new_v4()))
                .to_string_lossy()
                .to_string();
            ducklake.data_path = dl_dir
                .join("data")
                .to_string_lossy()
                .to_string();
            std::fs::create_dir_all(&ducklake.data_path).expect("ducklake data dir");
        }
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
    ) -> anyhow::Result<splake::query::duckdb::QueryResult> {
        self.query_engine.execute_query(sql).await
    }

    pub fn query_engine(&self) -> &QueryEngine {
        &self.query_engine
    }
}
