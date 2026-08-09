use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::query::{self, QueryEngine};
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

pub struct TestPipeline {
    pub cache_dir: TempDir,
    pub pipeline: IngestPipeline,
    query_engine: QueryEngine,
}

impl TestPipeline {
    pub async fn new(mut config: Config) -> Self {
        let cache_dir = TempDir::new().expect("tempdir");
        config.query.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
        let run_id = Uuid::new_v4();
        match config.ducklake.catalog_type.as_str() {
            "postgres" => {
                // Isolate concurrent test runs in the shared local Postgres catalog.
                let schema = format!("perf_{}", run_id.simple());
                config.ducklake.metadata_schema = schema;
            }
            _ => {
                let dl_dir = cache_dir.path().join("ducklake");
                std::fs::create_dir_all(&dl_dir).expect("ducklake dir");
                config.ducklake.catalog_type = "sqlite".to_string();
                config.ducklake.metadata_path = dl_dir
                    .join(format!("metadata-{}.sqlite", run_id))
                    .to_string_lossy()
                    .to_string();
                config.ducklake.metadata_schema = "main".to_string();
            }
        }
        if config.ducklake.data_path.contains("://") {
            // Keep object-storage-backed paths for integration validation, but isolate each run.
            let base = config.ducklake.data_path.trim_end_matches('/');
            config.ducklake.data_path = format!("{}/tests/{}/", base, run_id);
        } else {
            // Default to object storage for integration tests to validate committed data persistence.
            config.ducklake.data_path = format!("s3://warehouse/ducklake/tests/{}/", run_id);
        }
        let pipeline = IngestPipeline::new(&config).await.expect("ingest pipeline");

        let query_engine = query::create_query_engine(&config, Arc::new(pipeline.storage.clone()))
            .await
            .expect("query engine");

        Self {
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
