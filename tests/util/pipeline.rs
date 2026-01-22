use softprobe_otlp_backend::config::Config;
use softprobe_otlp_backend::query::{self, QueryEngine};
use softprobe_otlp_backend::storage::IngestPipeline;
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
        let pipeline = IngestPipeline::new(&config).await.expect("ingest pipeline");
        let query_engine = query::create_query_engine(&config)
            .await
            .expect("query engine");
        Self {
            config,
            cache_dir,
            pipeline,
            query_engine,
        }
    }

    pub fn builder() -> TestPipelineBuilder {
        TestPipelineBuilder::default()
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

#[derive(Default)]
pub struct TestPipelineBuilder {
    config: Option<Config>,
}

impl TestPipelineBuilder {
    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub async fn build(self) -> TestPipeline {
        let config = self
            .config
            .unwrap_or_else(|| Config::load().expect("config"));
        TestPipeline::new(config).await
    }
}
