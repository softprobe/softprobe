use crate::catalog::DropdownCatalog;
use crate::config::Config;
use crate::models::{Log, Metric, Span};
use crate::runtime_engine::{DuckLakeScope, DuckLakeScopeResolver};
use crate::storage::ducklake::DuckLakeWriter;
use crate::storage::Storage;
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;

/// Operational ingest surface for one tenant-bound [`Storage`].
#[derive(Clone)]
pub struct IngestEngine {
    storage: Arc<Storage>,
}

impl IngestEngine {
    pub fn from_storage(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub fn writer(&self) -> Arc<DuckLakeWriter> {
        self.storage.writer.clone()
    }

    pub async fn add_spans(&self, items: Vec<Span>, _request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.storage.writer.write_span_batches(vec![items]).await
    }

    pub async fn add_logs(&self, items: Vec<Log>, _request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.storage.writer.write_log_batches(vec![items]).await
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, _request_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.storage.writer.write_metric_batches(vec![items]).await
    }

    /// No-op: ingest is flush-through (OTel collector batches upstream).
    pub async fn force_flush_spans(&self) -> Result<()> {
        Ok(())
    }

    /// No-op: ingest is flush-through (OTel collector batches upstream).
    pub async fn force_flush_logs(&self) -> Result<()> {
        Ok(())
    }

    /// No-op: ingest is flush-through (OTel collector batches upstream).
    pub async fn force_flush_metrics(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct IngestPipeline {
    pub storage: Storage,
    /// Shared with maintenance scheduler for TTL prune.
    pub dropdown_catalog: Option<Arc<DropdownCatalog>>,
    cache_dir: Option<PathBuf>,
}

impl IngestPipeline {
    pub async fn new(config: &Config) -> Result<Self> {
        let dropdown_catalog = DropdownCatalog::connect(config).await?;
        let tenant_ducklake = DuckLakeScopeResolver::connect(config).await?;
        let writer =
            Arc::new(DuckLakeWriter::new(config, dropdown_catalog.clone(), tenant_ducklake).await?);
        let cache_dir = config.ingest_engine.cache_dir.as_ref().map(PathBuf::from);
        let storage = Storage::new(writer);

        Ok(Self {
            storage,
            dropdown_catalog,
            cache_dir,
        })
    }

    /// Build [`Storage`] (tenant-bound writer) for one registry row.
    pub async fn build_tenant_storage(
        config: &Config,
        dropdown_catalog: Option<Arc<DropdownCatalog>>,
        tenant_ducklake: Option<DuckLakeScopeResolver>,
        _tenant_id: String,
        scope: DuckLakeScope,
    ) -> Result<Storage> {
        let mut scoped_config = config.clone();
        let mut ducklake = scoped_config.ducklake_or_default();
        ducklake.metadata_schema = scope.metadata_schema;
        ducklake.data_path = scope.data_path;
        scoped_config.ducklake = Some(ducklake);
        let writer = Arc::new(
            DuckLakeWriter::new_scope_bound(&scoped_config, dropdown_catalog, tenant_ducklake)
                .await?,
        );
        Ok(Storage::new(writer))
    }

    pub async fn add_spans(&self, items: Vec<Span>, request_size: usize) -> Result<()> {
        IngestEngine::from_storage(Arc::new(self.storage.clone()))
            .add_spans(items, request_size)
            .await
    }

    pub async fn add_logs(&self, items: Vec<Log>, request_size: usize) -> Result<()> {
        IngestEngine::from_storage(Arc::new(self.storage.clone()))
            .add_logs(items, request_size)
            .await
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, request_size: usize) -> Result<()> {
        IngestEngine::from_storage(Arc::new(self.storage.clone()))
            .add_metrics(items, request_size)
            .await
    }

    pub async fn write_span_batches(&self, batches: Vec<Vec<Span>>) -> Result<()> {
        self.storage.writer.write_span_batches(batches).await
    }

    pub async fn write_log_batches(&self, batches: Vec<Vec<Log>>) -> Result<()> {
        self.storage.writer.write_log_batches(batches).await
    }

    pub async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> Result<()> {
        self.storage.writer.write_metric_batches(batches).await
    }

    /// No-op: ingest is flush-through.
    pub async fn force_flush_spans(&self) -> Result<()> {
        Ok(())
    }

    /// No-op: ingest is flush-through.
    pub async fn force_flush_logs(&self) -> Result<()> {
        Ok(())
    }

    /// No-op: ingest is flush-through.
    pub async fn force_flush_metrics(&self) -> Result<()> {
        Ok(())
    }

    pub fn writer(&self) -> Arc<DuckLakeWriter> {
        self.storage.writer.clone()
    }

    pub fn cache_dir(&self) -> Option<PathBuf> {
        self.cache_dir.clone()
    }
}
