use crate::catalog::DropdownCatalog;
use crate::config::Config;
use crate::models::{Log, Metric, Span};
use crate::runtime_engine::{DuckLakeScope, DuckLakeScopeResolver};
use crate::storage::buffer::{FlushCallback, FlushFuture, PreAddCallback, PreAddFuture};
use crate::storage::ducklake::DuckLakeWriter;
use crate::storage::{create_log_buffer, create_metric_buffer, create_span_buffer, Storage};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;

fn default_span_pre_add() -> Arc<PreAddCallback<Span>> {
    Arc::new(
        move |items: Vec<Span>, _request_size: usize| -> PreAddFuture<Span> {
            Box::pin(async move { Ok(items) })
        },
    )
}

fn default_log_pre_add() -> Arc<PreAddCallback<Log>> {
    Arc::new(
        move |items: Vec<Log>, _request_size: usize| -> PreAddFuture<Log> {
            Box::pin(async move { Ok(items) })
        },
    )
}

fn default_metric_pre_add() -> Arc<PreAddCallback<Metric>> {
    Arc::new(
        move |items: Vec<Metric>, _request_size: usize| -> PreAddFuture<Metric> {
            Box::pin(async move { Ok(items) })
        },
    )
}

fn span_flush_callback(
    writer: Arc<DuckLakeWriter>,
    _cache_dir: Option<PathBuf>,
) -> Arc<FlushCallback<Span>> {
    Arc::new(move |batches: Vec<Vec<Span>>| -> FlushFuture {
        let writer = writer.clone();
        Box::pin(async move { writer.write_span_batches(batches).await })
    })
}

fn log_flush_callback(
    writer: Arc<DuckLakeWriter>,
    _cache_dir: Option<PathBuf>,
) -> Arc<FlushCallback<Log>> {
    Arc::new(move |batches: Vec<Vec<Log>>| -> FlushFuture {
        let writer = writer.clone();
        Box::pin(async move { writer.write_log_batches(batches).await })
    })
}

fn metric_flush_callback(
    writer: Arc<DuckLakeWriter>,
    _cache_dir: Option<PathBuf>,
) -> Arc<FlushCallback<Metric>> {
    Arc::new(move |batches: Vec<Vec<Metric>>| -> FlushFuture {
        let writer = writer.clone();
        Box::pin(async move { writer.write_metric_batches(batches).await })
    })
}

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

    pub async fn add_spans(&self, items: Vec<Span>, request_size: usize) -> Result<()> {
        self.storage
            .span_buffer
            .add_items(items, request_size)
            .await
    }

    pub async fn add_logs(&self, items: Vec<Log>, request_size: usize) -> Result<()> {
        self.storage.log_buffer.add_items(items, request_size).await
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, request_size: usize) -> Result<()> {
        self.storage
            .metric_buffer
            .add_items(items, request_size)
            .await
    }

    pub async fn force_flush_spans(&self) -> Result<()> {
        self.storage.span_buffer.force_flush().await
    }

    pub async fn force_flush_logs(&self) -> Result<()> {
        self.storage.log_buffer.force_flush().await
    }

    pub async fn force_flush_metrics(&self) -> Result<()> {
        self.storage.metric_buffer.force_flush().await
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

        let span_buffer = create_span_buffer(
            config,
            Some(default_span_pre_add()),
            span_flush_callback(writer.clone(), cache_dir.clone()),
        )
        .await?;
        let log_buffer = create_log_buffer(
            config,
            Some(default_log_pre_add()),
            log_flush_callback(writer.clone(), cache_dir.clone()),
        )
        .await?;
        let metric_buffer = create_metric_buffer(
            config,
            Some(default_metric_pre_add()),
            metric_flush_callback(writer.clone(), cache_dir.clone()),
        )
        .await?;

        let storage = Storage::new(
            writer,
            span_buffer,
            log_buffer,
            metric_buffer,
            cache_dir.clone(),
        );

        Ok(Self {
            storage,
            dropdown_catalog,
            cache_dir,
        })
    }

    /// Build [`Storage`] (spans/logs/metrics buffers + tenant-bound writer) for one registry row.
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
        let writer =
            Arc::new(DuckLakeWriter::new(&scoped_config, dropdown_catalog, tenant_ducklake).await?);
        let cache_dir = config.ingest_engine.cache_dir.as_ref().map(PathBuf::from);
        let span_buffer = create_span_buffer(
            config,
            Some(default_span_pre_add()),
            span_flush_callback(writer.clone(), cache_dir.clone()),
        )
        .await?;
        let log_buffer = create_log_buffer(
            config,
            Some(default_log_pre_add()),
            log_flush_callback(writer.clone(), cache_dir.clone()),
        )
        .await?;
        let metric_buffer = create_metric_buffer(
            config,
            Some(default_metric_pre_add()),
            metric_flush_callback(writer.clone(), cache_dir.clone()),
        )
        .await?;
        Ok(Storage::new(
            writer,
            span_buffer,
            log_buffer,
            metric_buffer,
            cache_dir,
        ))
    }

    pub async fn add_spans(&self, items: Vec<Span>, request_size: usize) -> Result<()> {
        self.storage
            .span_buffer
            .add_items(items, request_size)
            .await
    }

    pub async fn add_logs(&self, items: Vec<Log>, request_size: usize) -> Result<()> {
        self.storage.log_buffer.add_items(items, request_size).await
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, request_size: usize) -> Result<()> {
        self.storage
            .metric_buffer
            .add_items(items, request_size)
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

    pub async fn force_flush_spans(&self) -> Result<()> {
        self.storage.span_buffer.force_flush().await
    }

    pub async fn force_flush_logs(&self) -> Result<()> {
        self.storage.log_buffer.force_flush().await
    }

    pub async fn force_flush_metrics(&self) -> Result<()> {
        self.storage.metric_buffer.force_flush().await
    }

    pub async fn run_optimizer_once(&self) -> Result<()> {
        Ok(())
    }

    pub fn list_wal_files(&self, _kind: &str) -> Result<Vec<PathBuf>> {
        Ok(Vec::new())
    }

    pub fn list_staged_files(&self, _kind: &str) -> Result<Vec<PathBuf>> {
        Ok(Vec::new())
    }

    pub fn writer(&self) -> Arc<DuckLakeWriter> {
        self.storage.writer.clone()
    }

    pub fn cache_dir(&self) -> Option<PathBuf> {
        self.cache_dir.clone()
    }
}
