use crate::catalog::DropdownCatalog;
use crate::config::Config;
use crate::models::{Log, Metric, Span};
use crate::storage::buffer::{FlushCallback, FlushFuture, PreAddCallback, PreAddFuture};
use crate::storage::{
    create_log_buffer, create_metric_buffer, create_span_buffer, DuckLakeWriter, Storage,
};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct IngestEngine {
    writer: Arc<DuckLakeWriter>,
}

#[derive(Clone)]
pub struct IngestPipeline {
    pub storage: Storage,
    ingest_engine: Arc<IngestEngine>,
    /// Shared with maintenance scheduler for TTL prune.
    pub dropdown_catalog: Option<Arc<DropdownCatalog>>,
    cache_dir: Option<PathBuf>,
}

impl IngestEngine {
    pub fn new(writer: Arc<DuckLakeWriter>) -> Self {
        Self { writer }
    }

    pub fn span_pre_add_callback(&self) -> Arc<PreAddCallback<Span>> {
        Arc::new(
            move |items: Vec<Span>, _request_size: usize| -> PreAddFuture<Span> {
                Box::pin(async move { Ok(items) })
            },
        )
    }

    pub fn log_pre_add_callback(&self) -> Arc<PreAddCallback<Log>> {
        Arc::new(
            move |items: Vec<Log>, _request_size: usize| -> PreAddFuture<Log> {
                Box::pin(async move { Ok(items) })
            },
        )
    }

    pub fn metric_pre_add_callback(&self) -> Arc<PreAddCallback<Metric>> {
        Arc::new(
            move |items: Vec<Metric>, _request_size: usize| -> PreAddFuture<Metric> {
                Box::pin(async move { Ok(items) })
            },
        )
    }

    pub fn span_flush_callback(
        &self,
        writer: Arc<DuckLakeWriter>,
        _cache_dir: Option<PathBuf>,
    ) -> Arc<FlushCallback<Span>> {
        Arc::new(move |batches: Vec<Vec<Span>>| -> FlushFuture {
            let writer = writer.clone();
            Box::pin(async move { writer.write_span_batches(batches).await })
        })
    }

    pub fn log_flush_callback(
        &self,
        writer: Arc<DuckLakeWriter>,
        _cache_dir: Option<PathBuf>,
    ) -> Arc<FlushCallback<Log>> {
        Arc::new(move |batches: Vec<Vec<Log>>| -> FlushFuture {
            let writer = writer.clone();
            Box::pin(async move { writer.write_log_batches(batches).await })
        })
    }

    pub fn metric_flush_callback(
        &self,
        writer: Arc<DuckLakeWriter>,
        _cache_dir: Option<PathBuf>,
    ) -> Arc<FlushCallback<Metric>> {
        Arc::new(move |batches: Vec<Vec<Metric>>| -> FlushFuture {
            let writer = writer.clone();
            Box::pin(async move { writer.write_metric_batches(batches).await })
        })
    }
}

impl IngestPipeline {
    pub async fn new(config: &Config) -> Result<Self> {
        let dropdown_catalog = DropdownCatalog::connect(config).await?;
        let writer = Arc::new(
            DuckLakeWriter::new(config, dropdown_catalog.clone())
                .await?,
        );
        let ingest_engine = Arc::new(IngestEngine::new(writer.clone()));
        let cache_dir = config.ingest_engine.cache_dir.as_ref().map(PathBuf::from);

        let span_buffer = create_span_buffer(
            config,
            Some(ingest_engine.span_pre_add_callback()),
            ingest_engine.span_flush_callback(writer.clone(), cache_dir.clone()),
        )
        .await?;
        let log_buffer = create_log_buffer(
            config,
            Some(ingest_engine.log_pre_add_callback()),
            ingest_engine.log_flush_callback(writer.clone(), cache_dir.clone()),
        )
        .await?;
        let metric_buffer = create_metric_buffer(
            config,
            Some(ingest_engine.metric_pre_add_callback()),
            ingest_engine.metric_flush_callback(writer.clone(), cache_dir.clone()),
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
            ingest_engine,
            dropdown_catalog,
            cache_dir,
        })
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
        self.ingest_engine.writer.clone()
    }

    pub fn cache_dir(&self) -> Option<PathBuf> {
        self.cache_dir.clone()
    }
}
