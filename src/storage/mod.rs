pub mod buffer;
pub mod ducklake;
pub mod iceberg;
pub mod schema;
pub mod transaction;

use crate::config::Config;
use crate::models::{Log, Metric, Span};
use anyhow::Result;
use buffer::SimpleBuffer;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use ducklake::DuckLakeWriter as IcebergWriter;
use buffer::{FlushCallback, PreAddCallback};

// Type aliases for buffers using unified domain models
pub type SpanBuffer = SimpleBuffer<Span>;
pub type LogBuffer = SimpleBuffer<Log>;
pub type MetricBuffer = SimpleBuffer<Metric>;

/// Tiered storage interface for query-time access.
pub trait TieredStorage: Send + Sync {
    fn iceberg_writer(&self) -> Arc<IcebergWriter>;
    fn snapshot_buffered_spans_sync(&self) -> Option<Vec<Span>>;
    fn snapshot_buffered_logs_sync(&self) -> Option<Vec<Log>>;
    fn snapshot_buffered_metrics_sync(&self) -> Option<Vec<Metric>>;
    fn list_staged_files(&self, kind: &str) -> Result<Vec<PathBuf>>;
    fn staged_watermark_signature(&self, kind: &str) -> String;
}

/// Storage components for buffer + staged + Iceberg access.
#[derive(Clone)]
pub struct Storage {
    pub iceberg_writer: Arc<IcebergWriter>,
    pub span_buffer: SpanBuffer,
    pub log_buffer: LogBuffer,
    pub metric_buffer: MetricBuffer,
    cache_dir: Option<PathBuf>,
}

impl Storage {
    pub fn new(
        iceberg_writer: Arc<IcebergWriter>,
        span_buffer: SpanBuffer,
        log_buffer: LogBuffer,
        metric_buffer: MetricBuffer,
        cache_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            iceberg_writer,
            span_buffer,
            log_buffer,
            metric_buffer,
            cache_dir,
        }
    }
}

impl TieredStorage for Storage {
    fn iceberg_writer(&self) -> Arc<IcebergWriter> {
        self.iceberg_writer.clone()
    }

    fn snapshot_buffered_spans_sync(&self) -> Option<Vec<Span>> {
        self.span_buffer.snapshot_items_sync()
    }

    fn snapshot_buffered_logs_sync(&self) -> Option<Vec<Log>> {
        self.log_buffer.snapshot_items_sync()
    }

    fn snapshot_buffered_metrics_sync(&self) -> Option<Vec<Metric>> {
        self.metric_buffer.snapshot_items_sync()
    }

    fn list_staged_files(&self, kind: &str) -> Result<Vec<PathBuf>> {
        let Some(cache_dir) = self.cache_dir.as_ref() else {
            return Ok(Vec::new());
        };
        let staged_dir = cache_dir.join(kind);
        Ok(list_parquet_files(&staged_dir)?)
    }

    fn staged_watermark_signature(&self, kind: &str) -> String {
        let Some(cache_dir) = self.cache_dir.as_ref() else {
            return String::new();
        };
        let marker_path = cache_dir
            .join("staged_watermarks")
            .join(format!("{kind}.txt"));
        if let Ok(metadata) = std::fs::metadata(&marker_path) {
            if let Ok(modified) = metadata.modified() {
                let modified = chrono::DateTime::<chrono::Utc>::from(modified);
                return modified.to_rfc3339();
            }
        }
        String::new()
    }
}

/// Create span buffer with Iceberg writer as flush callback
pub async fn create_span_buffer(
    config: &Config,
    pre_add_callback: Option<Arc<PreAddCallback<Span>>>,
    flush_callback: Arc<FlushCallback<Span>>,
) -> Result<SpanBuffer> {
    Ok(SpanBuffer::new(
        "spans".to_string(),
        config.span_buffering.clone(),
        pre_add_callback,
        flush_callback,
    ))
}

/// Create log buffer with Iceberg writer as flush callback
pub async fn create_log_buffer(
    config: &Config,
    pre_add_callback: Option<Arc<PreAddCallback<Log>>>,
    flush_callback: Arc<FlushCallback<Log>>,
) -> Result<LogBuffer> {
    Ok(LogBuffer::new(
        "logs".to_string(),
        config.span_buffering.clone(), // Reuse same config for now
        pre_add_callback,
        flush_callback,
    ))
}

/// Create metric buffer with Iceberg writer as flush callback
pub async fn create_metric_buffer(
    config: &Config,
    pre_add_callback: Option<Arc<PreAddCallback<Metric>>>,
    flush_callback: Arc<FlushCallback<Metric>>,
) -> Result<MetricBuffer> {
    Ok(MetricBuffer::new(
        "metrics".to_string(),
        config.span_buffering.clone(), // Reuse same config for now
        pre_add_callback,
        flush_callback,
    ))
}

fn list_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_parquet_files(dir, &mut files)?;
    Ok(files)
}

fn collect_parquet_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, out)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    Ok(())
}
