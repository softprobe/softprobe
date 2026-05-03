pub mod buffer;
pub mod ducklake;
pub mod iceberg;
pub mod schema;
pub mod transaction;

use crate::config::Config;
use crate::models::{Log, Metric, Span};
use anyhow::Result;
use buffer::SimpleBuffer;
use std::path::PathBuf;
use std::sync::Arc;

use buffer::{FlushCallback, PreAddCallback};
pub use ducklake::DuckLakeWriter;

// Type aliases for buffers using unified domain models
pub type SpanBuffer = SimpleBuffer<Span>;
pub type LogBuffer = SimpleBuffer<Log>;
pub type MetricBuffer = SimpleBuffer<Metric>;

/// Tiered storage interface for query-time access.
pub trait TieredStorage: Send + Sync {
    fn writer(&self) -> Arc<DuckLakeWriter>;
    /// Monotonic counter bumped after each successful DuckLake table mutation; query workers use it
    /// to reattach so catalog metadata matches the writer connection.
    fn catalog_write_generation(&self) -> u64;
    fn snapshot_buffered_spans_sync(&self) -> Option<Vec<Span>>;
    fn snapshot_buffered_logs_sync(&self) -> Option<Vec<Log>>;
    fn snapshot_buffered_metrics_sync(&self) -> Option<Vec<Metric>>;
    fn list_staged_files(&self, kind: &str) -> Result<Vec<PathBuf>>;
    fn staged_watermark_signature(&self, kind: &str) -> String;
}

/// Storage components for buffer + staged + Iceberg access.
#[derive(Clone)]
pub struct Storage {
    pub writer: Arc<DuckLakeWriter>,
    pub span_buffer: SpanBuffer,
    pub log_buffer: LogBuffer,
    pub metric_buffer: MetricBuffer,
}

impl Storage {
    pub fn new(
        writer: Arc<DuckLakeWriter>,
        span_buffer: SpanBuffer,
        log_buffer: LogBuffer,
        metric_buffer: MetricBuffer,
        _cache_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            writer,
            span_buffer,
            log_buffer,
            metric_buffer,
        }
    }
}

impl TieredStorage for Storage {
    fn writer(&self) -> Arc<DuckLakeWriter> {
        self.writer.clone()
    }

    fn catalog_write_generation(&self) -> u64 {
        self.writer.catalog_write_generation()
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
    fn list_staged_files(&self, _kind: &str) -> Result<Vec<PathBuf>> {
        Ok(Vec::new())
    }
    fn staged_watermark_signature(&self, _kind: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::TieredStorage;

    #[tokio::test]
    async fn tiered_storage_default_paths() {
        let (storage, _t) = crate::test_support::sample_storage().await.expect("storage");
        assert!(storage.list_staged_files("any").unwrap().is_empty());
        assert!(storage.staged_watermark_signature("any").is_empty());
        let _ = storage.writer();
        assert_eq!(
            storage.snapshot_buffered_spans_sync().as_ref().map(|v| v.len()),
            Some(0)
        );
        assert_eq!(
            storage.snapshot_buffered_logs_sync().as_ref().map(|v| v.len()),
            Some(0)
        );
        assert_eq!(
            storage.snapshot_buffered_metrics_sync().as_ref().map(|v| v.len()),
            Some(0)
        );
    }
}
