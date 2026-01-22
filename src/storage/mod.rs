pub mod buffer;
pub mod iceberg;
pub mod schema;
pub mod transaction;

use crate::config::Config;
use crate::ingest_engine::IngestEngine;
use crate::models::{Log, Metric, Span};
use crate::storage::iceberg::arrow::{
    logs_to_record_batch, metrics_to_record_batch, spans_to_record_batch,
};
use anyhow::Result;
use buffer::SimpleBuffer;
use std::pin::Pin;
use std::sync::Arc;

pub use iceberg::IcebergWriter;

// Type aliases for buffers using unified domain models
pub type SpanBuffer = SimpleBuffer<Span>;
pub type LogBuffer = SimpleBuffer<Log>;
pub type MetricBuffer = SimpleBuffer<Metric>;

/// Simplified storage - only Iceberg writer needed
pub struct Storage {
    pub iceberg_writer: Arc<IcebergWriter>,
    pub ingest_engine: Option<Arc<IngestEngine>>,
}

pub struct IngestPipeline {
    pub storage: Storage,
    pub span_buffer: SpanBuffer,
    pub log_buffer: LogBuffer,
    pub metric_buffer: MetricBuffer,
}

impl IngestPipeline {
    pub async fn new(config: &Config) -> Result<Self> {
        let storage = create_storage(config).await?;
        let span_buffer = create_span_buffer(
            config,
            storage.iceberg_writer.clone(),
            storage.ingest_engine.clone(),
        )
        .await?;
        let log_buffer = create_log_buffer(
            config,
            storage.iceberg_writer.clone(),
            storage.ingest_engine.clone(),
        )
        .await?;
        let metric_buffer = create_metric_buffer(
            config,
            storage.iceberg_writer.clone(),
            storage.ingest_engine.clone(),
        )
        .await?;

        Ok(Self {
            storage,
            span_buffer,
            log_buffer,
            metric_buffer,
        })
    }

    pub async fn add_spans(&self, items: Vec<Span>, request_size: usize) -> Result<()> {
        self.span_buffer.add_items(items, request_size).await
    }

    pub async fn add_logs(&self, items: Vec<Log>, request_size: usize) -> Result<()> {
        self.log_buffer.add_items(items, request_size).await
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, request_size: usize) -> Result<()> {
        self.metric_buffer.add_items(items, request_size).await
    }

    pub async fn write_span_batches(&self, batches: Vec<Vec<Span>>) -> Result<()> {
        self.storage
            .iceberg_writer
            .write_span_batches(batches)
            .await
    }

    pub async fn write_log_batches(&self, batches: Vec<Vec<Log>>) -> Result<()> {
        self.storage.iceberg_writer.write_log_batches(batches).await
    }

    pub async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> Result<()> {
        self.storage
            .iceberg_writer
            .write_metric_batches(batches)
            .await
    }

    pub async fn write_wal_spans(&self, items: Vec<Span>, request_size: usize) -> Result<()> {
        let Some(engine) = &self.storage.ingest_engine else {
            return Err(anyhow::anyhow!("ingest engine not enabled"));
        };
        let schema = self.storage.iceberg_writer.spans_schema().await?;
        engine
            .wal_writer()
            .write_batch(
                "spans",
                &items,
                request_size,
                schema.as_ref(),
                spans_to_record_batch,
            )
            .await
    }

    pub async fn write_wal_logs(&self, items: Vec<Log>, request_size: usize) -> Result<()> {
        let Some(engine) = &self.storage.ingest_engine else {
            return Err(anyhow::anyhow!("ingest engine not enabled"));
        };
        let schema = self.storage.iceberg_writer.logs_schema().await?;
        engine
            .wal_writer()
            .write_batch(
                "logs",
                &items,
                request_size,
                schema.as_ref(),
                logs_to_record_batch,
            )
            .await
    }

    pub async fn write_wal_metrics(&self, items: Vec<Metric>, request_size: usize) -> Result<()> {
        let Some(engine) = &self.storage.ingest_engine else {
            return Err(anyhow::anyhow!("ingest engine not enabled"));
        };
        let schema = self.storage.iceberg_writer.metrics_schema().await?;
        engine
            .wal_writer()
            .write_batch(
                "metrics",
                &items,
                request_size,
                schema.as_ref(),
                metrics_to_record_batch,
            )
            .await
    }

    pub async fn force_flush_spans(&self) -> Result<()> {
        self.span_buffer.force_flush().await
    }

    pub async fn force_flush_logs(&self) -> Result<()> {
        self.log_buffer.force_flush().await
    }

    pub async fn force_flush_metrics(&self) -> Result<()> {
        self.metric_buffer.force_flush().await
    }

    pub async fn run_optimizer_once(&self) -> Result<()> {
        if let Some(engine) = &self.storage.ingest_engine {
            engine.run_optimizer_once().await?;
        }
        Ok(())
    }

    pub fn list_wal_files(&self, kind: &str) -> Result<Vec<std::path::PathBuf>> {
        let Some(engine) = &self.storage.ingest_engine else {
            return Ok(Vec::new());
        };
        engine.list_wal_files(kind)
    }

    pub fn list_staged_files(&self, kind: &str) -> Result<Vec<std::path::PathBuf>> {
        let Some(engine) = &self.storage.ingest_engine else {
            return Ok(Vec::new());
        };
        engine.list_staged_files(kind)
    }
}

pub async fn create_storage(config: &Config) -> anyhow::Result<Storage> {
    let iceberg_writer = Arc::new(IcebergWriter::new(config).await?);
    let ingest_engine = if config.ingest_engine.enabled {
        let engine = Arc::new(IngestEngine::new(config, iceberg_writer.clone()).await?);
        engine.start_optimizer();
        Some(engine)
    } else {
        None
    };

    Ok(Storage {
        iceberg_writer,
        ingest_engine,
    })
}

/// Create span buffer with Iceberg writer as flush callback
pub async fn create_span_buffer(
    config: &Config,
    iceberg_writer: Arc<IcebergWriter>,
    ingest_engine: Option<Arc<IngestEngine>>,
) -> Result<SpanBuffer> {
    let (pre_add_callback, flush_callback): (Option<Arc<_>>, Arc<_>) = if let Some(engine) =
        ingest_engine
    {
        (
            Some(engine.span_pre_add_callback()),
            engine.span_flush_callback(),
        )
    } else {
        // Create flush callback that writes session batches to Iceberg
        // Each session becomes a row group in a single Parquet file
        let flush_callback = Arc::new(move |session_batches: Vec<Vec<Span>>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
            let writer = iceberg_writer.clone();

            Box::pin(async move {
                writer.write_span_batches(session_batches).await
            })
        });
        (None, flush_callback)
    };

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
    iceberg_writer: Arc<IcebergWriter>,
    ingest_engine: Option<Arc<IngestEngine>>,
) -> Result<LogBuffer> {
    let (pre_add_callback, flush_callback): (Option<Arc<_>>, Arc<_>) = if let Some(engine) =
        ingest_engine
    {
        (
            Some(engine.log_pre_add_callback()),
            engine.log_flush_callback(),
        )
    } else {
        // Create flush callback that writes log batches to Iceberg
        let flush_callback = Arc::new(move |log_batches: Vec<Vec<Log>>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
            let writer = iceberg_writer.clone();

            Box::pin(async move {
                writer.write_log_batches(log_batches).await
            })
        });
        (None, flush_callback)
    };

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
    iceberg_writer: Arc<IcebergWriter>,
    ingest_engine: Option<Arc<IngestEngine>>,
) -> Result<MetricBuffer> {
    let (pre_add_callback, flush_callback): (Option<Arc<_>>, Arc<_>) = if let Some(engine) =
        ingest_engine
    {
        (
            Some(engine.metric_pre_add_callback()),
            engine.metric_flush_callback(),
        )
    } else {
        // Create flush callback that writes metric batches to Iceberg
        // Metrics are organized by metric_name, NOT session_id
        let flush_callback = Arc::new(move |metric_batches: Vec<Vec<Metric>>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
            let writer = iceberg_writer.clone();

            Box::pin(async move {
                writer.write_metric_batches(metric_batches).await
            })
        });
        (None, flush_callback)
    };

    Ok(MetricBuffer::new(
        "metrics".to_string(),
        config.span_buffering.clone(), // Reuse same config for now
        pre_add_callback,
        flush_callback,
    ))
}
