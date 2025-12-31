pub mod buffer;
pub mod iceberg;
pub mod schema;
pub mod transaction;

use std::pin::Pin;
use std::sync::Arc;
use crate::config::Config;
use crate::models::{Span, Log};
use buffer::SimpleBuffer;
use anyhow::Result;

pub use iceberg::IcebergWriter;

// Type aliases for buffers using unified domain models
pub type SpanBuffer = SimpleBuffer<Span>;
pub type LogBuffer = SimpleBuffer<Log>;

/// Simplified storage - only Iceberg writer needed
pub struct Storage {
    pub iceberg_writer: Arc<IcebergWriter>,
}

pub async fn create_storage(config: &Config) -> anyhow::Result<Storage> {
    let iceberg_writer = Arc::new(IcebergWriter::new(config).await?);

    Ok(Storage {
        iceberg_writer,
    })
}

/// Create span buffer with Iceberg writer as flush callback
pub async fn create_span_buffer(
    config: &Config,
    iceberg_writer: Arc<IcebergWriter>,
) -> Result<SpanBuffer> {
    // Create flush callback that writes session batches to Iceberg
    // Each session becomes a row group in a single Parquet file
    let flush_callback = Arc::new(move |session_batches: Vec<Vec<Span>>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        let writer = iceberg_writer.clone();

        Box::pin(async move {
            writer.write_span_batches(session_batches).await
        })
    });

    Ok(SpanBuffer::new(
        "spans".to_string(),
        config.span_buffering.clone(),
        flush_callback,
    ))
}

/// Create log buffer with Iceberg writer as flush callback
pub async fn create_log_buffer(
    config: &Config,
    iceberg_writer: Arc<IcebergWriter>,
) -> Result<LogBuffer> {
    // Create flush callback that writes log batches to Iceberg
    let flush_callback = Arc::new(move |log_batches: Vec<Vec<Log>>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        let writer = iceberg_writer.clone();

        Box::pin(async move {
            writer.write_log_batches(log_batches).await
        })
    });

    Ok(LogBuffer::new(
        "logs".to_string(),
        config.span_buffering.clone(), // Reuse same config for now
        flush_callback,
    ))
}
