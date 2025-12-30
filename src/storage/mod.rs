pub mod iceberg;
pub mod schema;
pub mod span_buffer;
pub mod transaction;

use std::pin::Pin;
use std::sync::Arc;
use crate::config::Config;
use span_buffer::SimpleSpanBuffer;
use anyhow::Result;

pub use iceberg::IcebergWriter;

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

/// Create simple span buffer with Iceberg writer as flush callback
pub async fn create_span_buffer(
    config: &Config,
    iceberg_writer: Arc<IcebergWriter>,
) -> Result<SimpleSpanBuffer> {
    // Create flush callback that writes session batches to Iceberg
    // Each session becomes a row group in a single Parquet file
    let flush_callback = Arc::new(move |session_batches: Vec<Vec<span_buffer::SpanData>>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        let writer = iceberg_writer.clone();

        Box::pin(async move {
            writer.write_session_batches(session_batches).await
        })
    });

    Ok(SimpleSpanBuffer::new(
        config.span_buffering.clone(),
        flush_callback,
    ))
}
