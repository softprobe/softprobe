/// Modular Iceberg storage implementation
///
/// This module provides a clean, composable architecture for Iceberg operations:
/// - catalog: Shared REST catalog initialization
/// - tables: Table schema definitions (traces, logs)
/// - arrow: Arrow RecordBatch conversions
/// - writer: Generic write logic with retry and row group isolation

pub mod catalog;
pub mod tables;
pub mod arrow;
pub mod writer;

use crate::config::Config;
use crate::models::{Span, Log, Metric};
use anyhow::Result;
use iceberg::{TableIdent, Catalog, TableCreation};
use std::sync::Arc;
use tracing::info;

pub use catalog::IcebergCatalog;
pub use tables::{TraceTable, OtlpLogsTable, OtlpMetricsTable};
pub use writer::TableWriter;

/// Main Iceberg writer - manages traces, logs, and metrics tables
pub struct IcebergWriter {
    #[allow(dead_code)] // Will be used for query methods
    catalog: Arc<IcebergCatalog>,
    spans_writer: TableWriter,
    logs_writer: TableWriter,
    metrics_writer: TableWriter,
}

impl IcebergWriter {
    pub async fn new(config: &Config) -> Result<Self> {
        info!("Initializing Iceberg writers for spans, logs, and metrics");

        // Initialize shared catalog
        let catalog = Arc::new(IcebergCatalog::new(config).await?);

        // Create table identifiers
        let spans_table_ident = TableIdent::from_strs(&["default", TraceTable::table_name()])?;
        let logs_table_ident = TableIdent::from_strs(&["default", OtlpLogsTable::table_name()])?;
        let metrics_table_ident = TableIdent::from_strs(&["default", OtlpMetricsTable::table_name()])?;

        // Ensure tables exist
        ensure_table_exists(catalog.catalog(), &spans_table_ident, TableType::Spans).await?;
        ensure_table_exists(catalog.catalog(), &logs_table_ident, TableType::Logs).await?;
        ensure_table_exists(catalog.catalog(), &metrics_table_ident, TableType::Metrics).await?;

        // Create writers
        let spans_writer = TableWriter::new(catalog.catalog().clone(), spans_table_ident);
        let logs_writer = TableWriter::new(catalog.catalog().clone(), logs_table_ident);
        let metrics_writer = TableWriter::new(catalog.catalog().clone(), metrics_table_ident);

        info!("Iceberg writers initialized successfully");

        Ok(Self {
            catalog,
            spans_writer,
            logs_writer,
            metrics_writer,
        })
    }

    /// Write span batches to traces table
    /// Each batch becomes a separate row group for session isolation
    pub async fn write_span_batches(&self, batches: Vec<Vec<Span>>) -> Result<()> {
        self.spans_writer.write_batches(batches, Span::to_record_batch).await
    }

    /// Write log batches to logs table
    /// Each batch becomes a separate row group for session isolation
    pub async fn write_log_batches(&self, batches: Vec<Vec<Log>>) -> Result<()> {
        self.logs_writer.write_batches(batches, arrow::logs_to_record_batch).await
    }

    /// Write metric batches to metrics table
    /// Each batch becomes a separate row group for metric_name isolation
    pub async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> Result<()> {
        self.metrics_writer.write_batches(batches, arrow::metrics_to_record_batch).await
    }
}

/// Table type for table creation
enum TableType {
    Spans,
    Logs,
    Metrics,
}

/// Ensure a table exists, create it if it doesn't
async fn ensure_table_exists(
    catalog: &Arc<iceberg_catalog_rest::RestCatalog>,
    table_ident: &TableIdent,
    table_type: TableType,
) -> Result<()> {
    // Check if table already exists
    match catalog.table_exists(table_ident).await {
        Ok(true) => {
            info!("Table {} already exists, using existing table", table_ident);
            return Ok(());
        }
        Ok(false) => {
            info!("Table {} does not exist, creating it", table_ident);
        }
        Err(_) => {
            info!("Table {} existence check failed, assuming it doesn't exist", table_ident);
        }
    }

    // Create table based on type
    let (schema, partition_spec, sort_order, properties) = match table_type {
        TableType::Spans => {
            let schema = TraceTable::schema();
            let partition_spec = TraceTable::partition_spec(&schema)?;
            let sort_order = TraceTable::sort_order(&schema)?;
            let properties = TraceTable::table_properties();
            (schema, partition_spec, sort_order, properties)
        }
        TableType::Logs => {
            let schema = OtlpLogsTable::schema();
            let partition_spec = OtlpLogsTable::partition_spec(&schema)?;
            let sort_order = OtlpLogsTable::sort_order(&schema)?;
            let properties = OtlpLogsTable::table_properties();
            (schema, partition_spec, sort_order, properties)
        }
        TableType::Metrics => {
            let schema = OtlpMetricsTable::schema();
            let partition_spec = OtlpMetricsTable::partition_spec(&schema)?;
            let sort_order = OtlpMetricsTable::sort_order(&schema)?;
            let properties = OtlpMetricsTable::table_properties();
            (schema, partition_spec, sort_order, properties)
        }
    };

    let table_creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .schema(schema)
        .partition_spec(partition_spec)
        .sort_order(sort_order)
        .properties(properties)
        .build();

    // Create the table
    match catalog.create_table(&table_ident.namespace(), table_creation).await {
        Ok(_) => {
            info!("Successfully created table: {}", table_ident);
            Ok(())
        }
        Err(e) if e.to_string().contains("already exists") => {
            info!("Table {} already exists (concurrent creation), continuing", table_ident);
            Ok(())
        }
        Err(e) => {
            Err(anyhow::anyhow!("Failed to create table {}: {}", table_ident, e))
        }
    }
}
