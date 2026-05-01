use anyhow::Result;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use iceberg::{
    spec::DataFileFormat,
    spec::PartitionKey,
    spec::{Literal, Schema as IcebergSchema, Struct},
    transaction::{ApplyTransactionAction, Transaction},
    writer::base_writer::data_file_writer::DataFileWriterBuilder,
    writer::file_writer::{
        location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        rolling_writer::RollingFileWriterBuilder,
        ParquetWriterBuilder,
    },
    writer::partitioning::clustered_writer::ClusteredWriter,
    writer::partitioning::PartitioningWriter,
    Catalog, TableIdent,
};
use iceberg_catalog_rest::RestCatalog;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Generic writer for Iceberg tables
/// Handles write retries, row group isolation, and transaction commits
pub struct TableWriter {
    catalog: Arc<RestCatalog>,
    table_ident: TableIdent,
}

fn is_retryable_commit_error(error_msg: &str) -> bool {
    error_msg.contains("CatalogCommitConflicts")
        || error_msg.contains("commit conflict")
        || error_msg.contains("commit state is unknown")
        || error_msg.contains("commit state unknown")
}

impl TableWriter {
    pub fn new(catalog: Arc<RestCatalog>, table_ident: TableIdent) -> Self {
        Self {
            catalog,
            table_ident,
        }
    }

    pub async fn current_schema(&self) -> Result<Arc<IcebergSchema>> {
        let table = self.catalog.load_table(&self.table_ident).await?;
        Ok(table.metadata().current_schema().clone())
    }

    /// Write multiple batches to a single Parquet file with row groups
    /// Each batch becomes a separate row group in the file for optimal query performance
    ///
    /// Generic over T to support both Span and Log batches
    pub async fn write_batches<T, F>(&self, batches: Vec<Vec<T>>, to_record_batch: F) -> Result<()>
    where
        T: Clone,
        F: Fn(&[T], &IcebergSchema) -> Result<RecordBatch>,
    {
        if batches.is_empty() {
            return Ok(());
        }

        let total_records: usize = batches.iter().map(|b| b.len()).sum();
        let batch_count = batches.len();
        info!(
            "Writing {} batches ({} total records) to single Parquet file with {} row groups",
            batch_count, total_records, batch_count
        );

        // Retry logic for handling catalog commit conflicts
        const MAX_RETRIES: u32 = 5;
        const INITIAL_BACKOFF_MS: u64 = 100;
        let mut retry_count = 0;

        loop {
            match self.write_batches_inner(&batches, &to_record_batch).await {
                Ok(()) => {
                    if retry_count > 0 {
                        info!(
                            "Successfully wrote {} batches to Iceberg after {} retries",
                            batch_count, retry_count
                        );
                    } else {
                        info!("Successfully wrote {} batches to Iceberg", batch_count);
                    }
                    return Ok(());
                }
                Err(e) => {
                    let error_msg = e.to_string();

                    if is_retryable_commit_error(&error_msg) {
                        if retry_count < MAX_RETRIES {
                            retry_count += 1;
                            let backoff_ms = INITIAL_BACKOFF_MS * (2_u64.pow(retry_count - 1));
                            info!(
                                "Catalog commit conflict, retrying ({}/{}) after {}ms",
                                retry_count, MAX_RETRIES, backoff_ms
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms))
                                .await;
                            continue;
                        } else {
                            error!(
                                "Failed after {} retries due to commit conflicts",
                                MAX_RETRIES
                            );
                            return Err(e);
                        }
                    } else {
                        error!("Failed to write batches: {}", error_msg);
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Write multiple RecordBatches to a single Parquet file with row groups
    /// Each RecordBatch becomes a separate row group in the file.
    pub async fn write_record_batches(&self, record_batches: Vec<RecordBatch>) -> Result<()> {
        if record_batches.is_empty() {
            return Ok(());
        }

        let total_records: usize = record_batches.iter().map(|b| b.num_rows()).sum();
        let batch_count = record_batches.len();
        info!(
            "Writing {} record batches ({} total records) to single Parquet file with {} row groups",
            batch_count, total_records, batch_count
        );

        const MAX_RETRIES: u32 = 5;
        const INITIAL_BACKOFF_MS: u64 = 100;
        let mut retry_count = 0;

        loop {
            match self.write_record_batches_inner(&record_batches).await {
                Ok(()) => {
                    if retry_count > 0 {
                        info!(
                            "Successfully wrote {} record batches to Iceberg after {} retries",
                            batch_count, retry_count
                        );
                    } else {
                        info!(
                            "Successfully wrote {} record batches to Iceberg",
                            batch_count
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if is_retryable_commit_error(&error_msg) {
                        if retry_count < MAX_RETRIES {
                            retry_count += 1;
                            let backoff_ms = INITIAL_BACKOFF_MS * (2_u64.pow(retry_count - 1));
                            info!(
                                "Catalog commit conflict, retrying ({}/{}) after {}ms",
                                retry_count, MAX_RETRIES, backoff_ms
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms))
                                .await;
                            continue;
                        } else {
                            error!(
                                "Failed after {} retries due to commit conflicts",
                                MAX_RETRIES
                            );
                            return Err(e);
                        }
                    } else {
                        error!("Failed to write record batches: {}", error_msg);
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Inner write method for multiple batches
    async fn write_batches_inner<T, F>(&self, batches: &[Vec<T>], to_record_batch: &F) -> Result<()>
    where
        T: Clone,
        F: Fn(&[T], &IcebergSchema) -> Result<RecordBatch>,
    {
        // Load the table from the catalog (reload on each retry to get fresh metadata)
        let table = self.catalog.load_table(&self.table_ident).await?;

        // Generate unique batch ID for file naming
        let batch_id = uuid::Uuid::new_v4().to_string();

        // Create location and file name generators
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("batch_{}", batch_id),
            None,
            DataFileFormat::Parquet,
        );

        // Create parquet writer properties with zstd compression
        use parquet::basic::Compression;
        let writer_props = parquet::file::properties::WriterProperties::builder()
            .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?))
            .build();

        let parquet_writer_builder =
            ParquetWriterBuilder::new(writer_props, table.metadata().current_schema().clone());

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
        let mut clustered_writer = ClusteredWriter::new(data_file_writer_builder);

        let partition_spec = table.metadata().default_partition_spec().as_ref().clone();
        let schema = table.metadata().current_schema().clone();

        for (batch_idx, batch_records) in batches.iter().enumerate() {
            let record_batch = to_record_batch(batch_records, table.metadata().current_schema())?;
            let partition_value = extract_partition_value(&record_batch)?;
            let pk = PartitionKey::new(partition_spec.clone(), schema.clone(), partition_value);
            debug!(
                "Writing row group {}/{} with {} records",
                batch_idx + 1,
                batches.len(),
                batch_records.len()
            );
            clustered_writer.write(pk, record_batch).await?;
        }

        let data_files = clustered_writer.close().await?;
        info!("Closed writer, created {} data file(s)", data_files.len());

        // Add the data files to the table through a transaction
        let transaction = Transaction::new(&table);
        let action = transaction.fast_append().add_data_files(data_files);
        let transaction = action.apply(transaction)?;
        transaction.commit(self.catalog.as_ref()).await?;

        Ok(())
    }

    async fn write_record_batches_inner(&self, record_batches: &[RecordBatch]) -> Result<()> {
        let table = self.catalog.load_table(&self.table_ident).await?;

        let batch_id = uuid::Uuid::new_v4().to_string();
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("batch_{}", batch_id),
            None,
            DataFileFormat::Parquet,
        );

        use parquet::basic::Compression;
        let writer_props = parquet::file::properties::WriterProperties::builder()
            .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?))
            .build();

        let parquet_writer_builder =
            ParquetWriterBuilder::new(writer_props, table.metadata().current_schema().clone());

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
        let mut clustered_writer = ClusteredWriter::new(data_file_writer_builder);

        let partition_spec = table.metadata().default_partition_spec().as_ref().clone();
        let schema = table.metadata().current_schema().clone();

        for (batch_idx, record_batch) in record_batches.iter().enumerate() {
            let partition_value = extract_partition_value(record_batch)?;
            let pk = PartitionKey::new(partition_spec.clone(), schema.clone(), partition_value);
            debug!(
                "Writing row group {}/{} with {} records",
                batch_idx + 1,
                record_batches.len(),
                record_batch.num_rows()
            );
            clustered_writer.write(pk, record_batch.clone()).await?;
        }

        let data_files = clustered_writer.close().await?;
        info!("Closed writer, created {} data file(s)", data_files.len());

        let transaction = Transaction::new(&table);
        let action = transaction.fast_append().add_data_files(data_files);
        let transaction = action.apply(transaction)?;
        transaction.commit(self.catalog.as_ref()).await?;

        Ok(())
    }
}

/// Extract partition value from RecordBatch
/// Finds record_date column by name (not by position, since promoted columns may come after it)
fn extract_partition_value(batch: &RecordBatch) -> Result<Struct> {
    use arrow::array::AsArray;
    use arrow::datatypes::Date32Type;

    // Find record_date column by name (not by position, since promoted columns may come after it)
    let record_date_idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "record_date")
        .ok_or_else(|| anyhow::anyhow!("record_date column not found in schema"))?;
    let record_date_col = batch.column(record_date_idx);

    // Extract first value from Date32Array
    let date_array = record_date_col.as_primitive::<Date32Type>();
    let days_since_epoch = date_array.value(0);

    // Verify all values are the same (defensive check)
    if date_array.len() > 1 {
        for i in 1..date_array.len() {
            if date_array.value(i) != days_since_epoch {
                return Err(anyhow::anyhow!(
                    "All records in a batch must have the same record_date for partition compatibility"
                ));
            }
        }
    }

    // Extract date for logging
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let record_date = epoch + chrono::Duration::days(days_since_epoch as i64);

    debug!(
        "Computed partition value: record_date={} (days since epoch: {})",
        record_date, days_since_epoch
    );

    // Create partition value: Struct with Literal::date()
    Ok(Struct::from_iter([Some(Literal::date(days_since_epoch))]))
}
