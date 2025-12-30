use crate::config::Config;
use crate::storage::span_buffer::SpanData;
use anyhow::Result;
use arrow::array::{
    Array, ArrayRef, StringArray, TimestampMicrosecondArray, Date32Array,
    MapArray, ListArray, StructArray, AsArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::{error, info};
use chrono::NaiveDate;
use std::collections::HashMap;

use iceberg::{
    TableIdent,
    spec::{Schema as IcebergSchema, NestedField, Type, PrimitiveType, PartitionSpec, UnboundPartitionField, SortOrder, MapType, ListType, StructType, Literal, Struct},
    Catalog, CatalogBuilder, TableCreation,
    writer::{IcebergWriter as IcebergWriterTrait, IcebergWriterBuilder},
    writer::base_writer::data_file_writer::DataFileWriterBuilder,
    writer::file_writer::{ParquetWriterBuilder, location_generator::{DefaultLocationGenerator, DefaultFileNameGenerator}},
    spec::DataFileFormat,
    transaction::{Transaction, ApplyTransactionAction},
};

use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};

pub struct IcebergWriter {
    catalog: Arc<RestCatalog>,
    table_ident: TableIdent,
}

impl IcebergWriter {
    pub async fn new(config: &Config) -> Result<Self> {
        info!("Initializing Iceberg writer with REST catalog: {}", config.iceberg.catalog_uri);

        // Configure REST catalog with proper settings
        let mut catalog_props = HashMap::new();
        catalog_props.insert(REST_CATALOG_PROP_URI.to_string(), config.iceberg.catalog_uri.clone());

        // Use warehouse from config; can be overridden via ICEBERG_WAREHOUSE env
        let warehouse_uri = std::env::var("ICEBERG_WAREHOUSE")
            .unwrap_or_else(|_| config.iceberg.warehouse.clone());
        catalog_props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri.clone());
        info!("Using Iceberg warehouse: {}", warehouse_uri);

        // Add bearer token for catalog authentication if provided (e.g., Cloudflare R2)
        if let Some(ref token) = config.iceberg.catalog_token {
            info!("Adding bearer token for catalog authentication (token: {}...)",
                &token.chars().take(10).collect::<String>());
            // Standard "token" property (used by PyIceberg and iceberg-rust)
            catalog_props.insert("token".to_string(), token.clone());
            info!("Token property added to catalog_props");
        }

        // Log all properties being sent to catalog (excluding sensitive values)
        info!("Catalog properties: uri={}, warehouse={}, token_present={}",
            config.iceberg.catalog_uri,
            warehouse_uri,
            config.iceberg.catalog_token.is_some());

        // For testing/development environments with TLS interception (e.g., sandboxed containers),
        // we need to disable certificate validation. This is controlled by environment variable.
        // WARNING: Never use this in production!
        let http_client = if std::env::var("ICEBERG_DISABLE_TLS_VALIDATION").is_ok() {
            info!("⚠️  TLS certificate validation DISABLED (ICEBERG_DISABLE_TLS_VALIDATION set)");
            info!("⚠️  This should ONLY be used in sandboxed test environments with TLS interception");
            reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()?
        } else {
            reqwest::Client::builder().build()?
        };

        // Add S3 configuration if available
        if let Some(ref endpoint) = config.s3.endpoint {
            catalog_props.insert("s3.endpoint".to_string(), endpoint.clone());
            info!("Using S3 endpoint: {}", endpoint);
        }
        if let Some(ref access_key) = config.s3.access_key_id {
            catalog_props.insert("s3.access-key-id".to_string(), access_key.clone());
        }
        if let Some(ref secret_key) = config.s3.secret_access_key {
            catalog_props.insert("s3.secret-access-key".to_string(), secret_key.clone());
        }
        catalog_props.insert("s3.region".to_string(), config.storage.s3_region.clone());

        let catalog = RestCatalogBuilder::default()
            .with_client(http_client)
            .load("rest-catalog", catalog_props)
            .await?;
        
        // Parse table identifier from config: use namespace "default" and configured table_name
        let table_ident = TableIdent::from_strs(&["default", config.iceberg.table_name.as_str()])?;
        
        info!("Iceberg writer initialized for table: {}", table_ident);
        
        let writer = Self {
            catalog: Arc::new(catalog),
            table_ident,
        };
        
        // Ensure the table exists, create it if it doesn't
        writer.ensure_table_exists().await?;
        
        Ok(writer)
    }
    
    /// Creates the raw_sessions table if it doesn't exist
    /// 
    /// **Canonical Schema Source**: `schemas/iceberg/raw_sessions.sql`
    /// 
    /// This function implements the table creation via Iceberg REST catalog API.
    /// The schema definition in `schemas/iceberg/raw_sessions.sql` is the authoritative
    /// source of truth. Any changes to the table schema should be made in both:
    /// 1. `schemas/iceberg/raw_sessions.sql` (canonical SQL definition)
    /// 2. This function and `create_raw_sessions_schema()` (Rust implementation)
    /// 
    /// **Why Rust code instead of SQL scripts?**
    /// - Iceberg REST catalog doesn't execute SQL directly
    /// - Using SQL would require Spark/Trino infrastructure, adding complexity
    /// - This approach works directly with the REST catalog API
    /// - The SQL file serves as documentation and reference for other tools
    async fn ensure_table_exists(&self) -> Result<()> {
        // Check if table already exists
        match self.catalog.table_exists(&self.table_ident).await {
            Ok(true) => {
                info!("Table {} already exists, using existing table (preserving all data)", self.table_ident);
                // Table exists - just use it. Iceberg supports schema evolution,
                // and we should never drop existing tables as it deletes all data.
                return Ok(());
            }
            Ok(false) => {
                info!("Table {} does not exist, creating it", self.table_ident);
            }
            Err(_) => {
                // If table_exists() fails, assume table doesn't exist and try to create it
                info!("Table {} existence check failed, assuming it doesn't exist and creating", self.table_ident);
            }
        }

        info!("Creating table {} with raw_sessions schema", self.table_ident);
        info!("Schema reference: schemas/iceberg/raw_sessions.sql (canonical source)");
        
        // Create table schema matching schemas/iceberg/raw_sessions.sql
        let schema = self.create_raw_sessions_schema();
        
        // Create partition spec: partition by record_date (day partitioning for partition pruning)
        // record_date is field ID 16, use Identity transform since it's already a DATE type
        let partition_field = UnboundPartitionField {
            source_id: 16, // record_date field ID
            field_id: Some(1000), // partition field ID (must be unique, starting from 1000)
            transform: iceberg::spec::Transform::Identity,
            name: "record_date".to_string(),
        };
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_unbound_field(partition_field)?
            .build()?;

        // Create sort order for optimal data layout
        let sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_fields(vec![
                iceberg::spec::SortField::builder()
                    .source_id(1) // session_id
                    .direction(iceberg::spec::SortDirection::Ascending)
                    .null_order(iceberg::spec::NullOrder::First)
                    .transform(iceberg::spec::Transform::Identity)
                    .build(),
                iceberg::spec::SortField::builder()
                    .source_id(2) // trace_id
                    .direction(iceberg::spec::SortDirection::Ascending)
                    .null_order(iceberg::spec::NullOrder::First)
                    .transform(iceberg::spec::Transform::Identity)
                    .build(),
                iceberg::spec::SortField::builder()
                    .source_id(10) // timestamp
                    .direction(iceberg::spec::SortDirection::Ascending)
                    .null_order(iceberg::spec::NullOrder::First)
                    .transform(iceberg::spec::Transform::Identity)
                    .build(),
            ])
            .build(&schema)?;
        
        // Create table properties
        let mut properties = HashMap::new();
        properties.insert("write.format.default".to_string(), "parquet".to_string());
        properties.insert("format-version".to_string(), "2".to_string());
        properties.insert("write.target-file-size-bytes".to_string(), "134217728".to_string());
        properties.insert("write.parquet.compression-codec".to_string(), "zstd".to_string());
        properties.insert("write.parquet.compression-level".to_string(), "3".to_string());
        for id_col in ["session_id", "trace_id", "span_id", "parent_span_id"] {
            properties.insert(
                format!("write.parquet.bloom-filter-enabled.{}", id_col),
                "true".to_string(),
            );
            properties.insert(
                format!("write.parquet.bloom-filter-fpp.{}", id_col),
                "0.01".to_string(),
            );
        }
        
        let table_creation = TableCreation::builder()
            .name(self.table_ident.name().to_string())
            .schema(schema)
            .partition_spec(partition_spec)
            .sort_order(sort_order)
            .properties(properties)
            .build();
        
        // Create the table
        match self.catalog.create_table(&self.table_ident.namespace(), table_creation).await {
            Ok(_) => {
                info!("Successfully created table: {}", self.table_ident);
            }
            Err(e) if e.to_string().contains("already exists") => {
                info!("Table {} already exists (concurrent creation)", self.table_ident);
                // This is fine - another process may have created it
            }
            Err(e) => return Err(e.into()),
        }
        
        Ok(())
    }
    
    /// Create the schema for raw_sessions table matching raw_sessions.sql
    /// 
    /// **Canonical Source**: `schemas/iceberg/raw_sessions.sql`
    /// 
    /// This function builds the Iceberg schema programmatically to match the SQL definition.
    /// Field IDs are explicitly set to ensure compatibility across schema evolutions.
    /// 
    /// Schema structure:
    /// - Primary Identifiers: session_id, trace_id, span_id, parent_span_id
    /// - Application Context: app_id, organization_id, tenant_id
    /// - Span Metadata: message_type, span_kind, timestamp, end_timestamp
    /// - Complex Types: attributes (MAP<STRING, STRING>), events (ARRAY<STRUCT>)
    /// - Status: status_code, status_message
    /// - Partition Key: record_date (DATE, field ID 16)
    fn create_raw_sessions_schema(&self) -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                // Primary Identifiers
                NestedField::required(1, "session_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "trace_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "span_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(4, "parent_span_id", Type::Primitive(PrimitiveType::String)).into(),
                
                // Application Context
                NestedField::required(5, "app_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(6, "organization_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(7, "tenant_id", Type::Primitive(PrimitiveType::String)).into(),
                
                // Span Metadata
                NestedField::required(8, "message_type", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(9, "span_kind", Type::Primitive(PrimitiveType::String)).into(),
                // Use TIMESTAMPTZ for partition day transform compatibility
                NestedField::required(10, "timestamp", Type::Primitive(PrimitiveType::Timestamptz)).into(),
                NestedField::optional(11, "end_timestamp", Type::Primitive(PrimitiveType::Timestamptz)).into(),
                
                // Attributes MAP<STRING, STRING>
                NestedField::optional(
                    12,
                    "attributes",
                    Type::Map(
                        MapType::new(
                            // key field id 17
                            NestedField::required(17, "key", Type::Primitive(PrimitiveType::String)).into(),
                            // value field id 18
                            NestedField::optional(18, "value", Type::Primitive(PrimitiveType::String)).into(),
                        )
                    ),
                ).into(),
                
                // Events ARRAY<STRUCT<name STRING, timestamp TIMESTAMP, attributes MAP<STRING, STRING>>>
                NestedField::optional(
                    13,
                    "events",
                    Type::List(
                        ListType::new(
                            NestedField::optional(
                                19,
                                "element",
                                Type::Struct(
                                    StructType::new(vec![
                                        // name
                                        NestedField::required(20, "name", Type::Primitive(PrimitiveType::String)).into(),
                                        // timestamp (with tz)
                                        NestedField::required(21, "timestamp", Type::Primitive(PrimitiveType::Timestamptz)).into(),
                                        // attributes within event
                                        NestedField::optional(
                                            22,
                                            "attributes",
                                            Type::Map(
                                                MapType::new(
                                                    NestedField::required(23, "key", Type::Primitive(PrimitiveType::String)).into(),
                                                    NestedField::optional(24, "value", Type::Primitive(PrimitiveType::String)).into(),
                                                )
                                            ),
                                        ).into(),
                                    ])
                                ),
                            ).into()
                        )
                    ),
                ).into(),
                
                // Status
                NestedField::optional(14, "status_code", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(15, "status_message", Type::Primitive(PrimitiveType::String)).into(),
                
                // Derived Partition Key (physical column used for partitioning)
                NestedField::required(16, "record_date", Type::Primitive(PrimitiveType::Date)).into(),
            ])
            .build()
            .unwrap()
    }

    /// Write multiple session batches to a single Parquet file with row groups
    /// Each session becomes a separate row group in the file for optimal query performance
    pub async fn write_session_batches(&self, session_batches: Vec<Vec<SpanData>>) -> Result<()> {
        if session_batches.is_empty() {
            return Ok(());
        }

        let total_spans: usize = session_batches.iter().map(|b| b.len()).sum();
        let session_count = session_batches.len();
        info!("Writing {} sessions ({} total spans) to single Parquet file with {} row groups",
              session_count, total_spans, session_count);

        // Retry logic for handling catalog commit conflicts
        const MAX_RETRIES: u32 = 5;
        const INITIAL_BACKOFF_MS: u64 = 100;
        let mut retry_count = 0;

        loop {
            match self.write_session_batches_inner(&session_batches).await {
                Ok(()) => {
                    if retry_count > 0 {
                        info!("Successfully wrote {} sessions to Iceberg after {} retries", session_count, retry_count);
                    } else {
                        info!("Successfully wrote {} sessions to Iceberg", session_count);
                    }
                    return Ok(());
                }
                Err(e) => {
                    let error_msg = e.to_string();

                    if error_msg.contains("CatalogCommitConflicts") || error_msg.contains("commit conflict") {
                        if retry_count < MAX_RETRIES {
                            retry_count += 1;
                            let backoff_ms = INITIAL_BACKOFF_MS * (2_u64.pow(retry_count - 1));
                            info!("Catalog commit conflict, retrying ({}/{}) after {}ms",
                                  retry_count, MAX_RETRIES, backoff_ms);
                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                            continue;
                        } else {
                            error!("Failed after {} retries due to commit conflicts", MAX_RETRIES);
                            return Err(e);
                        }
                    } else {
                        error!("Failed to write session batches: {}", error_msg);
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Inner write method for multiple session batches
    async fn write_session_batches_inner(&self, session_batches: &[Vec<SpanData>]) -> Result<()> {
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

        // Create a parquet file writer builder
        let parquet_writer_builder = ParquetWriterBuilder::new(
            writer_props,
            table.metadata().current_schema().clone(),
            None,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        // Create a data file writer using parquet file writer builder
        let partition_spec_id = table.metadata().default_partition_spec_id();
        
        // Compute partition value directly from first span's timestamp
        // Note: The span_buffer ensures all spans in a flush have the same date by grouping by date first.
        // This validation is a defensive check to catch any bugs.
        let first_spans = session_batches.first()
            .ok_or_else(|| anyhow::anyhow!("No session batches to write"))?;
        let first_span = first_spans.first()
            .ok_or_else(|| anyhow::anyhow!("First session batch is empty"))?;
        
        // Compute record_date from timestamp (same logic as in build_record_batch_arrays)
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let expected_date = first_span.timestamp.date_naive();
        let days_since_epoch = (expected_date - epoch).num_days() as i32;
        
        // Defensive validation: ensure ALL spans across ALL batches have the same date
        // (This should never fail if span_buffer is working correctly, but serves as a safety check)
        for (batch_idx, session_spans) in session_batches.iter().enumerate() {
            for (span_idx, span) in session_spans.iter().enumerate() {
                let span_date = span.timestamp.date_naive();
                if span_date != expected_date {
                    return Err(anyhow::anyhow!(
                        "All spans in a file must have the same record_date for partition compatibility. \
                        Expected: {}, but found {} in batch {} span {}. \
                        This indicates a bug in span_buffer grouping logic.",
                        expected_date, span_date, batch_idx, span_idx
                    ));
                }
            }
        }
        
        // Create partition value: Struct with Literal::date()
        let partition_value = Struct::from_iter([Some(Literal::date(days_since_epoch))]);
        info!("Computed partition value: record_date={} (days since epoch: {}). Writing {} batches with {} total spans.",
              expected_date, days_since_epoch, session_batches.len(), 
              session_batches.iter().map(|b| b.len()).sum::<usize>());
        
        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, Some(partition_value), partition_spec_id);

        // Build the data file writer
        let mut data_file_writer = data_file_writer_builder.build().await?;

        // Write each session as a separate row group
        for (session_idx, session_spans) in session_batches.iter().enumerate() {
            let record_batch = self.convert_spans_to_record_batch(session_spans, table.metadata().current_schema())?;
            info!("Writing row group {}/{} with {} spans",
                  session_idx + 1, session_batches.len(), session_spans.len());
            data_file_writer.write(record_batch).await?;
        }

        // Close the writer and get the data files (single file with multiple row groups)
        let data_files = data_file_writer.close().await?;
        info!("Closed writer, created {} data file(s)", data_files.len());

        // Add the data files to the table through a transaction
        let transaction = Transaction::new(&table);
        let action = transaction.fast_append().add_data_files(data_files);
        let transaction = action.apply(transaction)?;
        transaction.commit(self.catalog.as_ref()).await?;

        Ok(())
    }

    /// Debug function: Query ALL spans without any filter to test table visibility
    pub async fn query_all_spans_debug(&self, limit: usize) -> Result<Vec<crate::api::query::SpanData>> {
        use futures::StreamExt;

        info!("DEBUG: Querying ALL spans (no predicate) with limit {}", limit);

        // Load the table from the catalog
        let table = self.catalog.load_table(&self.table_ident).await?;

        // Debug: Log table metadata
        let metadata = table.metadata();
        info!("DEBUG: Table current snapshot ID: {:?}", metadata.current_snapshot().map(|s| s.snapshot_id()));
        if let Some(snapshot) = metadata.current_snapshot() {
            info!("DEBUG: Snapshot manifest-list: {}", snapshot.manifest_list());
            info!("DEBUG: Snapshot summary: {:?}", snapshot.summary());
        }

        // Create a table scan WITHOUT any filter
        let scan = table.scan().build()?;

        info!("DEBUG: Scan created without predicate (full table scan)");

        // Execute the scan and convert to Arrow stream
        let mut arrow_stream = scan.to_arrow().await?;

        let mut result_spans = Vec::new();
        let mut batch_count = 0;
        let mut total_rows = 0;

        // Process each batch
        while let Some(batch_result) = arrow_stream.next().await {
            let batch = batch_result?;
            batch_count += 1;
            total_rows += batch.num_rows();

            info!("DEBUG: Processing batch {} with {} rows", batch_count, batch.num_rows());

            // Debug: Print first few session_ids in this batch
            if batch.num_rows() > 0 {
                if let Some(session_id_col_idx) = batch.schema().fields().iter().position(|f| f.name() == "session_id") {
                    let session_id_array = batch.column(session_id_col_idx);
                    let sample_size = std::cmp::min(5, session_id_array.len());
                    let sample_ids: Result<Vec<String>> = (0..sample_size)
                        .map(|i| Self::get_string_value(session_id_array, i))
                        .collect();
                    if let Ok(ids) = sample_ids {
                        info!("DEBUG: Sample session_ids in batch: {:?}", ids);
                    }
                }
            }

            // Extract spans up to limit
            for row_idx in 0..batch.num_rows() {
                if result_spans.len() >= limit {
                    break;
                }
                let span_data = self.extract_span_from_batch(&batch, row_idx)?;
                result_spans.push(span_data);
            }

            if result_spans.len() >= limit {
                break;
            }
        }

        info!("DEBUG: Scanned {} batches, {} total rows, returning {} spans", batch_count, total_rows, result_spans.len());
        Ok(result_spans)
    }

    /// Query spans by session_id from the Iceberg table
    /// Uses predicate pushdown to efficiently filter at manifest and row group level
    pub async fn query_by_session_id(&self, session_id: &str) -> Result<Vec<crate::api::query::SpanData>> {
        use futures::StreamExt;
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        info!("Querying spans for session_id: {} (with predicate pushdown)", session_id);

        // Load the table from the catalog
        let table = self.catalog.load_table(&self.table_ident).await?;

        // Debug: Log table metadata
        let metadata = table.metadata();
        info!("Table current snapshot ID: {:?}", metadata.current_snapshot().map(|s| s.snapshot_id()));
        if let Some(snapshot) = metadata.current_snapshot() {
            info!("Table snapshot summary: {:?}", snapshot.summary());
        }
        info!("Table format version: {}", metadata.format_version());

        // Create a predicate for session_id equality
        // This enables:
        // 1. Manifest filtering - skip files that don't contain this session_id
        // 2. Row group filtering - skip row groups using Parquet min/max statistics
        // 3. Row-level filtering - only materialize matching rows
        let predicate = Reference::new("session_id").equal_to(Datum::string(session_id));

        // Create a table scan with session_id filter
        // The predicate will be pushed down to:
        // - Iceberg manifests (file-level filtering)
        // - Parquet row groups (using statistics/min/max)
        // - Arrow compute (row-level filtering)
        let scan = table
            .scan()
            .with_filter(predicate.clone())
            .build()?;

        info!("Scan created with predicate: session_id = '{}'", session_id);
        info!("Predicate: {:?}", predicate);

        // Execute the scan and convert to Arrow stream
        // Only matching row groups from matching files will be read
        let mut arrow_stream = scan.to_arrow().await?;

        let mut result_spans = Vec::new();
        let mut batch_count = 0;

        // Process each batch (already filtered by predicate)
        while let Some(batch_result) = arrow_stream.next().await {
            let batch = batch_result?;
            batch_count += 1;

            info!("Processing batch {} with {} rows (post-filter)", batch_count, batch.num_rows());

            // Debug: Print first few session_ids in this batch
            if batch.num_rows() > 0 {
                if let Some(session_id_col_idx) = batch.schema().fields().iter().position(|f| f.name() == "session_id") {
                    let session_id_array = batch.column(session_id_col_idx);
                    let sample_size = std::cmp::min(3, session_id_array.len());
                    let sample_ids: Result<Vec<String>> = (0..sample_size)
                        .map(|i| Self::get_string_value(session_id_array, i))
                        .collect();
                    if let Ok(ids) = sample_ids {
                        info!("Sample session_ids in batch: {:?}", ids);
                    }
                }
            }

            // All rows in the batch should match the session_id due to predicate pushdown
            // But we still need to extract the span data
            for row_idx in 0..batch.num_rows() {
                let span_data = self.extract_span_from_batch(&batch, row_idx)?;
                result_spans.push(span_data);
            }
        }

        info!("Scanned {} batches total", batch_count);
        info!("Found {} spans for session_id: {} (using predicate pushdown)", result_spans.len(), session_id);
        Ok(result_spans)
    }

    /// Extract a single span from a RecordBatch at given row index
    fn extract_span_from_batch(&self, batch: &RecordBatch, row_idx: usize) -> Result<crate::api::query::SpanData> {
        use arrow::datatypes::DataType;
        let schema = batch.schema();

        // Helper to get string value - handles both StringArray (i32) and LargeStringArray (i64)
        let get_string = |name: &str| -> Result<String> {
            let idx = schema.fields().iter().position(|f| f.name() == name)
                .ok_or_else(|| anyhow::anyhow!("{} column not found", name))?;
            let column = batch.column(idx);

            match column.data_type() {
                DataType::Utf8 => Ok(column.as_string::<i32>().value(row_idx).to_string()),
                DataType::LargeUtf8 => Ok(column.as_string::<i64>().value(row_idx).to_string()),
                other => Err(anyhow::anyhow!("{} column has unexpected type: {:?}", name, other))
            }
        };

        let get_optional_string = |name: &str| -> Result<Option<String>> {
            let idx = schema.fields().iter().position(|f| f.name() == name)
                .ok_or_else(|| anyhow::anyhow!("{} column not found", name))?;
            let column = batch.column(idx);

            if column.is_null(row_idx) {
                return Ok(None);
            }

            match column.data_type() {
                DataType::Utf8 => {
                    let array = column.as_string::<i32>();
                    Ok(Some(array.value(row_idx).to_string()))
                },
                DataType::LargeUtf8 => {
                    let array = column.as_string::<i64>();
                    Ok(Some(array.value(row_idx).to_string()))
                },
                other => Err(anyhow::anyhow!("{} column has unexpected type: {:?}", name, other))
            }
        };

        let get_optional_timestamp = |name: &str| -> Result<Option<String>> {
            let idx = schema.fields().iter().position(|f| f.name() == name)
                .ok_or_else(|| anyhow::anyhow!("{} column not found", name))?;
            let array = batch.column(idx).as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
            Ok(if array.is_null(row_idx) {
                None
            } else {
                let micros = array.value(row_idx);
                chrono::DateTime::from_timestamp_micros(micros)
                    .map(|dt| dt.to_rfc3339())
            })
        };

        // Extract basic fields
        let trace_id = get_string("trace_id")?;
        let span_id = get_string("span_id")?;
        let parent_span_id = get_optional_string("parent_span_id")?;
        let app_id = get_string("app_id")?;
        let message_type = get_string("message_type")?;

        // Extract timestamps
        let timestamp_idx = schema.fields().iter().position(|f| f.name() == "timestamp")
            .ok_or_else(|| anyhow::anyhow!("timestamp column not found"))?;
        let timestamp_array = batch.column(timestamp_idx).as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        let timestamp_micros = timestamp_array.value(row_idx);
        let timestamp = chrono::DateTime::from_timestamp_micros(timestamp_micros)
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp"))?
            .to_rfc3339();

        let end_timestamp = get_optional_timestamp("end_timestamp")?;
        let organization_id = get_optional_string("organization_id")?;
        let tenant_id = get_optional_string("tenant_id")?;
        let span_kind = get_optional_string("span_kind")?;
        let status_code = get_optional_string("status_code")?;
        let status_message = get_optional_string("status_message")?;

        // Extract attributes (MAP<STRING, STRING>)
        let attributes = self.extract_attributes_map(batch, row_idx)?;

        // Extract events (LIST<STRUCT>)
        let events = self.extract_events_list(batch, row_idx)?;

        Ok(crate::api::query::SpanData {
            trace_id,
            span_id,
            parent_span_id,
            app_id,
            organization_id,
            tenant_id,
            message_type,
            span_kind,
            timestamp,
            end_timestamp,
            attributes,
            events,
            status_code,
            status_message,
        })
    }

    /// Helper to extract string value from ArrayRef handling both Utf8 and LargeUtf8
    fn get_string_value(array: &arrow::array::ArrayRef, idx: usize) -> Result<String> {
        use arrow::datatypes::DataType;
        match array.data_type() {
            DataType::Utf8 => Ok(array.as_string::<i32>().value(idx).to_string()),
            DataType::LargeUtf8 => Ok(array.as_string::<i64>().value(idx).to_string()),
            other => Err(anyhow::anyhow!("Expected string array, got {:?}", other))
        }
    }

    /// Helper to check if string value is null
    fn is_string_null(array: &arrow::array::ArrayRef, idx: usize) -> bool {
        array.is_null(idx)
    }

    /// Extract attributes MAP<STRING, STRING> from a RecordBatch row
    fn extract_attributes_map(&self, batch: &RecordBatch, row_idx: usize) -> Result<serde_json::Value> {
        use arrow::array::MapArray;

        let schema = batch.schema();
        let idx = schema.fields().iter().position(|f| f.name() == "attributes")
            .ok_or_else(|| anyhow::anyhow!("attributes column not found"))?;

        let map_array = batch.column(idx).as_any().downcast_ref::<MapArray>()
            .ok_or_else(|| anyhow::anyhow!("attributes column is not a MapArray"))?;

        // Check if this row's map is null
        if map_array.is_null(row_idx) {
            return Ok(serde_json::json!({}));
        }

        // Get the slice of entries for this row
        let entries = map_array.value(row_idx);
        let entries_struct = entries.as_any().downcast_ref::<arrow::array::StructArray>()
            .ok_or_else(|| anyhow::anyhow!("Map entries are not a StructArray"))?;

        // Extract keys and values arrays
        let keys = entries_struct.column(0);
        let values = entries_struct.column(1);

        // Build JSON object
        let mut map = serde_json::Map::new();
        for i in 0..entries_struct.len() {
            let key = Self::get_string_value(keys, i)?;
            let value = if Self::is_string_null(values, i) {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(Self::get_string_value(values, i)?)
            };
            map.insert(key, value);
        }

        Ok(serde_json::Value::Object(map))
    }

    /// Extract events LIST<STRUCT<name, timestamp, attributes>> from a RecordBatch row
    fn extract_events_list(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<serde_json::Value>> {
        use arrow::array::{ListArray, StructArray, MapArray};

        let schema = batch.schema();
        let idx = schema.fields().iter().position(|f| f.name() == "events")
            .ok_or_else(|| anyhow::anyhow!("events column not found"))?;

        let list_array = batch.column(idx).as_any().downcast_ref::<ListArray>()
            .ok_or_else(|| anyhow::anyhow!("events column is not a ListArray"))?;

        // Check if this row's list is null
        if list_array.is_null(row_idx) {
            return Ok(vec![]);
        }

        // Get the slice of events for this row
        let events_array = list_array.value(row_idx);
        let events_struct = events_array.as_any().downcast_ref::<StructArray>()
            .ok_or_else(|| anyhow::anyhow!("Events are not a StructArray"))?;

        // Extract the three fields: name, timestamp, attributes
        let names = events_struct.column(0);
        let timestamps = events_struct.column(1).as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        let attributes_maps = events_struct.column(2).as_any().downcast_ref::<MapArray>()
            .ok_or_else(|| anyhow::anyhow!("Event attributes are not a MapArray"))?;

        let mut events = Vec::new();
        for i in 0..events_struct.len() {
            let name = Self::get_string_value(names, i)?;
            let timestamp_micros = timestamps.value(i);
            let timestamp = chrono::DateTime::from_timestamp_micros(timestamp_micros)
                .ok_or_else(|| anyhow::anyhow!("Invalid event timestamp"))?
                .to_rfc3339();

            // Extract event attributes map
            let event_attrs = if attributes_maps.is_null(i) {
                serde_json::json!({})
            } else {
                let entries = attributes_maps.value(i);
                let entries_struct = entries.as_any().downcast_ref::<StructArray>()
                    .ok_or_else(|| anyhow::anyhow!("Event attributes entries are not a StructArray"))?;

                let keys = entries_struct.column(0);
                let values = entries_struct.column(1);

                let mut map = serde_json::Map::new();
                for j in 0..entries_struct.len() {
                    let key = Self::get_string_value(keys, j)?;
                    let value = if Self::is_string_null(values, j) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::String(Self::get_string_value(values, j)?)
                    };
                    map.insert(key, value);
                }
                serde_json::Value::Object(map)
            };

            events.push(serde_json::json!({
                "name": name,
                "timestamp": timestamp,
                "attributes": event_attrs
            }));
        }

        Ok(events)
    }

    /// Convert span batch to Arrow RecordBatch using Iceberg table schema
    fn convert_spans_to_record_batch(&self, spans: &[SpanData], iceberg_schema: &IcebergSchema) -> Result<RecordBatch> {
        // Convert Iceberg schema to Arrow schema
        let arrow_schema = Arc::new(Schema::try_from(iceberg_schema)?);

        let num_spans = spans.len();
        self.build_record_batch_arrays(spans, num_spans, arrow_schema)
    }
    
    fn build_record_batch_arrays(&self, spans: &[SpanData], _num_spans: usize, schema: Arc<Schema>) -> Result<RecordBatch> {
        info!("Arrow schema field count: {}", schema.fields().len());
        for (i, f) in schema.fields().iter().enumerate() {
            info!("Field[{}]: {} : {:?}", i, f.name(), f.data_type());
        }

        // Extract field definitions from schema to preserve field IDs
        // Clone the Field and wrap in Arc to create a FieldRef
        let attributes_field = Arc::new(schema.field_with_name("attributes")
            .map_err(|e| anyhow::anyhow!("attributes field not found in schema: {}", e))?.clone());

        let events_field = Arc::new(schema.field_with_name("events")
            .map_err(|e| anyhow::anyhow!("events field not found in schema: {}", e))?.clone());

        // Build arrays for each column
        // Extract session_id from each span's attributes (or fallback to trace_id)
        let session_ids: ArrayRef = Arc::new(StringArray::from(
            spans.iter()
                .map(|s| s.attributes.get("sp.session.id")
                    .map(|id| id.as_str())
                    .unwrap_or(s.trace_id.as_str()))
                .collect::<Vec<_>>()
        ));

        let trace_ids: ArrayRef = Arc::new(StringArray::from(
            spans.iter().map(|s| s.trace_id.as_str()).collect::<Vec<_>>()
        ));

        let span_ids: ArrayRef = Arc::new(StringArray::from(
            spans.iter().map(|s| s.span_id.as_str()).collect::<Vec<_>>()
        ));

        let parent_span_ids: ArrayRef = Arc::new(StringArray::from(
            spans.iter()
                .map(|s| s.parent_span_id.as_deref())
                .collect::<Vec<_>>()
        ));

        let app_ids: ArrayRef = Arc::new(StringArray::from(
            spans.iter().map(|s| s.app_id.as_str()).collect::<Vec<_>>()
        ));

        let organization_ids: ArrayRef = Arc::new(StringArray::from(
            spans.iter()
                .map(|s| s.organization_id.as_deref())
                .collect::<Vec<_>>()
        ));

        let tenant_ids: ArrayRef = Arc::new(StringArray::from(
            spans.iter()
                .map(|s| s.tenant_id.as_deref())
                .collect::<Vec<_>>()
        ));

        let message_types: ArrayRef = Arc::new(StringArray::from(
            spans.iter().map(|s| s.message_type.as_str()).collect::<Vec<_>>()
        ));

        let span_kinds: ArrayRef = Arc::new(StringArray::from(
            spans.iter()
                .map(|s| s.span_kind.as_deref())
                .collect::<Vec<_>>()
        ));

        // Convert timestamps to microseconds since epoch (TIMESTAMPTZ)
        let timestamps: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(
                spans.iter()
                    .map(|s| s.timestamp.timestamp_micros())
                    .collect::<Vec<_>>()
            ).with_timezone_utc()
        );

        let end_timestamps: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(
                spans.iter()
                    .map(|s| s.end_timestamp.map(|t| t.timestamp_micros()))
                    .collect::<Vec<_>>()
            ).with_timezone_utc()
        );

        // Build attributes MAP<STRING, STRING> for each span
        let attributes_array = Self::build_attributes_array(spans, &attributes_field)?;

        // Build events LIST<STRUCT> for each span
        let events_array = Self::build_events_array(spans, &events_field)?;

        let status_codes: ArrayRef = Arc::new(StringArray::from(
            spans.iter()
                .map(|s| s.status_code.as_deref())
                .collect::<Vec<_>>()
        ));

        let status_messages: ArrayRef = Arc::new(StringArray::from(
            spans.iter()
                .map(|s| s.status_message.as_deref())
                .collect::<Vec<_>>()
        ));

        // record_date: derive from each span's timestamp for proper partition assignment
        // This enables partition pruning for date-range queries
        // IMPORTANT: All spans in a batch must have the same record_date for partition compatibility
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let record_date_values: Vec<i32> = spans.iter()
            .map(|s| {
                // Extract date from span timestamp (UTC)
                let span_date = s.timestamp.date_naive();
                (span_date - epoch).num_days() as i32
            })
            .collect();
        
        // Verify all spans have the same record_date (required for partition compatibility)
        if let Some(first_date) = record_date_values.first() {
            if record_date_values.iter().any(|&d| d != *first_date) {
                let unique_dates: std::collections::HashSet<i32> = record_date_values.iter().copied().collect();
                return Err(anyhow::anyhow!(
                    "All spans in a batch must have the same record_date for partition compatibility. Found {} unique dates: {:?}",
                    unique_dates.len(),
                    unique_dates
                ));
            }
        }
        
        let record_dates: ArrayRef = Arc::new(Date32Array::from(record_date_values));

        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                session_ids,
                trace_ids,
                span_ids,
                parent_span_ids,
                app_ids,
                organization_ids,
                tenant_ids,
                message_types,
                span_kinds,
                timestamps,
                end_timestamps,
                attributes_array,
                events_array,
                status_codes,
                status_messages,
                record_dates,
            ],
        )?;

        info!("Created Arrow RecordBatch with {} rows", record_batch.num_rows());
        Ok(record_batch)
    }

    /// Build attributes MAP<STRING, STRING> array for each span
    fn build_attributes_array(spans: &[SpanData], attributes_field: &arrow::datatypes::FieldRef) -> Result<ArrayRef> {
        use arrow::datatypes::DataType;

        // Build keys and values arrays for all attributes across all spans
        let mut all_keys = Vec::new();
        let mut all_values = Vec::new();
        let mut offsets = vec![0i32];
        let mut current_offset = 0i32;

        for span in spans {
            // Add this span's attributes
            for (key, value) in &span.attributes {
                all_keys.push(key.as_str());
                all_values.push(value.as_str());
                current_offset += 1;
            }
            offsets.push(current_offset);
        }

        // Create key and value arrays
        let keys_array: ArrayRef = Arc::new(StringArray::from(all_keys));
        let values_array: ArrayRef = Arc::new(StringArray::from(all_values));

        // Extract the struct field definition from the map data type
        // This preserves the field IDs from the Iceberg schema
        let map_data_type = attributes_field.data_type();
        let (entries_field, _) = if let DataType::Map(f, _) = map_data_type {
            (f.clone(), true)
        } else {
            return Err(anyhow::anyhow!("Expected Map type for attributes field"));
        };

        // Extract key and value field definitions from the struct
        let struct_fields = if let DataType::Struct(fields) = entries_field.data_type() {
            fields.clone()
        } else {
            return Err(anyhow::anyhow!("Expected Struct type in Map entries"));
        };

        // Build struct array using the schema's field definitions (preserves field IDs)
        let entries_struct = StructArray::new(
            struct_fields,
            vec![keys_array, values_array],
            None,
        );

        // Create offsets buffer (defines boundaries for each map)
        let offsets_buffer = OffsetBuffer::new(offsets.into());

        // Use the entries field from schema (has correct field IDs)
        let map_array = MapArray::try_new(
            entries_field,
            offsets_buffer,
            entries_struct,
            None,
            false, // keys are not sorted
        )?;

        Ok(Arc::new(map_array))
    }

    /// Build events LIST<STRUCT> array for each span
    fn build_events_array(spans: &[SpanData], events_field: &arrow::datatypes::FieldRef) -> Result<ArrayRef> {
        use arrow::datatypes::DataType;

        // Extract field definitions from the schema to preserve field IDs
        // events_field is LIST<STRUCT<...>>
        let element_field = if let DataType::List(f) = events_field.data_type() {
            f.clone()
        } else {
            return Err(anyhow::anyhow!("Expected List type for events field"));
        };

        // element_field is STRUCT<name, timestamp, attributes>
        let struct_fields = if let DataType::Struct(fields) = element_field.data_type() {
            fields.clone()
        } else {
            return Err(anyhow::anyhow!("Expected Struct type in List element"));
        };

        // Extract the attributes field from the struct (it's a MAP)
        let event_attr_field = struct_fields.iter()
            .find(|f| f.name() == "attributes")
            .ok_or_else(|| anyhow::anyhow!("attributes field not found in event struct"))?;

        // Extract the map entry field from attributes
        let event_attr_entries_field = if let DataType::Map(f, _) = event_attr_field.data_type() {
            f.clone()
        } else {
            return Err(anyhow::anyhow!("Expected Map type for event attributes field"));
        };

        // Extract key/value field definitions from the map entry struct
        let event_attr_struct_fields = if let DataType::Struct(fields) = event_attr_entries_field.data_type() {
            fields.clone()
        } else {
            return Err(anyhow::anyhow!("Expected Struct type in Map entries"));
        };

        // Build arrays for all event fields across all spans
        let mut all_event_names = Vec::new();
        let mut all_event_timestamps = Vec::new();
        let mut all_event_attr_keys = Vec::new();
        let mut all_event_attr_values = Vec::new();
        let mut event_attr_offsets = vec![0i32];
        let mut list_offsets = vec![0i32];

        let mut current_event_offset = 0i32;
        let mut current_attr_offset = 0i32;

        for span in spans {
            // Add this span's events
            for event in &span.events {
                all_event_names.push(event.name.as_str());
                all_event_timestamps.push(event.timestamp.timestamp_micros());

                // Add event's attributes
                for (key, value) in &event.attributes {
                    all_event_attr_keys.push(key.as_str());
                    all_event_attr_values.push(value.as_str());
                    current_attr_offset += 1;
                }
                event_attr_offsets.push(current_attr_offset);
                current_event_offset += 1;
            }
            list_offsets.push(current_event_offset);
        }

        // Build event names array
        let names_array: ArrayRef = Arc::new(StringArray::from(all_event_names));

        // Build event timestamps array
        let timestamps_array: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(all_event_timestamps).with_timezone_utc()
        );

        // Build event attributes map using schema's field definitions
        let event_attr_keys_array: ArrayRef = Arc::new(StringArray::from(all_event_attr_keys));
        let event_attr_values_array: ArrayRef = Arc::new(StringArray::from(all_event_attr_values));

        let event_attr_entries = StructArray::new(
            event_attr_struct_fields,  // Use schema's field definitions (has field IDs)
            vec![event_attr_keys_array, event_attr_values_array],
            None,
        );

        let event_attr_offsets_buffer = OffsetBuffer::new(event_attr_offsets.into());

        let event_attr_map = MapArray::try_new(
            event_attr_entries_field,  // Use schema's field definition (has field ID)
            event_attr_offsets_buffer,
            event_attr_entries,
            None,
            false, // keys are not sorted
        )?;

        // Build struct array for events using schema's struct fields
        let struct_arrays: Vec<ArrayRef> = vec![
            names_array,
            timestamps_array,
            Arc::new(event_attr_map),
        ];

        let struct_array = StructArray::new(
            struct_fields,  // Use schema's field definitions (has field IDs)
            struct_arrays,
            None, // No nulls for struct elements
        );

        // Build list array wrapping the struct array
        let list_offsets_buffer = OffsetBuffer::new(list_offsets.into());

        let list_array = ListArray::try_new(
            element_field,  // Use schema's field definition (has field ID)
            list_offsets_buffer,
            Arc::new(struct_array),
            None, // No nulls - all spans have a list (may be empty)
        )?;

        Ok(Arc::new(list_array))
    }

}

#[cfg(test)]
mod iceberg_test {
    use super::*;
    use crate::storage::span_buffer::{SpanData, SpanEvent};
    use std::collections::HashMap;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Field, DataType, Fields};

    fn create_test_config() -> Config {
        Config::default()
    }

    fn create_span_with_attributes() -> SpanData {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), "session_123".to_string());
        attributes.insert("http.method".to_string(), "POST".to_string());
        attributes.insert("http.url".to_string(), "/api/test".to_string());

        SpanData {
            trace_id: "trace_001".to_string(),
            span_id: "span_001".to_string(),
            parent_span_id: None,
            app_id: "test_app".to_string(),
            organization_id: Some("org_123".to_string()),
            tenant_id: None,
            message_type: "test_message".to_string(),
            span_kind: Some("INTERNAL".to_string()),
            timestamp: chrono::Utc::now(),
            end_timestamp: None,
            attributes,
            events: vec![],
            status_code: Some("OK".to_string()),
            status_message: None,
        }
    }

    fn create_span_with_events() -> SpanData {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), "session_456".to_string());

        let mut event1_attrs = HashMap::new();
        event1_attrs.insert("event.key1".to_string(), "value1".to_string());
        event1_attrs.insert("event.key2".to_string(), "value2".to_string());

        let mut event2_attrs = HashMap::new();
        event2_attrs.insert("error.type".to_string(), "TimeoutError".to_string());

        let events = vec![
            SpanEvent {
                name: "request_started".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: event1_attrs,
            },
            SpanEvent {
                name: "error_occurred".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: event2_attrs,
            },
        ];

        SpanData {
            trace_id: "trace_002".to_string(),
            span_id: "span_002".to_string(),
            parent_span_id: None,
            app_id: "test_app".to_string(),
            organization_id: Some("org_456".to_string()),
            tenant_id: None,
            message_type: "test_with_events".to_string(),
            span_kind: Some("CLIENT".to_string()),
            timestamp: chrono::Utc::now(),
            end_timestamp: None,
            attributes,
            events,
            status_code: Some("ERROR".to_string()),
            status_message: Some("Timeout".to_string()),
        }
    }

    fn create_attributes_field() -> arrow::datatypes::FieldRef {
        // Create the MAP<STRING, STRING> field structure
        // MAP is stored as LIST<STRUCT<key_value<key, value>>>
        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Utf8, true));

        let struct_fields = Fields::from(vec![key_field, value_field]);
        let entries_field = Arc::new(Field::new("key_value", DataType::Struct(struct_fields), false));

        Arc::new(Field::new("attributes", DataType::Map(entries_field, false), false))
    }

    fn create_events_field() -> arrow::datatypes::FieldRef {
        // Create the LIST<STRUCT<name, timestamp, attributes>> field structure
        // First create the attributes MAP field for events
        let event_key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let event_value_field = Arc::new(Field::new("value", DataType::Utf8, true));
        let event_struct_fields = Fields::from(vec![event_key_field, event_value_field]);
        let event_entries_field = Arc::new(Field::new("key_value", DataType::Struct(event_struct_fields), false));
        let event_attributes_field = Arc::new(Field::new("attributes", DataType::Map(event_entries_field, false), false));

        // Now create the event struct fields
        let event_name_field = Arc::new(Field::new("name", DataType::Utf8, false));
        let event_timestamp_field = Arc::new(Field::new(
            "timestamp",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("+00:00".into())),
            false,
        ));

        let event_fields = Fields::from(vec![event_name_field, event_timestamp_field, event_attributes_field]);
        let event_struct = Arc::new(Field::new("event", DataType::Struct(event_fields), false));

        Arc::new(Field::new("events", DataType::List(event_struct), false))
    }

    #[test]
    fn test_build_attributes_array_single_span() {
        let span = create_span_with_attributes();
        let spans = vec![span];
        let attributes_field = create_attributes_field();

        let result = IcebergWriter::build_attributes_array(&spans, &attributes_field);
        assert!(result.is_ok(), "build_attributes_array should succeed");

        let array_ref = result.unwrap();
        let map_array = array_ref.as_any().downcast_ref::<MapArray>().unwrap();

        // Verify array length matches number of spans
        assert_eq!(map_array.len(), 1, "Should have 1 map entry for 1 span");

        // Verify the map is not null
        assert_eq!(map_array.null_count(), 0, "Map should not have null entries");
    }

    #[test]
    fn test_build_attributes_array_multiple_spans() {
        let span1 = create_span_with_attributes();
        let mut span2 = create_span_with_attributes();
        span2.span_id = "span_002".to_string();
        span2.attributes.insert("custom.field".to_string(), "custom_value".to_string());

        let spans = vec![span1, span2];
        let attributes_field = create_attributes_field();

        let result = IcebergWriter::build_attributes_array(&spans, &attributes_field);
        assert!(result.is_ok(), "build_attributes_array should succeed");

        let array_ref = result.unwrap();
        let map_array = array_ref.as_any().downcast_ref::<MapArray>().unwrap();

        // Verify array length matches number of spans
        assert_eq!(map_array.len(), 2, "Should have 2 map entries for 2 spans");

        // Verify structure: each map should contain key-value pairs
        assert_eq!(map_array.null_count(), 0, "Maps should not have null entries");
    }

    #[test]
    fn test_build_attributes_array_empty_attributes() {
        let mut span = create_span_with_attributes();
        span.attributes.clear(); // Empty attributes

        let spans = vec![span];
        let attributes_field = create_attributes_field();

        let result = IcebergWriter::build_attributes_array(&spans, &attributes_field);
        assert!(result.is_ok(), "build_attributes_array should handle empty attributes");

        let array_ref = result.unwrap();
        let map_array = array_ref.as_any().downcast_ref::<MapArray>().unwrap();

        // Even with empty attributes, we should have 1 map entry (just empty)
        assert_eq!(map_array.len(), 1, "Should have 1 map entry even with empty attributes");
    }

    #[test]
    fn test_build_events_array_single_span_with_events() {
        let span = create_span_with_events();
        let spans = vec![span.clone()];
        let events_field = create_events_field();

        let result = IcebergWriter::build_events_array(&spans, &events_field);
        assert!(result.is_ok(), "build_events_array should succeed");

        let array_ref = result.unwrap();
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        // Verify array length matches number of spans
        assert_eq!(list_array.len(), 1, "Should have 1 list entry for 1 span");

        // Verify the list is not null
        assert_eq!(list_array.null_count(), 0, "List should not have null entries");

        // Verify the list contains the correct number of events
        let values = list_array.values();
        let struct_array = values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.len(), 2, "Should have 2 events in the list");
    }

    #[test]
    fn test_build_events_array_empty_events() {
        let mut span = create_span_with_events();
        span.events.clear(); // No events

        let spans = vec![span];
        let events_field = create_events_field();

        let result = IcebergWriter::build_events_array(&spans, &events_field);
        assert!(result.is_ok(), "build_events_array should handle empty events");

        let array_ref = result.unwrap();
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        // Should have 1 list entry (empty list)
        assert_eq!(list_array.len(), 1, "Should have 1 list entry even with no events");
    }

    #[test]
    fn test_build_events_array_multiple_spans() {
        let span1 = create_span_with_events();
        let mut span2 = create_span_with_events();
        span2.span_id = "span_003".to_string();

        // Add one more event to span2
        let mut event3_attrs = HashMap::new();
        event3_attrs.insert("additional.key".to_string(), "additional_value".to_string());
        span2.events.push(SpanEvent {
            name: "additional_event".to_string(),
            timestamp: chrono::Utc::now(),
            attributes: event3_attrs,
        });

        let spans = vec![span1, span2];
        let events_field = create_events_field();

        let result = IcebergWriter::build_events_array(&spans, &events_field);
        assert!(result.is_ok(), "build_events_array should succeed");

        let array_ref = result.unwrap();
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        // Verify array length matches number of spans
        assert_eq!(list_array.len(), 2, "Should have 2 list entries for 2 spans");

        // Verify total number of events across all spans
        let values = list_array.values();
        let struct_array = values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.len(), 5, "Should have 5 events total (2 + 3)");
    }

    #[test]
    fn test_build_events_array_structure() {
        let span = create_span_with_events();
        let spans = vec![span];
        let events_field = create_events_field();

        let result = IcebergWriter::build_events_array(&spans, &events_field);
        assert!(result.is_ok(), "build_events_array should succeed");

        let array_ref = result.unwrap();
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        // Get the struct array from the list
        let values = list_array.values();
        let struct_array = values.as_any().downcast_ref::<StructArray>().unwrap();

        // Verify struct has 3 fields: name, timestamp, attributes
        assert_eq!(struct_array.num_columns(), 3, "Event struct should have 3 fields");

        // Verify field names
        let name_array = struct_array.column(0).as_string::<i32>();
        assert_eq!(name_array.len(), 2, "Should have 2 event names");
        assert_eq!(name_array.value(0), "request_started", "First event name should match");
        assert_eq!(name_array.value(1), "error_occurred", "Second event name should match");

        // Verify timestamp field exists
        let timestamp_array = struct_array.column(1);
        assert_eq!(timestamp_array.len(), 2, "Should have 2 timestamps");

        // Verify attributes field is a map
        let attributes_array = struct_array.column(2);
        let event_attrs_map = attributes_array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(event_attrs_map.len(), 2, "Should have 2 attribute maps (one per event)");
    }
}
