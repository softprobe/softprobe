pub mod arrow;
/// Modular Iceberg storage implementation
///
/// This module provides a clean, composable architecture for Iceberg operations:
/// - catalog: Shared REST catalog initialization
/// - tables: Table schema definitions (traces, logs)
/// - arrow: Arrow RecordBatch conversions
/// - writer: Generic write logic with retry and row group isolation
pub mod catalog;
pub mod tables;
pub mod writer;

use crate::config::Config;
use crate::models::{Log, Metric, Span};
use ::arrow::record_batch::RecordBatch;
use anyhow::Result;
use iceberg::io::FileIOBuilder;
use iceberg::spec::{
    DataContentType, FormatVersion, ManifestContentType, ManifestList, ManifestListWriter,
};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;
use tracing::warn;

pub use catalog::IcebergCatalog;
pub use tables::{OtlpLogsTable, OtlpMetricsTable, TraceTable};
pub use writer::TableWriter;

/// Main Iceberg writer - manages traces, logs, and metrics tables
pub struct IcebergWriter {
    #[allow(dead_code)] // Will be used for query methods
    catalog: Arc<IcebergCatalog>,
    spans_writer: TableWriter,
    logs_writer: TableWriter,
    metrics_writer: TableWriter,
    spans_table_ident: TableIdent,
    logs_table_ident: TableIdent,
    metrics_table_ident: TableIdent,
    cache_dir: Option<PathBuf>,
}

impl IcebergWriter {
    pub async fn new(config: &Config) -> Result<Self> {
        info!("Initializing Iceberg writers for spans, logs, and metrics");

        // Initialize shared catalog
        let catalog = Arc::new(IcebergCatalog::new(config).await?);

        let namespace_name = config.iceberg.namespace.as_str();
        // Ensure configured namespace exists for Lakekeeper REST catalog
        let namespace = NamespaceIdent::from_strs([namespace_name])?;
        ensure_namespace_exists(catalog.catalog(), &namespace).await?;

        // Create table identifiers
        let spans_table_ident = TableIdent::from_strs(&[namespace_name, TraceTable::table_name()])?;
        let logs_table_ident =
            TableIdent::from_strs(&[namespace_name, OtlpLogsTable::table_name()])?;
        let metrics_table_ident =
            TableIdent::from_strs(&[namespace_name, OtlpMetricsTable::table_name()])?;

        // Ensure tables exist
        ensure_table_exists(catalog.catalog(), &spans_table_ident, TableType::Spans).await?;
        ensure_table_exists(catalog.catalog(), &logs_table_ident, TableType::Logs).await?;
        ensure_table_exists(catalog.catalog(), &metrics_table_ident, TableType::Metrics).await?;

        // Create writers
        let spans_writer = TableWriter::new(catalog.catalog().clone(), spans_table_ident.clone());
        let logs_writer = TableWriter::new(catalog.catalog().clone(), logs_table_ident.clone());
        let metrics_writer =
            TableWriter::new(catalog.catalog().clone(), metrics_table_ident.clone());

        info!("Iceberg writers initialized successfully");

        let writer = Self {
            catalog,
            spans_writer,
            logs_writer,
            metrics_writer,
            spans_table_ident,
            logs_table_ident,
            metrics_table_ident,
            cache_dir: config.ingest_engine.cache_dir.as_ref().map(PathBuf::from),
        };

        writer.refresh_metadata_pointers().await;

        Ok(writer)
    }

    /// Write span batches to traces table
    /// Each batch becomes a separate row group for session isolation
    pub async fn write_span_batches(&self, batches: Vec<Vec<Span>>) -> Result<()> {
        self.spans_writer
            .write_batches(batches, Span::to_record_batch)
            .await?;
        self.update_metadata_pointer(&self.spans_table_ident, TraceTable::table_name())
            .await;
        // Download files in background (non-blocking)
        let table_ident = self.spans_table_ident.clone();
        let table_name = TraceTable::table_name().to_string();
        let cache_dir = self.cache_dir.clone();
        let catalog = self.catalog.clone();
        tokio::spawn(async move {
            if let Some(cache_dir) = cache_dir {
                if let Ok(table) = catalog.catalog().load_table(&table_ident).await {
                    Self::download_files_in_background(&table, &cache_dir, &table_name).await;
                }
            }
        });
        Ok(())
    }

    /// Write log batches to logs table
    /// Each batch becomes a separate row group for session isolation
    pub async fn write_log_batches(&self, batches: Vec<Vec<Log>>) -> Result<()> {
        self.logs_writer
            .write_batches(batches, arrow::logs_to_record_batch)
            .await?;
        self.update_metadata_pointer(&self.logs_table_ident, OtlpLogsTable::table_name())
            .await;
        // Download files in background (non-blocking)
        let table_ident = self.logs_table_ident.clone();
        let table_name = OtlpLogsTable::table_name().to_string();
        let cache_dir = self.cache_dir.clone();
        let catalog = self.catalog.clone();
        tokio::spawn(async move {
            if let Some(cache_dir) = cache_dir {
                if let Ok(table) = catalog.catalog().load_table(&table_ident).await {
                    Self::download_files_in_background(&table, &cache_dir, &table_name).await;
                }
            }
        });
        Ok(())
    }

    /// Write metric batches to metrics table
    /// Each batch becomes a separate row group for metric_name isolation
    pub async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> Result<()> {
        self.metrics_writer
            .write_batches(batches, arrow::metrics_to_record_batch)
            .await?;
        self.update_metadata_pointer(&self.metrics_table_ident, OtlpMetricsTable::table_name())
            .await;
        // Download files in background (non-blocking)
        let table_ident = self.metrics_table_ident.clone();
        let table_name = OtlpMetricsTable::table_name().to_string();
        let cache_dir = self.cache_dir.clone();
        let catalog = self.catalog.clone();
        tokio::spawn(async move {
            if let Some(cache_dir) = cache_dir {
                if let Ok(table) = catalog.catalog().load_table(&table_ident).await {
                    Self::download_files_in_background(&table, &cache_dir, &table_name).await;
                }
            }
        });
        Ok(())
    }

    pub async fn write_span_record_batches(&self, record_batches: Vec<RecordBatch>) -> Result<()> {
        self.spans_writer
            .write_record_batches(record_batches)
            .await?;
        self.update_metadata_pointer(&self.spans_table_ident, TraceTable::table_name())
            .await;
        Ok(())
    }

    pub async fn write_log_record_batches(&self, record_batches: Vec<RecordBatch>) -> Result<()> {
        self.logs_writer
            .write_record_batches(record_batches)
            .await?;
        self.update_metadata_pointer(&self.logs_table_ident, OtlpLogsTable::table_name())
            .await;
        Ok(())
    }

    pub async fn write_metric_record_batches(
        &self,
        record_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        self.metrics_writer
            .write_record_batches(record_batches)
            .await?;
        self.update_metadata_pointer(&self.metrics_table_ident, OtlpMetricsTable::table_name())
            .await;
        Ok(())
    }

    pub async fn spans_schema(&self) -> Result<std::sync::Arc<iceberg::spec::Schema>> {
        self.spans_writer.current_schema().await
    }

    pub async fn logs_schema(&self) -> Result<std::sync::Arc<iceberg::spec::Schema>> {
        self.logs_writer.current_schema().await
    }

    pub async fn metrics_schema(&self) -> Result<std::sync::Arc<iceberg::spec::Schema>> {
        self.metrics_writer.current_schema().await
    }

    async fn refresh_metadata_pointers(&self) {
        self.update_metadata_pointer(&self.spans_table_ident, TraceTable::table_name())
            .await;
        self.update_metadata_pointer(&self.logs_table_ident, OtlpLogsTable::table_name())
            .await;
        self.update_metadata_pointer(&self.metrics_table_ident, OtlpMetricsTable::table_name())
            .await;
    }

    /// Download files in background after commit (static method for use in spawn)
    async fn download_files_in_background(
        table: &iceberg::table::Table,
        cache_dir: &PathBuf,
        table_name: &str,
    ) {
        let table_cache_dir = cache_dir.join("iceberg_metadata").join(table_name);
        let data_dir = table_cache_dir.join("data");
        if let Err(_) = std::fs::create_dir_all(&data_dir) {
            return;
        }

        let mut downloaded_files = Vec::new();

        if let Some(snapshot) = table.metadata().current_snapshot() {
            match snapshot
                .load_manifest_list(table.file_io(), table.metadata())
                .await
            {
                Ok(manifest_list) => {
                    for entry in manifest_list.entries() {
                        if entry.content == iceberg::spec::ManifestContentType::Deletes {
                            continue;
                        }
                        if let Ok(manifest) = entry.load_manifest(table.file_io()).await {
                            let mut download_tasks = Vec::new();
                            for manifest_entry in manifest.entries() {
                                if manifest_entry.is_alive()
                                    && manifest_entry.content_type() == iceberg::spec::DataContentType::Data
                                {
                                    let file_path = manifest_entry.file_path();
                                    if file_path.contains("://") {
                                        // Download in parallel
                                        let file_path = file_path.to_string();
                                        let data_dir = data_dir.clone();
                                        let file_io = table.file_io().clone();
                                        download_tasks.push(tokio::spawn(async move {
                                            Self::download_single_file(&file_path, &data_dir, &file_io).await
                                        }));
                                    }
                                }
                            }
                            // Wait for all downloads and collect results
                            for task in download_tasks {
                                if let Ok(Some(local)) = task.await {
                                    downloaded_files.push(local);
                                }
                            }
                        }
                    }
                }
                Err(_) => {}
            }
        }

        // Update data_files.json with downloaded local paths
        if !downloaded_files.is_empty() {
            let data_files_json_path = table_cache_dir.join("data_files.json");
            if let Ok(existing) = std::fs::read_to_string(&data_files_json_path) {
                if let Ok(mut data_files_value) = serde_json::from_str::<serde_json::Value>(&existing) {
                    if let Some(files_array) = data_files_value.get_mut("files").and_then(|f| f.as_array_mut()) {
                        // Replace S3 paths with local paths where available
                        for file in files_array.iter_mut() {
                            if let Some(s3_path) = file.as_str() {
                                if s3_path.contains("://") {
                                    // Check if this S3 path has a corresponding local file
                                    for local_path in &downloaded_files {
                                        // Extract filename from both paths and compare
                                        let s3_filename = s3_path.rsplit('/').next().unwrap_or("");
                                        let local_filename = local_path.rsplit('/').next().unwrap_or("");
                                        if s3_filename == local_filename || local_path.contains(s3_filename) {
                                            *file = serde_json::Value::String(local_path.clone());
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        // Write updated data_files.json
                        let temp_path = data_files_json_path.with_extension("json.tmp");
                        if let Ok(json_str) = serde_json::to_string(&data_files_value) {
                            if std::fs::write(&temp_path, json_str).is_ok() {
                                let _ = std::fs::rename(&temp_path, &data_files_json_path);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Download a single file in background, returns local path if successful
    async fn download_single_file(
        remote_path: &str,
        data_dir: &PathBuf,
        file_io: &iceberg::io::FileIO,
    ) -> Option<String> {
        let filename = remote_path
            .rsplit('/')
            .next()
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                remote_path.hash(&mut hasher);
                format!("{:x}.parquet", hasher.finish())
            });

        let local_path = data_dir.join(&filename);
        if local_path.exists() {
            return Some(local_path.to_string_lossy().to_string()); // Already cached
        }

        if let Ok(input) = file_io.new_input(remote_path) {
            if let Ok(bytes) = input.read().await {
                let temp_path = local_path.with_extension("parquet.tmp");
                if std::fs::write(&temp_path, &bytes).is_ok() {
                    if std::fs::rename(&temp_path, &local_path).is_ok() {
                        return Some(local_path.to_string_lossy().to_string());
                    }
                }
            }
        }
        None
    }

    /// Check if a Parquet file exists in local cache, return local path if found
    fn check_local_cache(&self, remote_path: &str, cache_dir: &PathBuf) -> Option<String> {
        if !remote_path.contains("://") {
            // Already a local path
            return Some(remote_path.to_string());
        }

        // Extract filename from path
        let filename = remote_path
            .rsplit('/')
            .next()
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                remote_path.hash(&mut hasher);
                format!("{:x}.parquet", hasher.finish())
            });

        let local_path = cache_dir.join("data").join(&filename);
        if local_path.exists() {
            Some(local_path.to_string_lossy().to_string())
        } else {
            None
        }
    }

    async fn update_metadata_pointer(&self, table_ident: &TableIdent, table_name: &str) {
        let Some(cache_dir) = &self.cache_dir else {
            return;
        };
        let debug_pin = std::env::var("ICEBERG_METADATA_PIN_DEBUG").ok().as_deref() == Some("1");
        let table = match self.catalog.catalog().load_table(table_ident).await {
            Ok(table) => table,
            Err(err) => {
                warn!(
                    "Failed to load Iceberg table {} for metadata pointer: {}",
                    table_name, err
                );
                return;
            }
        };
        let table_location = table.metadata().location();
        let normalize_uri = |path: &str| -> String {
            if path.starts_with("http://") || path.starts_with("https://") {
                let trimmed = path
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");
                if let Some((_, rest)) = trimmed.split_once('/') {
                    if let Some((bucket, object_path)) = rest.split_once('/') {
                        return format!("s3://{}/{}", bucket, object_path);
                    }
                }
            }
            path.to_string()
        };
        let metadata_location = match table.metadata_location() {
            Some(location) => location.to_string(),
            None => {
                warn!("Iceberg table {} missing metadata location", table_name);
                return;
            }
        };
        let metadata_location = normalize_uri(&metadata_location);

        let metadata_dir = cache_dir.join("iceberg_metadata");
        if let Err(err) = std::fs::create_dir_all(&metadata_dir) {
            warn!(
                "Failed to create Iceberg metadata cache dir {:?}: {}",
                metadata_dir, err
            );
            return;
        }
        let pointer_path = metadata_dir.join(format!("{table_name}.json"));
        if let Ok(contents) = std::fs::read_to_string(&pointer_path) {
            #[derive(serde::Deserialize)]
            struct ExistingPointer {
                metadata_location: Option<String>,
                snapshot_id: Option<i64>,
                data_files_path: Option<String>,
            }
            if let Ok(existing) = serde_json::from_str::<ExistingPointer>(&contents) {
                let snapshot_match = existing.snapshot_id == table.metadata().current_snapshot_id();
                let location_match = existing
                    .metadata_location
                    .as_deref()
                    .map(normalize_uri)
                    .as_deref()
                    == Some(metadata_location.as_str());
                let data_files_ok = existing
                    .data_files_path
                    .as_deref()
                    .map(|path| std::path::Path::new(path).exists())
                    .unwrap_or(false);
                if snapshot_match && location_match && data_files_ok {
                    return;
                }
            }
        }
        let table_cache_dir = metadata_dir.join(table_name);
        let table_metadata_dir = table_cache_dir.join("metadata");
        if let Err(err) = std::fs::create_dir_all(&table_metadata_dir) {
            warn!(
                "Failed to create Iceberg table metadata dir {:?}: {}",
                table_metadata_dir, err
            );
            return;
        }
        if let Ok(entries) = std::fs::read_dir(&table_metadata_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Err(err) = std::fs::remove_file(&path) {
                        warn!("Failed to remove stale metadata file {:?}: {}", path, err);
                    }
                }
            }
        }
        let metadata_file_name = metadata_location
            .rsplit('/')
            .next()
            .unwrap_or("metadata.json");
        let metadata_file_path = table_metadata_dir.join(metadata_file_name);
        if let Ok(input) = table.file_io().new_input(&metadata_location) {
            match input.read().await {
                Ok(bytes) => {
                    let temp_metadata_path =
                        table_metadata_dir.join(format!("{metadata_file_name}.tmp"));
                    if let Err(err) = std::fs::write(&temp_metadata_path, &bytes) {
                        warn!(
                            "Failed to write Iceberg metadata file {:?}: {}",
                            temp_metadata_path, err
                        );
                    } else if let Err(err) =
                        std::fs::rename(&temp_metadata_path, &metadata_file_path)
                    {
                        warn!(
                            "Failed to move Iceberg metadata file {:?}: {}",
                            metadata_file_path, err
                        );
                    } else if debug_pin {
                        eprintln!(
                            "[pin] wrote metadata file {} -> {:?}",
                            metadata_location, metadata_file_path
                        );
                    }
                }
                Err(err) => {
                    warn!(
                        "Failed to read Iceberg metadata {}: {}",
                        metadata_location, err
                    );
                }
            }
        } else {
            warn!(
                "Failed to create Iceberg metadata reader for {}",
                metadata_location
            );
        }

        let metadata_base = metadata_location
            .rsplit_once('/')
            .map(|(base, _)| base)
            .unwrap_or(table_location);
        let resolve_remote_path = |path: &str| -> String {
            if path.contains("://") {
                normalize_uri(path)
            } else {
                let trimmed = path.trim_start_matches('/');
                let trimmed = trimmed.strip_prefix("metadata/").unwrap_or(trimmed);
                format!("{}/{}", metadata_base.trim_end_matches('/'), trimmed)
            }
        };
        let resolve_local_path = |path: &str| -> PathBuf {
            let trimmed = path.trim_start_matches('/');
            let trimmed = trimmed.strip_prefix("metadata/").unwrap_or(trimmed);
            table_metadata_dir.join(trimmed)
        };

        let mut pinned_manifest_list_path: Option<PathBuf> = None;
        if let Ok(contents) = std::fs::read(&metadata_file_path) {
            let decoded = if metadata_file_name.ends_with(".gz.metadata.json")
                || metadata_file_name.ends_with(".gz")
            {
                let mut decoder = flate2::read::GzDecoder::new(contents.as_slice());
                let mut decoded = Vec::new();
                if decoder.read_to_end(&mut decoded).is_ok() {
                    decoded
                } else {
                    contents.clone()
                }
            } else {
                contents.clone()
            };
            if let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(&decoded) {
                let current_snapshot_id = json
                    .get("current-snapshot-id")
                    .and_then(|v| v.as_i64())
                    .or_else(|| json.get("current_snapshot_id").and_then(|v| v.as_i64()));
                if let Some(snapshot_id) = current_snapshot_id {
                    if let Some(snapshots) = json.get("snapshots").and_then(|v| v.as_array()) {
                        for idx in 0..snapshots.len() {
                            let snapshot = snapshots[idx].clone();
                            let snap_id = snapshot
                                .get("snapshot-id")
                                .and_then(|v| v.as_i64())
                                .or_else(|| snapshot.get("snapshot_id").and_then(|v| v.as_i64()));
                            if snap_id == Some(snapshot_id) {
                                let manifest_list_path = snapshot
                                    .get("manifest-list")
                                    .and_then(|v| v.as_str())
                                    .or_else(|| {
                                        snapshot.get("manifest_list").and_then(|v| v.as_str())
                                    });
                                if let Some(manifest_list_path) = manifest_list_path {
                                    let manifest_list_name = manifest_list_path
                                        .rsplit('/')
                                        .next()
                                        .unwrap_or(manifest_list_path);
                                    let remote_path = resolve_remote_path(manifest_list_path);
                                    let local_path = table_metadata_dir.join(manifest_list_name);
                                    pinned_manifest_list_path = Some(local_path.clone());
                                    if let Some(parent) = local_path.parent() {
                                        let _ = std::fs::create_dir_all(parent);
                                    }
                                    if let Ok(input) = table.file_io().new_input(&remote_path) {
                                        if let Ok(bytes) = input.read().await {
                                            let temp_path = local_path.with_extension("tmp");
                                            if std::fs::write(&temp_path, &bytes).is_ok() {
                                                let _ = std::fs::rename(&temp_path, &local_path);
                                                if debug_pin {
                                                    eprintln!(
                                                        "[pin] wrote manifest list {} -> {:?}",
                                                        remote_path, local_path
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    if let Some(snapshot_obj) = snapshot.as_object() {
                                        let local_rel = local_path.to_string_lossy().to_string();
                                        if snapshot_obj.contains_key("manifest-list") {
                                            json["snapshots"][idx]["manifest-list"] =
                                                serde_json::Value::String(local_rel.clone());
                                        }
                                        if snapshot_obj.contains_key("manifest_list") {
                                            json["snapshots"][idx]["manifest_list"] =
                                                serde_json::Value::String(local_rel);
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
                if pinned_manifest_list_path.is_some() {
                    if let Ok(serialized) = serde_json::to_vec(&json) {
                        let mut output = Vec::new();
                        if metadata_file_name.ends_with(".gz.metadata.json")
                            || metadata_file_name.ends_with(".gz")
                        {
                            let mut encoder = flate2::write::GzEncoder::new(
                                &mut output,
                                flate2::Compression::default(),
                            );
                            let _ = encoder.write_all(&serialized);
                            let _ = encoder.finish();
                        } else {
                            output = serialized;
                        }
                        if !output.is_empty() {
                            let temp_path = metadata_file_path.with_extension("tmp");
                            if std::fs::write(&temp_path, &output).is_ok() {
                                let _ = std::fs::rename(&temp_path, &metadata_file_path);
                                if debug_pin {
                                    eprintln!(
                                        "[pin] rewrote metadata file with local manifest list {:?}",
                                        metadata_file_path
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut data_files_path: Option<PathBuf> = None;
        let normalize_data_path = |path: &str| -> String {
            if path.starts_with("http://") || path.starts_with("https://") {
                let trimmed = path
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");
                if let Some((_, rest)) = trimmed.split_once('/') {
                    if let Some((bucket, object_path)) = rest.split_once('/') {
                        return format!("s3://{}/{}", bucket, object_path);
                    }
                }
            }
            path.to_string()
        };
        if let Some(snapshot) = table.metadata().current_snapshot() {
            let manifest_list_path = snapshot.manifest_list();
            if !manifest_list_path.contains("://") {
                let remote_path = resolve_remote_path(manifest_list_path);
                let local_path = resolve_local_path(manifest_list_path);
                if let Some(parent) = local_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                if let Ok(input) = table.file_io().new_input(&remote_path) {
                    if let Ok(bytes) = input.read().await {
                        let temp_path = local_path.with_extension("tmp");
                        if std::fs::write(&temp_path, &bytes).is_ok() {
                            let _ = std::fs::rename(&temp_path, &local_path);
                        }
                    }
                }
            }

            match snapshot
                .load_manifest_list(table.file_io(), table.metadata())
                .await
            {
                Ok(manifest_list) => {
                    let mut data_files = Vec::new();
                    for entry in manifest_list.entries() {
                        if entry.content == ManifestContentType::Deletes {
                            continue;
                        }
                        let manifest_path = entry.manifest_path.as_str();
                        let (remote_path, local_path) = if manifest_path.contains("://") {
                            let name = manifest_path.rsplit('/').next().unwrap_or(manifest_path);
                            (manifest_path.to_string(), table_metadata_dir.join(name))
                        } else {
                            (
                                resolve_remote_path(manifest_path),
                                resolve_local_path(manifest_path),
                            )
                        };
                        if let Some(parent) = local_path.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        if let Ok(input) = table.file_io().new_input(&remote_path) {
                            if let Ok(bytes) = input.read().await {
                                let temp_path = local_path.with_extension("tmp");
                                if std::fs::write(&temp_path, &bytes).is_ok() {
                                    let _ = std::fs::rename(&temp_path, &local_path);
                                }
                            }
                        }
                        if let Ok(manifest) = entry.load_manifest(table.file_io()).await {
                            for manifest_entry in manifest.entries() {
                                if manifest_entry.is_alive()
                                    && manifest_entry.content_type() == DataContentType::Data
                                {
                                    let file_path = normalize_data_path(manifest_entry.file_path());
                                    // Check if file is already cached locally
                                    if let Some(local_path) = self.check_local_cache(&file_path, &table_cache_dir) {
                                        data_files.push(local_path);
                                    } else {
                                        // Use remote path - files will be downloaded lazily on first query
                                        data_files.push(file_path);
                                    }
                                }
                            }
                        }
                    }
                    if !data_files.is_empty() {
                        let path = table_cache_dir.join("data_files.json");
                        let payload = serde_json::json!({
                            "snapshot_id": snapshot.snapshot_id(),
                            "files": data_files,
                        });
                        let temp_path = path.with_extension("tmp");
                        if std::fs::write(&temp_path, payload.to_string()).is_ok() {
                            let _ = std::fs::rename(&temp_path, &path);
                            data_files_path = Some(path);
                        }
                    }
                }
                Err(err) => {
                    warn!("Failed to load manifest list for {}: {}", table_name, err);
                }
            }
        }

        if let Some(local_manifest_list_path) = pinned_manifest_list_path {
            if let Ok(bytes) = std::fs::read(&local_manifest_list_path) {
                let format_version = table.metadata().format_version();
                if let Ok(manifest_list) = ManifestList::parse_with_version(&bytes, format_version)
                {
                    let mut rewritten_entries = Vec::new();
                    for mut entry in manifest_list.consume_entries() {
                        let manifest_path = entry.manifest_path.clone();
                        let local_manifest_path = if manifest_path.contains("://") {
                            let name = manifest_path
                                .rsplit('/')
                                .next()
                                .unwrap_or(manifest_path.as_str());
                            table_metadata_dir.join(name)
                        } else {
                            resolve_local_path(&manifest_path)
                        };
                        if local_manifest_path.exists() {
                            entry.manifest_path = local_manifest_path.to_string_lossy().to_string();
                        }
                        rewritten_entries.push(entry);
                    }
                    if let Some(snapshot) = table.metadata().current_snapshot() {
                        let file_io = FileIOBuilder::new_fs_io().build();
                        match file_io {
                            Ok(file_io) => {
                                let output_path =
                                    local_manifest_list_path.to_string_lossy().to_string();
                                match file_io.new_output(&output_path) {
                                    Ok(output) => {
                                        let parent_snapshot_id: Option<i64> = snapshot.parent_snapshot_id();
                                        let mut writer = match format_version {
                                            FormatVersion::V1 => ManifestListWriter::v1(
                                                output,
                                                snapshot.snapshot_id(),
                                                parent_snapshot_id,
                                            ),
                                            FormatVersion::V2 => ManifestListWriter::v2(
                                                output,
                                                snapshot.snapshot_id(),
                                                parent_snapshot_id,
                                                snapshot.sequence_number(),
                                            ),
                                        };
                                        if let Err(err) =
                                            writer.add_manifests(rewritten_entries.into_iter())
                                        {
                                            warn!(
                                                "Failed to rewrite manifest list entries for {}: {}",
                                                table_name, err
                                            );
                                        } else if let Err(err) = writer.close().await {
                                            warn!(
                                                "Failed to write manifest list for {}: {}",
                                                table_name, err
                                            );
                                        } else if debug_pin {
                                            eprintln!(
                                                "[pin] rewrote manifest list with local manifest paths {:?}",
                                                local_manifest_list_path
                                            );
                                        }
                                    }
                                    Err(err) => {
                                        warn!(
                                            "Failed to open manifest list output {:?}: {}",
                                            local_manifest_list_path, err
                                        );
                                    }
                                }
                            }
                            Err(err) => {
                                warn!("Failed to build local FileIO for manifest list: {}", err);
                            }
                        }
                    }
                }
            }
        }

        let temp_path = metadata_dir.join(format!("{table_name}.json.tmp"));
        let final_path = pointer_path;
        let payload = serde_json::json!({
            "table_location": table_location,
            "pinned_table_location": table_cache_dir.to_string_lossy(),
            "metadata_file": metadata_file_name,
            "metadata_location": metadata_location,
            "snapshot_id": table.metadata().current_snapshot_id(),
            "data_files_path": data_files_path.as_ref().map(|path| path.to_string_lossy().to_string()),
        });
        if let Err(err) = std::fs::write(&temp_path, payload.to_string()) {
            warn!(
                "Failed to write Iceberg metadata pointer {:?}: {}",
                temp_path, err
            );
            return;
        }
        if let Err(err) = std::fs::rename(&temp_path, &final_path) {
            warn!(
                "Failed to move Iceberg metadata pointer {:?}: {}",
                final_path, err
            );
        }
    }
}

/// Table type for table creation
enum TableType {
    Spans,
    Logs,
    Metrics,
}

/// Ensure a namespace exists, create it if it doesn't
async fn ensure_namespace_exists(
    catalog: &Arc<iceberg_catalog_rest::RestCatalog>,
    namespace: &NamespaceIdent,
) -> Result<()> {
    match catalog.namespace_exists(namespace).await {
        Ok(true) => {
            info!(
                "Namespace {} already exists, using existing namespace",
                namespace
            );
            Ok(())
        }
        Ok(false) => {
            info!("Namespace {} does not exist, creating it", namespace);
            if let Err(err) = catalog.create_namespace(namespace, HashMap::new()).await {
                let message = err.to_string();
                if message.contains("already exists") {
                    info!(
                        "Namespace {} already exists after create attempt",
                        namespace
                    );
                } else {
                    return Err(err.into());
                }
            }
            Ok(())
        }
        Err(err) => {
            info!(
                "Namespace {} existence check failed, attempting to create it",
                namespace
            );
            if let Err(err) = catalog.create_namespace(namespace, HashMap::new()).await {
                let message = err.to_string();
                if message.contains("already exists") {
                    info!(
                        "Namespace {} already exists after create attempt",
                        namespace
                    );
                } else {
                    return Err(err.into());
                }
            }
            if !err.to_string().contains("already exists") {
                info!("Namespace {} existence check error: {}", namespace, err);
            }
            Ok(())
        }
    }
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
            info!(
                "Table {} existence check failed, assuming it doesn't exist",
                table_ident
            );
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
    match catalog
        .create_table(&table_ident.namespace(), table_creation)
        .await
    {
        Ok(_) => {
            info!("Successfully created table: {}", table_ident);
            Ok(())
        }
        Err(e) if e.to_string().contains("already exists") => {
            info!(
                "Table {} already exists (concurrent creation), continuing",
                table_ident
            );
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!(
            "Failed to create table {}: {}",
            table_ident,
            e
        )),
    }
}
