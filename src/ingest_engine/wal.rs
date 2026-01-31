use crate::config::Config;
use crate::ingest_engine::fs::list_parquet_files;
use crate::storage::buffer::Bufferable;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, Utc};
use iceberg::spec::Schema as IcebergSchema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct WalWriter {
    wal_prefix: String,
    wal_dir: Option<PathBuf>,
    manifest_writer: Option<Arc<WalManifestWriter>>,
}

impl WalWriter {
    pub async fn new(config: &Config) -> Result<Self> {
        let manifest_writer = config.ingest_engine.wal_dir.as_ref().map(|dir| {
            Arc::new(WalManifestWriter::new(
                PathBuf::from(dir),
                std::time::Duration::from_secs(
                    config.ingest_engine.wal_manifest_update_interval_seconds,
                ),
                config.ingest_engine.wal_manifest_max_pending_files,
            ))
        });

        Ok(Self {
            wal_prefix: config.ingest_engine.wal_prefix.clone(),
            wal_dir: config.ingest_engine.wal_dir.as_ref().map(PathBuf::from),
            manifest_writer,
        })
    }

    pub fn wal_dir(&self) -> Option<&PathBuf> {
        self.wal_dir.as_ref()
    }

    pub fn list_wal_files(&self, kind: &str) -> Result<Vec<PathBuf>> {
        let Some(wal_dir) = self.wal_dir.as_ref() else {
            return Ok(Vec::new());
        };
        let dir = wal_dir.join("wal").join(kind);
        Ok(list_parquet_files(&dir)?)
    }

    pub async fn write_batch<T, F>(
        &self,
        kind: &str,
        items: &[T],
        request_body_size: usize,
        schema: &IcebergSchema,
        to_record_batch: F,
    ) -> Result<()>
    where
        T: Bufferable + Clone,
        F: Fn(&[T], &IcebergSchema) -> Result<RecordBatch>,
    {
        if items.is_empty() {
            return Ok(());
        }

        let _ = request_body_size;
        let mut sorted = items.to_vec();
        sorted.sort_by(|a, b| {
            let date_a = a.partition_key();
            let date_b = b.partition_key();
            date_a
                .cmp(&date_b)
                .then_with(|| a.grouping_key().cmp(&b.grouping_key()))
                .then_with(|| a.compare_for_sort(b))
        });

        let date_groups = group_items_by_date(&sorted);
        let now = Utc::now();

        for (date, date_items) in date_groups {
            let grouped = group_items_by_key(&date_items);
            let mut record_batches = Vec::with_capacity(grouped.len());
            for items in grouped {
                if items.is_empty() {
                    continue;
                }
                record_batches.push(to_record_batch(&items, schema)?);
            }

            if record_batches.is_empty() {
                continue;
            }

            let wal_dir = self.wal_dir.as_ref().ok_or_else(|| {
                anyhow::anyhow!("local WAL requires ingest_engine.wal_dir to be configured")
            })?;
            let bytes = write_parquet_bytes(schema, &record_batches)?;
            let cache_path = wal_dir
                .join("wal")
                .join(kind)
                .join(format!("{:04}", date.year()))
                .join(format!("{:02}", date.month()))
                .join(format!("{:02}", date.day()))
                .join(format!(
                    "{}-{}.parquet",
                    now.format("%Y%m%dT%H%M%SZ"),
                    Uuid::new_v4().simple()
                ));
            if let Some(parent) = cache_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let tmp_path = cache_path.with_extension("parquet.tmp");
            tokio::fs::write(&tmp_path, &bytes).await?;
            tokio::fs::rename(&tmp_path, &cache_path).await?;
            if let Some(writer) = &self.manifest_writer {
                writer.record(kind, &cache_path).await?;
            }
        }

        Ok(())
    }

    pub async fn read_record_batches(&self, kind: &str) -> Result<Vec<RecordBatch>> {
        let wal_dir = self.wal_dir.as_ref().ok_or_else(|| {
            anyhow::anyhow!("local WAL requires ingest_engine.wal_dir to be configured")
        })?;
        let wal_path = wal_dir.join("wal").join(kind);
        let watermark = self.read_wal_watermark(kind);
        let mut files = list_parquet_files(&wal_path)?;
        files.sort();

        let mut batches = Vec::new();
        for path in files {
            let metadata = match std::fs::metadata(&path) {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            let modified = match metadata.modified() {
                Ok(modified) => DateTime::<Utc>::from(modified),
                Err(_) => continue,
            };
            if modified <= watermark {
                continue;
            }
            let file = std::fs::File::open(&path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let mut reader = builder.build()?;
            while let Some(batch) = reader.next() {
                batches.push(batch?);
            }
        }

        Ok(batches)
    }

    pub fn read_wal_watermark(&self, kind: &str) -> chrono::DateTime<chrono::Utc> {
        let Some(wal_dir) = self.wal_dir.as_ref() else {
            return chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::UNIX_EPOCH);
        };
        let root = wal_dir.join("wal_watermarks");
        let path = wal_watermark_path(&root, &self.wal_prefix, kind);
        match std::fs::read_to_string(&path) {
            Ok(contents) => chrono::DateTime::parse_from_rfc3339(contents.trim())
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| {
                    chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::UNIX_EPOCH)
                }),
            Err(_) => chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::UNIX_EPOCH),
        }
    }

    pub async fn update_wal_watermark(
        &self,
        kind: &str,
        watermark: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let Some(wal_dir) = self.wal_dir.as_ref() else {
            return Ok(());
        };
        let watermark_dir = wal_dir.join("wal_watermarks");
        let path = wal_watermark_path(&watermark_dir, &self.wal_prefix, kind);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let old_watermark = if let Ok(content) = tokio::fs::read_to_string(&path).await {
            chrono::DateTime::parse_from_rfc3339(&content)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        } else {
            None
        };

        let timestamp = watermark.to_rfc3339();
        tokio::fs::write(path, timestamp).await?;

        if let Some(old_watermark) = old_watermark {
            let cleanup_threshold = old_watermark + chrono::Duration::seconds(1);
            self.cleanup_old_wal_files(kind, cleanup_threshold).await?;
        }

        Ok(())
    }

    async fn cleanup_old_wal_files(
        &self,
        kind: &str,
        watermark: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let Some(wal_dir) = self.wal_dir.as_ref() else {
            return Ok(());
        };
        let dir = wal_dir.join("wal").join(kind);

        let files = list_parquet_files(&dir)?;
        let mut deleted_count = 0;
        let mut skipped_count = 0;
        let mut error_count = 0;

        for file_path in files {
            match std::fs::metadata(&file_path) {
                Ok(metadata) => {
                    if let Ok(modified) = metadata.modified() {
                        let modified = DateTime::<Utc>::from(modified);
                        if modified < watermark {
                            if let Err(err) = tokio::fs::remove_file(&file_path).await {
                                error_count += 1;
                                if error_count <= 3 {
                                    warn!("Failed to remove old WAL file {:?}: {}", file_path, err);
                                }
                            } else {
                                deleted_count += 1;
                            }
                        } else {
                            skipped_count += 1;
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        if deleted_count > 0 {
            info!(
                "Cleaned up {} old WAL files for {} (kept {} recent)",
                deleted_count, kind, skipped_count
            );
        }
        if error_count > 3 {
            warn!("Failed to delete {} WAL files (showing first 3 errors)", error_count);
        }

        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WalManifestFile {
    updated_at: String,
    files: Vec<String>,
}

struct WalManifestWriter {
    wal_dir: PathBuf,
    update_interval: std::time::Duration,
    max_pending: usize,
    pending: tokio::sync::Mutex<HashMap<String, Vec<String>>>,
    last_flush: tokio::sync::Mutex<HashMap<String, std::time::Instant>>,
}

impl WalManifestWriter {
    fn new(wal_dir: PathBuf, update_interval: std::time::Duration, max_pending: usize) -> Self {
        Self {
            wal_dir,
            update_interval,
            max_pending,
            pending: tokio::sync::Mutex::new(HashMap::new()),
            last_flush: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn record(&self, kind: &str, path: &Path) -> Result<()> {
        let path_str = path.to_string_lossy().to_string();
        {
            let mut pending = self.pending.lock().await;
            pending.entry(kind.to_string()).or_default().push(path_str);
        }

        let should_flush = {
            let mut last_flush = self.last_flush.lock().await;
            let last = last_flush
                .entry(kind.to_string())
                .or_insert_with(std::time::Instant::now);
            let pending_len = self
                .pending
                .lock()
                .await
                .get(kind)
                .map(|items| items.len())
                .unwrap_or(0);
            if last.elapsed() >= self.update_interval || pending_len >= self.max_pending {
                *last = std::time::Instant::now();
                true
            } else {
                false
            }
        };

        if should_flush {
            self.flush(kind).await?;
        }

        Ok(())
    }

    async fn flush(&self, kind: &str) -> Result<()> {
        let pending = {
            let mut pending = self.pending.lock().await;
            pending.remove(kind).unwrap_or_default()
        };
        if pending.is_empty() {
            return Ok(());
        }

        let manifest_dir = self.wal_dir.join("wal_manifest");
        tokio::fs::create_dir_all(&manifest_dir).await?;
        let manifest_path = manifest_dir.join(format!("{kind}.json"));

        let mut existing_files = HashSet::new();
        if let Ok(contents) = tokio::fs::read_to_string(&manifest_path).await {
            if let Ok(existing) = serde_json::from_str::<WalManifestFile>(&contents) {
                for file in existing.files {
                    existing_files.insert(file);
                }
            }
        }

        for file in pending {
            existing_files.insert(file);
        }

        let mut files: Vec<String> = existing_files.into_iter().collect();
        files.sort();

        let manifest = WalManifestFile {
            updated_at: chrono::Utc::now().to_rfc3339(),
            files,
        };
        let payload = serde_json::to_vec(&manifest)?;
        let tmp_path = manifest_dir.join(format!("{kind}.json.tmp"));
        tokio::fs::write(&tmp_path, payload).await?;
        tokio::fs::rename(&tmp_path, &manifest_path).await?;

        Ok(())
    }
}

fn wal_watermark_path(root: &Path, wal_prefix: &str, kind: &str) -> PathBuf {
    let sanitized = wal_prefix.replace('/', "_");
    root.join(sanitized).join(format!("{kind}.txt"))
}

fn group_items_by_date<T: Bufferable>(items: &[T]) -> Vec<(chrono::NaiveDate, Vec<T>)> {
    let mut grouped: HashMap<chrono::NaiveDate, Vec<T>> = HashMap::new();
    for item in items {
        grouped.entry(item.partition_key()).or_default().push(item.clone());
    }
    let mut groups: Vec<(chrono::NaiveDate, Vec<T>)> = grouped.into_iter().collect();
    groups.sort_by_key(|(date, _)| *date);
    groups
}

fn group_items_by_key<T: Bufferable>(items: &[T]) -> Vec<Vec<T>> {
    let mut grouped: HashMap<String, Vec<T>> = HashMap::new();
    for item in items {
        grouped
            .entry(item.grouping_key())
            .or_default()
            .push(item.clone());
    }
    grouped.into_values().collect()
}

fn write_parquet_bytes(schema: &IcebergSchema, record_batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let writer_props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3)?,
        ))
        .build();
    let arrow_schema = Arc::new(arrow::datatypes::Schema::try_from(schema)?);
    let mut buffer = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, arrow_schema, Some(writer_props))?;
    for record_batch in record_batches {
        writer.write(record_batch)?;
    }
    writer.close()?;
    Ok(buffer)
}
