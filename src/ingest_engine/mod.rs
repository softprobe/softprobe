use crate::config::Config;
use crate::ingest_engine::fs::list_parquet_files;
use crate::models::{Log, Metric, Span};
use crate::storage::buffer::{
    Bufferable, FlushCallback, FlushFuture, PreAddCallback, PreAddFuture,
};

mod fs;
use crate::storage::iceberg::arrow::{
    logs_to_record_batch, metrics_to_record_batch, spans_to_record_batch,
};
use crate::storage::iceberg::IcebergWriter;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use iceberg::spec::Schema as IcebergSchema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct IngestEngine {
    wal_writer: Arc<WalWriter>,
    cache_dir: Option<PathBuf>,
    span_queue: Arc<FlushQueue>,
    log_queue: Arc<FlushQueue>,
    metric_queue: Arc<FlushQueue>,
    optimizer_interval_seconds: u64,
    #[allow(dead_code)]
    iceberg_writer: Arc<IcebergWriter>,
}

impl IngestEngine {
    pub async fn new(config: &Config, iceberg_writer: Arc<IcebergWriter>) -> Result<Self> {
        let wal_writer = Arc::new(WalWriter::new(config).await?);
        let cache_dir = config.ingest_engine.cache_dir.as_ref().map(PathBuf::from);

        let engine = Self {
            wal_writer,
            cache_dir,
            span_queue: Arc::new(FlushQueue::default()),
            log_queue: Arc::new(FlushQueue::default()),
            metric_queue: Arc::new(FlushQueue::default()),
            optimizer_interval_seconds: config.ingest_engine.optimizer_interval_seconds,
            iceberg_writer,
        };

        if config.ingest_engine.replay_wal_on_startup {
            engine.replay_wal().await?;
        }

        Ok(engine)
    }

    pub fn wal_writer(&self) -> Arc<WalWriter> {
        self.wal_writer.clone()
    }

    pub fn list_wal_files(&self, kind: &str) -> Result<Vec<PathBuf>> {
        let Some(cache_dir) = self.cache_dir.as_ref() else {
            return Ok(Vec::new());
        };
        let wal_dir = cache_dir.join("wal").join(kind);
        Ok(list_parquet_files(&wal_dir)?)
    }

    pub fn list_staged_files(&self, kind: &str) -> Result<Vec<PathBuf>> {
        let Some(cache_dir) = self.cache_dir.as_ref() else {
            return Ok(Vec::new());
        };
        let staged_dir = cache_dir.join(kind);
        Ok(list_parquet_files(&staged_dir)?)
    }

    pub fn span_pre_add_callback(&self) -> Arc<PreAddCallback<Span>> {
        // WAL writes now happen during buffer flush, not per-request.
        // This reduces small-file creation from 180K/hour to ~3.6K/hour at 50 QPS.
        Arc::new(
            move |items: Vec<Span>, _request_size: usize| -> PreAddFuture<Span> {
                Box::pin(async move {
                    Ok(items)
                })
            },
        )
    }

    pub fn log_pre_add_callback(&self) -> Arc<PreAddCallback<Log>> {
        // WAL writes now happen during buffer flush, not per-request.
        // This reduces small-file creation from 180K/hour to ~3.6K/hour at 50 QPS.
        Arc::new(
            move |items: Vec<Log>, _request_size: usize| -> PreAddFuture<Log> {
                Box::pin(async move {
                    Ok(items)
                })
            },
        )
    }

    pub fn metric_pre_add_callback(&self) -> Arc<PreAddCallback<Metric>> {
        // WAL writes now happen during buffer flush, not per-request.
        // This reduces small-file creation from 180K/hour to ~3.6K/hour at 50 QPS.
        Arc::new(
            move |items: Vec<Metric>, _request_size: usize| -> PreAddFuture<Metric> {
                Box::pin(async move {
                    Ok(items)
                })
            },
        )
    }

    pub fn span_flush_callback(&self) -> Arc<FlushCallback<Span>> {
        let cache_dir = self.cache_dir.clone();
        let queue = self.span_queue.clone();
        let engine = self.clone();
        let iceberg_writer = self.iceberg_writer.clone();
        let wal_writer = self.wal_writer.clone();
        Arc::new(move |batches: Vec<Vec<Span>>| -> FlushFuture {
            let cache_dir = cache_dir.clone();
            let queue = queue.clone();
            let engine = engine.clone();
            let iceberg_writer = iceberg_writer.clone();
            let wal_writer = wal_writer.clone();
            Box::pin(async move {
                let watermark = Utc::now();
                let schema = iceberg_writer.spans_schema().await?;
                
                // Write to WAL for crash recovery (REQUIRED)
                for batch in &batches {
                    if !batch.is_empty() {
                        wal_writer
                            .write_batch("spans", batch, 0, schema.as_ref(), spans_to_record_batch)
                            .await?;
                    }
                }
                
                // Write to staged cache for queries (REQUIRED)
                stage_batches(
                    "spans",
                    schema.as_ref(),
                    batches,
                    spans_to_record_batch,
                    cache_dir,
                    queue,
                )
                .await?;
                engine.update_wal_watermark("spans", watermark).await?;
                Ok(())
            })
        })
    }

    pub fn log_flush_callback(&self) -> Arc<FlushCallback<Log>> {
        let cache_dir = self.cache_dir.clone();
        let queue = self.log_queue.clone();
        let engine = self.clone();
        let iceberg_writer = self.iceberg_writer.clone();
        let wal_writer = self.wal_writer.clone();
        Arc::new(move |batches: Vec<Vec<Log>>| -> FlushFuture {
            let cache_dir = cache_dir.clone();
            let queue = queue.clone();
            let engine = engine.clone();
            let iceberg_writer = iceberg_writer.clone();
            let wal_writer = wal_writer.clone();
            Box::pin(async move {
                let watermark = Utc::now();
                let schema = iceberg_writer.logs_schema().await?;
                
                // Write to WAL for crash recovery (REQUIRED)
                for batch in &batches {
                    if !batch.is_empty() {
                        wal_writer
                            .write_batch("logs", batch, 0, schema.as_ref(), logs_to_record_batch)
                            .await?;
                    }
                }
                
                // Write to staged cache for queries (REQUIRED)
                stage_batches(
                    "logs",
                    schema.as_ref(),
                    batches,
                    logs_to_record_batch,
                    cache_dir,
                    queue,
                )
                .await?;
                engine.update_wal_watermark("logs", watermark).await?;
                Ok(())
            })
        })
    }

    pub fn metric_flush_callback(&self) -> Arc<FlushCallback<Metric>> {
        let cache_dir = self.cache_dir.clone();
        let queue = self.metric_queue.clone();
        let engine = self.clone();
        let iceberg_writer = self.iceberg_writer.clone();
        let wal_writer = self.wal_writer.clone();
        Arc::new(move |batches: Vec<Vec<Metric>>| -> FlushFuture {
            let cache_dir = cache_dir.clone();
            let queue = queue.clone();
            let engine = engine.clone();
            let iceberg_writer = iceberg_writer.clone();
            let wal_writer = wal_writer.clone();
            Box::pin(async move {
                let watermark = Utc::now();
                let schema = iceberg_writer.metrics_schema().await?;
                
                // Write to WAL for crash recovery (REQUIRED)
                for batch in &batches {
                    if !batch.is_empty() {
                        wal_writer
                            .write_batch("metrics", batch, 0, schema.as_ref(), metrics_to_record_batch)
                            .await?;
                    }
                }
                
                // Write to staged cache for queries (REQUIRED)
                stage_batches(
                    "metrics",
                    schema.as_ref(),
                    batches,
                    metrics_to_record_batch,
                    cache_dir,
                    queue,
                )
                .await?;
                engine.update_wal_watermark("metrics", watermark).await?;
                Ok(())
            })
        })
    }

    pub fn span_queue(&self) -> Arc<FlushQueue> {
        self.span_queue.clone()
    }

    pub fn log_queue(&self) -> Arc<FlushQueue> {
        self.log_queue.clone()
    }

    pub fn metric_queue(&self) -> Arc<FlushQueue> {
        self.metric_queue.clone()
    }

    pub fn start_optimizer(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        let interval_seconds = self.optimizer_interval_seconds;
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
            loop {
                ticker.tick().await;
                if let Err(err) = engine.optimize_once().await {
                    warn!("Optimizer run failed: {}", err);
                }
            }
        });
    }

    pub async fn run_optimizer_once(&self) -> Result<()> {
        self.optimize_once().await
    }

    async fn optimize_once(&self) -> Result<()> {
        self.optimize_spans().await?;
        self.optimize_logs().await?;
        self.optimize_metrics().await?;
        Ok(())
    }

    async fn optimize_spans(&self) -> Result<()> {
        let staged = self.span_queue.drain().await;
        if staged.is_empty() {
            return Ok(());
        }

        let grouped = group_by_date(staged);
        for (date, items) in grouped {
            let mut combined = Vec::new();
            for item in &items {
                combined.extend(item.record_batches.clone());
            }

            if combined.is_empty() {
                continue;
            }

            if let Err(err) = self
                .iceberg_writer
                .write_span_record_batches(combined)
                .await
            {
                warn!("Optimizer failed to commit spans for {}: {}", date, err);
                for item in items {
                    self.span_queue.push(item).await;
                }
                return Err(err);
            }

            cleanup_cache(items).await;
        }
        Ok(())
    }

    async fn optimize_logs(&self) -> Result<()> {
        let staged = self.log_queue.drain().await;
        if staged.is_empty() {
            return Ok(());
        }

        let grouped = group_by_date(staged);
        for (date, items) in grouped {
            let mut combined = Vec::new();
            for item in &items {
                combined.extend(item.record_batches.clone());
            }

            if combined.is_empty() {
                continue;
            }

            if let Err(err) = self.iceberg_writer.write_log_record_batches(combined).await {
                warn!("Optimizer failed to commit logs for {}: {}", date, err);
                for item in items {
                    self.log_queue.push(item).await;
                }
                return Err(err);
            }

            cleanup_cache(items).await;
        }
        Ok(())
    }

    async fn optimize_metrics(&self) -> Result<()> {
        let staged = self.metric_queue.drain().await;
        if staged.is_empty() {
            return Ok(());
        }

        let grouped = group_by_date(staged);
        for (date, items) in grouped {
            let mut combined = Vec::new();
            for item in &items {
                combined.extend(item.record_batches.clone());
            }

            if combined.is_empty() {
                continue;
            }

            if let Err(err) = self
                .iceberg_writer
                .write_metric_record_batches(combined)
                .await
            {
                warn!("Optimizer failed to commit metrics for {}: {}", date, err);
                for item in items {
                    self.metric_queue.push(item).await;
                }
                return Err(err);
            }

            cleanup_cache(items).await;
        }
        Ok(())
    }

    async fn update_wal_watermark(
        &self,
        kind: &str,
        watermark: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let Some(cache_dir) = &self.cache_dir else {
            return Ok(());
        };
        let watermark_dir = cache_dir.join("wal_watermarks");
        let path = wal_watermark_path(&watermark_dir, &self.wal_writer.wal_prefix, kind);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        // Read the OLD watermark before updating (for cleanup)
        let old_watermark = if let Ok(content) = tokio::fs::read_to_string(&path).await {
            chrono::DateTime::parse_from_rfc3339(&content)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        } else {
            None
        };
        
        // Write the NEW watermark
        let timestamp = watermark.to_rfc3339();
        tokio::fs::write(path, timestamp).await?;
        
        // Clean up WAL files older than the OLD watermark
        // This ensures WAL files from previous flushes (which are now in staged) are deleted
        // Add a small buffer (1 second) to account for filesystem timestamp precision
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
        let Some(cache_dir) = &self.cache_dir else {
            return Ok(());
        };
        let wal_dir = cache_dir.join("wal").join(kind);
        
        let files = list_parquet_files(&wal_dir)?;
        let mut deleted_count = 0;
        let mut skipped_count = 0;
        let mut error_count = 0;
        
        for file_path in files {
            // Check file modification time
            match std::fs::metadata(&file_path) {
                Ok(metadata) => {
                    if let Ok(modified) = metadata.modified() {
                        let modified = DateTime::<Utc>::from(modified);
                        // Delete files strictly older than watermark (not equal)
                        // This ensures we keep the current flush's WAL files
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
            info!("Cleaned up {} old WAL files for {} (kept {} recent)", deleted_count, kind, skipped_count);
        }
        if error_count > 3 {
            warn!("Failed to delete {} WAL files (showing first 3 errors)", error_count);
        }
        
        Ok(())
    }

    async fn replay_wal(&self) -> Result<()> {
        self.replay_kind("spans", self.span_queue.clone()).await?;
        self.replay_kind("logs", self.log_queue.clone()).await?;
        self.replay_kind("metrics", self.metric_queue.clone())
            .await?;
        Ok(())
    }

    async fn replay_kind(&self, kind: &str, queue: Arc<FlushQueue>) -> Result<()> {
        let record_batches = self.wal_writer.read_record_batches(kind).await?;
        if record_batches.is_empty() {
            return Ok(());
        }

        let mut grouped: std::collections::HashMap<NaiveDate, Vec<RecordBatch>> =
            std::collections::HashMap::new();
        for batch in record_batches {
            let date = record_batch_partition_date(&batch)?;
            grouped.entry(date).or_default().push(batch);
        }

        let schema = match kind {
            "spans" => self.iceberg_writer.spans_schema().await?,
            "logs" => self.iceberg_writer.logs_schema().await?,
            "metrics" => self.iceberg_writer.metrics_schema().await?,
            _ => return Err(anyhow::anyhow!("unknown WAL kind: {}", kind)),
        };

        for (date, batches) in grouped {
            stage_record_batches(
                kind,
                date,
                batches,
                schema.as_ref(),
                self.cache_dir.clone(),
                queue.clone(),
            )
            .await?;
        }

        Ok(())
    }
}

fn wal_watermark_path(root: &Path, wal_prefix: &str, kind: &str) -> PathBuf {
    let sanitized = wal_prefix.replace('/', "_");
    root.join(sanitized).join(format!("{kind}.txt"))
}

pub struct FlushQueue {
    items: Mutex<Vec<StagedBatch>>,
}

impl Default for FlushQueue {
    fn default() -> Self {
        Self {
            items: Mutex::new(Vec::new()),
        }
    }
}

impl FlushQueue {
    pub async fn push(&self, item: StagedBatch) {
        let mut items = self.items.lock().await;
        items.push(item);
    }

    pub async fn drain(&self) -> Vec<StagedBatch> {
        let mut items = self.items.lock().await;
        std::mem::take(&mut *items)
    }
}

pub struct StagedBatch {
    pub date: NaiveDate,
    pub record_batches: Vec<RecordBatch>,
    pub cache_path: Option<PathBuf>,
}

pub struct WalWriter {
    wal_prefix: String,
    cache_dir: Option<PathBuf>,
    manifest_writer: Option<Arc<WalManifestWriter>>,
}

impl WalWriter {
    pub async fn new(config: &Config) -> Result<Self> {
        let manifest_writer = config.ingest_engine.cache_dir.as_ref().map(|dir| {
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
            cache_dir: config.ingest_engine.cache_dir.as_ref().map(PathBuf::from),
            manifest_writer,
        })
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

            let cache_dir = self.cache_dir.as_ref().ok_or_else(|| {
                anyhow::anyhow!("local WAL requires ingest_engine.cache_dir to be configured")
            })?;
            let bytes = write_parquet_bytes(schema, &record_batches)?;
            let cache_path = cache_dir
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
            // Write atomically to avoid partially-written parquet being observed by readers.
            let tmp_path = cache_path.with_extension("parquet.tmp");
            tokio::fs::write(&tmp_path, &bytes).await?;
            tokio::fs::rename(&tmp_path, &cache_path).await?;
            if let Some(writer) = &self.manifest_writer {
                writer.record(kind, &cache_path).await?;
            }
        }

        Ok(())
    }

    async fn read_record_batches(&self, kind: &str) -> Result<Vec<RecordBatch>> {
        let cache_dir = self.cache_dir.as_ref().ok_or_else(|| {
            anyhow::anyhow!("local WAL requires ingest_engine.cache_dir to be configured")
        })?;
        let wal_dir = cache_dir.join("wal").join(kind);
        let watermark = self.read_wal_watermark(kind);
        let mut files = list_parquet_files(&wal_dir)?;
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

    fn read_wal_watermark(&self, kind: &str) -> chrono::DateTime<chrono::Utc> {
        let Some(cache_dir) = self.cache_dir.as_ref() else {
            return chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::UNIX_EPOCH);
        };
        let root = cache_dir.join("wal_watermarks");
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
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WalManifestFile {
    updated_at: String,
    files: Vec<String>,
}

struct WalManifestWriter {
    cache_dir: PathBuf,
    update_interval: std::time::Duration,
    max_pending: usize,
    pending: tokio::sync::Mutex<std::collections::HashMap<String, Vec<String>>>,
    last_flush: tokio::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>,
}

impl WalManifestWriter {
    fn new(cache_dir: PathBuf, update_interval: std::time::Duration, max_pending: usize) -> Self {
        Self {
            cache_dir,
            update_interval,
            max_pending,
            pending: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            last_flush: tokio::sync::Mutex::new(std::collections::HashMap::new()),
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

        let manifest_dir = self.cache_dir.join("wal_manifest");
        tokio::fs::create_dir_all(&manifest_dir).await?;
        let manifest_path = manifest_dir.join(format!("{kind}.json"));

        let mut existing_files = std::collections::HashSet::new();
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

async fn stage_batches<T, F>(
    kind: &str,
    schema: &IcebergSchema,
    batches: Vec<Vec<T>>,
    to_record_batch: F,
    cache_dir: Option<PathBuf>,
    queue: Arc<FlushQueue>,
) -> Result<()>
where
    T: Bufferable + Send + Sync + 'static,
    F: Fn(&[T], &IcebergSchema) -> Result<RecordBatch> + Send + Sync + 'static + Copy,
{
    if batches.is_empty() {
        return Ok(());
    }

    let date = batches
        .first()
        .and_then(|batch| batch.first())
        .ok_or_else(|| anyhow::anyhow!("missing batch data for {}", kind))?
        .partition_key();

    let mut record_batches = Vec::with_capacity(batches.len());
    for batch in &batches {
        if batch.is_empty() {
            continue;
        }
        record_batches.push(to_record_batch(batch, schema)?);
    }

    let cache_path = if let Some(cache_dir) = cache_dir.as_ref() {
        let cache_path = write_parquet_cache(kind, date, schema, &record_batches, cache_dir)?;
        Some(cache_path)
    } else {
        None
    };

    queue
        .push(StagedBatch {
            date,
            record_batches,
            cache_path,
        })
        .await;

    Ok(())
}

async fn stage_record_batches(
    kind: &str,
    date: NaiveDate,
    record_batches: Vec<RecordBatch>,
    schema: &IcebergSchema,
    cache_dir: Option<PathBuf>,
    queue: Arc<FlushQueue>,
) -> Result<()> {
    if record_batches.is_empty() {
        return Ok(());
    }

    let cache_path = if let Some(cache_dir) = cache_dir.as_ref() {
        let cache_path = write_parquet_cache(kind, date, schema, &record_batches, cache_dir)?;
        Some(cache_path)
    } else {
        None
    };

    queue
        .push(StagedBatch {
            date,
            record_batches,
            cache_path,
        })
        .await;

    Ok(())
}

fn write_parquet_cache(
    kind: &str,
    date: NaiveDate,
    schema: &IcebergSchema,
    record_batches: &[RecordBatch],
    cache_dir: &Path,
) -> Result<PathBuf> {
    let cache_path = cache_dir
        .join(kind)
        .join(format!("{:04}", date.year()))
        .join(format!("{:02}", date.month()))
        .join(format!("{:02}", date.day()))
        .join(format!("{}.parquet", Uuid::new_v4().simple()));
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write atomically: write to a temp file in the same directory, then rename.
    // This prevents DuckDB readers from observing partially-written parquet files.
    let tmp_path = cache_path.with_extension("parquet.tmp");

    let writer_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?))
        .build();
    let arrow_schema = Arc::new(arrow::datatypes::Schema::try_from(schema)?);
    let file = std::fs::File::create(&tmp_path)?;
    let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(writer_props))?;

    for record_batch in record_batches {
        writer.write(record_batch)?;
    }
    writer.close()?;
    std::fs::rename(&tmp_path, &cache_path)?;

    // Touch a marker so query engines can cheaply detect new staged files without scanning
    // the entire directory tree on every query.
    let marker_dir = cache_dir.join("staged_watermarks");
    std::fs::create_dir_all(&marker_dir)?;
    let marker_path = marker_dir.join(format!("{kind}.txt"));
    let marker_tmp = marker_dir.join(format!("{kind}.txt.tmp"));
    std::fs::write(&marker_tmp, chrono::Utc::now().to_rfc3339())?;
    std::fs::rename(&marker_tmp, &marker_path)?;

    info!("Staged {} parquet cache file at {:?}", kind, cache_path);

    Ok(cache_path)
}

fn write_parquet_bytes(schema: &IcebergSchema, record_batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let writer_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?))
        .build();
    let arrow_schema = Arc::new(arrow::datatypes::Schema::try_from(schema)?);
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, Some(writer_props))?;
    for record_batch in record_batches {
        writer.write(record_batch)?;
    }
    writer.close()?;
    Ok(buffer)
}

fn record_batch_partition_date(batch: &RecordBatch) -> Result<NaiveDate> {
    use arrow::array::AsArray;
    use arrow::datatypes::Date32Type;

    let num_columns = batch.num_columns();
    if num_columns == 0 {
        return Err(anyhow::anyhow!("record batch has no columns"));
    }
    let record_date_col = batch.column(num_columns - 1);
    let date_array = record_date_col.as_primitive::<Date32Type>();
    if date_array.is_empty() {
        return Err(anyhow::anyhow!("record batch has no rows"));
    }
    let days_since_epoch = date_array.value(0);
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    Ok(epoch + chrono::Duration::days(days_since_epoch as i64))
}

fn group_items_by_date<T: Bufferable + Clone>(items: &[T]) -> Vec<(NaiveDate, Vec<T>)> {
    let mut groups = Vec::new();
    let mut current_date: Option<NaiveDate> = None;
    let mut current_group: Vec<T> = Vec::new();

    for item in items {
        let item_date = item.partition_key();
        match &current_date {
            None => {
                current_date = Some(item_date);
                current_group.push(item.clone());
            }
            Some(date) if date == &item_date => {
                current_group.push(item.clone());
            }
            Some(date) => {
                groups.push((*date, std::mem::take(&mut current_group)));
                current_date = Some(item_date);
                current_group.push(item.clone());
            }
        }
    }

    if let Some(date) = current_date {
        if !current_group.is_empty() {
            groups.push((date, current_group));
        }
    }

    groups
}

fn group_items_by_key<T: Bufferable + Clone>(items: &[T]) -> Vec<Vec<T>> {
    let mut groups = Vec::new();
    let mut current_key: Option<String> = None;
    let mut current_group: Vec<T> = Vec::new();

    for item in items {
        let key = item.grouping_key();
        match &current_key {
            None => {
                current_key = Some(key.clone());
                current_group.push(item.clone());
            }
            Some(k) if k == &key => {
                current_group.push(item.clone());
            }
            Some(_) => {
                groups.push(std::mem::take(&mut current_group));
                current_key = Some(key.clone());
                current_group.push(item.clone());
            }
        }
    }

    if current_key.is_some() && !current_group.is_empty() {
        groups.push(current_group);
    }

    groups
}

fn group_by_date(
    staged: Vec<StagedBatch>,
) -> std::collections::HashMap<NaiveDate, Vec<StagedBatch>> {
    let mut grouped: std::collections::HashMap<NaiveDate, Vec<StagedBatch>> =
        std::collections::HashMap::new();
    for item in staged {
        grouped.entry(item.date).or_default().push(item);
    }
    grouped
}

async fn cleanup_cache(items: Vec<StagedBatch>) {
    for item in items {
        if let Some(path) = item.cache_path {
            if let Err(err) = tokio::fs::remove_file(&path).await {
                warn!("Failed to remove staged cache file {:?}: {}", path, err);
            }
        }
    }
}
