use crate::config::Config;
use crate::models::{Log, Metric, Span};
use crate::storage::buffer::{
    Bufferable, FlushCallback, FlushFuture, PreAddCallback, PreAddFuture,
};
use crate::storage::{
    create_log_buffer, create_metric_buffer, create_span_buffer, IcebergWriter, Storage,
    TieredStorage,
};

mod fs;
mod wal;
use wal::WalWriter;
use crate::storage::iceberg::arrow::{
    logs_to_record_batch, metrics_to_record_batch, spans_to_record_batch,
};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate, Utc};
use iceberg::spec::Schema as IcebergSchema;
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
    span_queue: Arc<FlushQueue>,
    log_queue: Arc<FlushQueue>,
    metric_queue: Arc<FlushQueue>,
}

#[derive(Clone)]
pub struct IngestPipeline {
    pub storage: Storage,
    ingest_engine: Arc<IngestEngine>,
    cache_dir: Option<PathBuf>,
}

impl IngestEngine {
    pub async fn new(config: &Config) -> Result<Self> {
        let wal_writer = Arc::new(WalWriter::new(config).await?);

        let engine = Self {
            wal_writer,
            span_queue: Arc::new(FlushQueue::default()),
            log_queue: Arc::new(FlushQueue::default()),
            metric_queue: Arc::new(FlushQueue::default()),
        };

        Ok(engine)
    }

    pub fn wal_writer(&self) -> Arc<WalWriter> {
        self.wal_writer.clone()
    }

    pub fn list_wal_files(&self, kind: &str) -> Result<Vec<PathBuf>> {
        self.wal_writer.list_wal_files(kind)
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

    pub fn span_flush_callback(
        &self,
        iceberg_writer: Arc<IcebergWriter>,
        cache_dir: Option<PathBuf>,
    ) -> Arc<FlushCallback<Span>> {
        let queue = self.span_queue.clone();
        let wal_writer = self.wal_writer.clone();
        Arc::new(move |batches: Vec<Vec<Span>>| -> FlushFuture {
            let cache_dir = cache_dir.clone();
            let queue = queue.clone();
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
                wal_writer.update_wal_watermark("spans", watermark).await?;
                Ok(())
            })
        })
    }

    pub fn log_flush_callback(
        &self,
        iceberg_writer: Arc<IcebergWriter>,
        cache_dir: Option<PathBuf>,
    ) -> Arc<FlushCallback<Log>> {
        let queue = self.log_queue.clone();
        let wal_writer = self.wal_writer.clone();
        Arc::new(move |batches: Vec<Vec<Log>>| -> FlushFuture {
            let cache_dir = cache_dir.clone();
            let queue = queue.clone();
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
                wal_writer.update_wal_watermark("logs", watermark).await?;
                Ok(())
            })
        })
    }

    pub fn metric_flush_callback(
        &self,
        iceberg_writer: Arc<IcebergWriter>,
        cache_dir: Option<PathBuf>,
    ) -> Arc<FlushCallback<Metric>> {
        let queue = self.metric_queue.clone();
        let wal_writer = self.wal_writer.clone();
        Arc::new(move |batches: Vec<Vec<Metric>>| -> FlushFuture {
            let cache_dir = cache_dir.clone();
            let queue = queue.clone();
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
                wal_writer.update_wal_watermark("metrics", watermark).await?;
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

    pub async fn replay_wal_to_staged(
        &self,
        iceberg_writer: Arc<IcebergWriter>,
        cache_dir: Option<PathBuf>,
    ) -> Result<()> {
        self.replay_kind(
            iceberg_writer.clone(),
            cache_dir.clone(),
            "spans",
            self.span_queue.clone(),
        )
        .await?;
        self.replay_kind(
            iceberg_writer.clone(),
            cache_dir.clone(),
            "logs",
            self.log_queue.clone(),
        )
        .await?;
        self.replay_kind(iceberg_writer, cache_dir, "metrics", self.metric_queue.clone())
            .await?;
        Ok(())
    }

    async fn replay_kind(
        &self,
        iceberg_writer: Arc<IcebergWriter>,
        cache_dir: Option<PathBuf>,
        kind: &str,
        queue: Arc<FlushQueue>,
    ) -> Result<()> {
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
            "spans" => iceberg_writer.spans_schema().await?,
            "logs" => iceberg_writer.logs_schema().await?,
            "metrics" => iceberg_writer.metrics_schema().await?,
            _ => return Err(anyhow::anyhow!("unknown WAL kind: {}", kind)),
        };

        for (date, batches) in grouped {
            stage_record_batches(
                kind,
                date,
                batches,
                schema.as_ref(),
                cache_dir.clone(),
                queue.clone(),
            )
            .await?;
        }

        Ok(())
    }
}

impl IngestPipeline {
    pub async fn new(config: &Config) -> Result<Self> {
        let iceberg_writer = Arc::new(IcebergWriter::new(config).await?);
        let ingest_engine = Arc::new(IngestEngine::new(config).await?);
        let cache_dir = config.ingest_engine.cache_dir.as_ref().map(PathBuf::from);
        let replay_cache_dir = cache_dir.clone();
        let optimizer_cache_dir = cache_dir.clone();

        let span_buffer = create_span_buffer(
            config,
            Some(ingest_engine.span_pre_add_callback()),
            ingest_engine.span_flush_callback(iceberg_writer.clone(), cache_dir.clone()),
        )
        .await?;
        let log_buffer = create_log_buffer(
            config,
            Some(ingest_engine.log_pre_add_callback()),
            ingest_engine.log_flush_callback(iceberg_writer.clone(), cache_dir.clone()),
        )
        .await?;
        let metric_buffer = create_metric_buffer(
            config,
            Some(ingest_engine.metric_pre_add_callback()),
            ingest_engine.metric_flush_callback(iceberg_writer.clone(), cache_dir.clone()),
        )
        .await?;

        let storage = Storage::new(
            iceberg_writer.clone(),
            span_buffer,
            log_buffer,
            metric_buffer,
            cache_dir.clone(),
        );

        if config.ingest_engine.replay_wal_on_startup {
            ingest_engine
                .replay_wal_to_staged(iceberg_writer.clone(), replay_cache_dir)
                .await?;
        }

        start_optimizer_task(
            iceberg_writer.clone(),
            ingest_engine.span_queue(),
            ingest_engine.log_queue(),
            ingest_engine.metric_queue(),
            config.ingest_engine.optimizer_interval_seconds,
            optimizer_cache_dir,
        );

        Ok(Self {
            storage,
            ingest_engine,
            cache_dir,
        })
    }

    pub async fn add_spans(&self, items: Vec<Span>, request_size: usize) -> Result<()> {
        self.storage.span_buffer.add_items(items, request_size).await
    }

    pub async fn add_logs(&self, items: Vec<Log>, request_size: usize) -> Result<()> {
        self.storage.log_buffer.add_items(items, request_size).await
    }

    pub async fn add_metrics(&self, items: Vec<Metric>, request_size: usize) -> Result<()> {
        self.storage
            .metric_buffer
            .add_items(items, request_size)
            .await
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
        let schema = self.storage.iceberg_writer.spans_schema().await?;
        self.ingest_engine
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
        let schema = self.storage.iceberg_writer.logs_schema().await?;
        self.ingest_engine
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
        let schema = self.storage.iceberg_writer.metrics_schema().await?;
        self.ingest_engine
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
        self.storage.span_buffer.force_flush().await
    }

    pub async fn force_flush_logs(&self) -> Result<()> {
        self.storage.log_buffer.force_flush().await
    }

    pub async fn force_flush_metrics(&self) -> Result<()> {
        self.storage.metric_buffer.force_flush().await
    }

    pub async fn run_optimizer_once(&self) -> Result<()> {
        optimize_once(
            self.storage.iceberg_writer.clone(),
            self.ingest_engine.span_queue(),
            self.ingest_engine.log_queue(),
            self.ingest_engine.metric_queue(),
            self.cache_dir.as_deref(),
        )
        .await
    }

    pub fn list_wal_files(&self, kind: &str) -> Result<Vec<PathBuf>> {
        self.ingest_engine.list_wal_files(kind)
    }

    pub fn list_staged_files(&self, kind: &str) -> Result<Vec<PathBuf>> {
        self.storage.list_staged_files(kind)
    }
}

fn start_optimizer_task(
    iceberg_writer: Arc<IcebergWriter>,
    span_queue: Arc<FlushQueue>,
    log_queue: Arc<FlushQueue>,
    metric_queue: Arc<FlushQueue>,
    interval_seconds: u64,
    cache_dir: Option<PathBuf>,
) {
    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
        loop {
            ticker.tick().await;
            if let Err(err) = optimize_once(
                iceberg_writer.clone(),
                span_queue.clone(),
                log_queue.clone(),
                metric_queue.clone(),
                cache_dir.as_deref(),
            )
            .await
            {
                warn!("Optimizer run failed: {}", err);
            }
        }
    });
}

async fn optimize_once(
    iceberg_writer: Arc<IcebergWriter>,
    span_queue: Arc<FlushQueue>,
    log_queue: Arc<FlushQueue>,
    metric_queue: Arc<FlushQueue>,
    cache_dir: Option<&Path>,
) -> Result<()> {
    optimize_spans(iceberg_writer.clone(), span_queue, cache_dir).await?;
    optimize_logs(iceberg_writer.clone(), log_queue, cache_dir).await?;
    optimize_metrics(iceberg_writer, metric_queue, cache_dir).await?;
    Ok(())
}

async fn optimize_spans(
    iceberg_writer: Arc<IcebergWriter>,
    queue: Arc<FlushQueue>,
    cache_dir: Option<&Path>,
) -> Result<()> {
    let staged = queue.drain().await;
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

        if let Err(err) = iceberg_writer.write_span_record_batches(combined).await {
            warn!("Optimizer failed to commit spans for {}: {}", date, err);
            for item in items {
                queue.push(item).await;
            }
            return Err(err);
        }

        cleanup_cache("spans", cache_dir, items).await;
    }
    Ok(())
}

async fn optimize_logs(
    iceberg_writer: Arc<IcebergWriter>,
    queue: Arc<FlushQueue>,
    cache_dir: Option<&Path>,
) -> Result<()> {
    let staged = queue.drain().await;
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

        if let Err(err) = iceberg_writer.write_log_record_batches(combined).await {
            warn!("Optimizer failed to commit logs for {}: {}", date, err);
            for item in items {
                queue.push(item).await;
            }
            return Err(err);
        }

        cleanup_cache("logs", cache_dir, items).await;
    }
    Ok(())
}

async fn optimize_metrics(
    iceberg_writer: Arc<IcebergWriter>,
    queue: Arc<FlushQueue>,
    cache_dir: Option<&Path>,
) -> Result<()> {
    let staged = queue.drain().await;
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

        if let Err(err) = iceberg_writer.write_metric_record_batches(combined).await {
            warn!("Optimizer failed to commit metrics for {}: {}", date, err);
            for item in items {
                queue.push(item).await;
            }
            return Err(err);
        }

        cleanup_cache("metrics", cache_dir, items).await;
    }
    Ok(())
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

fn record_batch_partition_date(batch: &RecordBatch) -> Result<NaiveDate> {
    use arrow::array::AsArray;
    use arrow::datatypes::Date32Type;

    let num_columns = batch.num_columns();
    if num_columns == 0 {
        return Err(anyhow::anyhow!("record batch has no columns"));
    }
    // Find record_date column by name (not by position, since promoted columns may come after it)
    let record_date_idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "record_date")
        .ok_or_else(|| anyhow::anyhow!("record_date column not found in schema"))?;
    let record_date_col = batch.column(record_date_idx);
    let date_array = record_date_col.as_primitive::<Date32Type>();
    if date_array.is_empty() {
        return Err(anyhow::anyhow!("record batch has no rows"));
    }
    let days_since_epoch = date_array.value(0);
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    Ok(epoch + chrono::Duration::days(days_since_epoch as i64))
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

async fn cleanup_cache(kind: &str, cache_dir: Option<&Path>, items: Vec<StagedBatch>) {
    // Update watermark first so query workers refresh staged views before any file deletions.
    if let Some(cache_dir) = cache_dir {
        let marker_dir = cache_dir.join("staged_watermarks");
        let marker_path = marker_dir.join(format!("{kind}.txt"));
        let marker_tmp = marker_dir.join(format!("{kind}.txt.tmp"));
        if std::fs::create_dir_all(&marker_dir).is_ok() {
            if std::fs::write(&marker_tmp, chrono::Utc::now().to_rfc3339()).is_ok() {
                let _ = std::fs::rename(&marker_tmp, &marker_path);
            }
        }
    }

    for item in items {
        if let Some(path) = item.cache_path {
            if let Err(err) = tokio::fs::remove_file(&path).await {
                warn!("Failed to remove staged cache file {:?}: {}", path, err);
            }
        }
    }
}
