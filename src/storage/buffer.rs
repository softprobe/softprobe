use crate::config::SpanBufferConfig;
use anyhow::Result;
use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Trait for data that can be buffered and flushed to storage
pub trait Bufferable: Clone + Send + Sync + 'static {
    /// Get the partition key for partitioning (e.g., date)
    fn partition_key(&self) -> chrono::NaiveDate;

    /// Get the grouping key for row group organization
    /// - For traces/logs: session_id
    /// - For metrics: metric_name
    fn grouping_key(&self) -> String;

    /// Compare for sorting within a partition
    /// Order: grouping_key -> timestamp -> other fields
    fn compare_for_sort(&self, other: &Self) -> Ordering;

    /// Get the timestamp for time-based sorting
    fn timestamp(&self) -> chrono::DateTime<chrono::Utc>;
}

pub type FlushFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
pub type FlushCallback<T> = dyn Fn(Vec<Vec<T>>) -> FlushFuture + Send + Sync;
pub type PreAddFuture<T> = Pin<Box<dyn Future<Output = Result<Vec<T>>> + Send>>;
pub type PreAddCallback<T> = dyn Fn(Vec<T>, usize) -> PreAddFuture<T> + Send + Sync;

/// Generic buffer with size and time-based flushing
#[derive(Clone)]
pub struct SimpleBuffer<T: Bufferable> {
    name: String,             // For logging (e.g., "spans", "logs", "metrics")
    config: SpanBufferConfig, // TODO: Rename to BufferConfig
    items: Arc<Mutex<Vec<T>>>,
    current_size_bytes: Arc<AtomicUsize>,
    last_flush: Arc<Mutex<Instant>>,
    pre_add_callback: Option<Arc<PreAddCallback<T>>>,
    flush_callback: Arc<FlushCallback<T>>,
}

impl<T: Bufferable> SimpleBuffer<T> {
    pub fn new(
        name: String,
        config: SpanBufferConfig,
        pre_add_callback: Option<Arc<PreAddCallback<T>>>,
        flush_callback: Arc<FlushCallback<T>>,
    ) -> Self {
        let buffer = Self {
            name: name.clone(),
            config: config.clone(),
            items: Arc::new(Mutex::new(Vec::new())),
            current_size_bytes: Arc::new(AtomicUsize::new(0)),
            last_flush: Arc::new(Mutex::new(Instant::now())),
            pre_add_callback,
            flush_callback,
        };

        // Start background task for time-based flushing
        let buffer_clone = buffer.clone_for_background();
        tokio::spawn(async move {
            let check_interval = if cfg!(test) {
                Duration::from_millis(100) // Fast checks during testing
            } else {
                Duration::from_secs(std::cmp::max(
                    1,
                    buffer_clone.config.flush_interval_seconds / 2,
                ))
            };
            let mut interval_timer = interval(check_interval);

            loop {
                interval_timer.tick().await;
                if let Err(e) = buffer_clone.check_and_flush_by_time().await {
                    warn!(
                        "[{}] Failed to flush buffer by time: {}",
                        buffer_clone.name, e
                    );
                }
            }
        });

        buffer
    }

    fn clone_for_background(&self) -> Self {
        Self {
            name: self.name.clone(),
            config: self.config.clone(),
            items: self.items.clone(),
            current_size_bytes: self.current_size_bytes.clone(),
            last_flush: self.last_flush.clone(),
            pre_add_callback: self.pre_add_callback.clone(),
            flush_callback: self.flush_callback.clone(),
        }
    }

    /// Add items from a request, tracking the request body size
    pub async fn add_items(&self, items: Vec<T>, request_body_size: usize) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let mut items = if let Some(pre_add_callback) = &self.pre_add_callback {
            (pre_add_callback)(items, request_body_size).await?
        } else {
            items
        };

        if items.is_empty() {
            return Ok(());
        }

        let item_count = items.len();
        debug!(
            "[{}] Adding {} items with request body size {} bytes",
            self.name, item_count, request_body_size
        );

        // Determine if we should flush BEFORE releasing the lock to avoid TOCTOU race
        let should_flush = {
            let mut buffer = self.items.lock().await;
            buffer.append(&mut items);

            // Track approximate size using request body size as proxy
            self.current_size_bytes
                .fetch_add(request_body_size, AtomicOrdering::Relaxed);

            // Check limits while holding the lock - this is critical for correctness
            let current_size = self.current_size_bytes.load(AtomicOrdering::Relaxed);
            let current_count = buffer.len();

            current_count >= self.config.max_buffer_spans
                || current_size >= self.config.max_buffer_bytes
        }; // Lock is released here

        // Flush if needed (outside the lock to avoid holding it during I/O)
        if should_flush {
            let current_count = {
                let buffer = self.items.lock().await;
                buffer.len()
            };
            let current_size = self.current_size_bytes.load(AtomicOrdering::Relaxed);

            info!(
                "[{}] Buffer reached limit ({} items, {} bytes), flushing",
                self.name, current_count, current_size
            );
            self.flush().await?;
        }

        Ok(())
    }

    /// Check if we should flush based on time interval
    async fn check_and_flush_by_time(&self) -> Result<()> {
        let last_flush = *self.last_flush.lock().await;
        let elapsed = last_flush.elapsed();

        if elapsed >= Duration::from_secs(self.config.flush_interval_seconds) {
            let item_count = {
                let buffer = self.items.lock().await;
                buffer.len()
            };

            if item_count > 0 {
                info!(
                    "[{}] Buffer reached time limit ({} seconds), flushing {} items",
                    self.name, self.config.flush_interval_seconds, item_count
                );
                self.flush().await?;
            }
        }

        Ok(())
    }

    /// Flush all buffered items to storage
    async fn flush(&self) -> Result<()> {
        let mut buffer = self.items.lock().await;

        if buffer.is_empty() {
            return Ok(());
        }

        let item_count = buffer.len();
        let size_bytes = self.current_size_bytes.load(AtomicOrdering::Relaxed);

        info!(
            "[{}] Flushing {} items ({} bytes) to storage",
            self.name, item_count, size_bytes
        );

        // Sort items for data locality: date -> grouping_key -> timestamp
        // Partitioning by date is critical: all items in a file must have the same partition
        buffer.sort_by(|a, b| {
            let date_a = a.partition_key();
            let date_b = b.partition_key();

            date_a
                .cmp(&date_b)
                .then_with(|| a.grouping_key().cmp(&b.grouping_key()))
                .then_with(|| a.compare_for_sort(b))
        });

        // Take all items from buffer
        let batch = std::mem::take(&mut *buffer);

        // Reset counters
        self.current_size_bytes.store(0, AtomicOrdering::Relaxed);
        *self.last_flush.lock().await = Instant::now();

        // Release lock before I/O
        drop(buffer);

        // Group items by date first (partition key)
        // Each date group will be written to a separate Parquet file
        let date_groups = self.group_by_partition(&batch);

        info!(
            "[{}] Flushing items grouped by date: {} date partition(s)",
            self.name,
            date_groups.len()
        );

        // Write each date group as a separate file (each file has one partition value)
        for (date, date_items) in date_groups {
            info!(
                "[{}] Writing {} items for date {} to storage",
                self.name,
                date_items.len(),
                date
            );

            // Group items by grouping_key for row group isolation
            // For traces/logs: one row group per session_id
            // For metrics: one row group per metric_name
            let grouped = self.group_by_key(&date_items);

            info!(
                "[{}] Writing {} groups as separate row groups in single Parquet file for date {}",
                self.name,
                grouped.len(),
                date
            );

            // Collect batches (drop grouping_key, keep only items)
            let batches: Vec<Vec<T>> = grouped
                .into_iter()
                .map(|(key, items)| {
                    debug!(
                        "[{}] Preparing row group for {} ({} items)",
                        self.name,
                        key,
                        items.len()
                    );
                    items
                })
                .collect();

            // Write all groups for this date to a single Parquet file with multiple row groups
            (self.flush_callback)(batches).await?;
        }

        info!(
            "[{}] Successfully flushed {} items to storage",
            self.name, item_count
        );
        Ok(())
    }

    /// Group items by partition key (date)
    /// All items in a group will be written to the same file with the same partition value
    fn group_by_partition(&self, items: &[T]) -> Vec<(chrono::NaiveDate, Vec<T>)> {
        let mut groups = Vec::new();
        let mut current_date: Option<chrono::NaiveDate> = None;
        let mut current_group: Vec<T> = Vec::new();

        for item in items {
            let item_date = item.partition_key();

            match &current_date {
                None => {
                    // First item
                    current_date = Some(item_date);
                    current_group.push(item.clone());
                }
                Some(date) if date == &item_date => {
                    // Same date, add to current group
                    current_group.push(item.clone());
                }
                Some(date) => {
                    // New date, save current group and start new one
                    groups.push((*date, std::mem::take(&mut current_group)));
                    current_date = Some(item_date);
                    current_group.push(item.clone());
                }
            }
        }

        // Don't forget the last group
        if let Some(date) = current_date {
            if !current_group.is_empty() {
                groups.push((date, current_group));
            }
        }

        groups
    }

    /// Group sorted items by grouping_key
    /// Assumes items are already sorted by grouping_key (done in flush())
    fn group_by_key(&self, items: &[T]) -> Vec<(String, Vec<T>)> {
        let mut groups = Vec::new();
        let mut current_key: Option<String> = None;
        let mut current_group: Vec<T> = Vec::new();

        for item in items {
            let key = item.grouping_key();

            match &current_key {
                None => {
                    // First item
                    current_key = Some(key.clone());
                    current_group.push(item.clone());
                }
                Some(k) if k == &key => {
                    // Same key, add to current group
                    current_group.push(item.clone());
                }
                Some(k) => {
                    // New key, save current group and start new one
                    groups.push((k.clone(), std::mem::take(&mut current_group)));
                    current_key = Some(key.clone());
                    current_group.push(item.clone());
                }
            }
        }

        // Don't forget the last group
        if let Some(k) = current_key {
            if !current_group.is_empty() {
                groups.push((k, current_group));
            }
        }

        groups
    }

    /// Flush all buffered items (for shutdown)
    pub async fn flush_all(&self) -> Result<()> {
        let item_count = {
            let buffer = self.items.lock().await;
            buffer.len()
        };

        if item_count > 0 {
            info!(
                "[{}] Flushing all {} buffered items on shutdown",
                self.name, item_count
            );
            self.flush().await?;
        }

        Ok(())
    }

    /// Get current buffer statistics
    pub async fn stats(&self) -> BufferStats {
        let buffer = self.items.lock().await;
        BufferStats {
            buffered_items: buffer.len(),
            buffered_bytes: self.current_size_bytes.load(AtomicOrdering::Relaxed),
        }
    }

    /// Snapshot buffered items for union-read queries
    pub async fn snapshot_items(&self) -> Vec<T> {
        let buffer = self.items.lock().await;
        buffer.clone()
    }

    /// Get a snapshot of current buffer items (sync, non-blocking)
    /// Returns None if buffer is currently locked (rare, during flush)
    pub fn snapshot_items_sync(&self) -> Option<Vec<T>> {
        match self.items.try_lock() {
            Ok(guard) => Some(guard.clone()),
            Err(_) => None, // Buffer is being modified, skip for this query
        }
    }

    /// Get the last flush watermark for snapshot consistency
    pub async fn flush_watermark(&self) -> Instant {
        *self.last_flush.lock().await
    }

    /// Manually trigger flush (useful for testing)
    pub async fn force_flush(&self) -> Result<()> {
        self.flush().await
    }
}

#[derive(Debug)]
pub struct BufferStats {
    pub buffered_items: usize,
    pub buffered_bytes: usize,
}
