use crate::config::SpanBufferConfig;
use anyhow::Result;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Simplified span data for buffering
#[derive(Debug, Clone)]
pub struct SpanData {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub app_id: String,
    pub organization_id: Option<String>,
    pub tenant_id: Option<String>,
    pub message_type: String,
    pub span_kind: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub end_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub attributes: HashMap<String, String>,
    pub events: Vec<SpanEvent>,
    pub status_code: Option<String>,
    pub status_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub attributes: HashMap<String, String>,
}

type FlushFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type FlushCallback = dyn Fn(Vec<Vec<SpanData>>) -> FlushFuture + Send + Sync;

/// Simple global span buffer with size and time-based flushing
pub struct SimpleSpanBuffer {
    config: SpanBufferConfig,
    spans: Arc<Mutex<Vec<SpanData>>>,
    current_size_bytes: Arc<AtomicUsize>,
    last_flush: Arc<Mutex<Instant>>,
    flush_callback: Arc<FlushCallback>,
}

impl SimpleSpanBuffer {
    pub fn new(
        config: SpanBufferConfig,
        flush_callback: Arc<FlushCallback>,
    ) -> Self {
        let buffer = Self {
            config: config.clone(),
            spans: Arc::new(Mutex::new(Vec::new())),
            current_size_bytes: Arc::new(AtomicUsize::new(0)),
            last_flush: Arc::new(Mutex::new(Instant::now())),
            flush_callback,
        };

        // Start background task for time-based flushing
        let buffer_clone = buffer.clone_for_background();
        tokio::spawn(async move {
            let check_interval = if cfg!(test) {
                Duration::from_millis(100) // Fast checks during testing
            } else {
                Duration::from_secs(std::cmp::max(1, buffer_clone.config.flush_interval_seconds / 2))
            };
            let mut interval_timer = interval(check_interval);

            loop {
                interval_timer.tick().await;
                if let Err(e) = buffer_clone.check_and_flush_by_time().await {
                    warn!("Failed to flush buffer by time: {}", e);
                }
            }
        });

        buffer
    }

    fn clone_for_background(&self) -> Self {
        Self {
            config: self.config.clone(),
            spans: self.spans.clone(),
            current_size_bytes: self.current_size_bytes.clone(),
            last_flush: self.last_flush.clone(),
            flush_callback: self.flush_callback.clone(),
        }
    }

    /// Add spans from a request, tracking the request body size
    pub async fn add_spans(&self, mut spans: Vec<SpanData>, request_body_size: usize) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        let span_count = spans.len();
        debug!("Adding {} spans with request body size {} bytes", span_count, request_body_size);

        // Determine if we should flush BEFORE releasing the lock to avoid TOCTOU race
        let should_flush = {
            let mut buffer = self.spans.lock().await;
            buffer.append(&mut spans);

            // Track approximate size using request body size as proxy
            self.current_size_bytes.fetch_add(request_body_size, Ordering::Relaxed);

            // Check limits while holding the lock - this is critical for correctness
            let current_size = self.current_size_bytes.load(Ordering::Relaxed);
            let current_count = buffer.len();

            current_count >= self.config.max_buffer_spans || current_size >= self.config.max_buffer_bytes
        }; // Lock is released here

        // Flush if needed (outside the lock to avoid holding it during I/O)
        if should_flush {
            let current_count = {
                let buffer = self.spans.lock().await;
                buffer.len()
            };
            let current_size = self.current_size_bytes.load(Ordering::Relaxed);

            info!("Buffer reached limit ({} spans, {} bytes), flushing", current_count, current_size);
            self.flush().await?;
        }

        Ok(())
    }

    /// Check if we should flush based on time interval
    async fn check_and_flush_by_time(&self) -> Result<()> {
        let last_flush = *self.last_flush.lock().await;
        let elapsed = last_flush.elapsed();

        if elapsed >= Duration::from_secs(self.config.flush_interval_seconds) {
            let span_count = {
                let buffer = self.spans.lock().await;
                buffer.len()
            };

            if span_count > 0 {
                info!("Buffer reached time limit ({} seconds), flushing {} spans",
                      self.config.flush_interval_seconds, span_count);
                self.flush().await?;
            }
        }

        Ok(())
    }

    /// Flush all buffered spans to Iceberg
    async fn flush(&self) -> Result<()> {
        let mut buffer = self.spans.lock().await;

        if buffer.is_empty() {
            return Ok(());
        }

        let span_count = buffer.len();
        let size_bytes = self.current_size_bytes.load(Ordering::Relaxed);

        info!("Flushing {} spans ({} bytes) to Iceberg", span_count, size_bytes);

        // Sort spans for data locality: date -> session_id -> trace_id -> timestamp
        // Partitioning by date is critical: all spans in a file must have the same partition
        buffer.sort_by(|a, b| {
            let date_a = a.timestamp.date_naive();
            let date_b = b.timestamp.date_naive();
            
            date_a.cmp(&date_b)
                .then_with(|| {
                    // Extract session_id from attributes, fallback to trace_id
                    let session_a = a.attributes.get("sp.session.id").unwrap_or(&a.trace_id);
                    let session_b = b.attributes.get("sp.session.id").unwrap_or(&b.trace_id);
                    session_a.cmp(session_b)
                })
                .then_with(|| a.trace_id.cmp(&b.trace_id))
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        // Take all spans from buffer
        let batch = std::mem::take(&mut *buffer);

        // Reset counters
        self.current_size_bytes.store(0, Ordering::Relaxed);
        *self.last_flush.lock().await = Instant::now();

        // Release lock before I/O
        drop(buffer);

        // Group spans by date first (partition key)
        // Each date group will be written to a separate Parquet file
        let date_groups = self.group_by_date(&batch);
        
        info!("Flushing spans grouped by date: {} date partition(s)", date_groups.len());

        // Write each date group as a separate file (each file has one partition value)
        for (date, date_spans) in date_groups {
            info!("Writing {} spans for date {} to Iceberg", date_spans.len(), date);
            
            // Group spans by session_id for row group isolation (one row group per session)
            // This implements the design spec: "row group per session" for optimal query performance
            let session_groups = self.group_by_session(&date_spans);

            info!("Writing {} sessions as separate row groups in single Parquet file for date {}", session_groups.len(), date);

            // Collect session batches (drop session_id, keep only spans)
            let session_batches: Vec<Vec<SpanData>> = session_groups
                .into_iter()
                .map(|(session_id, spans)| {
                    debug!("Preparing row group for session {} ({} spans)", session_id, spans.len());
                    spans
                })
                .collect();

            // Write all sessions for this date to a single Parquet file with multiple row groups
            (self.flush_callback)(session_batches).await?;
        }

        info!("Successfully flushed {} spans to Iceberg", span_count);
        Ok(())
    }

    /// Group spans by date (partition key)
    /// All spans in a group will be written to the same file with the same partition value
    fn group_by_date(&self, spans: &[SpanData]) -> Vec<(chrono::NaiveDate, Vec<SpanData>)> {
        let mut groups = Vec::new();
        let mut current_date: Option<chrono::NaiveDate> = None;
        let mut current_group: Vec<SpanData> = Vec::new();

        for span in spans {
            let span_date = span.timestamp.date_naive();

            match &current_date {
                None => {
                    // First span
                    current_date = Some(span_date);
                    current_group.push(span.clone());
                }
                Some(date) if date == &span_date => {
                    // Same date, add to current group
                    current_group.push(span.clone());
                }
                Some(date) => {
                    // New date, save current group and start new one
                    groups.push((*date, std::mem::take(&mut current_group)));
                    current_date = Some(span_date);
                    current_group.push(span.clone());
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

    /// Group sorted spans by session_id
    /// Assumes spans are already sorted by session_id (done in flush())
    fn group_by_session(&self, spans: &[SpanData]) -> Vec<(String, Vec<SpanData>)> {
        let mut groups = Vec::new();
        let mut current_session_id: Option<String> = None;
        let mut current_group: Vec<SpanData> = Vec::new();

        for span in spans {
            let session_id = span.attributes
                .get("sp.session.id")
                .cloned()
                .unwrap_or_else(|| span.trace_id.clone());

            match &current_session_id {
                None => {
                    // First span
                    current_session_id = Some(session_id.clone());
                    current_group.push(span.clone());
                }
                Some(sid) if sid == &session_id => {
                    // Same session, add to current group
                    current_group.push(span.clone());
                }
                Some(sid) => {
                    // New session, save current group and start new one
                    groups.push((sid.clone(), std::mem::take(&mut current_group)));
                    current_session_id = Some(session_id.clone());
                    current_group.push(span.clone());
                }
            }
        }

        // Don't forget the last group
        if let Some(sid) = current_session_id {
            if !current_group.is_empty() {
                groups.push((sid, current_group));
            }
        }

        groups
    }

    /// Flush all buffered spans (for shutdown)
    pub async fn flush_all(&self) -> Result<()> {
        let span_count = {
            let buffer = self.spans.lock().await;
            buffer.len()
        };

        if span_count > 0 {
            info!("Flushing all {} buffered spans on shutdown", span_count);
            self.flush().await?;
        }

        Ok(())
    }

    /// Get current buffer statistics
    pub async fn stats(&self) -> BufferStats {
        let buffer = self.spans.lock().await;
        BufferStats {
            buffered_spans: buffer.len(),
            buffered_bytes: self.current_size_bytes.load(Ordering::Relaxed),
        }
    }

    /// Manually trigger flush (useful for testing)
    pub async fn force_flush(&self) -> Result<()> {
        self.flush().await
    }
}

#[derive(Debug)]
pub struct BufferStats {
    pub buffered_spans: usize,
    pub buffered_bytes: usize,
}

#[cfg(test)]
#[path = "span_buffer_test.rs"]
mod span_buffer_test;

#[cfg(test)]
#[path = "buffer_e2e_test.rs"]
mod buffer_e2e_test;
