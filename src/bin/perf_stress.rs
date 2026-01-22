use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use softprobe_otlp_backend::config::Config;
use softprobe_otlp_backend::models::{Log, Metric, Span};
use softprobe_otlp_backend::query;
use softprobe_otlp_backend::storage::IngestPipeline;
use tracing::{info, warn};
use tracing_subscriber;

#[derive(Parser)]
#[command(
    author = "SoftProbe Team",
    about = "Stress test the OTLP ingestion+query pipeline against Iceberg storage"
)]
struct Args {
    /// Path to the YAML configuration file. Uses CONFIG_FILE env fallback if omitted.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Duration of the stress run in seconds.
    #[arg(long, default_value_t = 60)]
    duration: u64,

    /// Spans per second to ingest via WAL.
    #[arg(long, default_value_t = 100)]
    span_qps: u32,

    /// Logs per second to ingest via WAL.
    #[arg(long, default_value_t = 200)]
    log_qps: u32,

    /// Metrics per second to ingest via WAL.
    #[arg(long, default_value_t = 200)]
    metric_qps: u32,

    /// Number of concurrent SQL workers running against DuckDB.
    #[arg(long, default_value_t = 4)]
    query_concurrency: usize,

    /// Milliseconds between each query execution per worker.
    #[arg(long, default_value_t = 1000)]
    query_interval_ms: u64,

    /// Seconds to wait for WAL watermarks/warm-up before recording steady-state query stats.
    #[arg(long, default_value_t = 10)]
    warmup_secs: u64,
}

#[derive(Default)]
struct ProducerStats {
    count: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
}

impl ProducerStats {
    fn inc_success(&self, delta: u64) {
        self.count
            .fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
    }

    fn inc_error(&self) {
        self.errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

struct QueryStats {
    durations: Mutex<Vec<Duration>>,
    executed: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    steady_executed: std::sync::atomic::AtomicU64,
    steady_errors: std::sync::atomic::AtomicU64,
    warmup_end: Instant,
}

impl QueryStats {
    fn new(warmup_start: Instant, warmup_duration: Duration) -> Self {
        Self {
            durations: Mutex::new(Vec::new()),
            executed: std::sync::atomic::AtomicU64::new(0),
            errors: std::sync::atomic::AtomicU64::new(0),
            steady_executed: std::sync::atomic::AtomicU64::new(0),
            steady_errors: std::sync::atomic::AtomicU64::new(0),
            warmup_end: warmup_start + warmup_duration,
        }
    }

    async fn record(&self, duration: Duration) {
        let now = Instant::now();
        self.executed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if now >= self.warmup_end {
            let mut guard = self.durations.lock().await;
            guard.push(duration);
            self.steady_executed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn record_error(&self) {
        let now = Instant::now();
        self.errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if now >= self.warmup_end {
            self.steady_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(config_path) = args.config.as_ref() {
        std::env::set_var("CONFIG_FILE", config_path);
    }

    tracing_subscriber::fmt::init();

    println!(
        "Loading config (CONFIG_FILE={:?})",
        std::env::var("CONFIG_FILE").ok()
    );
    let config = Config::load()?;
    println!(
        "Config loaded. Cache dir: {:?}",
        config.ingest_engine.cache_dir
    );

    let pipeline = IngestPipeline::new(&config).await?;
    let pipeline = Arc::new(pipeline);
    let query_engine = Arc::new(query::create_query_engine(&config).await?);
    let cache_dir_path = config.ingest_engine.cache_dir.as_ref().map(PathBuf::from);

    let deadline = Instant::now() + Duration::from_secs(args.duration);
    let span_stats = Arc::new(ProducerStats::default());
    let log_stats = Arc::new(ProducerStats::default());
    let metric_stats = Arc::new(ProducerStats::default());
    let warmup_secs = std::cmp::min(args.warmup_secs, args.duration);
    let warmup_duration = Duration::from_secs(warmup_secs);
    let warmup_start = Instant::now();
    let query_stats = Arc::new(QueryStats::new(warmup_start, warmup_duration));

    let mut tasks = Vec::new();

    if args.span_qps > 0 {
        tasks.push(tokio::spawn(run_span_writer(
            Arc::clone(&pipeline),
            args.span_qps,
            deadline,
            Arc::clone(&span_stats),
        )));
    }
    if args.log_qps > 0 {
        tasks.push(tokio::spawn(run_log_writer(
            Arc::clone(&pipeline),
            args.log_qps,
            deadline,
            Arc::clone(&log_stats),
        )));
    }
    if args.metric_qps > 0 {
        tasks.push(tokio::spawn(run_metric_writer(
            Arc::clone(&pipeline),
            args.metric_qps,
            deadline,
            Arc::clone(&metric_stats),
        )));
    }

    if let Some(cache_dir) = &cache_dir_path {
        wait_for_wal_watermarks(cache_dir, warmup_duration).await;
        wait_for_ready_parquet(cache_dir, warmup_duration).await;
        info!("Warm-up guard completed after {warmup_secs} seconds");
    } else {
        warn!("No ingest cache_dir configured; query workers may hit cold WAL files");
    }

    for idx in 0..args.query_concurrency {
        tasks.push(tokio::spawn(run_query_worker(
            Arc::clone(&query_engine),
            args.query_interval_ms,
            deadline,
            Arc::clone(&query_stats),
            idx,
        )));
    }

    for task in tasks {
        let _ = task.await?;
    }

    print_report(args, span_stats, log_stats, metric_stats, query_stats).await;
    Ok(())
}

async fn run_span_writer(
    pipeline: Arc<IngestPipeline>,
    qps: u32,
    deadline: Instant,
    stats: Arc<ProducerStats>,
) -> Result<()> {
    let interval =
        std::time::Duration::from_micros((1_000_000.0 / qps.max(1) as f64).max(1.0) as u64);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut counter = 0u64;
    while Instant::now() < deadline {
        ticker.tick().await;
        let span = sample_span(counter);
        match pipeline.add_spans(vec![span], 256).await {
            Ok(_) => stats.inc_success(1),
            Err(err) => {
                tracing::warn!("span write error: {}", err);
                stats.inc_error();
            }
        }
        counter += 1;
    }
    Ok(())
}

async fn run_log_writer(
    pipeline: Arc<IngestPipeline>,
    qps: u32,
    deadline: Instant,
    stats: Arc<ProducerStats>,
) -> Result<()> {
    let interval =
        std::time::Duration::from_micros((1_000_000.0 / qps.max(1) as f64).max(1.0) as u64);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut counter = 0u64;
    while Instant::now() < deadline {
        ticker.tick().await;
        let log = sample_log(counter);
        match pipeline.add_logs(vec![log], 256).await {
            Ok(_) => stats.inc_success(1),
            Err(err) => {
                tracing::warn!("log write error: {}", err);
                stats.inc_error();
            }
        }
        counter += 1;
    }
    Ok(())
}

async fn run_metric_writer(
    pipeline: Arc<IngestPipeline>,
    qps: u32,
    deadline: Instant,
    stats: Arc<ProducerStats>,
) -> Result<()> {
    let interval =
        std::time::Duration::from_micros((1_000_000.0 / qps.max(1) as f64).max(1.0) as u64);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut counter = 0u64;
    while Instant::now() < deadline {
        ticker.tick().await;
        let metric = sample_metric(counter);
        match pipeline.add_metrics(vec![metric], 256).await {
            Ok(_) => stats.inc_success(1),
            Err(err) => {
                tracing::warn!("metric write error: {}", err);
                stats.inc_error();
            }
        }
        counter += 1;
    }
    Ok(())
}

async fn run_query_worker(
    engine: Arc<query::QueryEngine>,
    interval_ms: u64,
    deadline: Instant,
    stats: Arc<QueryStats>,
    worker_id: usize,
) -> Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms.max(100)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let query_tables = ["union_logs", "union_spans", "union_metrics"];
    let mut idx = 0;
    while Instant::now() < deadline {
        ticker.tick().await;
        let table = query_tables[idx % query_tables.len()];
        let sql = format!(
            "SELECT COUNT(*) AS count FROM {table} \
             WHERE record_date >= DATE '{}'",
            (Utc::now() - chrono::Duration::days(1)).format("%Y-%m-%d")
        );
        let start = Instant::now();
        match engine.execute_query(&sql).await {
            Ok(_) => {
                let elapsed = start.elapsed();
                stats.record(elapsed).await;
            }
            Err(err) => {
                tracing::warn!("query worker {worker_id} error: {}", err);
                stats.record_error();
            }
        }
        idx += 1;
    }
    Ok(())
}

fn sample_span(counter: u64) -> Span {
    let session_id = format!("stress-span-{}", counter % 1024);
    let timestamp = Utc::now();
    let mut attributes = HashMap::new();
    attributes.insert(
        "sp.session.id".to_string(),
        format!("stress-span-{}", counter % 64),
    );

    Span {
        session_id,
        trace_id: uuid::Uuid::new_v4().to_string(),
        span_id: uuid::Uuid::new_v4().to_string(),
        parent_span_id: None,
        app_id: "stress-app".to_string(),
        organization_id: Some("stress-org".to_string()),
        tenant_id: Some("stress-tenant".to_string()),
        message_type: "stress-span".to_string(),
        span_kind: Some("SERVER".to_string()),
        timestamp,
        end_timestamp: Some(timestamp + chrono::Duration::milliseconds(5)),
        attributes,
        events: Vec::new(),
        status_code: Some("OK".to_string()),
        status_message: Some("Stress".to_string()),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
    }
}

fn sample_log(counter: u64) -> Log {
    let timestamp = Utc::now();
    let mut attributes = HashMap::new();
    attributes.insert("log.index".to_string(), counter.to_string());

    let mut resource_attributes = HashMap::new();
    resource_attributes.insert("service.name".to_string(), "stress-service".to_string());
    resource_attributes.insert("host.name".to_string(), "stress-worker".to_string());

    Log {
        session_id: Some(format!("stress-log-{}", counter % 128)),
        timestamp,
        observed_timestamp: Some(timestamp + chrono::Duration::milliseconds(1)),
        severity_number: 12,
        severity_text: "INFO".to_string(),
        body: format!("stress log {}", counter),
        attributes,
        resource_attributes,
        trace_id: Some(uuid::Uuid::new_v4().to_string()),
        span_id: Some(uuid::Uuid::new_v4().to_string()),
    }
}

fn sample_metric(counter: u64) -> Metric {
    let now = Utc::now();
    let mut attributes = HashMap::new();
    attributes.insert("stress.key".to_string(), format!("value-{}", counter % 8));

    let mut resource_attributes = HashMap::new();
    resource_attributes.insert("service.name".to_string(), "stress-service".to_string());

    Metric {
        metric_name: "stress.metric.latency".to_string(),
        description: "Stress latency".to_string(),
        unit: "ms".to_string(),
        metric_type: "gauge".to_string(),
        timestamp: now,
        value: 100.0 + (counter % 50) as f64,
        attributes,
        resource_attributes,
    }
}

async fn print_report(
    args: Args,
    span_stats: Arc<ProducerStats>,
    log_stats: Arc<ProducerStats>,
    metric_stats: Arc<ProducerStats>,
    query_stats: Arc<QueryStats>,
) {
    println!("\n========== Stress Test Report ==========");
    println!("Duration: {} seconds", args.duration);
    println!("Span QPS: {}/s", args.span_qps);
    println!("Log QPS: {}/s", args.log_qps);
    println!("Metric QPS: {}/s", args.metric_qps);
    println!("Query workers: {}", args.query_concurrency);

    print_producer_summary("span", span_stats);
    print_producer_summary("log", log_stats);
    print_producer_summary("metric", metric_stats);

    let total_queries = query_stats
        .executed
        .load(std::sync::atomic::Ordering::Relaxed);
    let query_errors = query_stats
        .errors
        .load(std::sync::atomic::Ordering::Relaxed);
    let durations = query_stats.durations.lock().await;
    let avg = durations
        .iter()
        .map(|d| d.as_millis())
        .sum::<u128>()
        .checked_div(durations.len().max(1) as u128)
        .unwrap_or(0);
    let p95 = percentile(&durations, 95);
    drop(durations);

    let steady_queries = query_stats
        .steady_executed
        .load(std::sync::atomic::Ordering::Relaxed);
    let steady_errors = query_stats
        .steady_errors
        .load(std::sync::atomic::Ordering::Relaxed);

    println!("Warm-up period: {} seconds", args.warmup_secs);
    println!("Total queries executed: {}", total_queries);
    println!("Total query errors: {}", query_errors);
    println!(
        "Steady-state queries recorded (post-warmup): {}",
        steady_queries
    );
    println!("Steady-state query errors: {}", steady_errors);
    println!("Steady-state avg latency: {} ms", avg);
    println!("Steady-state p95 latency: {} ms", p95);
    println!("=========================================");
}

async fn wait_for_wal_watermarks(cache_dir: &Path, timeout: Duration) {
    let start = Instant::now();
    let wal_dir = cache_dir.join("wal_watermarks").join("wal");
    let required = ["logs", "spans", "metrics"];
    while start.elapsed() < timeout {
        if required
            .iter()
            .all(|kind| wal_dir.join(format!("{kind}.txt")).exists())
        {
            info!(
                "Detected WAL watermarks at {:?} after {:?}",
                wal_dir,
                start.elapsed()
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    warn!(
        "Timed out waiting for WAL watermarks at {:?}; queries may hit cold files",
        wal_dir
    );
}

async fn wait_for_ready_parquet(cache_dir: &Path, timeout: Duration) {
    let start = Instant::now();
    let mut ready = false;
    while start.elapsed() < timeout {
        ready = ["spans", "logs", "metrics"]
            .iter()
            .all(|kind| has_stable_parquet(cache_dir, kind));
        if ready {
            info!(
                "Found ready parquet files for all kinds after {:?}",
                start.elapsed()
            );
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !ready {
        warn!(
            "Timed out waiting for stable parquet files at {:?}",
            cache_dir.join("spans")
        );
    }
}

fn has_stable_parquet(cache_dir: &Path, kind: &str) -> bool {
    let kind_dir = cache_dir.join(kind);
    if !kind_dir.exists() {
        return false;
    }
    if let Ok(entries) = std::fs::read_dir(&kind_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Ok(metadata) = path.metadata() {
                    if metadata.len() >= 64 * 1024 {
                        if let Ok(modified) = metadata.modified() {
                            if modified.elapsed().unwrap_or_default() >= Duration::from_secs(1) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

fn print_producer_summary(label: &str, stats: Arc<ProducerStats>) {
    let produced = stats.count.load(std::sync::atomic::Ordering::Relaxed);
    let errors = stats.errors.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "{} records produced: {} (errors: {})",
        label, produced, errors
    );
}

fn percentile(durations: &[Duration], percentile: usize) -> u128 {
    if durations.is_empty() {
        return 0;
    }
    let mut sorted = durations.to_owned();
    sorted.sort();
    let target = ((sorted.len() * percentile) + 99) / 100;
    let idx = target.min(sorted.len()).saturating_sub(1);
    sorted[idx].as_millis()
}
