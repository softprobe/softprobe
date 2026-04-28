use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest,
    metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span as OtlpSpan, Status};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{Metric as OtlpMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics};

use splake::models::{Log, Metric, Span};
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

    /// Service URL for API (e.g., http://localhost:8090). Required.
    #[arg(long)]
    service_url: String,
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
    by_kind: Mutex<HashMap<String, PerQueryStats>>,
    executed: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    steady_executed: std::sync::atomic::AtomicU64,
    steady_errors: std::sync::atomic::AtomicU64,
    warmup_end: Instant,
}

#[derive(Default, Clone)]
struct PerQueryStats {
    durations: Vec<Duration>,
    executed: u64,
    errors: u64,
}

impl QueryStats {
    fn new(warmup_start: Instant, warmup_duration: Duration) -> Self {
        Self {
            durations: Mutex::new(Vec::new()),
            by_kind: Mutex::new(HashMap::new()),
            executed: std::sync::atomic::AtomicU64::new(0),
            errors: std::sync::atomic::AtomicU64::new(0),
            steady_executed: std::sync::atomic::AtomicU64::new(0),
            steady_errors: std::sync::atomic::AtomicU64::new(0),
            warmup_end: warmup_start + warmup_duration,
        }
    }

    async fn record(&self, label: &str, duration: Duration) {
        let now = Instant::now();
        self.executed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if now >= self.warmup_end {
            let mut guard = self.durations.lock().await;
            guard.push(duration);
            self.steady_executed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let mut by_kind = self.by_kind.lock().await;
            let entry = by_kind.entry(label.to_string()).or_default();
            entry.executed += 1;
            entry.durations.push(duration);
        }
    }

    async fn record_error(&self, label: &str) {
        let now = Instant::now();
        self.errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if now >= self.warmup_end {
            self.steady_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let mut by_kind = self.by_kind.lock().await;
            let entry = by_kind.entry(label.to_string()).or_default();
            entry.errors += 1;
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

    let base_url = args.service_url.trim_end_matches('/').to_string();
    println!("Using service API at: {}", base_url);

    let http_client = reqwest::Client::new();
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
        tasks.push(tokio::spawn(run_span_writer_http(
            http_client.clone(),
            base_url.clone(),
            args.span_qps,
            deadline,
            Arc::clone(&span_stats),
        )));
    }
    if args.log_qps > 0 {
        tasks.push(tokio::spawn(run_log_writer_http(
            http_client.clone(),
            base_url.clone(),
            args.log_qps,
            deadline,
            Arc::clone(&log_stats),
        )));
    }
    if args.metric_qps > 0 {
        tasks.push(tokio::spawn(run_metric_writer_http(
            http_client.clone(),
            base_url.clone(),
            args.metric_qps,
            deadline,
            Arc::clone(&metric_stats),
        )));
    }

    for idx in 0..args.query_concurrency {
        tasks.push(tokio::spawn(run_query_worker_http(
            http_client.clone(),
            base_url.clone(),
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

// Helper functions to convert internal models to OTLP format
fn span_to_otlp(span: &Span) -> ExportTraceServiceRequest {
    let trace_id_bytes = hex::decode(&span.trace_id).unwrap_or_else(|_| {
        // If not hex, treat as UUID string and convert
        uuid::Uuid::parse_str(&span.trace_id)
            .map(|u| u.as_bytes().to_vec())
            .unwrap_or_else(|_| vec![0u8; 16])
    });
    let span_id_bytes = hex::decode(&span.span_id).unwrap_or_else(|_| {
        uuid::Uuid::parse_str(&span.span_id)
            .map(|u| u.as_bytes().to_vec())
            .unwrap_or_else(|_| vec![0u8; 8])
    });
    let parent_span_id_bytes = span.parent_span_id.as_ref().map(|id| {
        hex::decode(id).unwrap_or_else(|_| {
            uuid::Uuid::parse_str(id)
                .map(|u| u.as_bytes().to_vec())
                .unwrap_or_else(|_| vec![0u8; 8])
        })
    }).unwrap_or_default();

    let mut attributes = vec![];
    for (k, v) in &span.attributes {
        attributes.push(KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.clone())),
            }),
        });
    }

    let status_code = match span.status_code.as_deref() {
        Some("ERROR") => 2, // Status_StatusCode_ERROR
        _ => 1, // Status_StatusCode_OK
    };

    let otlp_span = OtlpSpan {
        trace_id: trace_id_bytes,
        span_id: span_id_bytes,
        parent_span_id: parent_span_id_bytes,
        name: span.message_type.clone(),
        kind: span.span_kind.as_ref()
            .and_then(|k| match k.as_str() {
                "SERVER" => Some(span::SpanKind::Server as i32),
                "CLIENT" => Some(span::SpanKind::Client as i32),
                _ => Some(span::SpanKind::Internal as i32),
            })
            .unwrap_or(span::SpanKind::Internal as i32),
        start_time_unix_nano: span.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64,
        end_time_unix_nano: span.end_timestamp
            .map(|t| t.timestamp_nanos_opt().unwrap_or(0) as u64)
            .unwrap_or(0),
        attributes,
        events: vec![],
        status: Some(Status {
            code: status_code,
            message: span.status_message.clone().unwrap_or_default(),
        }),
        ..Default::default()
    };

    let mut resource_attributes = vec![
        KeyValue {
            key: "sp.app.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(span.app_id.clone())),
            }),
        },
    ];

    if let Some(ref org_id) = span.organization_id {
        resource_attributes.push(KeyValue {
            key: "sp.organization.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(org_id.clone())),
            }),
        });
    }
    if let Some(ref tenant_id) = span.tenant_id {
        resource_attributes.push(KeyValue {
            key: "sp.tenant.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(tenant_id.clone())),
            }),
        });
    }

    let resource = Resource {
        attributes: resource_attributes,
        dropped_attributes_count: 0,
    };

    let scope = ScopeSpans {
        scope: Some(InstrumentationScope {
            name: "softprobe.stress".to_string(),
            version: "1.0.0".to_string(),
            ..Default::default()
        }),
        spans: vec![otlp_span],
        schema_url: String::new(),
    };

    let resource_spans = ResourceSpans {
        resource: Some(resource),
        scope_spans: vec![scope],
        schema_url: String::new(),
    };

    ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    }
}

fn log_to_otlp(log: &Log) -> ExportLogsServiceRequest {
    let trace_id_bytes = log.trace_id.as_ref().map(|id| {
        hex::decode(id).unwrap_or_else(|_| {
            uuid::Uuid::parse_str(id)
                .map(|u| u.as_bytes().to_vec())
                .unwrap_or_else(|_| vec![0u8; 16])
        })
    }).unwrap_or_default();
    let span_id_bytes = log.span_id.as_ref().map(|id| {
        hex::decode(id).unwrap_or_else(|_| {
            uuid::Uuid::parse_str(id)
                .map(|u| u.as_bytes().to_vec())
                .unwrap_or_else(|_| vec![0u8; 8])
        })
    }).unwrap_or_default();

    let mut attributes = vec![];
    for (k, v) in &log.attributes {
        attributes.push(KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.clone())),
            }),
        });
    }

    let log_record = LogRecord {
        time_unix_nano: log.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64,
        observed_time_unix_nano: log.observed_timestamp
            .map(|t| t.timestamp_nanos_opt().unwrap_or(0) as u64)
            .unwrap_or(0),
        severity_number: log.severity_number as i32,
        severity_text: log.severity_text.clone(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(log.body.clone())),
        }),
        attributes,
        trace_id: trace_id_bytes,
        span_id: span_id_bytes,
        flags: 0,
        ..Default::default()
    };

    let mut resource_attributes = vec![];
    for (k, v) in &log.resource_attributes {
        resource_attributes.push(KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.clone())),
            }),
        });
    }

    let resource = Resource {
        attributes: resource_attributes,
        dropped_attributes_count: 0,
    };

    let scope = ScopeLogs {
        scope: Some(InstrumentationScope {
            name: "softprobe.stress".to_string(),
            version: "1.0.0".to_string(),
            ..Default::default()
        }),
        log_records: vec![log_record],
        schema_url: String::new(),
    };

    let resource_logs = ResourceLogs {
        resource: Some(resource),
        scope_logs: vec![scope],
        schema_url: String::new(),
    };

    ExportLogsServiceRequest {
        resource_logs: vec![resource_logs],
    }
}

fn metric_to_otlp(metric: &Metric) -> ExportMetricsServiceRequest {
    let mut attributes = vec![];
    for (k, v) in &metric.attributes {
        attributes.push(KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.clone())),
            }),
        });
    }

    let data_point = NumberDataPoint {
        attributes,
        start_time_unix_nano: 0,
        time_unix_nano: metric.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64,
        value: Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(metric.value)),
        exemplars: vec![],
        flags: 0,
    };

    let otlp_metric = OtlpMetric {
        name: metric.metric_name.clone(),
        description: metric.description.clone(),
        unit: metric.unit.clone(),
        data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(
            opentelemetry_proto::tonic::metrics::v1::Gauge {
                data_points: vec![data_point],
            }
        )),
        metadata: vec![],
    };

    let mut resource_attributes = vec![];
    for (k, v) in &metric.resource_attributes {
        resource_attributes.push(KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.clone())),
            }),
        });
    }

    let resource = Resource {
        attributes: resource_attributes,
        dropped_attributes_count: 0,
    };

    let scope = ScopeMetrics {
        scope: Some(InstrumentationScope {
            name: "softprobe.stress".to_string(),
            version: "1.0.0".to_string(),
            ..Default::default()
        }),
        metrics: vec![otlp_metric],
        schema_url: String::new(),
    };

    let resource_metrics = ResourceMetrics {
        resource: Some(resource),
        scope_metrics: vec![scope],
        schema_url: String::new(),
    };

    ExportMetricsServiceRequest {
        resource_metrics: vec![resource_metrics],
    }
}

async fn run_span_writer_http(
    client: reqwest::Client,
    base_url: String,
    qps: u32,
    deadline: Instant,
    stats: Arc<ProducerStats>,
) -> Result<()> {
    let interval =
        std::time::Duration::from_micros((1_000_000.0 / qps.max(1) as f64).max(1.0) as u64);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut counter = 0u64;
    let url = format!("{}/v1/traces", base_url);
    while Instant::now() < deadline {
        ticker.tick().await;
        let span = sample_span(counter);
        let request = span_to_otlp(&span);
        match client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    stats.inc_success(1);
                } else {
                    let status = response.status();
                    let error_text = response.text().await.unwrap_or_default();
                    tracing::warn!("span write HTTP error {}: {}", status, error_text);
                    stats.inc_error();
                }
            }
            Err(err) => {
                tracing::warn!("span write HTTP request error: {}", err);
                stats.inc_error();
            }
        }
        counter += 1;
    }
    Ok(())
}

async fn run_log_writer_http(
    client: reqwest::Client,
    base_url: String,
    qps: u32,
    deadline: Instant,
    stats: Arc<ProducerStats>,
) -> Result<()> {
    let interval =
        std::time::Duration::from_micros((1_000_000.0 / qps.max(1) as f64).max(1.0) as u64);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut counter = 0u64;
    let url = format!("{}/v1/logs", base_url);
    while Instant::now() < deadline {
        ticker.tick().await;
        let log = sample_log(counter);
        let request = log_to_otlp(&log);
        match client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    stats.inc_success(1);
                } else {
                    let status = response.status();
                    let error_text = response.text().await.unwrap_or_default();
                    tracing::warn!("log write HTTP error {}: {}", status, error_text);
                    stats.inc_error();
                }
            }
            Err(err) => {
                tracing::warn!("log write HTTP request error: {}", err);
                stats.inc_error();
            }
        }
        counter += 1;
    }
    Ok(())
}

async fn run_metric_writer_http(
    client: reqwest::Client,
    base_url: String,
    qps: u32,
    deadline: Instant,
    stats: Arc<ProducerStats>,
) -> Result<()> {
    let interval =
        std::time::Duration::from_micros((1_000_000.0 / qps.max(1) as f64).max(1.0) as u64);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut counter = 0u64;
    let url = format!("{}/v1/metrics", base_url);
    while Instant::now() < deadline {
        ticker.tick().await;
        let metric = sample_metric(counter);
        let request = metric_to_otlp(&metric);
        match client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    stats.inc_success(1);
                } else {
                    let status = response.status();
                    let error_text = response.text().await.unwrap_or_default();
                    tracing::warn!("metric write HTTP error {}: {}", status, error_text);
                    stats.inc_error();
                }
            }
            Err(err) => {
                tracing::warn!("metric write HTTP request error: {}", err);
                stats.inc_error();
            }
        }
        counter += 1;
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct SqlQueryRequest {
    sql: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct SqlQueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    row_count: usize,
}

async fn run_query_worker_http(
    client: reqwest::Client,
    base_url: String,
    interval_ms: u64,
    deadline: Instant,
    stats: Arc<QueryStats>,
    worker_id: usize,
) -> Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms.max(100)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut idx = 0;
    let url = format!("{}/v1/query/sql", base_url);
    while Instant::now() < deadline {
        ticker.tick().await;
        let seed = (idx as u64).wrapping_add((worker_id as u64) * 1_000_000);
        let case = pick_query_case(seed);
        let (label, sql) = build_query(case, seed);
        let start = Instant::now();
        let request = SqlQueryRequest { sql };
        match client
            .post(&url)
            .json(&request)
            .send()
            .await
        {
            Ok(response) => {
                let elapsed = start.elapsed();
                if response.status().is_success() {
                    match response.json::<SqlQueryResponse>().await {
                        Ok(_) => {
                            stats.record(label, elapsed).await;
                        }
                        Err(err) => {
                            stats.record(label, elapsed).await;
                            tracing::warn!("query worker {worker_id} {label} JSON parse error: {}", err);
                            stats.record_error(label).await;
                        }
                    }
                } else {
                    let status = response.status();
                    let error_text = response.text().await.unwrap_or_default();
                    stats.record(label, elapsed).await;
                    eprintln!("ERROR: query worker {worker_id} {label} HTTP error {}: {}", status, error_text);
                    tracing::warn!("query worker {worker_id} {label} HTTP error {}: {}", status, error_text);
                    stats.record_error(label).await;
                }
            }
            Err(err) => {
                let elapsed = start.elapsed();
                // Record latency even for failures: alerts still pay the cost, and we want to see it.
                stats.record(label, elapsed).await;
                eprintln!("ERROR: query worker {worker_id} {label} HTTP request error: {}", err);
                tracing::warn!("query worker {worker_id} {label} HTTP request error: {}", err);
                stats.record_error(label).await;
            }
        }
        idx += 1;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum QueryCase {
    SpanErrorRate5m,
    SpanTop5xxPaths15m,
    SpanP95LatencyByPath5m,
    SpanSessionRecent,
    LogErrorRate5m,
    LogRecentErrorsSample,
    LogSessionRecent,
    MetricLatencyTimeseries10m,
    MetricLatencyMax5m,
    SpanErrorRate24h,
    MetricLatencyTimeseries24h,
}

fn pick_query_case(seed: u64) -> QueryCase {
    // Deterministic, interleaved schedule so short runs (or slow queries) still cover
    // many query types instead of getting "stuck" on the first weight bucket.
    const SCHEDULE: [QueryCase; 18] = [
        QueryCase::SpanErrorRate5m,
        QueryCase::LogErrorRate5m,
        QueryCase::MetricLatencyMax5m,
        QueryCase::SpanSessionRecent,
        QueryCase::SpanTop5xxPaths15m,
        QueryCase::LogRecentErrorsSample,
        QueryCase::MetricLatencyTimeseries10m,
        QueryCase::SpanP95LatencyByPath5m,
        QueryCase::LogSessionRecent,
        QueryCase::SpanSessionRecent,
        QueryCase::SpanErrorRate5m,
        QueryCase::LogErrorRate5m,
        QueryCase::MetricLatencyMax5m,
        QueryCase::SpanTop5xxPaths15m,
        QueryCase::SpanP95LatencyByPath5m,
        QueryCase::MetricLatencyTimeseries10m,
        QueryCase::SpanErrorRate24h,
        QueryCase::MetricLatencyTimeseries24h,
    ];
    SCHEDULE[(seed as usize) % SCHEDULE.len()]
}

fn build_query(case: QueryCase, seed: u64) -> (&'static str, String) {
    let date_filter = (Utc::now() - chrono::Duration::days(1)).format("%Y-%m-%d");
    // DuckDB returns CURRENT_TIMESTAMP as TIMESTAMP WITH TIME ZONE; subtracting INTERVAL from that
    // is not supported in some builds. Cast to TIMESTAMP for stable interval arithmetic.
    let now_ts = "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)";
    let hit = seed % 5 != 0; // 80/20 hit/miss drilldowns
    let session = if hit {
        format!("stress-session-{}", seed % 256)
    } else {
        format!("stress-session-miss-{}", seed % 10_000)
    };

    match case {
        QueryCase::SpanErrorRate5m => (
            "span_error_rate_5m",
            format!(
                "SELECT COUNT(*) AS errors \
                 FROM union_spans \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND timestamp >= ({now_ts} - INTERVAL '5 minutes') \
                   AND (http_response_status_code >= 500 OR status_code = 'ERROR')"
            ),
        ),
        QueryCase::SpanTop5xxPaths15m => (
            "span_top_5xx_paths_15m",
            format!(
                "SELECT http_request_path, COUNT(*) AS errors \
                 FROM union_spans \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND timestamp >= ({now_ts} - INTERVAL '15 minutes') \
                   AND http_response_status_code >= 500 \
                   AND http_request_path IS NOT NULL \
                 GROUP BY 1 \
                 ORDER BY errors DESC \
                 LIMIT 10"
            ),
        ),
        QueryCase::SpanP95LatencyByPath5m => (
            "span_p95_latency_by_path_5m",
            format!(
                "SELECT http_request_path, \
                        quantile_cont((EXTRACT(EPOCH FROM end_timestamp) - EXTRACT(EPOCH FROM timestamp)) * 1000.0, 0.95) AS p95_ms \
                 FROM union_spans \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND timestamp >= ({now_ts} - INTERVAL '5 minutes') \
                   AND end_timestamp IS NOT NULL \
                   AND http_request_path IS NOT NULL \
                 GROUP BY 1 \
                 ORDER BY p95_ms DESC \
                 LIMIT 10"
            ),
        ),
        QueryCase::SpanSessionRecent => (
            "span_session_recent",
            format!(
                "SELECT trace_id, span_id, timestamp, http_request_path, http_response_status_code \
                 FROM union_spans \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND session_id = '{session}' \
                 ORDER BY timestamp DESC \
                 LIMIT 50"
            ),
        ),
        QueryCase::LogErrorRate5m => (
            "log_error_rate_5m",
            format!(
                "SELECT COUNT(*) AS errors \
                 FROM union_logs \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND timestamp >= ({now_ts} - INTERVAL '5 minutes') \
                   AND severity_number >= 17"
            ),
        ),
        QueryCase::LogRecentErrorsSample => (
            "log_recent_errors_sample",
            format!(
                "SELECT timestamp, severity_text, body \
                 FROM union_logs \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND timestamp >= ({now_ts} - INTERVAL '5 minutes') \
                   AND severity_number >= 17 \
                 ORDER BY timestamp DESC \
                 LIMIT 50"
            ),
        ),
        QueryCase::LogSessionRecent => (
            "log_session_recent",
            format!(
                "SELECT timestamp, severity_text, body \
                 FROM union_logs \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND session_id = '{session}' \
                 ORDER BY timestamp DESC \
                 LIMIT 50"
            ),
        ),
        QueryCase::MetricLatencyTimeseries10m => (
            "metric_latency_timeseries_10m",
            format!(
                "SELECT date_trunc('minute', timestamp) AS t, AVG(value) AS avg_latency_ms \
                 FROM union_metrics \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND timestamp >= ({now_ts} - INTERVAL '10 minutes') \
                   AND metric_name = 'stress.metric.latency' \
                 GROUP BY 1 \
                 ORDER BY 1"
            ),
        ),
        QueryCase::MetricLatencyMax5m => (
            "metric_latency_max_5m",
            format!(
                "SELECT MAX(value) AS max_latency_ms \
                 FROM union_metrics \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND timestamp >= ({now_ts} - INTERVAL '5 minutes') \
                   AND metric_name = 'stress.metric.latency'"
            ),
        ),
        QueryCase::SpanErrorRate24h => (
            "span_error_rate_24h",
            format!(
                "SELECT COUNT(*) AS errors \
                 FROM union_spans \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND timestamp >= ({now_ts} - INTERVAL '24 hours') \
                   AND (http_response_status_code >= 500 OR status_code = 'ERROR')"
            ),
        ),
        QueryCase::MetricLatencyTimeseries24h => (
            "metric_latency_timeseries_24h",
            format!(
                "SELECT date_trunc('minute', timestamp) AS t, AVG(value) AS avg_latency_ms \
                 FROM union_metrics \
                 WHERE record_date >= DATE '{date_filter}' \
                   AND timestamp >= ({now_ts} - INTERVAL '24 hours') \
                   AND metric_name = 'stress.metric.latency' \
                 GROUP BY 1 \
                 ORDER BY 1"
            ),
        ),
    }
}

fn sample_span(counter: u64) -> Span {
    let timestamp = Utc::now();
    let session_id = format!("stress-session-{}", counter % 256);

    let app_id = format!("stress-app-{}", counter % 4);
    let method = if counter % 4 == 0 { "POST" } else { "GET" };
    let path = match counter % 8 {
        0 => "/api/login",
        1 => "/api/orders",
        2 => "/api/orders/123",
        3 => "/api/checkout",
        4 => "/api/catalog/search",
        5 => "/api/cart",
        6 => "/api/users/me",
        _ => "/healthz",
    };

    // Deterministic "bursty" error/latency pattern to mimic real alerting scenarios.
    let burst = (counter / 200) % 10 == 0;
    let is_error = burst && (counter % 10 == 0);
    let http_status = if is_error { 500 } else { 200 };
    let duration_ms = if is_error {
        900
    } else if burst {
        250
    } else {
        10 + (counter % 40) as i64
    };

    let mut attributes = HashMap::new();
    attributes.insert("sp.session.id".to_string(), session_id.clone());
    attributes.insert("http.request.method".to_string(), method.to_string());
    attributes.insert("http.request.path".to_string(), path.to_string());
    attributes.insert("http.response.status_code".to_string(), http_status.to_string());

    Span {
        session_id,
        trace_id: uuid::Uuid::new_v4().to_string(),
        span_id: uuid::Uuid::new_v4().to_string(),
        parent_span_id: None,
        app_id,
        organization_id: Some("stress-org".to_string()),
        tenant_id: Some("stress-tenant".to_string()),
        message_type: "http.server".to_string(),
        span_kind: Some("SERVER".to_string()),
        timestamp,
        end_timestamp: Some(timestamp + chrono::Duration::milliseconds(duration_ms)),
        attributes,
        events: Vec::new(),
        status_code: Some(if is_error { "ERROR" } else { "OK" }.to_string()),
        status_message: Some(if is_error { "synthetic error" } else { "ok" }.to_string()),
        http_request_method: Some(method.to_string()),
        http_request_path: Some(path.to_string()),
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: Some(http_status),
        http_response_headers: None,
        http_response_body: None,
    }
}

fn sample_log(counter: u64) -> Log {
    let timestamp = Utc::now();
    let burst = (counter / 200) % 10 == 0;
    let is_error = burst && (counter % 10 == 0);
    let severity_number = if is_error { 17 } else { 12 };
    let severity_text = if is_error { "ERROR" } else { "INFO" };
    let session_id = Some(format!("stress-session-{}", counter % 256));
    let path = match counter % 6 {
        0 => "/api/login",
        1 => "/api/orders",
        2 => "/api/checkout",
        3 => "/api/catalog/search",
        4 => "/api/cart",
        _ => "/api/users/me",
    };
    let mut attributes = HashMap::new();
    attributes.insert("log.index".to_string(), counter.to_string());
    attributes.insert("http.request.path".to_string(), path.to_string());

    let mut resource_attributes = HashMap::new();
    resource_attributes.insert(
        "service.name".to_string(),
        format!("stress-service-{}", counter % 4),
    );
    resource_attributes.insert("host.name".to_string(), "stress-worker".to_string());

    Log {
        session_id,
        timestamp,
        observed_timestamp: Some(timestamp + chrono::Duration::milliseconds(1)),
        severity_number,
        severity_text: severity_text.to_string(),
        body: if is_error {
            format!("http 5xx on {path} (synthetic) idx={counter}")
        } else {
            format!("request ok {path} idx={counter}")
        },
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
    attributes.insert(
        "service.name".to_string(),
        format!("stress-service-{}", counter % 4),
    );

    let mut resource_attributes = HashMap::new();
    resource_attributes.insert(
        "service.name".to_string(),
        format!("stress-service-{}", counter % 4),
    );

    let burst = (counter / 200) % 10 == 0;
    let value = if burst { 900.0 } else { 100.0 + (counter % 50) as f64 };

    Metric {
        metric_name: "stress.metric.latency".to_string(),
        description: "Stress latency".to_string(),
        unit: "ms".to_string(),
        metric_type: "gauge".to_string(),
        timestamp: now,
        value,
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

    // Per-query-type breakdown (post-warmup)
    let by_kind = query_stats.by_kind.lock().await.clone();
    if !by_kind.is_empty() {
        println!("\n---- Query Breakdown (post-warmup) ----");
        let mut ordered: BTreeMap<String, PerQueryStats> = BTreeMap::new();
        for (k, v) in by_kind {
            ordered.insert(k, v);
        }
        for (label, stats) in ordered {
            let avg = stats
                .durations
                .iter()
                .map(|d| d.as_millis())
                .sum::<u128>()
                .checked_div(stats.durations.len().max(1) as u128)
                .unwrap_or(0);
            let p95 = percentile(&stats.durations, 95);
            println!(
                "{:<30} executed={} errors={} avg={}ms p95={}ms",
                label, stats.executed, stats.errors, avg, p95
            );
        }
        println!("---------------------------------------");
    }
    println!("=========================================");
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
