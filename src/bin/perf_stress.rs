use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Metric as OtlpMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    span, ResourceSpans, ScopeSpans, Span as OtlpSpan, Status,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use softprobe_runtime::models::{Log, Metric, Span};
use tracing_subscriber;

#[derive(Parser, Clone)]
#[command(
    author = "SoftProbe Team",
    about = "Stress test the OTLP ingestion+query pipeline against DuckLake storage"
)]
struct Args {
    /// Path to the YAML configuration file. Uses CONFIG_FILE env fallback if omitted.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Duration of each stress phase in seconds.
    #[arg(long, default_value_t = 60)]
    duration: u64,

    /// Span events per second (offered load; open-loop).
    #[arg(long, default_value_t = 100)]
    span_qps: u32,

    /// Log events per second (offered load; open-loop).
    #[arg(long, default_value_t = 200)]
    log_qps: u32,

    /// Metric events per second (offered load; open-loop).
    #[arg(long, default_value_t = 200)]
    metric_qps: u32,

    /// Records per OTLP HTTP request (collector-style batching).
    #[arg(long, default_value_t = 1)]
    batch_size: u32,

    /// Max in-flight ingest HTTP requests across all signals.
    #[arg(long, default_value_t = 8)]
    ingest_concurrency: usize,

    /// Number of concurrent SQL workers running against DuckDB.
    #[arg(long, default_value_t = 4)]
    query_concurrency: usize,

    /// Milliseconds between each query execution per worker.
    #[arg(long, default_value_t = 1000)]
    query_interval_ms: u64,

    /// Seconds to wait for warm-up before recording steady-state stats.
    #[arg(long, default_value_t = 10)]
    warmup_secs: u64,

    /// Comma-separated phases: ingest, query, mixed (default: mixed).
    #[arg(long, default_value = "mixed")]
    phases: String,

    /// Service URL for API (e.g., http://localhost:8090). Required.
    #[arg(long)]
    service_url: String,

    /// Bearer token for `/v1/*` auth (control-plane). Defaults to `test-token`
    /// for local auth mocks; override with `--api-token` or `SOFTPROBE_API_TOKEN`.
    #[arg(long, default_value = "test-token")]
    api_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhaseKind {
    Ingest,
    Query,
    Mixed,
}

impl PhaseKind {
    fn parse_list(s: &str) -> Result<Vec<PhaseKind>> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let p = part.trim().to_ascii_lowercase();
            if p.is_empty() {
                continue;
            }
            out.push(match p.as_str() {
                "ingest" | "ingest_only" | "ingest-only" => PhaseKind::Ingest,
                "query" | "query_only" | "query-only" => PhaseKind::Query,
                "mixed" => PhaseKind::Mixed,
                other => anyhow::bail!(
                    "unknown phase '{other}' (expected ingest, query, or mixed)"
                ),
            });
        }
        if out.is_empty() {
            anyhow::bail!("--phases must list at least one of: ingest, query, mixed");
        }
        Ok(out)
    }

    fn label(self) -> &'static str {
        match self {
            PhaseKind::Ingest => "ingest_only",
            PhaseKind::Query => "query_only",
            PhaseKind::Mixed => "mixed",
        }
    }

    fn enable_ingest(self) -> bool {
        matches!(self, PhaseKind::Ingest | PhaseKind::Mixed)
    }

    fn enable_query(self) -> bool {
        matches!(self, PhaseKind::Query | PhaseKind::Mixed)
    }
}

#[derive(Default)]
struct ProducerStats {
    offered: AtomicU64,
    achieved: AtomicU64,
    errors: AtomicU64,
    drops: AtomicU64,
    steady_durations: Mutex<Vec<Duration>>,
    warmup_end: std::sync::OnceLock<Instant>,
}

impl ProducerStats {
    fn set_warmup_end(&self, end: Instant) {
        let _ = self.warmup_end.set(end);
    }

    fn inc_offered(&self, delta: u64) {
        self.offered.fetch_add(delta, Ordering::Relaxed);
    }

    fn inc_achieved(&self, delta: u64) {
        self.achieved.fetch_add(delta, Ordering::Relaxed);
    }

    fn inc_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_drop(&self, delta: u64) {
        self.drops.fetch_add(delta, Ordering::Relaxed);
    }

    async fn record_latency(&self, duration: Duration) {
        if let Some(end) = self.warmup_end.get() {
            if Instant::now() >= *end {
                self.steady_durations.lock().await.push(duration);
            }
        }
    }

    async fn snapshot(&self, duration_secs: f64) -> ProducerSnapshot {
        let offered = self.offered.load(Ordering::Relaxed);
        let achieved = self.achieved.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let drops = self.drops.load(Ordering::Relaxed);
        let durations = self.steady_durations.lock().await.clone();
        ProducerSnapshot {
            offered,
            achieved,
            errors,
            drops,
            offered_eps: offered as f64 / duration_secs.max(0.001),
            achieved_eps: achieved as f64 / duration_secs.max(0.001),
            p50_ms: percentile(&durations, 50),
            p95_ms: percentile(&durations, 95),
            p99_ms: percentile(&durations, 99),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ProducerSnapshot {
    offered: u64,
    achieved: u64,
    errors: u64,
    drops: u64,
    offered_eps: f64,
    achieved_eps: f64,
    p50_ms: u128,
    p95_ms: u128,
    p99_ms: u128,
}

struct QueryStats {
    durations: Mutex<Vec<Duration>>,
    by_kind: Mutex<HashMap<String, PerQueryStats>>,
    executed: AtomicU64,
    errors: AtomicU64,
    steady_executed: AtomicU64,
    steady_errors: AtomicU64,
    warmup_end: Instant,
}

#[derive(Default, Clone, Debug)]
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
            executed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            steady_executed: AtomicU64::new(0),
            steady_errors: AtomicU64::new(0),
            warmup_end: warmup_start + warmup_duration,
        }
    }

    async fn record(&self, label: &str, duration: Duration) {
        let now = Instant::now();
        self.executed.fetch_add(1, Ordering::Relaxed);
        if now >= self.warmup_end {
            let mut guard = self.durations.lock().await;
            guard.push(duration);
            self.steady_executed.fetch_add(1, Ordering::Relaxed);

            let mut by_kind = self.by_kind.lock().await;
            let entry = by_kind.entry(label.to_string()).or_default();
            entry.executed += 1;
            entry.durations.push(duration);
        }
    }

    async fn record_error(&self, label: &str) {
        let now = Instant::now();
        self.errors.fetch_add(1, Ordering::Relaxed);
        if now >= self.warmup_end {
            self.steady_errors.fetch_add(1, Ordering::Relaxed);

            let mut by_kind = self.by_kind.lock().await;
            let entry = by_kind.entry(label.to_string()).or_default();
            entry.errors += 1;
        }
    }

    async fn snapshot(&self, duration_secs: f64) -> QuerySnapshot {
        let durations = self.durations.lock().await.clone();
        let by_kind = self.by_kind.lock().await.clone();
        QuerySnapshot {
            executed: self.executed.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            steady_executed: self.steady_executed.load(Ordering::Relaxed),
            steady_errors: self.steady_errors.load(Ordering::Relaxed),
            qps: self.steady_executed.load(Ordering::Relaxed) as f64 / duration_secs.max(0.001),
            avg_ms: avg_ms(&durations),
            p50_ms: percentile(&durations, 50),
            p95_ms: percentile(&durations, 95),
            p99_ms: percentile(&durations, 99),
            by_kind,
        }
    }
}

#[derive(Clone, Debug)]
struct QuerySnapshot {
    executed: u64,
    errors: u64,
    steady_executed: u64,
    steady_errors: u64,
    qps: f64,
    avg_ms: u128,
    p50_ms: u128,
    p95_ms: u128,
    p99_ms: u128,
    by_kind: HashMap<String, PerQueryStats>,
}

#[derive(Clone, Debug)]
struct PhaseSnapshot {
    kind: PhaseKind,
    duration_secs: f64,
    span: ProducerSnapshot,
    log: ProducerSnapshot,
    metric: ProducerSnapshot,
    query: QuerySnapshot,
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

    let api_token = std::env::var("SOFTPROBE_API_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| args.api_token.clone());
    let mut headers = reqwest::header::HeaderMap::new();
    let auth_value = format!("Bearer {}", api_token.trim());
    headers.insert(
        reqwest::header::AUTHORIZATION,
        auth_value
            .parse()
            .expect("api_token must form a valid Authorization header value"),
    );
    let http_client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;

    let phases = PhaseKind::parse_list(&args.phases)?;
    println!(
        "Phases: {}",
        phases
            .iter()
            .map(|p| p.label())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "Batch size: {} | Ingest concurrency: {}",
        args.batch_size.max(1),
        args.ingest_concurrency.max(1)
    );

    let mut phase_results = Vec::new();
    for phase in phases {
        println!("\n########## PHASE: {} ##########", phase.label());
        let snap = run_phase(&args, &http_client, &base_url, phase).await?;
        print_phase_report(&args, &snap).await;
        phase_results.push(snap);
    }

    print_interference_summary(&phase_results);
    Ok(())
}

async fn run_phase(
    args: &Args,
    http_client: &reqwest::Client,
    base_url: &str,
    phase: PhaseKind,
) -> Result<PhaseSnapshot> {
    let deadline = Instant::now() + Duration::from_secs(args.duration);
    let warmup_secs = std::cmp::min(args.warmup_secs, args.duration);
    let warmup_duration = Duration::from_secs(warmup_secs);
    let warmup_start = Instant::now();
    let warmup_end = warmup_start + warmup_duration;

    let span_stats = Arc::new(ProducerStats::default());
    let log_stats = Arc::new(ProducerStats::default());
    let metric_stats = Arc::new(ProducerStats::default());
    span_stats.set_warmup_end(warmup_end);
    log_stats.set_warmup_end(warmup_end);
    metric_stats.set_warmup_end(warmup_end);
    let query_stats = Arc::new(QueryStats::new(warmup_start, warmup_duration));

    let batch_size = args.batch_size.max(1) as usize;
    let ingest_concurrency = args.ingest_concurrency.max(1);
    let semaphore = Arc::new(Semaphore::new(ingest_concurrency));

    let mut tasks = JoinSet::new();

    if phase.enable_ingest() {
        if args.span_qps > 0 {
            tasks.spawn(run_open_loop_writer(
                http_client.clone(),
                format!("{}/v1/traces", base_url),
                "span",
                args.span_qps,
                batch_size,
                deadline,
                Arc::clone(&semaphore),
                Arc::clone(&span_stats),
                SignalKind::Span,
            ));
        }
        if args.log_qps > 0 {
            tasks.spawn(run_open_loop_writer(
                http_client.clone(),
                format!("{}/v1/logs", base_url),
                "log",
                args.log_qps,
                batch_size,
                deadline,
                Arc::clone(&semaphore),
                Arc::clone(&log_stats),
                SignalKind::Log,
            ));
        }
        if args.metric_qps > 0 {
            tasks.spawn(run_open_loop_writer(
                http_client.clone(),
                format!("{}/v1/metrics", base_url),
                "metric",
                args.metric_qps,
                batch_size,
                deadline,
                Arc::clone(&semaphore),
                Arc::clone(&metric_stats),
                SignalKind::Metric,
            ));
        }
    }

    if phase.enable_query() {
        for idx in 0..args.query_concurrency {
            tasks.spawn(run_query_worker_http(
                http_client.clone(),
                base_url.to_string(),
                args.query_interval_ms,
                deadline,
                Arc::clone(&query_stats),
                idx,
            ));
        }
    }

    while let Some(res) = tasks.join_next().await {
        res??;
    }

    let duration_secs = args.duration as f64;
    Ok(PhaseSnapshot {
        kind: phase,
        duration_secs,
        span: span_stats.snapshot(duration_secs).await,
        log: log_stats.snapshot(duration_secs).await,
        metric: metric_stats.snapshot(duration_secs).await,
        query: query_stats.snapshot((args.duration.saturating_sub(warmup_secs)) as f64).await,
    })
}

#[derive(Clone, Copy)]
enum SignalKind {
    Span,
    Log,
    Metric,
}

async fn run_open_loop_writer(
    client: reqwest::Client,
    url: String,
    label: &'static str,
    events_per_sec: u32,
    batch_size: usize,
    deadline: Instant,
    semaphore: Arc<Semaphore>,
    stats: Arc<ProducerStats>,
    kind: SignalKind,
) -> Result<()> {
    let batches_per_sec = events_per_sec as f64 / batch_size as f64;
    let interval = Duration::from_secs_f64((1.0 / batches_per_sec.max(0.001)).max(0.000_001));
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut counter = 0u64;
    let mut in_flight: JoinSet<()> = JoinSet::new();

    while Instant::now() < deadline {
        ticker.tick().await;
        if Instant::now() >= deadline {
            break;
        }
        let batch_events = batch_size as u64;
        stats.inc_offered(batch_events);

        let permit = match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                stats.inc_drop(batch_events);
                continue;
            }
        };

        let start_counter = counter;
        counter = counter.wrapping_add(batch_events);

        let client = client.clone();
        let url = url.clone();
        let stats = Arc::clone(&stats);
        in_flight.spawn(async move {
            let _permit = permit;
            let body = match kind {
                SignalKind::Span => {
                    let spans: Vec<Span> = (0..batch_size)
                        .map(|i| sample_span(start_counter.wrapping_add(i as u64)))
                        .collect();
                    serde_json::to_value(spans_to_otlp(&spans)).ok()
                }
                SignalKind::Log => {
                    let logs: Vec<Log> = (0..batch_size)
                        .map(|i| sample_log(start_counter.wrapping_add(i as u64)))
                        .collect();
                    serde_json::to_value(logs_to_otlp(&logs)).ok()
                }
                SignalKind::Metric => {
                    let metrics: Vec<Metric> = (0..batch_size)
                        .map(|i| sample_metric(start_counter.wrapping_add(i as u64)))
                        .collect();
                    serde_json::to_value(metrics_to_otlp(&metrics)).ok()
                }
            };
            let Some(body) = body else {
                tracing::warn!("{label} batch serialize failed");
                stats.inc_error();
                return;
            };
            let start = Instant::now();
            match client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await
            {
                Ok(response) => {
                    let elapsed = start.elapsed();
                    stats.record_latency(elapsed).await;
                    if response.status().is_success() {
                        stats.inc_achieved(batch_events);
                    } else {
                        let status = response.status();
                        let error_text = response.text().await.unwrap_or_default();
                        tracing::warn!("{label} write HTTP error {}: {}", status, error_text);
                        stats.inc_error();
                    }
                }
                Err(err) => {
                    stats.record_latency(start.elapsed()).await;
                    tracing::warn!("{label} write HTTP request error: {}", err);
                    stats.inc_error();
                }
            }
        });
    }

    while in_flight.join_next().await.is_some() {}
    Ok(())
}

fn spans_to_otlp(spans: &[Span]) -> ExportTraceServiceRequest {
    let mut resource_spans = Vec::with_capacity(spans.len());
    for span in spans {
        resource_spans.extend(span_to_otlp(span).resource_spans);
    }
    ExportTraceServiceRequest { resource_spans }
}

fn logs_to_otlp(logs: &[Log]) -> ExportLogsServiceRequest {
    let mut resource_logs = Vec::with_capacity(logs.len());
    for log in logs {
        resource_logs.extend(log_to_otlp(log).resource_logs);
    }
    ExportLogsServiceRequest { resource_logs }
}

fn metrics_to_otlp(metrics: &[Metric]) -> ExportMetricsServiceRequest {
    let mut resource_metrics = Vec::with_capacity(metrics.len());
    for metric in metrics {
        resource_metrics.extend(metric_to_otlp(metric).resource_metrics);
    }
    ExportMetricsServiceRequest { resource_metrics }
}

// Helper functions to convert internal models to OTLP format
fn span_to_otlp(span: &Span) -> ExportTraceServiceRequest {
    let trace_id_bytes = hex::decode(&span.trace_id).unwrap_or_else(|_| {
        uuid::Uuid::parse_str(&span.trace_id)
            .map(|u| u.as_bytes().to_vec())
            .unwrap_or_else(|_| vec![0u8; 16])
    });
    let span_id_bytes = hex::decode(&span.span_id).unwrap_or_else(|_| {
        uuid::Uuid::parse_str(&span.span_id)
            .map(|u| u.as_bytes().to_vec())
            .unwrap_or_else(|_| vec![0u8; 8])
    });
    let parent_span_id_bytes = span
        .parent_span_id
        .as_ref()
        .map(|id| {
            hex::decode(id).unwrap_or_else(|_| {
                uuid::Uuid::parse_str(id)
                    .map(|u| u.as_bytes().to_vec())
                    .unwrap_or_else(|_| vec![0u8; 8])
            })
        })
        .unwrap_or_default();

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
        Some("ERROR") => 2,
        _ => 1,
    };

    let otlp_span = OtlpSpan {
        trace_id: trace_id_bytes,
        span_id: span_id_bytes,
        parent_span_id: parent_span_id_bytes,
        name: span.message_type.clone(),
        kind: span
            .span_kind
            .as_ref()
            .and_then(|k| match k.as_str() {
                "SERVER" => Some(span::SpanKind::Server as i32),
                "CLIENT" => Some(span::SpanKind::Client as i32),
                _ => Some(span::SpanKind::Internal as i32),
            })
            .unwrap_or(span::SpanKind::Internal as i32),
        start_time_unix_nano: span.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64,
        end_time_unix_nano: span
            .end_timestamp
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

    let mut resource_attributes = vec![KeyValue {
        key: "sp.app.id".to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(span.app_id.clone())),
        }),
    }];

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
    let trace_id_bytes = log
        .trace_id
        .as_ref()
        .map(|id| {
            hex::decode(id).unwrap_or_else(|_| {
                uuid::Uuid::parse_str(id)
                    .map(|u| u.as_bytes().to_vec())
                    .unwrap_or_else(|_| vec![0u8; 16])
            })
        })
        .unwrap_or_default();
    let span_id_bytes = log
        .span_id
        .as_ref()
        .map(|id| {
            hex::decode(id).unwrap_or_else(|_| {
                uuid::Uuid::parse_str(id)
                    .map(|u| u.as_bytes().to_vec())
                    .unwrap_or_else(|_| vec![0u8; 8])
            })
        })
        .unwrap_or_default();

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
        observed_time_unix_nano: log
            .observed_timestamp
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
        value: Some(
            opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                metric.value,
            ),
        ),
        exemplars: vec![],
        flags: 0,
    };

    let otlp_metric = OtlpMetric {
        name: metric.metric_name.clone(),
        description: metric.description.clone(),
        unit: metric.unit.clone(),
        data: Some(
            opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(
                opentelemetry_proto::tonic::metrics::v1::Gauge {
                    data_points: vec![data_point],
                },
            ),
        ),
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
        match client.post(&url).json(&request).send().await {
            Ok(response) => {
                let elapsed = start.elapsed();
                if response.status().is_success() {
                    match response.json::<SqlQueryResponse>().await {
                        Ok(_) => {
                            stats.record(label, elapsed).await;
                        }
                        Err(err) => {
                            stats.record(label, elapsed).await;
                            tracing::warn!(
                                "query worker {worker_id} {label} JSON parse error: {}",
                                err
                            );
                            stats.record_error(label).await;
                        }
                    }
                } else {
                    let status = response.status();
                    let error_text = response.text().await.unwrap_or_default();
                    stats.record(label, elapsed).await;
                    eprintln!(
                        "ERROR: query worker {worker_id} {label} HTTP error {}: {}",
                        status, error_text
                    );
                    tracing::warn!(
                        "query worker {worker_id} {label} HTTP error {}: {}",
                        status,
                        error_text
                    );
                    stats.record_error(label).await;
                }
            }
            Err(err) => {
                let elapsed = start.elapsed();
                stats.record(label, elapsed).await;
                eprintln!(
                    "ERROR: query worker {worker_id} {label} HTTP request error: {}",
                    err
                );
                tracing::warn!(
                    "query worker {worker_id} {label} HTTP request error: {}",
                    err
                );
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
    let now_ts = "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)";
    let hit = seed % 5 != 0;
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
    attributes.insert(
        "http.response.status_code".to_string(),
        http_status.to_string(),
    );

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
        resource_attributes: HashMap::new(),
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
    let value = if burst {
        900.0
    } else {
        100.0 + (counter % 50) as f64
    };

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

async fn print_phase_report(args: &Args, snap: &PhaseSnapshot) {
    println!("\n========== Phase Report: {} ==========", snap.kind.label());
    println!("Duration: {} seconds", args.duration);
    println!(
        "Offered events/s: span={} log={} metric={} | batch_size={}",
        args.span_qps, args.log_qps, args.metric_qps, args.batch_size
    );
    println!(
        "Query workers: {} (interval {}ms)",
        args.query_concurrency, args.query_interval_ms
    );
    println!("Warm-up period: {} seconds", args.warmup_secs);

    print_producer_snapshot("span", &snap.span);
    print_producer_snapshot("log", &snap.log);
    print_producer_snapshot("metric", &snap.metric);

    let total_achieved = snap.span.achieved + snap.log.achieved + snap.metric.achieved;
    let total_offered = snap.span.offered + snap.log.offered + snap.metric.offered;
    println!(
        "Total ingest: offered={:.1}/s achieved={:.1}/s ({}/{} events)",
        total_offered as f64 / snap.duration_secs.max(0.001),
        total_achieved as f64 / snap.duration_secs.max(0.001),
        total_achieved,
        total_offered
    );

    println!(
        "Queries: executed={} errors={} steady={} steady_errors={} qps={:.2}",
        snap.query.executed,
        snap.query.errors,
        snap.query.steady_executed,
        snap.query.steady_errors,
        snap.query.qps
    );
    println!(
        "Steady-state query latency: avg={}ms p50={}ms p95={}ms p99={}ms",
        snap.query.avg_ms, snap.query.p50_ms, snap.query.p95_ms, snap.query.p99_ms
    );

    if !snap.query.by_kind.is_empty() {
        println!("\n---- Query Breakdown (post-warmup) ----");
        let mut ordered: BTreeMap<String, PerQueryStats> = BTreeMap::new();
        for (k, v) in &snap.query.by_kind {
            ordered.insert(k.clone(), v.clone());
        }
        for (label, stats) in ordered {
            println!(
                "{:<30} executed={} errors={} avg={}ms p95={}ms",
                label,
                stats.executed,
                stats.errors,
                avg_ms(&stats.durations),
                percentile(&stats.durations, 95)
            );
        }
        println!("---------------------------------------");
    }
    println!("=========================================");
}

fn print_producer_snapshot(label: &str, snap: &ProducerSnapshot) {
    println!(
        "{label}: offered={:.1}/s achieved={:.1}/s events={}/{} http_errors={} drops={} latency p50={}ms p95={}ms p99={}ms",
        snap.offered_eps,
        snap.achieved_eps,
        snap.achieved,
        snap.offered,
        snap.errors,
        snap.drops,
        snap.p50_ms,
        snap.p95_ms,
        snap.p99_ms
    );
}

fn print_interference_summary(phases: &[PhaseSnapshot]) {
    let ingest = phases.iter().find(|p| p.kind == PhaseKind::Ingest);
    let query = phases.iter().find(|p| p.kind == PhaseKind::Query);
    let mixed = phases.iter().find(|p| p.kind == PhaseKind::Mixed);
    if ingest.is_none() && query.is_none() {
        return;
    }

    println!("\n========== Interference Summary ==========");
    if let (Some(ingest), Some(mixed)) = (ingest, mixed) {
        let ingest_total = ingest.span.achieved_eps + ingest.log.achieved_eps + ingest.metric.achieved_eps;
        let mixed_total = mixed.span.achieved_eps + mixed.log.achieved_eps + mixed.metric.achieved_eps;
        let delta = mixed_total - ingest_total;
        let pct = if ingest_total > 0.0 {
            100.0 * delta / ingest_total
        } else {
            0.0
        };
        println!(
            "Ingest achieved events/s: ingest_only={:.1} mixed={:.1} Δ={:+.1} ({:+.1}%)",
            ingest_total, mixed_total, delta, pct
        );
        println!(
            "Ingest p95 latency (span/log/metric): ingest_only={}/{}/{}ms mixed={}/{}/{}ms",
            ingest.span.p95_ms,
            ingest.log.p95_ms,
            ingest.metric.p95_ms,
            mixed.span.p95_ms,
            mixed.log.p95_ms,
            mixed.metric.p95_ms
        );
    }
    if let (Some(query), Some(mixed)) = (query, mixed) {
        let delta = mixed.query.p95_ms as i128 - query.query.p95_ms as i128;
        let pct = if query.query.p95_ms > 0 {
            100.0 * delta as f64 / query.query.p95_ms as f64
        } else {
            0.0
        };
        println!(
            "Query p95: query_only={}ms mixed={}ms Δ={:+}ms ({:+.1}%)",
            query.query.p95_ms, mixed.query.p95_ms, delta, pct
        );
        println!(
            "Query avg: query_only={}ms mixed={}ms",
            query.query.avg_ms, mixed.query.avg_ms
        );
        println!(
            "Query steady errors: query_only={} mixed={}",
            query.query.steady_errors, mixed.query.steady_errors
        );
    }
    println!("==========================================");
}

fn avg_ms(durations: &[Duration]) -> u128 {
    if durations.is_empty() {
        return 0;
    }
    durations
        .iter()
        .map(|d| d.as_millis())
        .sum::<u128>()
        .checked_div(durations.len() as u128)
        .unwrap_or(0)
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
