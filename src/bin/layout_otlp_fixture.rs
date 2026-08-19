//! OTLP metrics protobuf poster for metrics-layout fixtures.
//!
//! Reads NDJSON from stdin, one command per line:
//!   {"name":"layout_tall","kind":"gauge","labels":{"job":"tall","instance":"t0"},
//!    "points":[[1700000000000000000,1.0],...]}
//!   {"name":"layout_http","kind":"sum","labels":{"job":"job-0","instance":"inst-0"},
//!    "points":[[...],[...]]}
//!   {"name":"layout_latency","kind":"histogram","labels":{"job":"hist","instance":"h-0"},
//!    "bounds":[0.005,0.01],"points":[[ts,[1,1,1],3,0.1],...]}
//!
//! Usage:
//!   layout_otlp_fixture --url http://127.0.0.1:18091 --token local-dev-key < cmds.ndjson
//!   # Greptime G9 (OTLP metrics path, keep PromQL names):
//!   layout_otlp_fixture --url http://127.0.0.1:14000 --token "" \
//!     --metrics-path /v1/otlp/v1/metrics \
//!     --header 'x-greptime-otlp-metric-translation-strategy:NoTranslation' < cmds.ndjson

use anyhow::{Context, Result};
use clap::Parser;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, AggregationTemporality, Gauge, Histogram, HistogramDataPoint,
    Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use serde::Deserialize;
use serde_json::Value;
use std::io::{BufRead, BufReader};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:18091")]
    url: String,
    #[arg(long, default_value = "local-dev-key")]
    token: String,
    /// HTTP path for OTLP metrics POST (Softprobe `/v1/metrics`; Greptime `/v1/otlp/v1/metrics`).
    #[arg(long, default_value = "/v1/metrics")]
    metrics_path: String,
    /// Extra request headers as `Name:Value` (repeatable). Used for Greptime OTLP translation.
    #[arg(long = "header", value_name = "Name:Value")]
    headers: Vec<String>,
    /// When set, emit layout_ingest_heartbeat once per second forever (AC-Q0 / G9).
    #[arg(long, default_value_t = 0.0)]
    heartbeat_secs: f64,
}

fn parse_header(raw: &str) -> Result<(String, String)> {
    let (k, v) = raw
        .split_once(':')
        .with_context(|| format!("header must be Name:Value, got {raw}"))?;
    Ok((k.trim().to_string(), v.trim().to_string()))
}

#[derive(Debug, Deserialize)]
struct Cmd {
    name: String,
    kind: String,
    #[serde(default)]
    labels: std::collections::HashMap<String, String>,
    #[serde(default)]
    points: Vec<Value>,
    #[serde(default)]
    bounds: Vec<f64>,
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}

fn resource_from_labels(labels: &std::collections::HashMap<String, String>) -> Resource {
    let mut attrs = Vec::new();
    if let Some(job) = labels.get("job").or_else(|| labels.get("service.name")) {
        attrs.push(kv("service.name", job));
    }
    if let Some(inst) = labels
        .get("instance")
        .or_else(|| labels.get("service.instance.id"))
    {
        attrs.push(kv("service.instance.id", inst));
    }
    Resource {
        attributes: attrs,
        dropped_attributes_count: 0,
    }
}

fn point_attrs(labels: &std::collections::HashMap<String, String>) -> Vec<KeyValue> {
    labels
        .iter()
        .filter(|(k, _)| {
            !matches!(
                k.as_str(),
                "job" | "instance" | "service.name" | "service.instance.id"
            )
        })
        .map(|(k, v)| kv(k, v))
        .collect()
}

fn build_request(cmd: &Cmd) -> Result<ExportMetricsServiceRequest> {
    Ok(ExportMetricsServiceRequest {
        resource_metrics: vec![resource_metrics_for_cmd(cmd)?],
    })
}

fn resource_metrics_for_cmd(cmd: &Cmd) -> Result<ResourceMetrics> {
    let resource = resource_from_labels(&cmd.labels);
    let pattrs = point_attrs(&cmd.labels);
    let data = match cmd.kind.as_str() {
        "gauge" => {
            let mut dps = Vec::new();
            for p in &cmd.points {
                let arr = p.as_array().context("gauge point array")?;
                let ts = arr[0]
                    .as_u64()
                    .or_else(|| arr[0].as_i64().map(|v| v as u64))
                    .context("ts")?;
                let val = arr[1].as_f64().context("val")?;
                dps.push(NumberDataPoint {
                    attributes: pattrs.clone(),
                    start_time_unix_nano: 0,
                    time_unix_nano: ts,
                    exemplars: vec![],
                    flags: 0,
                    value: Some(number_data_point::Value::AsDouble(val)),
                });
            }
            metric::Data::Gauge(Gauge { data_points: dps })
        }
        "sum" => {
            let mut dps = Vec::new();
            for p in &cmd.points {
                let arr = p.as_array().context("sum point array")?;
                let ts = arr[0]
                    .as_u64()
                    .or_else(|| arr[0].as_i64().map(|v| v as u64))
                    .context("ts")?;
                let val = arr[1].as_f64().context("val")?;
                dps.push(NumberDataPoint {
                    attributes: pattrs.clone(),
                    start_time_unix_nano: 0,
                    time_unix_nano: ts,
                    exemplars: vec![],
                    flags: 0,
                    value: Some(number_data_point::Value::AsDouble(val)),
                });
            }
            metric::Data::Sum(Sum {
                data_points: dps,
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })
        }
        "histogram" => {
            let mut dps = Vec::new();
            for p in &cmd.points {
                let arr = p.as_array().context("hist point")?;
                let ts = arr[0]
                    .as_u64()
                    .or_else(|| arr[0].as_i64().map(|v| v as u64))
                    .context("ts")?;
                let buckets: Vec<u64> = arr[1]
                    .as_array()
                    .context("buckets")?
                    .iter()
                    .map(|v| v.as_u64().unwrap_or(0))
                    .collect();
                let count = arr[2].as_u64().unwrap_or(0);
                let sum = arr[3].as_f64().unwrap_or(0.0);
                dps.push(HistogramDataPoint {
                    attributes: pattrs.clone(),
                    start_time_unix_nano: 0,
                    time_unix_nano: ts,
                    count,
                    sum: Some(sum),
                    bucket_counts: buckets,
                    explicit_bounds: cmd.bounds.clone(),
                    exemplars: vec![],
                    flags: 0,
                    min: None,
                    max: None,
                });
            }
            metric::Data::Histogram(Histogram {
                data_points: dps,
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })
        }
        other => anyhow::bail!("unknown kind {other}"),
    };

    Ok(ResourceMetrics {
        resource: Some(resource),
        scope_metrics: vec![ScopeMetrics {
            scope: None,
            metrics: vec![Metric {
                name: cmd.name.clone(),
                description: String::new(),
                unit: "1".into(),
                metadata: vec![],
                data: Some(data),
            }],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    })
}

fn build_batch_request(cmds: &[Cmd]) -> Result<ExportMetricsServiceRequest> {
    let mut resource_metrics = Vec::with_capacity(cmds.len());
    for cmd in cmds {
        resource_metrics.push(resource_metrics_for_cmd(cmd)?);
    }
    Ok(ExportMetricsServiceRequest { resource_metrics })
}

fn apply_headers(
    mut builder: reqwest::blocking::RequestBuilder,
    token: &str,
    extra: &[(String, String)],
) -> reqwest::blocking::RequestBuilder {
    if !token.is_empty() {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    builder = builder.header("Content-Type", "application/x-protobuf");
    for (k, v) in extra {
        builder = builder.header(k, v);
    }
    builder
}

fn post_with_retries(
    client: &reqwest::blocking::Client,
    url: &str,
    token: &str,
    extra: &[(String, String)],
    bytes: Vec<u8>,
) -> Result<()> {
    let mut last_err = String::new();
    for attempt in 0..24 {
        let resp = apply_headers(client.post(url), token, extra)
            .body(bytes.clone())
            .send()
            .context("post")?;
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        if status.is_success() {
            return Ok(());
        }
        last_err = format!("OTLP HTTP {status}: {body}");
        if body.contains("Transaction conflict")
            || body.contains("Failed to commit")
            || status.as_u16() == 503
        {
            std::thread::sleep(std::time::Duration::from_millis(80 * (attempt + 1) as u64));
            continue;
        }
        anyhow::bail!("{last_err}");
    }
    anyhow::bail!("{last_err}")
}

fn main() -> Result<()> {
    let args = Args::parse();
    let path = if args.metrics_path.starts_with('/') {
        args.metrics_path.clone()
    } else {
        format!("/{}", args.metrics_path)
    };
    let url = format!("{}{}", args.url.trim_end_matches('/'), path);
    let extra: Vec<(String, String)> = args
        .headers
        .iter()
        .map(|h| parse_header(h))
        .collect::<Result<_>>()?;
    // release_full F-collapse posts can take minutes under DuckLake commit pressure;
    // reqwest's default (~30s) aborts mid-fixture and leaves the ready gate unwritable.
    let timeout_secs: u64 = std::env::var("LAYOUT_OTLP_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .connect_timeout(std::time::Duration::from_secs(30))
        .build()
        .context("build http client")?;

    if args.heartbeat_secs > 0.0 {
        let mut n = 0u64;
        loop {
            n += 1;
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let cmd = Cmd {
                name: "layout_ingest_heartbeat".into(),
                kind: "gauge".into(),
                labels: [
                    ("job".into(), "heartbeat".into()),
                    ("instance".into(), "hb0".into()),
                ]
                .into_iter()
                .collect(),
                points: vec![serde_json::json!([now_ns, n as f64])],
                bounds: vec![],
            };
            let req = build_request(&cmd)?;
            let bytes = req.encode_to_vec();
            let _ = apply_headers(client.post(&url), &args.token, &extra)
                .body(bytes)
                .send();
            std::thread::sleep(std::time::Duration::from_secs_f64(args.heartbeat_secs));
        }
    }

    let stdin = std::io::stdin();
    let reader = BufReader::new(stdin.lock());
    let mut batches = 0u64;
    let mut points = 0u64;
    let mut pending: Vec<Cmd> = Vec::new();
    // Keep flush size near collector batch scale; tiny flushes amplify DuckLake
    // small-file storms on release_full. Override with LAYOUT_OTLP_FLUSH_EVERY.
    // Default 500 cuts HTTP round-trips for thin F-collapse (I=1) without huge payloads.
    let flush_every: usize = std::env::var("LAYOUT_OTLP_FLUSH_EVERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);

    let flush = |pending: &mut Vec<Cmd>,
                 batches: &mut u64,
                 client: &reqwest::blocking::Client,
                 url: &str,
                 token: &str,
                 extra: &[(String, String)]|
     -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let req = build_batch_request(pending)?;
        post_with_retries(client, url, token, extra, req.encode_to_vec())?;
        *batches += 1;
        pending.clear();
        Ok(())
    };

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let cmd: Cmd = serde_json::from_str(&line).with_context(|| format!("parse {line}"))?;
        points += cmd.points.len() as u64;
        pending.push(cmd);
        if pending.len() >= flush_every {
            flush(
                &mut pending,
                &mut batches,
                &client,
                &url,
                &args.token,
                &extra,
            )?;
        }
    }
    flush(
        &mut pending,
        &mut batches,
        &client,
        &url,
        &args.token,
        &extra,
    )?;
    eprintln!("layout_otlp_fixture ok batches={batches} points={points}");
    Ok(())
}
