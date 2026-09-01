//! High-cardinality OTLP metrics loadgen for Prom kill-case benchmarks.

use anyhow::Result;
use clap::Parser;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(about = "Post high-cardinality OTLP gauge metrics to Softprobe")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8090")]
    url: String,
    #[arg(long, default_value = "local-dev-key")]
    token: String,
    #[arg(long, default_value = "bench.http.requests")]
    metric: String,
    #[arg(long, default_value_t = 40)]
    jobs: usize,
    #[arg(long, default_value_t = 3)]
    instances: usize,
    #[arg(long, default_value_t = 30.0)]
    seconds: f64,
    #[arg(long, default_value_t = 0.4)]
    interval: f64,
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}

fn build_batch(
    metric_name: &str,
    jobs: usize,
    instances: usize,
    tick: u64,
    now_ns: u64,
) -> ExportMetricsServiceRequest {
    let mut resource_metrics = Vec::with_capacity(jobs * instances);
    for j in 0..jobs {
        let job = format!("svc-{j:03}");
        for i in 0..instances {
            let instance = format!("{job}-i{i}");
            let value = ((tick + j as u64 + i as u64) % 100) as f64;
            resource_metrics.push(ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        kv("service.name", &job),
                        kv("service.instance.id", &instance),
                        kv("deployment.environment", "bench"),
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: metric_name.into(),
                        description: "bench gauge".into(),
                        unit: "1".into(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![
                                    kv("http.method", "GET"),
                                    kv("sp.bench.shard", &format!("shard-{}", j % 8)),
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: now_ns,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(value)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            });
        }
    }
    ExportMetricsServiceRequest { resource_metrics }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let endpoint = format!("{}/v1/metrics", args.url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs_f64(args.seconds);
    let mut tick = 0u64;
    let mut posted = 0usize;
    while Instant::now() < deadline {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let req = build_batch(&args.metric, args.jobs, args.instances, tick, now_ns);
        let bytes = req.encode_to_vec();
        let resp = client
            .post(&endpoint)
            .header("content-type", "application/x-protobuf")
            .header("authorization", format!("Bearer {}", args.token))
            .body(bytes)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("ingest failed: {status} {body}");
        }
        posted += args.jobs * args.instances;
        tick += 1;
        tokio::time::sleep(Duration::from_secs_f64(args.interval)).await;
    }
    println!(
        "loadgen done points={posted} jobs={} instances={}",
        args.jobs, args.instances
    );
    Ok(())
}
