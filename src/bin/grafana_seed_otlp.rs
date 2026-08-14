//! Emit OTLP metrics protobuf for `make grafana-up` demo seeding.
//!
//! Writes fresh timestamps (last ~1h) so Grafana `now-1h` panels are non-empty.
//! Usage: `grafana_seed_otlp [out.bin]` then `curl -X POST …/v1/metrics --data-binary @out.bin`
//!
//! OTLP gauge encoding mirrors `tests/compat/support/prometheus.rs::gauge_series_otlp`
//! (kept here so the bin does not depend on the test harness).
//!
//! Demo shapes (so panels are not all identical ramps):
//! - checkout: linear +0.5 / 30s  → raw / avg_over_time rise; rate ≈ flat 0.0167/s
//! - payments: sine around 50     → visually distinct from checkout

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use std::f64::consts::PI;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let out = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/thelake-grafana-manual/seed-otlp.bin".into());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as u64;

    // Dense samples over the last hour so rate([5m]) / over_time panels have ≥2 points.
    let mut resource_metrics = Vec::new();
    for job in ["checkout", "payments"] {
        let mut data_points = Vec::new();
        // 30s spacing × 120 ≈ 1h
        for i in 0..120u64 {
            let ts = now.saturating_sub((119 - i) * 30 * 1_000_000_000);
            let value = match job {
                "checkout" => 40.0 + (i as f64) * 0.5,
                // ~10 minute period sine: visually not a ramp
                "payments" => 50.0 + 15.0 * (2.0 * PI * (i as f64) / 20.0).sin(),
                _ => unreachable!(),
            };
            data_points.push(NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: ts,
                exemplars: vec![],
                flags: 0,
                value: Some(number_data_point::Value::AsDouble(value)),
            });
        }
        resource_metrics.push(ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(job.into())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "http.requests".into(),
                    description: "grafana-up demo gauge".into(),
                    unit: "1".into(),
                    data: Some(Data::Gauge(Gauge { data_points })),
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        });
    }

    let bytes = ExportMetricsServiceRequest { resource_metrics }.encode_to_vec();
    if let Some(parent) = std::path::Path::new(&out).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(&out, &bytes).unwrap_or_else(|e| panic!("write {out}: {e}"));
    eprintln!("wrote {} bytes to {out}", bytes.len());
}
