use chrono::Utc;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use softprobe_runtime::compaction::executor::MaintenanceExecutor;
use softprobe_runtime::config::Config;

#[tokio::main]
async fn main() {
    if std::env::var("MAINTENANCE_RUN_ONCE").ok().as_deref() == Some("1") {
        let config = Config::load().expect("failed to load config");
        let executor = MaintenanceExecutor::new(&config)
            .await
            .expect("failed to create maintenance executor");
        let summary = executor.run_once().await.expect("maintenance run failed");
        println!("maintenance_tables={}", summary.tables.len());
        for table in summary.tables {
            println!(
                "table={} expired_snapshots={} compaction={:?} rewrite_manifests={:?} remove_orphans={:?}",
                table.table,
                table.metadata.expired_snapshots,
                table.compaction.status,
                table.rewrite_manifests.status,
                table.remove_orphan_files.status
            );
        }
        return;
    }

    let trace_id = vec![1u8; 16];
    let span_id = vec![2u8; 8];
    let now = Utc::now();
    let start = now.timestamp_nanos_opt().unwrap_or(0) as u64;
    let end = start + 1_000_000; // +1ms

    let span = Span {
        trace_id,
        span_id,
        parent_span_id: vec![],
        name: "demo-span".to_string(),
        kind: 1, // INTERNAL
        start_time_unix_nano: start,
        end_time_unix_nano: end,
        attributes: vec![KeyValue {
            key: "sp.session.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(format!(
                    "it-{}",
                    uuid::Uuid::new_v4()
                ))),
            }),
        }],
        ..Default::default()
    };

    let scope = ScopeSpans {
        scope: None,
        spans: vec![span],
        schema_url: String::new(),
    };
    let resource = Resource {
        attributes: vec![KeyValue {
            key: "sp.app.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "test_application".to_string(),
                )),
            }),
        }],
        dropped_attributes_count: 0,
    };
    let rs = ResourceSpans {
        resource: Some(resource),
        scope_spans: vec![scope],
        schema_url: String::new(),
    };
    let req = ExportTraceServiceRequest {
        resource_spans: vec![rs],
    };

    let bytes = req.encode_to_vec();
    let client = reqwest::Client::new();
    let url =
        std::env::var("OTLP_URL").unwrap_or_else(|_| "http://localhost:8090/v1/traces".to_string());
    let resp = client
        .post(url)
        .header("content-type", "application/x-protobuf")
        .body(bytes)
        .send()
        .await
        .unwrap();
    println!("status={}", resp.status());
    let txt = resp.text().await.unwrap();
    println!("body={}", txt);
}
