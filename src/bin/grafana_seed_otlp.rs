//! Deterministic OTLP protobuf seeder for the self-contained Grafana system lane.
//!
//! The seeder authenticates each fixed test tenant separately, sends metrics,
//! logs, and traces through the runtime HTTP OTLP endpoints, then polls the
//! native compatibility routes until all three signals are queryable without
//! cross-tenant leakage. Only a credential-free receipt is written.

use anyhow::{anyhow, Context, Result};
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span};
use prost::Message;
use reqwest::blocking::{Client, Response};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

const START_S: u64 = 1_700_000_000;
const END_S: u64 = 1_700_000_060;
const START_NS: u64 = START_S * 1_000_000_000;
const END_NS: u64 = END_S * 1_000_000_000;
const METRIC_NAME: &str = "grafana_phase4_requests_total";
const SERVICE_NAME: &str = "checkout";

#[derive(Debug, Clone)]
struct TenantPayloads {
    metrics: Vec<u8>,
    logs: Vec<u8>,
    traces: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct Receipt {
    schema_version: u8,
    status: String,
    base_url: String,
    window_start_s: u64,
    window_end_s: u64,
    tenants: Vec<TenantReceipt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct TenantReceipt {
    tenant_id: String,
    scope_provisioned: bool,
    metrics_sent: bool,
    logs_sent: bool,
    traces_sent: bool,
    metrics_queryable: bool,
    logs_queryable: bool,
    traces_queryable: bool,
    trace_id: String,
}

fn main() {
    let base_url = env::var("GRAFANA_SEED_SOFTPROBE_URL")
        .or_else(|_| env::var("SOFTPROBE_URL"))
        .unwrap_or_else(|_| "http://127.0.0.1:18090".into())
        .trim_end_matches('/')
        .to_string();
    let receipt_path = env::var("GRAFANA_SEED_RECEIPT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/compat/grafana/seed-receipt.json"));
    let tenant_a =
        env::var("SOFTPROBE_TENANT_A_ID").unwrap_or_else(|_| "grafana-phase4-tenant-a".into());
    let tenant_b =
        env::var("SOFTPROBE_TENANT_B_ID").unwrap_or_else(|_| "grafana-phase4-tenant-b".into());
    let key_a =
        env::var("SOFTPROBE_TENANT_A_API_KEY").unwrap_or_else(|_| "grafana-phase4-tenant-a".into());
    let key_b =
        env::var("SOFTPROBE_TENANT_B_API_KEY").unwrap_or_else(|_| "grafana-phase4-tenant-b".into());
    let admin_key =
        env::var("SOFTPROBE_ADMIN_API_KEY").unwrap_or_else(|_| "grafana-phase4-admin".into());

    let mut receipt = Receipt {
        schema_version: 1,
        status: "running".into(),
        base_url: "local-runtime".into(),
        window_start_s: START_S,
        window_end_s: END_S,
        tenants: vec![
            TenantReceipt::new(tenant_a.clone(), "a"),
            TenantReceipt::new(tenant_b.clone(), "b"),
        ],
        error: None,
    };

    match seed(
        &base_url,
        &[(tenant_a, key_a, "a"), (tenant_b, key_b, "b")],
        &admin_key,
        &mut receipt,
    ) {
        Ok(()) => {
            receipt.status = "pass".into();
            write_receipt(&receipt_path, &receipt).expect("write seed receipt");
            println!("seed receipt: {}", receipt_path.display());
        }
        Err(error) => {
            receipt.status = "failure".into();
            receipt.error = Some(error.to_string());
            write_receipt(&receipt_path, &receipt).expect("write failure receipt");
            eprintln!("Grafana OTLP seed failed: {error:#}");
            std::process::exit(1);
        }
    }
}

impl TenantReceipt {
    fn new(tenant_id: String, suffix: &str) -> Self {
        Self {
            tenant_id,
            scope_provisioned: false,
            metrics_sent: false,
            logs_sent: false,
            traces_sent: false,
            metrics_queryable: false,
            logs_queryable: false,
            traces_queryable: false,
            trace_id: trace_id_for(suffix),
        }
    }
}

fn seed(
    base_url: &str,
    tenants: &[(String, String, &str)],
    admin_key: &str,
    receipt: &mut Receipt,
) -> Result<()> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .build()
        .context("build seed HTTP client")?;

    wait_ready(&client, base_url)?;
    for (index, (tenant_id, api_key, _suffix)) in tenants.iter().enumerate() {
        let payloads = tenant_payloads(tenant_id);
        provision_tenant(&client, base_url, admin_key, tenant_id, index)?;
        receipt.tenants[index].scope_provisioned = true;
        if let Err(error) = send_payloads(
            &client,
            base_url,
            api_key,
            &payloads,
            &mut receipt.tenants[index],
        ) {
            return Err(anyhow!(
                "tenant {} partial ingest (metrics={}, logs={}, traces={}): {}",
                tenant_id,
                receipt.tenants[index].metrics_sent,
                receipt.tenants[index].logs_sent,
                receipt.tenants[index].traces_sent,
                error
            ));
        }
        receipt.tenants[index].metrics_sent = true;
        receipt.tenants[index].logs_sent = true;
        receipt.tenants[index].traces_sent = true;
    }

    // Cold-start DuckLake flush (first Parquet write + catalog DDL) can take
    // minutes; poll well past that instead of failing right after ingest.
    let last_attempt = 299;
    for attempt in 0..=last_attempt {
        let mut all_queryable = true;
        for (index, (tenant_id, api_key, suffix)) in tenants.iter().enumerate() {
            let result = query_tenant(&client, base_url, api_key, tenant_id, suffix, receipt);
            match result {
                Ok(()) => {
                    receipt.tenants[index].metrics_queryable = true;
                    receipt.tenants[index].logs_queryable = true;
                    receipt.tenants[index].traces_queryable = true;
                }
                Err(error) if attempt == last_attempt => {
                    return Err(anyhow!("tenant {tenant_id} queryability timeout: {error}"));
                }
                Err(_) => {
                    all_queryable = false;
                }
            }
        }
        if all_queryable {
            return Ok(());
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(anyhow!("queryability polling exhausted"))
}

fn provision_tenant(
    client: &Client,
    base_url: &str,
    admin_key: &str,
    tenant_id: &str,
    index: usize,
) -> Result<()> {
    let body = serde_json::json!({
        "tenantId": tenant_id,
        "storageHints": {
            "ducklakeMetadataSchema": format!("grafana_phase4_{index}"),
            "ducklakeDataPath": format!("s3://warehouse/grafana_phase4_{index}/"),
            "gcsBucket": "warehouse"
        }
    });
    let response = client
        .post(format!("{base_url}/v1/tenants"))
        .header("authorization", format!("Bearer {admin_key}"))
        .json(&body)
        .send()
        .context("provision Grafana test tenant")?;
    ensure_success(response, "/v1/tenants")
}

fn wait_ready(client: &Client, base_url: &str) -> Result<()> {
    let url = format!("{base_url}/ready");
    for _ in 0..60 {
        if let Ok(response) = client.get(&url).send() {
            if response.status().is_success() {
                return Ok(());
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(anyhow!("Softprobe readiness did not become healthy"))
}

fn send_payloads(
    client: &Client,
    base_url: &str,
    api_key: &str,
    payloads: &TenantPayloads,
    receipt: &mut TenantReceipt,
) -> Result<()> {
    send_protobuf(client, base_url, api_key, "/v1/metrics", &payloads.metrics)?;
    receipt.metrics_sent = true;
    send_protobuf(client, base_url, api_key, "/v1/logs", &payloads.logs)?;
    receipt.logs_sent = true;
    send_protobuf(client, base_url, api_key, "/v1/traces", &payloads.traces)?;
    receipt.traces_sent = true;
    Ok(())
}

fn send_protobuf(
    client: &Client,
    base_url: &str,
    api_key: &str,
    path: &str,
    body: &[u8],
) -> Result<()> {
    // Retry transient failures: the OTLP ingest port may not be ready the
    // instant /ready returns, and network errors are intermittent under load.
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=8 {
        match client
            .post(format!("{base_url}{path}"))
            .header("authorization", format!("Bearer {api_key}"))
            .header("content-type", "application/x-protobuf")
            .body(body.to_vec())
            .send()
        {
            Ok(response) => match ensure_success(response, path) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let retryable = e.to_string().contains("HTTP 5");
                    last_err = Some(e);
                    if !retryable {
                        return Err(last_err.unwrap());
                    }
                }
            },
            Err(e) => {
                last_err = Some(e.into());
            }
        }
        if attempt < 8 {
            eprintln!("retrying POST {path} (attempt {attempt})");
            thread::sleep(Duration::from_secs(2));
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("POST {path} failed")))
}

fn query_tenant(
    client: &Client,
    base_url: &str,
    api_key: &str,
    tenant_id: &str,
    suffix: &str,
    receipt: &Receipt,
) -> Result<()> {
    let other = receipt
        .tenants
        .iter()
        .find(|tenant| tenant.tenant_id != tenant_id)
        .map(|tenant| tenant.tenant_id.as_str())
        .unwrap_or_default();
    let headers = |request: reqwest::blocking::RequestBuilder| {
        request
            .header("authorization", format!("Bearer {api_key}"))
            .header("x-scope-orgid", tenant_id)
    };

    let metrics = headers(client.get(format!("{base_url}/api/v1/query")))
        .query(&[("query", METRIC_NAME), ("time", "1700000030")])
        .send()
        .context("query seeded metrics")?;
    let metrics = read_json_success(metrics, "/api/v1/query")?;
    if std::env::var("SEED_DEBUG").ok().as_deref() == Some("1") {
        // Non-fatal probe trace: show what the server actually returned so
        // warm-up read inconsistencies are observable without aborting the
        // retry loop.
        eprintln!(
            "SEED_DEBUG tenant={tenant_id} metrics body={}",
            &metrics.to_string()[..metrics.to_string().len().min(240)]
        );
    }
    assert_tenant_scope(&metrics, tenant_id, other)?;

    let logs = headers(client.get(format!("{base_url}/loki/api/v1/query_range")))
        .query(&[
            ("query", r#"{service_name="checkout"}"#),
            ("start", &START_NS.to_string()),
            ("end", &END_NS.to_string()),
            ("limit", "100"),
            ("direction", "forward"),
        ])
        .send()
        .context("query seeded logs")?;
    let logs = read_json_success(logs, "/loki/api/v1/query_range")?;
    if std::env::var("SEED_DEBUG").ok().as_deref() == Some("1") {
        eprintln!(
            "SEED_DEBUG tenant={tenant_id} logs body={}",
            &logs.to_string()[..logs.to_string().len().min(240)]
        );
    }
    assert_tenant_scope(&logs, tenant_id, other)?;

    let traces = headers(client.get(format!("{base_url}/api/search")))
        .query(&[
            ("q", r#"{ resource.service.name = "checkout" }"#),
            ("start", &START_S.to_string()),
            ("end", &END_S.to_string()),
            ("limit", "20"),
        ])
        .send()
        .context("query seeded traces")?;
    let traces = read_json_success(traces, "/api/search")?;
    // Tempo search results carry trace metadata, not tenant labels, so the
    // tenant-id scope assertion used for metrics/logs does not apply here.
    // Isolation is still proven deterministically: every tenant seeds its own
    // fixed trace ID and must see exactly that one, never the other's.
    let traces_text = traces.to_string();
    let own_trace_id = trace_id_for(suffix);
    let other_trace_id = trace_id_for(if suffix == "a" { "b" } else { "a" });
    if traces_text.contains(&other_trace_id) {
        return Err(anyhow!(
            "cross-tenant leakage detected for {tenant_id}: found trace {other_trace_id}"
        ));
    }
    if !traces_text.contains(&own_trace_id) {
        return Err(anyhow!(
            "response did not contain expected trace {own_trace_id}"
        ));
    }
    Ok(())
}

fn read_json_success(response: Response, path: &str) -> Result<Value> {
    let status = response.status();
    let body = response.text().context("read query response")?;
    if !status.is_success() {
        return Err(anyhow!("{path} returned HTTP {status}"));
    }
    serde_json::from_str(&body).with_context(|| format!("decode {path} response"))
}

fn ensure_success(response: Response, path: &str) -> Result<()> {
    let status = response.status();
    let _ = response.text();
    if status.is_success() {
        Ok(())
    } else {
        Err(anyhow!("{path} returned HTTP {status}"))
    }
}

fn assert_tenant_scope(value: &Value, expected: &str, other: &str) -> Result<()> {
    let text = value.to_string();
    if text.contains(other) {
        return Err(anyhow!("cross-tenant leakage detected for {expected}"));
    }
    if !text.contains(expected) {
        return Err(anyhow!(
            "response did not contain expected tenant {expected}"
        ));
    }
    Ok(())
}

fn write_receipt(path: &Path, receipt: &Receipt) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(receipt)?)?;
    Ok(())
}

fn tenant_payloads(tenant: &str) -> TenantPayloads {
    let suffix = if tenant.ends_with("-b") { "b" } else { "a" };
    let trace_id = trace_id_for(suffix);
    let span_id = if suffix == "a" {
        vec![0x11; 8]
    } else {
        vec![0x22; 8]
    };
    let resource = resource(tenant);
    let metrics = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource.clone()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(scope("grafana-seeder")),
                metrics: vec![Metric {
                    name: METRIC_NAME.into(),
                    description: "deterministic Grafana Phase 4 metric".into(),
                    unit: "1".into(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![
                            number_point(START_NS, 1.0, tenant),
                            number_point(START_NS + 30_000_000_000, 2.0, tenant),
                        ],
                    })),
                    metadata: Vec::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec();
    let logs = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource.clone()),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope("grafana-seeder")),
                log_records: vec![LogRecord {
                    time_unix_nano: START_NS + 15_000_000_000,
                    observed_time_unix_nano: START_NS + 15_000_000_000,
                    severity_number: 17,
                    severity_text: "ERROR".into(),
                    body: Some(string_any(&format!(
                        r#"{{"level":"error","message":"grafana phase4","tenant_marker":"{tenant}","trace_id":"{trace_id}"}}"#
                    ))),
                    attributes: vec![kv("tenant.marker", tenant), kv("trace_id", &trace_id)],
                    trace_id: hex::decode(&trace_id).unwrap(),
                    span_id: span_id.clone(),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec();
    let traces = ExportTraceServiceRequest {
        resource_spans: vec![
            ResourceSpans {
                resource: Some(resource.clone()),
                scope_spans: vec![ScopeSpans {
                    scope: Some(scope("grafana-seeder")),
                    spans: vec![
                        Span {
                            trace_id: hex::decode(&trace_id).unwrap(),
                            span_id: span_id.clone(),
                            name: "checkout".into(),
                            kind: span::SpanKind::Server as i32,
                            start_time_unix_nano: START_NS + 10_000_000_000,
                            end_time_unix_nano: START_NS + 11_000_000_000,
                            attributes: vec![kv("tenant.marker", tenant)],
                            status: Some(opentelemetry_proto::tonic::trace::v1::Status {
                                message: String::new(),
                                code: opentelemetry_proto::tonic::trace::v1::status::StatusCode::Ok
                                    as i32,
                            }),
                            ..Default::default()
                        },
                        {
                            // Child DB call with an event, proving topology and
                            // event preservation through the lake.
                            let child_span_id = span_id.iter().map(|b| b.wrapping_add(1)).collect();
                            Span {
                                trace_id: hex::decode(&trace_id).unwrap(),
                                span_id: child_span_id,
                                parent_span_id: span_id.clone(),
                                name: "db.query".into(),
                                kind: span::SpanKind::Client as i32,
                                start_time_unix_nano: START_NS + 10_100_000_000,
                                end_time_unix_nano: START_NS + 10_900_000_000,
                                attributes: vec![
                                    kv("tenant.marker", tenant),
                                    kv("db.system", "postgres"),
                                ],
                                events: vec![opentelemetry_proto::tonic::trace::v1::span::Event {
                                    time_unix_nano: START_NS + 10_500_000_000,
                                    name: "cache.miss".into(),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
            {
                // Second service in the same trace so rich assertions can prove
                // multi-ResourceSpans preservation (payments calls checkout).
                let mut payment_resource = resource.clone();
                payment_resource
                    .attributes
                    .push(kv("peer.service", "checkout"));
                ResourceSpans {
                    resource: Some(payment_resource),
                    scope_spans: vec![ScopeSpans {
                        scope: Some(scope("grafana-seeder")),
                        spans: vec![{
                            let child_span_id = span_id.iter().map(|b| b.wrapping_add(2)).collect();
                            Span {
                                trace_id: hex::decode(&trace_id).unwrap(),
                                span_id: child_span_id,
                                parent_span_id: span_id.clone(),
                                name: "charge".into(),
                                kind: span::SpanKind::Client as i32,
                                start_time_unix_nano: START_NS + 10_200_000_000,
                                end_time_unix_nano: START_NS + 10_800_000_000,
                                attributes: vec![
                                    kv("tenant.marker", tenant),
                                    kv("http.request.method", "POST"),
                                ],
                                links: vec![opentelemetry_proto::tonic::trace::v1::span::Link {
                                    trace_id: hex::decode(&trace_id).unwrap(),
                                    span_id: span_id.iter().map(|b| b.wrapping_add(1)).collect(),
                                    attributes: vec![kv("link.type", "cached_call")],
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }
            },
        ],
    }
    .encode_to_vec();
    TenantPayloads {
        metrics,
        logs,
        traces,
    }
}

fn resource(tenant: &str) -> Resource {
    Resource {
        attributes: vec![
            kv("service.name", SERVICE_NAME),
            kv("tenant.marker", tenant),
        ],
        ..Default::default()
    }
}

fn scope(name: &str) -> InstrumentationScope {
    InstrumentationScope {
        name: name.into(),
        version: "1.0.0".into(),
        ..Default::default()
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(string_any(value)),
    }
}

fn string_any(value: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value.into())),
    }
}

fn number_point(timestamp: u64, value: f64, tenant: &str) -> NumberDataPoint {
    NumberDataPoint {
        attributes: vec![kv("tenant.marker", tenant)],
        time_unix_nano: timestamp,
        value: Some(number_data_point::Value::AsDouble(value)),
        ..Default::default()
    }
}

fn trace_id_for(suffix: &str) -> String {
    if suffix == "b" {
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()
    } else {
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;

    #[test]
    fn payloads_are_deterministic_and_tenant_bound() {
        let tenant_a = tenant_payloads("grafana-phase4-tenant-a");
        let tenant_b = tenant_payloads("grafana-phase4-tenant-b");

        assert_eq!(trace_id_for("a"), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(trace_id_for("b"), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert_ne!(tenant_a.metrics, tenant_b.metrics);

        let metrics = ExportMetricsServiceRequest::decode(tenant_a.metrics.as_slice()).unwrap();
        let resource = metrics.resource_metrics[0].resource.as_ref().unwrap();
        let marker = resource
            .attributes
            .iter()
            .find(|attribute| attribute.key == "tenant.marker")
            .unwrap();
        let marker_value = marker
            .value
            .as_ref()
            .and_then(|value| value.value.as_ref())
            .and_then(|value| match value {
                any_value::Value::StringValue(value) => Some(value.as_str()),
                _ => None,
            })
            .unwrap_or_default();
        assert_eq!(marker_value, "grafana-phase4-tenant-a");
    }
}
