//! Shared Loki Phase 2 fixture/client/oracle helpers.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
#[cfg(feature = "integration-e2e")]
use std::process::Command;
#[cfg(feature = "integration-e2e")]
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tower::ServiceExt;

#[cfg(feature = "integration-e2e")]
use crate::compat_support::lifecycle;
use crate::compat_support::prometheus::{encode_query_owned, get_json_as, get_json_bearer};
use crate::compat_support::prometheus_oracle::build_tenant_router_with_state;

pub const PHASE2_EPOCH_NS: i64 = 1_786_827_600_000_000_000;
pub const FIXTURE_LAG_NS: i64 = 1_000_000_000;

/// Current UNIX epoch nanoseconds (test-clock helper for time-shift math).
pub fn system_time_now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX epoch")
            .as_nanos(),
    )
    .expect("nanoseconds fit i64")
}

#[derive(Debug, Clone, Deserialize)]
pub struct LokiFixture {
    pub evidence: EvidenceMetadata,
    pub capability: LokiCapability,
    pub records: Vec<LokiRecord>,
    pub cases: Vec<LokiCase>,
}

impl LokiFixture {
    pub fn shifted_by(&self, delta_ns: i64) -> Self {
        let mut shifted = self.clone();
        for record in &mut shifted.records {
            record.timestamp_ns = shift_timestamp_ns(&record.timestamp_ns, delta_ns);
        }
        for case in &mut shifted.cases {
            for bound in ["start", "end", "time"] {
                if let Some(value) = case.params.get_mut(bound) {
                    *value = shift_timestamp_ns(value, delta_ns);
                }
            }
        }
        shifted
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EvidenceMetadata {
    pub issue: String,
    pub phase: String,
    pub reference_manifest: String,
    pub reference_image: String,
    pub normalization: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LokiCapability {
    pub protocol: String,
    pub phase: String,
    pub supported_endpoints: Vec<String>,
    pub supported_features: Vec<String>,
    pub unsupported_features: Vec<String>,
    pub ordering_policy: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LokiRecord {
    pub timestamp_ns: String,
    pub line: String,
    pub stream: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
}

impl LokiRecord {
    pub fn timestamp(&self) -> i64 {
        self.timestamp_ns.parse().expect("fixture timestamp is i64")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LokiCase {
    pub id: String,
    pub path: String,
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    pub expect: LokiExpectation,
    #[serde(default)]
    pub differential: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LokiExpectation {
    pub status: u16,
    pub envelope: String,
    #[serde(default)]
    pub result_type: Option<String>,
    #[serde(default)]
    pub data_shape: Option<String>,
    #[serde(default)]
    pub softprobe_code: Option<String>,
    #[serde(default)]
    pub entry_lines: Vec<String>,
    #[serde(default)]
    pub entry_count: Option<usize>,
    #[serde(default)]
    pub values: Vec<String>,
}

pub fn fixture() -> LokiFixture {
    let path = fixture_dir().join("phase2.json");
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

pub fn fixture_for_now() -> LokiFixture {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_nanos();
    let now_ns = i64::try_from(now_ns).expect("current UNIX epoch nanoseconds fit in i64");
    let target_ns = now_ns
        .checked_sub(FIXTURE_LAG_NS)
        .expect("current UNIX epoch nanoseconds must be after the fixture lag");
    let delta_ns = target_ns
        .checked_sub(PHASE2_EPOCH_NS)
        .expect("current UNIX epoch nanoseconds must be after the Phase 2 epoch");
    fixture().shifted_by(delta_ns)
}

fn shift_timestamp_ns(value: &str, delta_ns: i64) -> String {
    value
        .parse::<i64>()
        .unwrap_or_else(|e| panic!("fixture timestamp is i64: {value:?}: {e}"))
        .checked_add(delta_ns)
        .unwrap_or_else(|| panic!("fixture timestamp overflow: {value:?} + {delta_ns}"))
        .to_string()
}

pub fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compat/loki")
}

pub(crate) fn manifest_reference_image(manifest: &serde_yaml::Value, protocol: &str) -> String {
    let reference = &manifest["references"][protocol];
    let image = reference["image"]
        .as_str()
        .unwrap_or_else(|| panic!("{protocol} image"));
    let tag = reference["tag"]
        .as_str()
        .unwrap_or_else(|| panic!("{protocol} tag"));
    match reference["digest"]
        .as_str()
        .filter(|digest| digest.strip_prefix("sha256:").is_some_and(is_sha256_digest))
    {
        Some(digest) => format!("{}@{}", image, digest),
        None => format!("{}:{}", image, tag),
    }
}

fn is_sha256_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn candidate_reference_image(variable: &str, image: &str) -> String {
    let image = image.trim();
    assert!(!image.is_empty(), "{variable} must not be empty");
    assert!(
        !image.chars().any(char::is_whitespace),
        "{variable} must not contain whitespace"
    );

    if let Some((repository, digest)) = image.rsplit_once("@sha256:") {
        assert!(!repository.is_empty(), "{variable} repository is required");
        assert!(
            is_sha256_digest(digest),
            "{variable} must use a 64-character sha256 digest"
        );
        return image.to_owned();
    }

    let (repository, tag) = image
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("{variable} must be repository@sha256:digest or repository:tag"));
    assert!(!repository.is_empty(), "{variable} repository is required");
    assert!(!tag.is_empty(), "{variable} tag is required");
    assert_ne!(
        tag, "latest",
        "{variable} candidate image must not use the latest tag"
    );
    assert!(!tag.contains('@'), "{variable} tag must be a candidate tag");
    image.to_owned()
}

pub fn reference_image_from_manifest() -> String {
    let manifest: serde_yaml::Value =
        serde_yaml::from_str(include_str!("../../../docs/compat/references.v0.yaml"))
            .expect("references.v0.yaml parses");
    std::env::var("LOKI_REFERENCE_IMAGE")
        .ok()
        .filter(|image| !image.trim().is_empty())
        .map(|image| candidate_reference_image("LOKI_REFERENCE_IMAGE", &image))
        .unwrap_or_else(|| manifest_reference_image(&manifest, "loki"))
}

#[cfg(feature = "integration-e2e")]
fn loki_reference_command() -> Vec<String> {
    vec![
        "-config.file=/etc/loki/local-config.yaml".into(),
        "-validation.reject-old-samples=false".into(),
        "-querier.query-ingesters-within=87600h".into(),
        "-store.max-query-length=87600h".into(),
    ]
}

pub async fn build_loki_router() -> (Router, softprobe_runtime::api::AppState, TempDir) {
    build_tenant_router_with_state().await
}

pub async fn ingest_records(router: &Router, records: &[LokiRecord], tenant_id: Option<&str>) {
    ingest_records_with_bearer(router, records, tenant_id, None).await;
}

pub async fn ingest_records_with_bearer(
    router: &Router,
    records: &[LokiRecord],
    tenant_id: Option<&str>,
    bearer: Option<&str>,
) {
    let request = ExportLogsServiceRequest {
        resource_logs: records
            .iter()
            .map(|record| ResourceLogs {
                resource: Some(Resource {
                    attributes: record
                        .stream
                        .iter()
                        .map(|(key, value)| string_kv(otel_label_name(key), value))
                        .collect(),
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: record.timestamp() as u64,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(record.line.clone())),
                        }),
                        attributes: record
                            .metadata
                            .iter()
                            .map(|(key, value)| string_kv(key, value))
                            .collect(),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    };
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/x-protobuf");
    if let Some(tenant_id) = tenant_id {
        builder = builder.header("x-test-tenant-id", tenant_id);
    }
    if let Some(bearer) = bearer {
        builder = builder.header("Authorization", format!("Bearer {bearer}"));
    }
    let response = router
        .clone()
        .oneshot(builder.body(Body::from(request.encode_to_vec())).unwrap())
        .await
        .expect("log ingest request");
    let status = response.status();
    if status != StatusCode::OK {
        // Include the response body (e.g. DuckLake write failure details) in
        // the panic message; the bare status code is rarely enough to debug.
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_default();
        assert_eq!(
            status,
            StatusCode::OK,
            "log ingest status={status} body={body}"
        );
    }
}

pub async fn flush_logs(state: &softprobe_runtime::api::AppState, tenant_id: &str) {
    state
        .engine_for_id(tenant_id)
        .await
        .expect("tenant engine")
        .ingest
        .force_flush_logs()
        .await
        .expect("flush logs");
}

pub async fn query_case(
    router: &Router,
    case: &LokiCase,
    tenant_id: Option<&str>,
) -> (StatusCode, Value) {
    let params = case
        .params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    get_json_as(
        router,
        &format!("{}?{}", case.path, encode_query_owned(&params)),
        tenant_id,
    )
    .await
}

pub async fn query_case_bearer(
    router: &Router,
    case: &LokiCase,
    bearer: &str,
) -> (StatusCode, Value) {
    let params = case
        .params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    get_json_bearer(
        router,
        &format!("{}?{}", case.path, encode_query_owned(&params)),
        bearer,
    )
    .await
}

#[cfg(feature = "integration-e2e")]
pub fn write_failure_artifacts(
    case: &LokiCase,
    lake_raw: Option<&Value>,
    oracle_raw: Option<&Value>,
) -> std::io::Result<std::path::PathBuf> {
    lifecycle::write_failure_artifacts(
        "loki",
        &case.id,
        &case.path,
        &case.params,
        lake_raw,
        oracle_raw,
        normalize_loki_response,
        "LOKI_RAW_ARTIFACT",
        "LOKI_NORMALIZED_ARTIFACT",
    )
}

#[cfg(feature = "integration-e2e")]
pub fn normalize_loki_response(mut body: Value) -> Value {
    if let Some(data) = body.get_mut("data").and_then(Value::as_object_mut) {
        data.remove("stats");
    }
    if let Some(series) = body.get_mut("data").and_then(Value::as_array_mut) {
        if series.iter().all(Value::is_object) {
            series.sort_by_key(|member| serde_json::to_string(member).unwrap_or_default());
        }
    }
    if let Some(result) = body.pointer_mut("/data/result") {
        if let Some(streams) = result.as_array_mut() {
            streams.sort_by_key(|stream| {
                serde_json::to_string(stream.get("stream").unwrap_or(&Value::Null))
                    .unwrap_or_default()
            });
            for stream in streams {
                if let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) {
                    for value in values {
                        if let Some(timestamp) = value.get_mut(0) {
                            *timestamp = Value::String(
                                timestamp
                                    .as_str()
                                    .map(str::to_string)
                                    .unwrap_or_else(|| timestamp.to_string()),
                            );
                        }
                    }
                }
            }
        }
    }
    body
}

pub fn assert_case_contract(case: &LokiCase, status: StatusCode, body: &Value) {
    assert_eq!(status.as_u16(), case.expect.status, "case {}", case.id);
    match case.expect.envelope.as_str() {
        "success" => {
            assert_eq!(body["status"], "success", "case {}", case.id);
            assert!(body.get("data").is_some(), "case {} missing data", case.id);
            if let Some(result_type) = &case.expect.result_type {
                assert_eq!(
                    body["data"]["resultType"],
                    result_type.as_str(),
                    "case {}",
                    case.id
                );
            }
            if let Some(shape) = &case.expect.data_shape {
                match shape.as_str() {
                    "array" => assert!(body["data"].is_array(), "case {}", case.id),
                    "streams" => assert!(body["data"]["result"].is_array(), "case {}", case.id),
                    other => panic!("case {} unknown data shape {other}", case.id),
                }
            }
            if let Some(count) = case.expect.entry_count {
                let actual = body["data"]["result"]
                    .as_array()
                    .into_iter()
                    .flat_map(|streams| streams.iter())
                    .filter_map(|stream| stream["values"].as_array())
                    .map(Vec::len)
                    .sum::<usize>();
                assert_eq!(actual, count, "case {}", case.id);
            }
            if !case.expect.entry_lines.is_empty() {
                let actual = body["data"]["result"]
                    .as_array()
                    .into_iter()
                    .flat_map(|streams| streams.iter())
                    .filter_map(|stream| stream["values"].as_array())
                    .flat_map(|values| values.iter())
                    .filter_map(|value| value.get(1).and_then(Value::as_str))
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                assert_eq!(actual, case.expect.entry_lines, "case {}", case.id);
            }
            if !case.expect.values.is_empty() {
                let actual = body["data"]
                    .as_array()
                    .expect("array data for discovery case");
                assert_eq!(
                    actual,
                    &case
                        .expect
                        .values
                        .iter()
                        .map(|value| Value::String(value.clone()))
                        .collect::<Vec<_>>(),
                    "case {}",
                    case.id
                );
            }
        }
        "error" => {
            assert_eq!(body["status"], "error", "case {}", case.id);
            if let Some(code) = &case.expect.softprobe_code {
                assert!(
                    body["error"].as_str().unwrap_or_default().starts_with(code),
                    "case {} error={}",
                    case.id,
                    body
                );
            }
        }
        other => panic!("case {} unknown envelope {other}", case.id),
    }
}

fn otel_label_name(label: &str) -> String {
    match label {
        "service_name" => "service.name".into(),
        "service_namespace" => "service.namespace".into(),
        "deployment_environment" => "deployment.environment".into(),
        "k8s_namespace_name" => "k8s.namespace.name".into(),
        "k8s_cluster_name" => "k8s.cluster.name".into(),
        "cloud_region" => "cloud.region".into(),
        other => other.into(),
    }
}

fn string_kv(key: impl Into<String>, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

#[cfg(feature = "integration-e2e")]
pub fn require_docker() {
    lifecycle::require_docker("Docker is required for Loki differential tests");
}

#[cfg(feature = "integration-e2e")]
pub struct LokiOracle {
    _service: lifecycle::ReferenceService,
    _work: TempDir,
    pub base: String,
}

#[cfg(feature = "integration-e2e")]
pub fn start_loki_oracle(records: &[LokiRecord], readiness_case: &LokiCase) -> LokiOracle {
    let work = TempDir::new().expect("loki work");
    // Loki's native push format has one stream per label set; keep the oracle
    // input equivalent to the OTLP fixture while preserving metadata fields.
    let mut by_stream: BTreeMap<BTreeMap<String, String>, Vec<Value>> = BTreeMap::new();
    for record in records {
        let mut value = vec![
            Value::String(record.timestamp_ns.clone()),
            Value::String(record.line.clone()),
        ];
        if !record.metadata.is_empty() {
            value.push(serde_json::to_value(&record.metadata).expect("metadata json"));
        }
        by_stream
            .entry(record.stream.clone())
            .or_default()
            .push(Value::Array(value));
    }
    let streams = by_stream
        .into_iter()
        .map(|(stream, values)| serde_json::json!({"stream": stream, "values": values}))
        .collect::<Vec<_>>();
    let payload = serde_json::json!({"streams": streams});
    let payload_path = work.path().join("push.json");
    std::fs::write(
        &payload_path,
        serde_json::to_vec(&payload).expect("push json"),
    )
    .expect("push");

    let image = reference_image_from_manifest();
    let service = lifecycle::start_reference_service(
        "thelake-loki",
        &image,
        &["-p".into(), "127.0.0.1::3100".into()],
        &loki_reference_command(),
        "3100",
        &[],
        "/ready",
        Duration::from_secs(60),
        "Docker is required for Loki differential tests",
    );
    let base = service.base.clone();
    let push_url = format!("{base}/loki/api/v1/push");
    let push = Command::new("curl")
        .args([
            "-sf",
            "-X",
            "POST",
            "-H",
            "content-type: application/json",
            "--data-binary",
            &format!("@{}", payload_path.display()),
            &push_url,
        ])
        .output()
        .expect("push Loki fixture");
    assert!(
        push.status.success(),
        "Loki push failed: {}",
        String::from_utf8_lossy(&push.stderr)
    );
    wait_loki_result(&base, readiness_case, Duration::from_secs(60));
    LokiOracle {
        _service: service,
        _work: work,
        base,
    }
}

#[cfg(feature = "integration-e2e")]
pub fn loki_readiness_url(base: &str, case: &LokiCase) -> String {
    loki_case_url(base, case)
}

#[cfg(feature = "integration-e2e")]
pub fn query_loki_oracle(base: &str, case: &LokiCase) -> Value {
    let url = loki_case_url(base, case);
    let output = Command::new("curl")
        .args(["-sf", &url])
        .output()
        .unwrap_or_else(|e| panic!("oracle curl {url}: {e}"));
    assert!(
        output.status.success(),
        "oracle curl failed {url}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("oracle JSON")
}

#[cfg(feature = "integration-e2e")]
fn loki_case_url(base: &str, case: &LokiCase) -> String {
    let params = case
        .params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    format!("{base}{}?{}", case.path, encode_query_owned(&params))
}

#[cfg(feature = "integration-e2e")]
fn wait_loki_result(base: &str, case: &LokiCase, timeout: Duration) {
    let url = loki_readiness_url(base, case);
    wait_loki_result_with_probe(&url, timeout, Duration::from_millis(250), curl_loki_probe)
        .unwrap_or_else(|error| panic!("{error}"));
}

#[cfg(feature = "integration-e2e")]
#[derive(Debug, Clone)]
struct LokiProbeObservation {
    status: Option<u16>,
    body: String,
    detail: String,
}

#[cfg(feature = "integration-e2e")]
fn curl_loki_probe(url: &str) -> LokiProbeObservation {
    let observation = Command::new("curl")
        .args(["-sS", "-w", "\n%{http_code}", url])
        .output();
    match observation {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let (body, status) = stdout
                .rsplit_once('\n')
                .map(|(body, status)| (body.to_string(), status.trim().parse::<u16>().ok()))
                .unwrap_or_else(|| (stdout.into_owned(), None));
            LokiProbeObservation {
                detail: format!(
                    "curl_exit={}, stderr={}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                status,
                body,
            }
        }
        Err(error) => LokiProbeObservation {
            status: None,
            body: String::new(),
            detail: format!("probe error={error}"),
        },
    }
}

#[cfg(feature = "integration-e2e")]
fn wait_loki_result_with_probe(
    url: &str,
    timeout: Duration,
    poll_interval: Duration,
    mut probe: impl FnMut(&str) -> LokiProbeObservation,
) -> Result<(), String> {
    let start = std::time::Instant::now();
    let mut last_observed = String::from("no response");

    loop {
        let observation = probe(url);
        let parsed = serde_json::from_str::<Value>(&observation.body);
        let result_count = parsed
            .as_ref()
            .ok()
            .and_then(|body| body.pointer("/data/result"))
            .and_then(Value::as_array)
            .map(Vec::len);
        last_observed = format!(
            "status={:?}, result_count={result_count:?}, body={:?}, detail={}",
            observation.status, observation.body, observation.detail
        );
        if observation.status == Some(StatusCode::OK.as_u16())
            && result_count.is_some_and(|count| count > 0)
        {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!(
                "timeout waiting for Loki query result at {url} after {timeout:?}; last response: {last_observed}"
            ));
        }
        std::thread::sleep(poll_interval);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "integration-e2e")]
    #[test]
    fn normalize_loki_response_ignores_only_execution_stats() {
        let response = serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [{
                    "stream": {"service_name": "checkout"},
                    "values": [["100", "checkout started"]],
                    "structuredMetadata": [{"request_id": "r1"}]
                }],
                "stats": {"summary": {"bytesProcessedPerSecond": 42}}
            }
        });

        let normalized = normalize_loki_response(response);

        assert_eq!(normalized["status"], "success");
        assert_eq!(normalized["data"]["resultType"], "streams");
        assert_eq!(
            normalized["data"]["result"][0]["stream"]["service_name"],
            "checkout"
        );
        assert_eq!(
            normalized["data"]["result"][0]["values"],
            serde_json::json!([["100", "checkout started"]])
        );
        assert_eq!(
            normalized["data"]["result"][0]["structuredMetadata"],
            serde_json::json!([{ "request_id": "r1" }])
        );
        assert!(normalized["data"].get("stats").is_none());
    }

    #[cfg(feature = "integration-e2e")]
    #[test]
    fn normalize_loki_response_sorts_series_members_only() {
        let response = serde_json::json!({
            "status": "success",
            "data": [
                {"service_name": "payments", "deployment_environment": "staging"},
                {"service_name": "checkout", "deployment_environment": "prod"}
            ]
        });
        let reordered = serde_json::json!({
            "status": "success",
            "data": [
                {"service_name": "checkout", "deployment_environment": "prod"},
                {"service_name": "payments", "deployment_environment": "staging"}
            ]
        });

        let normalized = normalize_loki_response(response);
        let reordered_normalized = normalize_loki_response(reordered);

        assert_eq!(normalized, reordered_normalized);
        assert_eq!(normalized["data"][0]["service_name"], "checkout");
        assert_eq!(normalized["data"][1]["service_name"], "payments");
    }

    #[test]
    fn shifted_fixture_moves_timestamps_and_absolute_bounds() {
        let fixture = fixture();
        let shifted = fixture.shifted_by(42);

        assert_eq!(shifted.records.len(), fixture.records.len());
        for (original, shifted) in fixture.records.iter().zip(&shifted.records) {
            assert_eq!(shifted.timestamp(), original.timestamp() + 42);
            assert_eq!(shifted.line, original.line);
            assert_eq!(shifted.stream, original.stream);
            assert_eq!(shifted.metadata, original.metadata);
        }
        assert_eq!(
            shifted.records[0].timestamp(),
            shifted.records[1].timestamp(),
            "duplicate nanosecond equality must be preserved"
        );

        for (original, shifted) in fixture.cases.iter().zip(&shifted.cases) {
            assert_eq!(shifted.id, original.id);
            assert_eq!(shifted.path, original.path);
            assert_eq!(shifted.expect.status, original.expect.status);
            assert_eq!(shifted.expect.entry_lines, original.expect.entry_lines);
            for (key, value) in &original.params {
                let shifted_value = shifted.params.get(key).expect("shifted parameter");
                if matches!(key.as_str(), "start" | "end" | "time") {
                    assert_eq!(
                        shifted_value,
                        &(value.parse::<i64>().unwrap() + 42).to_string(),
                        "absolute bound {key}"
                    );
                } else {
                    assert_eq!(shifted_value, value, "non-time parameter {key}");
                }
            }
        }
    }

    #[test]
    fn shifted_fixture_preserves_duration_parameters() {
        let fixture = fixture();
        let shifted = fixture.shifted_by(-7);
        let original = fixture
            .cases
            .iter()
            .find(|case| case.id == "loki-query-range-since-interval-step")
            .expect("duration case");
        let shifted = shifted
            .cases
            .iter()
            .find(|case| case.id == "loki-query-range-since-interval-step")
            .expect("shifted duration case");

        assert_eq!(shifted.params["since"], original.params["since"]);
        assert_eq!(shifted.params["interval"], original.params["interval"]);
        assert_eq!(shifted.params["step"], original.params["step"]);
        assert_eq!(
            shifted.params["end"],
            (original.params["end"].parse::<i64>().unwrap() - 7).to_string()
        );
    }

    #[test]
    fn fixture_for_now_keeps_fixture_timestamps_behind_current_time() {
        let before_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX epoch")
            .as_nanos();
        let before_ns =
            i64::try_from(before_ns).expect("current UNIX epoch nanoseconds fit in i64");

        let shifted = fixture_for_now();

        let after_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX epoch")
            .as_nanos();
        let after_ns = i64::try_from(after_ns).expect("current UNIX epoch nanoseconds fit in i64");
        let original_timestamp = fixture().records[0].timestamp();
        let delta_ns = shifted.records[0].timestamp() - original_timestamp;
        let min_delta_ns = before_ns
            .checked_sub(FIXTURE_LAG_NS)
            .and_then(|value| value.checked_sub(PHASE2_EPOCH_NS))
            .expect("minimum fixture delta fits in i64");
        let max_delta_ns = after_ns
            .checked_sub(FIXTURE_LAG_NS)
            .and_then(|value| value.checked_sub(PHASE2_EPOCH_NS))
            .expect("maximum fixture delta fits in i64");

        assert!(
            (min_delta_ns..=max_delta_ns).contains(&delta_ns),
            "fixture delta {delta_ns} was not approximately current time minus the 1-second lag"
        );
    }

    #[cfg(feature = "integration-e2e")]
    #[test]
    fn loki_oracle_accepts_deterministic_fixture_timestamps() {
        let command = super::loki_reference_command();
        for setting in [
            "-validation.reject-old-samples=false",
            "-querier.query-ingesters-within=87600h",
            "-store.max-query-length=87600h",
        ] {
            assert!(
                command.iter().any(|arg| arg == setting),
                "Loki oracle command is missing historical setting {setting}"
            );
        }
    }

    #[cfg(feature = "integration-e2e")]
    #[test]
    fn loki_oracle_readiness_probe_uses_a_fixture_selector_and_bounds() {
        let case = super::fixture()
            .cases
            .into_iter()
            .find(|case| case.differential)
            .expect("first differential case");
        assert_eq!(
            super::loki_readiness_url("http://127.0.0.1:3100", &case),
            "http://127.0.0.1:3100/loki/api/v1/query?direction=forward&limit=10&query=%7Bservice_name%3D%22checkout%22%7D+%7C%3D+%22checkout%22+%7C+json+%7C+level%3D%22info%22&time=1786827600000000002"
        );
    }

    #[cfg(feature = "integration-e2e")]
    #[test]
    fn loki_readiness_waits_for_a_nonempty_result_after_http_200() {
        let mut observations = vec![
            super::LokiProbeObservation {
                status: Some(200),
                body: r#"{"status":"success","data":{"result":[]}}"#.into(),
                detail: "empty result".into(),
            },
            super::LokiProbeObservation {
                status: Some(200),
                body: r#"{"status":"success","data":{"result":[{"stream":{},"values":[]}]}}"#
                    .into(),
                detail: "fixture result".into(),
            },
        ];
        let mut probes = 0;

        let result = super::wait_loki_result_with_probe(
            "http://127.0.0.1:3100/loki/api/v1/query",
            std::time::Duration::from_secs(1),
            std::time::Duration::ZERO,
            |_| {
                probes += 1;
                observations.remove(0)
            },
        );

        assert!(result.is_ok(), "readiness error: {result:?}");
        assert_eq!(probes, 2, "a 200 response with no result is not ready");
    }
}
