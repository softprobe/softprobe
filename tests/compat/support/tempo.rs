//! Shared Tempo Phase 3 fixture, OTLP, router, normalization, and oracle helpers.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::TempDir;
use tower::ServiceExt;

#[cfg(feature = "integration-e2e")]
use crate::compat_support::lifecycle;
use crate::compat_support::loki::{candidate_reference_image, manifest_reference_image};
use crate::compat_support::prometheus::encode_query_owned;
use crate::compat_support::prometheus_oracle::build_tenant_router_with_state;

#[derive(Debug, Clone, Deserialize)]
pub struct TempoFixture {
    pub evidence: TempoEvidence,
    pub capability: TempoCapability,
    pub records: Vec<TempoRecord>,
    pub cases: Vec<TempoCase>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TempoEvidence {
    pub issue: String,
    pub phase: String,
    pub reference_manifest: String,
    pub reference_image: String,
    pub normalization: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TempoCapability {
    pub protocol: String,
    pub phase: String,
    pub supported_endpoints: Vec<String>,
    pub supported_features: Vec<String>,
    pub unsupported_features: Vec<String>,
    pub fidelity_gaps: Vec<String>,
    pub ordering_policy: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TempoRecord {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: i32,
    pub start_time_unix_nano: String,
    pub end_time_unix_nano: String,
    pub resource: BTreeMap<String, String>,
    pub scope: TempoScope,
    pub attributes: BTreeMap<String, String>,
    pub status_code: i32,
    pub status_message: Option<String>,
    pub events: Vec<TempoEvent>,
    pub links: Vec<TempoLink>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TempoScope {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TempoEvent {
    pub name: String,
    pub timestamp_unix_nano: String,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TempoLink {
    pub trace_id: String,
    pub span_id: String,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
pub struct TempoLinkExpectation<'a> {
    pub trace_id: &'a str,
    pub span_id: &'a str,
    pub attributes: &'a [(&'a str, &'a str)],
}

#[derive(Debug, Clone, Copy)]
pub struct TempoSpanLinkExpectation<'a> {
    pub span_id: &'a str,
    pub links: &'a [TempoLinkExpectation<'a>],
}

#[derive(Debug, Clone, Deserialize)]
pub struct TempoCase {
    pub id: String,
    pub provenance: String,
    pub path: String,
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    pub expect: TempoExpectation,
    #[serde(default)]
    pub differential: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct TempoParameterMatrixCase {
    pub id: &'static str,
    pub path: &'static str,
}

pub const TEMPO_PARAMETER_MATRIX: &[TempoParameterMatrixCase] = &[
    TempoParameterMatrixCase {
        id: "tempo-params-v1-trace-lookup",
        path: "/api/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
    TempoParameterMatrixCase {
        id: "tempo-params-v2-trace-lookup",
        path: "/api/v2/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
    TempoParameterMatrixCase {
        id: "tempo-params-search",
        path: "/api/search",
    },
    TempoParameterMatrixCase {
        id: "tempo-params-tag-names",
        path: "/api/search/tags",
    },
    TempoParameterMatrixCase {
        id: "tempo-params-tag-values",
        path: "/api/search/tag/service.name/values",
    },
];

#[derive(Debug, Clone, Deserialize)]
pub struct TempoExpectation {
    pub status: u16,
    pub envelope: String,
    #[serde(default)]
    pub trace_count: Option<usize>,
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default)]
    pub softprobe_code: Option<String>,
}

pub fn fixture() -> TempoFixture {
    let path = fixture_dir().join("phase3.json");
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

pub fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compat/tempo")
}

pub fn reference_image_from_manifest() -> String {
    let manifest: serde_yaml::Value =
        serde_yaml::from_str(include_str!("../../../docs/compat/references.v0.yaml"))
            .expect("references.v0.yaml parses");
    std::env::var("TEMPO_REFERENCE_IMAGE")
        .ok()
        .filter(|image| !image.trim().is_empty())
        .map(|image| candidate_reference_image("TEMPO_REFERENCE_IMAGE", &image))
        .unwrap_or_else(|| manifest_reference_image(&manifest, "tempo"))
}

/// Reuse the shared local tenant router and lifecycle used by the Loki contracts.
pub async fn build_tempo_router() -> (Router, softprobe_runtime::api::AppState, TempDir) {
    build_tenant_router_with_state().await
}

pub async fn build_seeded_tempo_router(
    records: &[TempoRecord],
) -> (Router, softprobe_runtime::api::AppState, TempDir) {
    let (router, state, temp) = build_tempo_router().await;
    ingest_records(&router, records, None).await;
    flush_traces(&state, "local-sqlite-tenant").await;
    (router, state, temp)
}

pub async fn ingest_records(router: &Router, records: &[TempoRecord], bearer: Option<&str>) {
    let request = build_trace_request(records);
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf");
    if let Some(bearer) = bearer {
        builder = builder.header("Authorization", format!("Bearer {bearer}"));
    }
    let response = router
        .clone()
        .oneshot(builder.body(Body::from(request.encode_to_vec())).unwrap())
        .await
        .expect("trace ingest request");
    let status = response.status();
    assert_eq!(status, StatusCode::OK, "trace ingest status={status}");
}

pub async fn flush_traces(state: &softprobe_runtime::api::AppState, tenant_id: &str) {
    state
        .engine_for_id(tenant_id)
        .await
        .expect("tenant engine")
        .ingest
        .force_flush_spans()
        .await
        .expect("flush traces");
}

pub async fn query_case(
    router: &Router,
    case: &TempoCase,
    bearer: Option<&str>,
) -> (StatusCode, Value) {
    query_case_with_scope(router, case, bearer, None).await
}

pub async fn query_case_with_scope(
    router: &Router,
    case: &TempoCase,
    bearer: Option<&str>,
    scope_header: Option<&str>,
) -> (StatusCode, Value) {
    let params = case
        .params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let path = if params.is_empty() {
        case.path.clone()
    } else {
        format!("{}?{}", case.path, encode_query_owned(&params))
    };
    query_path_with_scope(router, &path, bearer, scope_header).await
}

pub async fn query_path_with_scope(
    router: &Router,
    path: &str,
    bearer: Option<&str>,
    scope_header: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method("GET").uri(path);
    if let Some(bearer) = bearer {
        request = request.header("Authorization", format!("Bearer {bearer}"));
    }
    if let Some(scope_header) = scope_header {
        request = request.header("X-Scope-OrgID", scope_header);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).expect("Tempo query request"))
        .await
        .expect("Tempo query response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("Tempo response body");
    let body = serde_json::from_slice(&body).expect("Tempo response JSON");
    (status, body)
}

pub fn v1_spans(body: &Value) -> Vec<&Value> {
    body["batches"]
        .as_array()
        .expect("v1 batches")
        .iter()
        .flat_map(|batch| {
            batch["scopeSpans"]
                .as_array()
                .expect("v1 scope spans")
                .iter()
                .flat_map(|scope| scope["spans"].as_array().expect("v1 spans").iter())
        })
        .collect()
}

pub fn v2_spans(body: &Value) -> Vec<&Value> {
    body["trace"]["resourceSpans"]
        .as_array()
        .expect("v2 resource spans")
        .iter()
        .flat_map(|resource| {
            resource["scopeSpans"]
                .as_array()
                .expect("v2 scope spans")
                .iter()
                .flat_map(|scope| scope["spans"].as_array().expect("v2 spans").iter())
        })
        .collect()
}

pub fn assert_all_resource_scope_groups_preserve_spans_and_link_attributes(
    body: &Value,
    expected_span_ids: &[&str],
    expected_linked_spans: &[TempoSpanLinkExpectation<'_>],
) {
    let resource_groups = body["batches"]
        .as_array()
        .or_else(|| body["trace"]["resourceSpans"].as_array())
        .expect("Tempo resource groups");
    let mut span_ids = Vec::new();
    let mut found_linked_span_ids = std::collections::BTreeSet::new();

    for resource in resource_groups {
        for scope in resource["scopeSpans"]
            .as_array()
            .expect("Tempo scope groups")
        {
            for span in scope["spans"].as_array().expect("Tempo spans") {
                let span_id = span["spanId"].as_str().expect("Tempo span ID");
                span_ids.push(span_id);
                if let Some(expected) = expected_linked_spans
                    .iter()
                    .find(|expected| expected.span_id == span_id)
                {
                    let links = span["links"]
                        .as_array()
                        .expect("expected linked span to contain links");
                    assert_eq!(
                        links.len(),
                        expected.links.len(),
                        "Tempo link count mismatch on span {span_id}"
                    );
                    for expected_link in expected.links {
                        let matching_links = links
                            .iter()
                            .filter(|link| {
                                link["traceId"] == expected_link.trace_id
                                    && link["spanId"] == expected_link.span_id
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(
                            matching_links.len(),
                            1,
                            "Tempo linked trace/span ID mismatch on span {span_id}"
                        );
                        let actual_attributes = matching_links[0]["attributes"]
                            .as_array()
                            .expect("Tempo link attributes")
                            .iter()
                            .map(|attribute| {
                                (
                                    attribute["key"]
                                        .as_str()
                                        .expect("Tempo link attribute key")
                                        .to_owned(),
                                    attribute["value"]["stringValue"]
                                        .as_str()
                                        .expect("Tempo link attribute string value")
                                        .to_owned(),
                                )
                            })
                            .collect::<Vec<_>>();
                        let mut actual_attributes = actual_attributes;
                        actual_attributes.sort();
                        let mut expected_attributes = expected_link
                            .attributes
                            .iter()
                            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                            .collect::<Vec<_>>();
                        expected_attributes.sort();
                        assert_eq!(
                            actual_attributes, expected_attributes,
                            "Tempo link attributes mismatch on span {span_id}"
                        );
                    }
                    found_linked_span_ids.insert(span_id);
                } else if let Some(links) = span.get("links") {
                    assert!(
                        links.as_array().is_some_and(Vec::is_empty),
                        "unexpected links on unlinked span {span_id}"
                    );
                }
            }
        }
    }

    assert_eq!(span_ids, expected_span_ids, "Tempo span IDs/order mismatch");
    assert_eq!(
        found_linked_span_ids,
        expected_linked_spans
            .iter()
            .map(|expected| expected.span_id)
            .collect(),
        "Tempo linked span IDs mismatch"
    );
}

pub fn assert_case_contract(case: &TempoCase, status: StatusCode, body: &Value) {
    assert_eq!(status.as_u16(), case.expect.status, "case {}", case.id);
    match case.expect.envelope.as_str() {
        "v1" => {
            let spans = v1_spans(body);
            if let Some(count) = case.expect.trace_count {
                assert_eq!(spans.len(), count, "case {}", case.id);
            }
        }
        "v2" => {
            let spans = v2_spans(body);
            if let Some(count) = case.expect.trace_count {
                assert_eq!(spans.len(), count, "case {}", case.id);
            }
        }
        "search" => {
            let traces = body["traces"]
                .as_array()
                .unwrap_or_else(|| panic!("case {} missing search traces", case.id));
            if let Some(count) = case.expect.trace_count {
                assert_eq!(traces.len(), count, "case {}", case.id);
            }
        }
        "tag_names" => assert_eq!(body["tagNames"], json_strings(&case.expect.values)),
        "tag_values" => assert_eq!(body["tagValues"], json_strings(&case.expect.values)),
        "not_found" => assert_eq!(body["message"], "trace not found", "case {}", case.id),
        "error" => {
            if let Some(code) = &case.expect.softprobe_code {
                assert_eq!(
                    body["softprobe_code"].as_str(),
                    Some(code.as_str()),
                    "case {}",
                    case.id
                );
            }
        }
        other => panic!("case {} unknown envelope {other}", case.id),
    }
}

fn json_strings(values: &[String]) -> Value {
    Value::Array(values.iter().cloned().map(Value::String).collect())
}

fn build_trace_request(records: &[TempoRecord]) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: records
            .iter()
            .map(|record| ResourceSpans {
                resource: Some(Resource {
                    attributes: record
                        .resource
                        .iter()
                        .map(|(key, value)| string_kv(key, value))
                        .collect(),
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(
                        opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                            name: record.scope.name.clone(),
                            version: record.scope.version.clone(),
                            ..Default::default()
                        },
                    ),
                    spans: vec![Span {
                        trace_id: hex::decode(&record.trace_id).expect("trace id hex"),
                        span_id: hex::decode(&record.span_id).expect("span id hex"),
                        parent_span_id: record
                            .parent_span_id
                            .as_deref()
                            .map(|id| hex::decode(id).expect("parent span id hex"))
                            .unwrap_or_default(),
                        name: record.name.clone(),
                        kind: record.kind,
                        start_time_unix_nano: record
                            .start_time_unix_nano
                            .parse()
                            .expect("start timestamp"),
                        end_time_unix_nano: record
                            .end_time_unix_nano
                            .parse()
                            .expect("end timestamp"),
                        attributes: record
                            .attributes
                            .iter()
                            .map(|(key, value)| string_kv(key, value))
                            .collect(),
                        events: record
                            .events
                            .iter()
                            .map(|event| span::Event {
                                time_unix_nano: event
                                    .timestamp_unix_nano
                                    .parse()
                                    .expect("event timestamp"),
                                name: event.name.clone(),
                                attributes: event
                                    .attributes
                                    .iter()
                                    .map(|(key, value)| string_kv(key, value))
                                    .collect(),
                                ..Default::default()
                            })
                            .collect(),
                        status: Some(Status {
                            code: record.status_code,
                            message: record.status_message.clone().unwrap_or_default(),
                        }),
                        links: record
                            .links
                            .iter()
                            .map(|link| span::Link {
                                trace_id: hex::decode(&link.trace_id).expect("link trace id hex"),
                                span_id: hex::decode(&link.span_id).expect("link span id hex"),
                                attributes: link
                                    .attributes
                                    .iter()
                                    .map(|(key, value)| string_kv(key, value))
                                    .collect(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    }
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

#[cfg(feature = "integration-e2e")]
pub fn write_failure_artifacts(
    case: &TempoCase,
    lake_raw: Option<&Value>,
    oracle_raw: Option<&Value>,
) -> std::io::Result<PathBuf> {
    let dir = lifecycle::write_failure_artifacts(
        "tempo",
        &case.id,
        &case.path,
        &case.params,
        lake_raw,
        oracle_raw,
        normalize_tempo_response,
        "TEMPO_RAW_ARTIFACT",
        "TEMPO_NORMALIZED_ARTIFACT",
    )?;
    let reference = serde_json::json!({
        "reference_manifest": "docs/compat/references.v0.yaml",
        "reference_image": reference_image_from_manifest(),
    });
    std::fs::write(
        dir.join("reference.json"),
        serde_json::to_vec_pretty(&reference).expect("reference metadata json"),
    )?;
    Ok(dir)
}

#[cfg(feature = "integration-e2e")]
pub fn normalize_tempo_response(mut body: Value) -> Value {
    normalize_value(&mut body, None);
    body
}

#[cfg(feature = "integration-e2e")]
fn normalize_value(value: &mut Value, key: Option<&str>) {
    match value {
        Value::Array(values) => {
            for value in values.iter_mut() {
                normalize_value(value, key);
            }
            if key == Some("attributes") {
                values.sort_by_key(|value| value["key"].to_string());
            }
            if matches!(key, Some("traces" | "tagNames" | "tagValues")) {
                values.sort_by_key(|value| value.to_string());
            }
            if key == Some("spans") {
                values.sort_by_key(|value| {
                    (
                        value["traceId"].to_string(),
                        value["startTimeUnixNano"].to_string(),
                        value["spanId"].to_string(),
                    )
                });
            }
            if key == Some("links") {
                values.sort_by_key(|value| {
                    (value["traceId"].to_string(), value["spanId"].to_string())
                });
            }
            if matches!(key, Some("batches" | "resourceSpans")) {
                normalize_resource_scope_groups(values);
            }
        }
        Value::Object(map) => {
            for metadata_key in ["metrics", "serviceStats", "spanSet", "spanSets"] {
                map.remove(metadata_key);
            }
            if key == Some("scope")
                && map
                    .get("attributes")
                    .and_then(Value::as_array)
                    .is_some_and(Vec::is_empty)
            {
                map.remove("attributes");
            }
            if key == Some("links") {
                if map.get("flags") == Some(&Value::from(0)) {
                    map.remove("flags");
                }
                if map.get("traceState") == Some(&Value::String(String::new())) {
                    map.remove("traceState");
                }
            }
            for (name, value) in map.iter_mut() {
                normalize_value(value, Some(name));
                if name.ends_with("UnixNano") {
                    let normalized = match &*value {
                        Value::String(value) => value.clone(),
                        other => other.to_string(),
                    };
                    *value = Value::String(normalized);
                }
            }
        }
        _ => {}
    }
}

#[cfg(feature = "integration-e2e")]
fn normalize_resource_scope_groups(values: &mut Vec<Value>) {
    let groups = std::mem::take(values);
    let mut merged = BTreeMap::<String, Value>::new();

    for group in groups {
        let resource = group
            .get("resource")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let resource_key = resource.to_string();
        let scopes = group
            .get("scopeSpans")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        for scope_group in scopes {
            let scope = scope_group
                .get("scope")
                .cloned()
                .unwrap_or_else(|| Value::Object(Default::default()));
            let scope_key = scope.to_string();
            let key = format!("{resource_key}\u{0}{scope_key}");
            let merged_group = merged.entry(key).or_insert_with(|| {
                serde_json::json!({
                    "resource": resource,
                    "scopeSpans": [{"scope": scope, "spans": []}]
                })
            });
            let destination = merged_group["scopeSpans"][0]["spans"]
                .as_array_mut()
                .expect("canonical Tempo span group");
            if let Some(spans) = scope_group.get("spans").and_then(Value::as_array) {
                destination.extend(spans.iter().cloned());
            }
        }
    }

    *values = merged.into_values().collect();
}

#[cfg(feature = "integration-e2e")]
pub fn require_docker() {
    lifecycle::require_docker("Docker is required for Tempo differential tests");
}

#[cfg(feature = "integration-e2e")]
pub struct TempoOracle {
    _service: lifecycle::ReferenceService,
    _work: TempDir,
    pub base: String,
}

#[cfg(feature = "integration-e2e")]
impl TempoOracle {
    pub fn wait_for_known_trace(&self, trace_id: &str, timeout: std::time::Duration) {
        self._service
            .wait_queryable(&format!("{}/api/traces/{trace_id}", self.base), timeout);
    }

    pub fn wait_for_search_case(&self, case: &TempoCase, timeout: std::time::Duration) {
        let url = tempo_case_url(&self.base, case);
        let start = std::time::Instant::now();
        let mut last_observed;

        loop {
            let observation = std::process::Command::new("curl")
                .args(["-sS", "-w", "\n%{http_code}", &url])
                .output();
            let ready = match observation {
                Ok(output) => {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let (body, status) = stdout
                        .rsplit_once('\n')
                        .map(|(body, status)| (body, status.trim().parse::<u16>().ok()))
                        .unwrap_or((stdout.as_ref(), None));
                    let parsed = serde_json::from_str::<Value>(body);
                    let trace_count = parsed
                        .as_ref()
                        .ok()
                        .and_then(|body| body["traces"].as_array())
                        .map(Vec::len);
                    last_observed = format!(
                        "status={status:?}, trace_count={trace_count:?}, stderr={}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                    status == Some(StatusCode::OK.as_u16()) && trace_count.is_some_and(|n| n > 0)
                }
                Err(error) => {
                    last_observed = format!("probe error={error}");
                    false
                }
            };
            if ready {
                return;
            }
            if start.elapsed() > timeout {
                panic!(
                    "timeout waiting for Tempo search index at {url} after {timeout:?}; last observed {last_observed}"
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    }
}

#[cfg(feature = "integration-e2e")]
pub fn start_tempo_oracle(records: &[TempoRecord], search_case: &TempoCase) -> TempoOracle {
    let work = TempDir::new().expect("tempo work");
    {
        // The Tempo image runs as an arbitrary uid; on Linux bind mounts the
        // host dir must be world-writable or `mkdir /tmp/tempo/blocks` fails.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(work.path(), std::fs::Permissions::from_mode(0o777))
            .expect("chmod tempo work dir");
    }
    let payload_path = work.path().join("traces.pb");
    std::fs::write(&payload_path, build_trace_request(records).encode_to_vec())
        .expect("tempo payload");
    let config_path = work.path().join("tempo.yaml");
    std::fs::write(
        &config_path,
        r#"server:
  http_listen_port: 3200
  grpc_listen_port: 9095
distributor:
  receivers:
    otlp:
      protocols:
        http:
          endpoint: 0.0.0.0:4318
ingester:
  complete_block_timeout: 1s
  max_block_duration: 1s
compactor:
  compaction:
    block_retention: 1h
storage:
  trace:
    backend: local
    wal:
      ingestion_time_range_slack: 87600h
      path: /tmp/tempo/wal
    local:
      path: /tmp/tempo/blocks
    blocklist_poll: 1s
query_frontend:
  search:
    query_backend_after: 87600h
    query_ingesters_until: 87600h
"#,
    )
    .expect("tempo config");

    let image = reference_image_from_manifest();
    let service = lifecycle::start_reference_service(
        "thelake-tempo",
        &image,
        &[
            "-p".into(),
            "127.0.0.1::3200".into(),
            "-p".into(),
            "127.0.0.1::4318".into(),
            "-v".into(),
            format!("{}:/etc/tempo.yaml:ro", config_path.display()),
            "-v".into(),
            format!("{}:/tmp/tempo", work.path().display()),
        ],
        &["-config.file=/etc/tempo.yaml".into()],
        "3200",
        &["4318"],
        "/ready",
        // First CI run pulls the pinned image; allow slow starts.
        std::time::Duration::from_secs(180),
        "Docker is required for Tempo differential tests",
    );

    let base = service.base.clone();
    let otlp_port = service.port("4318");
    let push_url = format!("http://127.0.0.1:{otlp_port}/v1/traces");
    let push = std::process::Command::new("curl")
        .args([
            "-sf",
            "-X",
            "POST",
            "-H",
            "content-type: application/x-protobuf",
            "--data-binary",
            &format!("@{}", payload_path.display()),
            &push_url,
        ])
        .output()
        .expect("push Tempo fixture");
    assert!(
        push.status.success(),
        "Tempo OTLP push failed: {}",
        String::from_utf8_lossy(&push.stderr)
    );
    let known_trace_id = records
        .iter()
        .map(|record| record.trace_id.as_str())
        .find(|trace_id| !trace_id.is_empty())
        .expect("Tempo fixture must contain a trace id");
    let oracle = TempoOracle {
        _service: service,
        _work: work,
        base,
    };
    oracle.wait_for_known_trace(&known_trace_id, std::time::Duration::from_secs(180));
    oracle.wait_for_search_case(search_case, std::time::Duration::from_secs(180));
    oracle
}

#[cfg(feature = "integration-e2e")]
pub fn query_tempo_oracle(base: &str, case: &TempoCase) -> Value {
    let url = tempo_case_url(base, case);
    let output = std::process::Command::new("curl")
        .args(["-sf", &url])
        .output()
        .unwrap_or_else(|e| panic!("oracle curl {url}: {e}"));
    assert!(
        output.status.success(),
        "oracle curl failed {url}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("Tempo oracle JSON")
}

#[cfg(feature = "integration-e2e")]
fn tempo_case_url(base: &str, case: &TempoCase) -> String {
    let params = case
        .params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let path = if params.is_empty() {
        case.path.clone()
    } else {
        format!("{}?{}", case.path, encode_query_owned(&params))
    };
    format!("{base}{path}")
}
