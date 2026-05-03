//! Inject lookup: OTLP inject spans + session rule evaluation (ported from Go `proxybackend`).

use anyhow::{anyhow, Result};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AvValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData};
use prost::Message;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct InjectLookupRequest {
    pub session_id: String,
    pub service_name: String,
    pub traffic_direction: String,
    pub url_host: String,
    pub url_path: String,
    pub request_method: String,
}

#[derive(Debug, Clone)]
pub struct MockResponse {
    pub status_code: i64,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RuleLayer {
    SessionPolicy = 0,
    CaseEmbedded = 1,
    SessionRules = 2,
}

#[derive(Debug, Clone)]
pub struct InjectRuleMatch {
    pub layer: RuleLayer,
    pub order: usize,
    pub rule: InjectRule,
    pub source: &'static str,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InjectRule {
    /// Optional human-readable label (logs, suite mocks); omit when unused.
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub priority: i64,
    pub when: InjectRuleWhen,
    pub then: InjectRuleThen,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InjectRuleWhen {
    #[serde(default)]
    pub direction: String,
    #[serde(default)]
    pub service: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub path_prefix: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InjectRuleThen {
    pub action: String,
    pub response: Option<InjectMockResponse>,
    pub error: Option<InjectErrorResponse>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InjectMockResponse {
    pub status: i64,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    pub body: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InjectErrorResponse {
    pub status: i64,
    pub body: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct CaseFileEnvelope {
    #[serde(default)]
    rules: Vec<InjectRule>,
}

/// Synthetic strict-policy fallback rule (`select_inject_rule`); not matched by name.
pub const STRICT_POLICY_RULE_NAME: &str = "policy-strict-miss";
const STRICT_POLICY_ERROR: &str = "strict policy requires a mock rule match";

pub fn normalize_otlp_body(body: &[u8]) -> Result<ExportTraceServiceRequest> {
    if let Ok(req) = ExportTraceServiceRequest::decode(body) {
        return Ok(req);
    }
    serde_json::from_slice(body).map_err(|e| anyhow!("invalid otlp payload: {e}"))
}

/// Resolves replay lookup from Softprobe inject spans (`sp.span.type=inject`). Not used for
/// generic OTLP ingest; callers must send the inject-shaped trace envelope.
pub fn parse_inject_lookup(req: &ExportTraceServiceRequest) -> Result<InjectLookupRequest> {
    for rs in &req.resource_spans {
        let svc = resource_attr_string(rs.resource.as_ref(), "service.name");
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                if span_attr_string(span, "sp.span.type") != "inject" {
                    continue;
                }
                let method = first_non_empty(
                    span_attr_string(span, "http.request.method"),
                    span_attr_string(span, "http.request.header.:method"),
                );
                let path = first_non_empty(
                    span_attr_string(span, "url.path"),
                    span_attr_string(span, "http.request.header.:path"),
                );
                return Ok(InjectLookupRequest {
                    session_id: span_attr_string(span, "sp.session.id"),
                    service_name: first_non_empty(
                        span_attr_string(span, "sp.service.name"),
                        svc.clone(),
                    ),
                    traffic_direction: span_attr_string(span, "sp.traffic.direction"),
                    url_host: span_attr_string(span, "url.host"),
                    url_path: path,
                    request_method: method,
                });
            }
        }
    }
    Err(anyhow!("inject span not found"))
}

fn resource_attr_string(res: Option<&Resource>, key: &str) -> String {
    let Some(r) = res else {
        return String::new();
    };
    for kv in &r.attributes {
        if kv.key == key {
            return any_value_string(kv.value.as_ref());
        }
    }
    String::new()
}

fn span_attr_string(span: &Span, key: &str) -> String {
    for kv in &span.attributes {
        if kv.key == key {
            return any_value_string(kv.value.as_ref());
        }
    }
    String::new()
}

fn any_value_string(v: Option<&AnyValue>) -> String {
    let Some(v) = v else {
        return String::new();
    };
    match &v.value {
        Some(AvValue::StringValue(s)) => s.clone(),
        Some(AvValue::IntValue(i)) => i.to_string(),
        Some(AvValue::BoolValue(b)) => b.to_string(),
        _ => String::new(),
    }
}

fn first_non_empty(a: String, b: String) -> String {
    if !a.trim().is_empty() {
        a
    } else {
        b
    }
}

pub fn parse_inject_rules_document(payload: &[u8]) -> Result<Vec<InjectRule>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    #[derive(Deserialize)]
    struct Doc {
        #[serde(default)]
        rules: Vec<InjectRule>,
    }
    let doc: Doc = serde_json::from_slice(payload)?;
    Ok(doc.rules)
}

pub fn case_embedded_rules(case_bytes: &[u8]) -> Vec<InjectRule> {
    if case_bytes.is_empty() {
        return Vec::new();
    }
    serde_json::from_slice::<CaseFileEnvelope>(case_bytes)
        .map(|e| e.rules)
        .unwrap_or_default()
}

pub fn is_strict_external_http_policy(policy: &[u8]) -> bool {
    if policy.is_empty() {
        return false;
    }
    #[derive(Deserialize)]
    struct P {
        #[serde(rename = "externalHttp")]
        external_http: Option<String>,
    }
    serde_json::from_slice::<P>(policy)
        .ok()
        .and_then(|p| p.external_http)
        .map(|s| s == "strict")
        .unwrap_or(false)
}

pub fn rule_matches_inject(rule: &InjectRule, req: &InjectLookupRequest) -> bool {
    if !rule.when.direction.is_empty() && rule.when.direction != req.traffic_direction {
        return false;
    }
    if !rule.when.service.is_empty() && rule.when.service != req.service_name {
        return false;
    }
    if !rule.when.host.is_empty() && rule.when.host != req.url_host {
        return false;
    }
    if !rule.when.method.is_empty() && rule.when.method != req.request_method {
        return false;
    }
    if !rule.when.path.is_empty() && rule.when.path != req.url_path {
        return false;
    }
    if !rule.when.path_prefix.is_empty() && !req.url_path.starts_with(&rule.when.path_prefix) {
        return false;
    }
    true
}

pub fn select_inject_rule(
    req: &InjectLookupRequest,
    policy_strict: bool,
    case_rules: &[InjectRule],
    session_rules: &[InjectRule],
) -> Option<InjectRuleMatch> {
    let policy_rules: Vec<InjectRule> = if policy_strict {
        vec![InjectRule {
            name: STRICT_POLICY_RULE_NAME.to_string(),
            priority: 0,
            when: InjectRuleWhen::default(),
            then: InjectRuleThen {
                action: "error".to_string(),
                response: None,
                error: Some(InjectErrorResponse {
                    status: 500,
                    body: Some(json!(STRICT_POLICY_ERROR)),
                }),
            },
        }]
    } else {
        Vec::new()
    };

    let mut winner: Option<InjectRuleMatch> = None;

    macro_rules! consider {
        ($rules:expr, $layer:expr, $source:expr) => {
            for (i, rule) in $rules.iter().enumerate() {
                if !rule_matches_inject(rule, req) {
                    continue;
                }
                let candidate = InjectRuleMatch {
                    layer: $layer,
                    order: i,
                    rule: rule.clone(),
                    source: $source,
                };
                if winner
                    .as_ref()
                    .map(|w: &InjectRuleMatch| better_inject_rule(&candidate, w))
                    .unwrap_or(true)
                {
                    winner = Some(candidate);
                }
            }
        };
    }

    consider!(&policy_rules, RuleLayer::SessionPolicy, "policy");
    consider!(case_rules, RuleLayer::CaseEmbedded, "case");
    consider!(session_rules, RuleLayer::SessionRules, "session");

    winner
}

fn better_inject_rule(candidate: &InjectRuleMatch, current: &InjectRuleMatch) -> bool {
    if candidate.rule.priority != current.rule.priority {
        return candidate.rule.priority > current.rule.priority;
    }
    if candidate.layer != current.layer {
        return candidate.layer > current.layer;
    }
    candidate.order > current.order
}

pub fn build_mock_response(rule: &InjectRule) -> Option<MockResponse> {
    let r = rule.then.response.as_ref()?;
    let mut headers = Vec::new();
    for (k, v) in &r.headers {
        headers.push((k.clone(), v.clone()));
    }
    let body = r.body.as_ref().map(normalize_mock_body).unwrap_or_default();
    let status = if r.status == 0 { 200 } else { r.status };
    Some(MockResponse {
        status_code: status,
        headers,
        body,
    })
}

pub fn build_error_response(rule: &InjectRule) -> (i64, String) {
    let e = match &rule.then.error {
        Some(e) => e,
        None => return (500, String::new()),
    };
    let status = if e.status == 0 { 500 } else { e.status };
    let msg = e.body.as_ref().map(normalize_mock_body).unwrap_or_default();
    (status, msg)
}

fn normalize_mock_body(raw: &serde_json::Value) -> String {
    if let Ok(s) = serde_json::from_value::<String>(raw.clone()) {
        return s;
    }
    raw.to_string()
}

/// Encode inject hit as OTLP protobuf (`TracesData` with one span carrying mock attrs).
pub fn encode_inject_response_proto(response: &MockResponse) -> Result<Vec<u8>> {
    use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
    let otlp_attrs = mock_response_key_values(response);

    let td = TracesData {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![0u8; 16],
                    span_id: vec![0u8; 8],
                    parent_span_id: vec![],
                    name: "sp.inject.response".to_string(),
                    kind: SpanKind::Internal as i32,
                    start_time_unix_nano: 0,
                    end_time_unix_nano: 0,
                    attributes: otlp_attrs,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut buf = Vec::new();
    td.encode(&mut buf)?;
    Ok(buf)
}

fn mock_response_key_values(response: &MockResponse) -> Vec<KeyValue> {
    let mut out = vec![KeyValue {
        key: "http.response.status_code".to_string(),
        value: Some(AnyValue {
            value: Some(AvValue::IntValue(response.status_code)),
        }),
    }];
    for (n, v) in &response.headers {
        out.push(KeyValue {
            key: format!("http.response.header.{n}"),
            value: Some(AnyValue {
                value: Some(AvValue::StringValue(v.clone())),
            }),
        });
    }
    if !response.body.is_empty() {
        out.push(KeyValue {
            key: "http.response.body".to_string(),
            value: Some(AnyValue {
                value: Some(AvValue::StringValue(response.body.clone())),
            }),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    fn string_kv(key: &str, v: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AvValue::StringValue(v.to_string())),
            }),
        }
    }

    fn inject_lookup_request_sample() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_kv("service.name", "my-svc")],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        attributes: vec![
                            string_kv("sp.span.type", "inject"),
                            string_kv("sp.session.id", "sess-1"),
                            string_kv("sp.service.name", ""),
                            string_kv("sp.traffic.direction", "outbound"),
                            string_kv("url.host", "api.example"),
                            string_kv("http.request.method", "POST"),
                            string_kv("url.path", "/v1/x"),
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn normalize_otlp_body_proto_roundtrip() {
        let req = inject_lookup_request_sample();
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let out = normalize_otlp_body(&buf).unwrap();
        assert!(!out.resource_spans.is_empty());
    }

    #[test]
    fn normalize_otlp_body_json() {
        let j = br#"{"resourceSpans":[]}"#;
        let out = normalize_otlp_body(j).unwrap();
        assert!(out.resource_spans.is_empty());
    }

    #[test]
    fn parse_inject_lookup_ok() {
        let req = inject_lookup_request_sample();
        let lu = parse_inject_lookup(&req).unwrap();
        assert_eq!(lu.session_id, "sess-1");
        assert_eq!(lu.service_name, "my-svc");
        assert_eq!(lu.request_method, "POST");
        assert_eq!(lu.url_path, "/v1/x");
        assert_eq!(lu.url_host, "api.example");
    }

    #[test]
    fn parse_inject_lookup_missing_span_errors() {
        let req = ExportTraceServiceRequest::default();
        assert!(parse_inject_lookup(&req).is_err());
    }

    #[test]
    fn parse_inject_rules_document_empty_and_rules() {
        assert!(parse_inject_rules_document(b"").unwrap().is_empty());
        let doc = br#"{"rules":[{"name":"r1","priority":1,"when":{},"then":{"action":"mock","response":{"status":200}}}]} "#;
        let rules = parse_inject_rules_document(doc).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].name, "r1");
    }

    #[test]
    fn case_embedded_rules_empty_and_ok() {
        assert!(case_embedded_rules(b"").is_empty());
        let case = br#"{"rules":[],"version":"1"}"#;
        assert!(case_embedded_rules(case).is_empty());
    }

    #[test]
    fn is_strict_external_http_policy_detects() {
        assert!(!is_strict_external_http_policy(b""));
        assert!(!is_strict_external_http_policy(br#"{"externalHttp":"lax"}"#));
        assert!(is_strict_external_http_policy(
            br#"{"externalHttp":"strict"}"#
        ));
    }

    #[test]
    fn rule_matches_inject_matrix() {
        let req = InjectLookupRequest {
            session_id: "s".into(),
            service_name: "svc".into(),
            traffic_direction: "out".into(),
            url_host: "h".into(),
            url_path: "/api".into(),
            request_method: "GET".into(),
        };
        let rule = InjectRule {
            name: "a".into(),
            priority: 1,
            when: InjectRuleWhen {
                direction: "out".into(),
                service: "svc".into(),
                host: "h".into(),
                method: "GET".into(),
                path: "/api".into(),
                path_prefix: "".into(),
            },
            then: InjectRuleThen {
                action: "mock".into(),
                response: None,
                error: None,
            },
        };
        assert!(rule_matches_inject(&rule, &req));
        let mut bad = rule.clone();
        bad.when.method = "POST".into();
        assert!(!rule_matches_inject(&bad, &req));
        let mut px = rule.clone();
        px.when.path = "".into();
        px.when.path_prefix = "/ap".into();
        assert!(rule_matches_inject(&px, &req));
    }

    #[test]
    fn select_inject_rule_priority_and_strict_policy() {
        let req = InjectLookupRequest {
            session_id: "s".into(),
            service_name: "svc".into(),
            traffic_direction: "".into(),
            url_host: "".into(),
            url_path: "/x".into(),
            request_method: "GET".into(),
        };
        let session_rule = InjectRule {
            name: "sr".into(),
            priority: 1,
            when: InjectRuleWhen {
                path: "/x".into(),
                ..Default::default()
            },
            then: InjectRuleThen {
                action: "mock".into(),
                response: Some(InjectMockResponse {
                    status: 200,
                    headers: Default::default(),
                    body: None,
                }),
                error: None,
            },
        };
        let hit = select_inject_rule(&req, false, &[], &[session_rule.clone()]).unwrap();
        assert_eq!(hit.layer, RuleLayer::SessionRules);

        let strict_hit = select_inject_rule(&req, true, &[], &[]).unwrap();
        assert_eq!(strict_hit.rule.name, STRICT_POLICY_RULE_NAME);

        let hi = InjectRule {
            name: "hi".into(),
            priority: 99,
            when: InjectRuleWhen {
                path: "/x".into(),
                ..Default::default()
            },
            then: InjectRuleThen {
                action: "mock".into(),
                response: Some(InjectMockResponse {
                    status: 201,
                    headers: Default::default(),
                    body: None,
                }),
                error: None,
            },
        };
        let winner = select_inject_rule(&req, false, &[hi.clone()], &[session_rule]).unwrap();
        assert_eq!(winner.rule.name, "hi");
    }

    #[test]
    fn build_mock_and_error_response_helpers() {
        let mock_rule = InjectRule {
            name: "m".into(),
            priority: 0,
            when: Default::default(),
            then: InjectRuleThen {
                action: "mock".into(),
                response: Some(InjectMockResponse {
                    status: 0,
                    headers: [("X-T".into(), "v".into())].into_iter().collect(),
                    body: Some(serde_json::json!("plain")),
                }),
                error: None,
            },
        };
        let mr = build_mock_response(&mock_rule).unwrap();
        assert_eq!(mr.status_code, 200);
        assert_eq!(mr.body, "plain");

        let err_rule = InjectRule {
            name: "e".into(),
            priority: 0,
            when: Default::default(),
            then: InjectRuleThen {
                action: "error".into(),
                response: None,
                error: Some(InjectErrorResponse {
                    status: 0,
                    body: Some(serde_json::json!("oops")),
                }),
            },
        };
        let (st, msg) = build_error_response(&err_rule);
        assert_eq!(st, 500);
        assert!(msg.contains("oops"));
    }

    #[test]
    fn encode_inject_response_proto_non_empty() {
        let mr = MockResponse {
            status_code: 201,
            headers: vec![("H".into(), "V".into())],
            body: "b".into(),
        };
        let bytes = encode_inject_response_proto(&mr).unwrap();
        assert!(!bytes.is_empty());
    }
}
