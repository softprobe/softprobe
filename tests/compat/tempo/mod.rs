//! Tempo Phase 0/3 compatibility contracts and evidence-backed cases.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::compat::backends::traces::{TraceData, TraceSpan};
use softprobe_runtime::compat::envelopes::{
    error_envelope, error_response, success_envelope_minimal,
};
use softprobe_runtime::compat::errors::{CompatError, CompatErrorCode};
use softprobe_runtime::compat::tempo::encode::{trace_v1_response, trace_v2_response};
use softprobe_runtime::compat::tenant::{ProtocolScope, QueryLimits, TenantContext};
use std::collections::BTreeSet;
use std::net::TcpListener;
use tempfile::TempDir;
use tower::ServiceExt;

#[cfg(feature = "integration-e2e")]
use crate::compat_support::conformance::CompatExecutionRecorder;
#[cfg(feature = "integration-e2e")]
use crate::compat_support::conformance::{descriptor_for_case, select_cases};
use crate::compat_support::conformance::{parse_case_selection, select_differential_cases};

use crate::compat_support::tempo::{
    assert_all_resource_scope_groups_preserve_spans_and_link_attributes, assert_case_contract,
    build_seeded_tempo_router, fixture, flush_traces, ingest_records, query_case,
    query_case_with_scope, query_path_with_scope, reference_image_from_manifest,
    TempoLinkExpectation, TempoParameterMatrixCase, TempoSpanLinkExpectation,
    TEMPO_PARAMETER_MATRIX,
};
use crate::util::config::file_backed_test_config;

const EXPECTED_TEMPO_LINKS: &[TempoSpanLinkExpectation<'static>] = &[
    TempoSpanLinkExpectation {
        span_id: "AAAAAAAAAAE=",
        links: &[TempoLinkExpectation {
            trace_id: "u7u7u7u7u7u7u7u7u7u7uw==",
            span_id: "AAAAAAAAAAM=",
            attributes: &[("link.kind", "async"), ("link.source", "checkout")],
        }],
    },
    TempoSpanLinkExpectation {
        span_id: "AAAAAAAAAAQ=",
        links: &[TempoLinkExpectation {
            trace_id: "u7u7u7u7u7u7u7u7u7u7uw==",
            span_id: "AAAAAAAAAAM=",
            attributes: &[("link.kind", "cache"), ("link.source", "grandchild")],
        }],
    },
];

#[cfg(feature = "integration-e2e")]
use crate::compat_support::tempo::{
    build_tempo_router, normalize_tempo_response, query_tempo_oracle, require_docker,
    start_tempo_oracle, write_failure_artifacts,
};

fn load_fixture(name: &str) -> serde_json::Value {
    let path = format!(
        "{}/tests/compat/fixtures/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&raw).expect("parse fixture")
}

fn skip_if_sandbox_cannot_bind_test_port(test_id: &str) -> bool {
    match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => {
            drop(listener);
            false
        }
        Err(error) => {
            eprintln!("SKIP {test_id}: sandbox cannot bind a local Wiremock port: {error}");
            true
        }
    }
}

#[test]
fn tempo_scope_header_must_match_tenant() {
    let err = TenantContext::from_authenticated(
        TenantInfo {
            tenant_id: "tenant-a".into(),
            bucket_name: "b".into(),
            dataset_id: "d".into(),
        },
        ProtocolScope::Tempo,
        Some("other"),
        QueryLimits::default(),
    )
    .unwrap_err();
    assert_eq!(err.code, CompatErrorCode::Forbidden);
}

#[test]
fn tempo_error_fixture_matches_envelope_helper() {
    let expected = load_fixture("tempo_error_unsupported.json");
    let actual = error_envelope(ProtocolScope::Tempo, &CompatError::unsupported("tempo_api"));
    assert_eq!(actual, expected);
}

#[test]
fn tempo_success_minimal_fixture_matches_helper() {
    let expected = load_fixture("tempo_success_minimal.json");
    assert_eq!(success_envelope_minimal(ProtocolScope::Tempo), expected);
}

#[test]
fn tempo_phase3_fixture_reference_image_tracks_immutable_manifest_pin() {
    let fixture = fixture();
    assert_eq!(
        fixture.evidence.reference_image,
        reference_image_from_manifest()
    );
    assert!(
        fixture.evidence.reference_image.contains("@sha256:"),
        "Tempo evidence must identify the immutable manifest reference"
    );
}

#[test]
fn tempo_phase3_fixture_has_issue_evidence_capabilities_and_full_get_matrix() {
    let fixture = fixture();
    assert_eq!(fixture.evidence.issue, "#31");
    assert_eq!(fixture.evidence.phase, "Phase 3");
    assert_eq!(
        fixture.evidence.reference_manifest,
        "docs/compat/references.v0.yaml"
    );
    assert!(fixture
        .evidence
        .normalization
        .contains("normalize_tempo_response"));
    assert_eq!(fixture.capability.protocol, "tempo");
    assert_eq!(fixture.capability.phase, "phase_3");
    assert_eq!(
        fixture.capability.ordering_policy,
        "trace_start_time_asc,trace_id_asc; spans_start_time_asc,span_id_asc; tag_names_asc; tag_values_asc"
    );
    assert_eq!(fixture.capability.supported_endpoints.len(), 5);
    for feature in [
        "v1_trace_lookup",
        "v2_trace_lookup",
        "search",
        "tag_names",
        "tag_values",
        "traceql_resource_selectors",
        "traceql_span_selectors",
        "traceql_intrinsic_filters",
        "nanosecond_timing",
        "topology_parent_span_ids",
        "resource_attributes_where_stored",
        "span_attributes",
        "status",
        "events",
        "instrumentation_scope",
        "links",
        "deterministic_ordering",
        "empty_results",
        "tenant_scoping",
    ] {
        assert!(
            fixture
                .capability
                .supported_features
                .iter()
                .any(|value| value == feature),
            "missing supported capability {feature}"
        );
    }
    for feature in [
        "traceql_pipelines",
        "traceql_aggregations",
        "event_fields",
        "link_fields",
        "instrumentation_scope_fields",
        "tag_query_filters",
    ] {
        assert!(
            fixture
                .capability
                .unsupported_features
                .iter()
                .any(|value| value == feature),
            "missing unsupported capability {feature}"
        );
    }
    assert!(fixture.capability.fidelity_gaps.is_empty());
    let primary_resource = &fixture.records[0].resource;
    let primary_scope = &fixture.records[0].scope;
    assert_eq!(fixture.records.len(), 5);
    assert_eq!(
        fixture
            .records
            .iter()
            .map(|record| record.trace_id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ])
    );
    assert!(fixture.records[..3].iter().all(|record| {
        &record.resource == primary_resource
            && record.scope.name == primary_scope.name
            && record.scope.version == primary_scope.version
    }));
    assert_ne!(fixture.records[3].resource, *primary_resource);
    assert_ne!(fixture.records[3].scope.name, primary_scope.name);
    assert_eq!(
        fixture.records[1].parent_span_id.as_deref(),
        Some("0000000000000001")
    );
    assert_eq!(
        fixture.records[2].parent_span_id.as_deref(),
        Some("0000000000000002")
    );
    assert_eq!(
        fixture.records[3].parent_span_id.as_deref(),
        Some("0000000000000001")
    );
    assert!(fixture.records[0]
        .events
        .iter()
        .any(|event| event.name == "exception"));
    assert!(fixture.records[2]
        .events
        .iter()
        .any(|event| event.name == "cache.miss"));
    assert!(fixture.records[0].links.iter().any(|link| {
        link.trace_id == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" && link.span_id == "0000000000000003"
    }));
    assert!(fixture.records[2].links.iter().any(|link| {
        link.trace_id == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" && link.span_id == "0000000000000003"
    }));
    assert_eq!(fixture.records[4].resource["service.name"], "worker");
    assert_ne!(fixture.records[4].scope.name, primary_scope.name);

    let endpoints = fixture
        .cases
        .iter()
        .map(|case| {
            let path = case.path.split('?').next().unwrap();
            if path.starts_with("/api/v2/traces/") {
                "/api/v2/traces/{trace_id}"
            } else if path.starts_with("/api/traces/") {
                "/api/traces/{trace_id}"
            } else if path.starts_with("/api/search/tag/") && path.ends_with("/values") {
                "/api/search/tag/{tag}/values"
            } else {
                path
            }
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        endpoints,
        BTreeSet::from([
            "/api/traces/{trace_id}",
            "/api/v2/traces/{trace_id}",
            "/api/search",
            "/api/search/tags",
            "/api/search/tag/{tag}/values",
        ])
    );

    let mut ids = BTreeSet::new();
    assert!(fixture.cases.iter().any(|case| case.differential));
    let resource_selector = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-search-resource-selector")
        .expect("Tempo resource selector differential case");
    assert_eq!(
        resource_selector.params.get("start").map(String::as_str),
        Some("1700000000"),
        "resource selector differential case must pin the fixture start time"
    );
    assert_eq!(
        resource_selector.params.get("end").map(String::as_str),
        Some("1700000003"),
        "resource selector differential case must pin the fixture end time"
    );
    for case in fixture.cases {
        assert!(ids.insert(case.id.clone()), "duplicate case ID {}", case.id);
        assert!(!case.id.is_empty());
        assert!(case.expect.status > 0);
        assert!(!case.provenance.is_empty(), "case {} provenance", case.id);
    }
}

#[cfg(test)]
mod selector_tests {
    use super::*;

    #[test]
    fn selector_parser_combines_case_ids_and_ignores_suite_sentinel() {
        let selected = parse_case_selection(
            "Tempo",
            Some("tempo-v1-trace-fidelity,tempo-search-resource-selector"),
            Some("__suite__"),
        )
        .expect("selector");

        assert_eq!(
            selected,
            Some(BTreeSet::from([
                "tempo-v1-trace-fidelity".into(),
                "tempo-search-resource-selector".into(),
            ]))
        );
    }

    #[test]
    fn selected_cases_filter_only_requested_differential_fixture_cases() {
        let fixture = fixture();
        let selection = BTreeSet::from([
            "tempo-v1-trace-fidelity".into(),
            "tempo-search-resource-selector".into(),
        ]);

        let cases = select_differential_cases("Tempo", &fixture.cases, Some(&selection), |case| {
            case.differential.then_some(case.id.clone())
        })
        .expect("selected cases");
        assert_eq!(
            cases
                .iter()
                .map(|case| case.id.as_str())
                .collect::<Vec<_>>(),
            vec!["tempo-v1-trace-fidelity", "tempo-search-resource-selector"]
        );
    }

    #[test]
    fn selector_rejects_unknown_and_non_differential_case_ids() {
        let fixture = fixture();

        for case_id in ["missing-tempo-case", "tempo-search-span-selector"] {
            let selection = BTreeSet::from([case_id.to_string()]);
            let error =
                select_differential_cases("Tempo", &fixture.cases, Some(&selection), |case| {
                    case.differential.then_some(case.id.clone())
                })
                .expect_err("rejected");
            assert!(
                error.contains(case_id),
                "error should name {case_id}: {error}"
            );
        }
    }

    #[test]
    fn absent_selector_preserves_all_differential_cases() {
        let fixture = fixture();
        let cases = select_differential_cases("Tempo", &fixture.cases, None, |case| {
            case.differential.then_some(case.id.clone())
        })
        .expect("all differential cases");
        assert_eq!(
            cases.len(),
            fixture
                .cases
                .iter()
                .filter(|case| case.differential)
                .count()
        );
    }
}

#[tokio::test]
async fn tempo_phase3_contract_cases_cover_routes_envelopes_errors_and_empty_results() {
    let fixture = fixture();
    let (router, _state, _temp) = build_seeded_tempo_router(&fixture.records).await;

    for case in &fixture.cases {
        let (status, body) = query_case(&router, case, None).await;
        assert_case_contract(case, status, &body);
    }
}

#[tokio::test]
async fn tempo_phase3_trace_responses_preserve_otlp_fidelity_and_ordering() {
    let fixture = fixture();
    let (router, _state, _temp) = build_seeded_tempo_router(&fixture.records).await;

    let v1 = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-v1-trace-fidelity")
        .expect("v1 fidelity case");
    let (_, body) = query_case(&router, v1, None).await;
    assert_all_resource_scope_groups_preserve_spans_and_link_attributes(
        &body,
        &[
            "AAAAAAAAAAE=",
            "AAAAAAAAAAI=",
            "AAAAAAAAAAQ=",
            "AAAAAAAAAAU=",
        ],
        EXPECTED_TEMPO_LINKS,
    );
    let v1_spans = crate::compat_support::tempo::v1_spans(&body);
    let span = v1_spans[0];
    assert_eq!(span["traceId"], "qqqqqqqqqqqqqqqqqqqqqg==");
    assert_eq!(span["spanId"], "AAAAAAAAAAE=");
    assert!(span.get("parentSpanId").is_none());
    assert_eq!(span["startTimeUnixNano"], "1700000000123456789");
    assert_eq!(span["endTimeUnixNano"], "1700000002123456789");
    let http_method = span["attributes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|attribute| attribute["key"] == "http.method")
        .expect("http.method attribute");
    assert_eq!(http_method["value"]["stringValue"], "GET");
    assert_eq!(span["status"]["code"], "STATUS_CODE_ERROR");
    assert_eq!(span["status"]["message"], "upstream failed");
    assert_eq!(span["events"][0]["name"], "exception");
    assert_eq!(span["events"][0]["timeUnixNano"], "1700000001123456789");
    let resource_attributes = body["batches"][0]["resource"]["attributes"]
        .as_array()
        .expect("resource attributes");
    assert!(resource_attributes.iter().any(|attribute| {
        attribute["key"] == "service.name" && attribute["value"]["stringValue"] == "api"
    }));
    assert!(resource_attributes.iter().any(|attribute| {
        attribute["key"] == "deployment.environment" && attribute["value"]["stringValue"] == "prod"
    }));
    assert_eq!(
        body["batches"][0]["scopeSpans"][0]["scope"],
        serde_json::json!({
            "name": "tempo.phase3.fixture",
            "version": "1.0.0",
            "attributes": []
        })
    );
    assert_eq!(span["links"][0]["traceId"], "u7u7u7u7u7u7u7u7u7u7uw==");

    let v2 = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-v2-trace-fidelity")
        .expect("v2 fidelity case");
    let (_, body) = query_case(&router, v2, None).await;
    assert_all_resource_scope_groups_preserve_spans_and_link_attributes(
        &body,
        &[
            "AAAAAAAAAAE=",
            "AAAAAAAAAAI=",
            "AAAAAAAAAAQ=",
            "AAAAAAAAAAU=",
        ],
        EXPECTED_TEMPO_LINKS,
    );
    let spans = crate::compat_support::tempo::v2_spans(&body);
    assert_eq!(spans[0]["traceId"], "qqqqqqqqqqqqqqqqqqqqqg==");
    assert_eq!(
        spans
            .iter()
            .map(|span| span["spanId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "AAAAAAAAAAE=",
            "AAAAAAAAAAI=",
            "AAAAAAAAAAQ=",
            "AAAAAAAAAAU="
        ]
    );
}

#[tokio::test]
async fn tempo_phase3_trace_topology_and_search_order_are_deterministic() {
    let fixture = fixture();
    let (router, _state, _temp) = build_seeded_tempo_router(&fixture.records).await;

    let v2 = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-v2-trace-fidelity")
        .expect("v2 fidelity case");
    let (_, body) = query_case(&router, v2, None).await;
    let spans = crate::compat_support::tempo::v2_spans(&body);
    assert_eq!(spans[2]["parentSpanId"], "AAAAAAAAAAI=");
    assert_eq!(spans[3]["parentSpanId"], "AAAAAAAAAAE=");
    assert_eq!(spans[2]["events"][0]["name"], "cache.miss");
    assert_eq!(spans[2]["links"][0]["traceId"], "u7u7u7u7u7u7u7u7u7u7uw==");

    let search = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-search-deterministic-order")
        .expect("ordering case");
    let (_, body) = query_case(&router, search, None).await;
    assert_eq!(
        body["traces"]
            .as_array()
            .expect("search traces")
            .iter()
            .map(|trace| trace["traceID"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ]
    );
    // Tempo search metadata uses its hexadecimal compatibility representation;
    // OTLP trace payload IDs on the lookup routes are Base64 encoded.
    assert_ne!(body["traces"][0]["traceID"], "qqqqqqqqqqqqqqqqqqqqqg==");
}

#[tokio::test]
async fn tempo_phase3_tag_projection_matches_tempo_event_and_link_tags() {
    let fixture = fixture();
    let (router, _state, _temp) = build_seeded_tempo_router(&fixture.records).await;

    let tag_names = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-tag-names")
        .expect("tag names case");
    let (status, body) = query_case(&router, tag_names, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["tagNames"],
        serde_json::json!([
            "db.system",
            "deployment.environment",
            "exception.message",
            "exception.type",
            "http.method",
            "http.route",
            "link.kind",
            "link.source",
            "peer.service",
            "queue.name",
            "service.name"
        ])
    );
}

#[tokio::test]
async fn tempo_phase3_traceql_filters_cover_resource_span_intrinsic_and_unsupported_syntax() {
    let fixture = fixture();
    let (router, _state, _temp) = build_seeded_tempo_router(&fixture.records).await;

    for id in [
        "tempo-search-resource-selector",
        "tempo-search-span-selector",
        "tempo-search-service-filter",
        "tempo-search-intrinsic-filter",
        "tempo-search-malformed-traceql",
        "tempo-search-instrumentation-name",
        "tempo-search-instrumentation-version",
        "tempo-search-name-filter",
        "tempo-search-status-filter",
        "tempo-search-span-status-code-gte-2",
        "tempo-search-duration-filter",
        "tempo-search-valid-bounds",
        "tempo-search-invalid-bound-value",
        "tempo-search-reversed-bounds",
        "tempo-search-duration-bounds",
        "tempo-search-limit-zero",
        "tempo-search-limit-one",
        "tempo-search-limit-exceeded",
        "tempo-search-tags",
        "tempo-search-empty-result",
        "tempo-search-unsupported-pipeline",
    ] {
        let case = fixture.cases.iter().find(|case| case.id == id).unwrap();
        let (status, body) = query_case(&router, case, None).await;
        assert_case_contract(case, status, &body);
    }
}

#[tokio::test]
async fn tempo_phase3_traceql_rejects_mixed_numeric_string_values() {
    let fixture = fixture();
    let (router, _state, _temp) = build_seeded_tempo_router(&fixture.records).await;
    let (status, body) = query_path_with_scope(
        &router,
        "/api/search?q=%7B%20span.http.status_code%20%3E%3D%20%22slow%22%20%7D",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["softprobe_code"], "bad_request");
}

#[tokio::test]
async fn tempo_phase3_malformed_trace_ids_use_tempo_bad_request_envelope() {
    if skip_if_sandbox_cannot_bind_test_port(
        "tempo_phase3_malformed_trace_ids_use_tempo_bad_request_envelope",
    ) {
        return;
    }
    let temp = TempDir::new().expect("malformed trace ID temp");
    let (router, _state, _mock) = crate::compat_support::auth::authenticated_router(
        std::sync::Arc::new(file_backed_test_config(&temp)),
        "tenant-auth",
        true,
    )
    .await;

    for path in [
        "/api/traces/not-a-valid-trace-id",
        "/api/v2/traces/not-a-valid-trace-id",
        "/api/traces/00010203",
        "/api/v2/traces/00010203",
    ] {
        let (status, body) =
            query_path_with_scope(&router, path, Some("tenant-auth-key"), Some("tenant-auth"))
                .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_eq!(body["softprobe_code"], "bad_request", "{path}: {body}");
    }
}

#[tokio::test]
async fn tempo_phase3_unknown_trace_lookup_params_are_explicitly_unsupported() {
    if skip_if_sandbox_cannot_bind_test_port(
        "tempo_phase3_unknown_trace_lookup_params_are_explicitly_unsupported",
    ) {
        return;
    }
    let temp = TempDir::new().expect("unknown lookup parameter temp");
    let (router, _state, _mock) = crate::compat_support::auth::authenticated_router(
        std::sync::Arc::new(file_backed_test_config(&temp)),
        "tenant-auth",
        true,
    )
    .await;

    for path in [
        "/api/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?bogus=1",
        "/api/v2/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?bogus=1",
    ] {
        let (status, body) =
            query_path_with_scope(&router, path, Some("tenant-auth-key"), Some("tenant-auth"))
                .await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{path}: {body}");
        assert_eq!(
            body["softprobe_code"], "unsupported_feature",
            "{path}: {body}"
        );
    }
}

#[tokio::test]
async fn tempo_phase3_trace_lookup_bounds_apply_to_v1_and_v2_routes() {
    if skip_if_sandbox_cannot_bind_test_port(
        "tempo_phase3_trace_lookup_bounds_apply_to_v1_and_v2_routes",
    ) {
        return;
    }
    let fixture = fixture();
    let (router, _state, _temp) = build_seeded_tempo_router(&fixture.records).await;

    for path in [
        "/api/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?end=1700000000",
        "/api/v2/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?end=1700000000",
        "/api/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?start=1700000003",
        "/api/v2/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?start=1700000003",
    ] {
        let (status, body) = query_path_with_scope(&router, path, None, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}: {body}");
        assert_eq!(body["message"], "trace not found", "{path}: {body}");
    }
}

#[tokio::test]
async fn tempo_phase3_typed_search_params_preserve_nanoseconds_duration_and_limit() {
    let fixture = fixture();
    let (router, _state, _temp) = build_seeded_tempo_router(&fixture.records).await;
    let (status, body) = query_path_with_scope(
        &router,
        "/api/search?start=1700000000.123456789&end=1700000003.123456789&minDuration=2s&maxDuration=2s&limit=1",
        None,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["traces"].as_array().expect("search traces").len(), 1);
    assert_eq!(
        body["traces"][0]["traceID"],
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(
        body["traces"][0]["startTimeUnixNano"],
        "1700000000123456789"
    );
    assert_eq!(body["traces"][0]["durationMs"], 2000);
}

#[tokio::test]
async fn tempo_phase3_malformed_stored_ids_use_public_error_response() {
    fn data(trace_id: &str, span_id: &str, links: Vec<serde_json::Value>) -> TraceData {
        TraceData {
            spans: vec![TraceSpan {
                trace_id: trace_id.into(),
                span_id: span_id.into(),
                parent_span_id: None,
                name: "malformed".into(),
                kind: Some("SPAN_KIND_SERVER".into()),
                start_time_unix_nano: 1,
                end_time_unix_nano: Some(2),
                attributes: Vec::new(),
                status_code: None,
                status_message: None,
                events: Vec::new(),
                service_name: Some("tempo-test".into()),
                resource_attributes: Vec::new(),
                instrumentation_scope: None,
                links,
            }],
        }
    }

    for data in [
        data("not-a-valid-trace-id", "0001020304050607", Vec::new()),
        data("00010203", "0001020304050607", Vec::new()),
        data("000102030405060708090a0b0c0d0e0f", "00010203", Vec::new()),
        data(
            "000102030405060708090a0b0c0d0e0f",
            "0001020304050607",
            vec![serde_json::json!({
                "traceId": "00010203",
                "spanId": "0001020304050607"
            })],
        ),
    ] {
        for encode in [trace_v1_response, trace_v2_response] {
            let error = match encode(&data, 100_000) {
                Ok(_) => panic!("malformed stored Tempo ID unexpectedly encoded"),
                Err(error) => error,
            };
            assert_eq!(error.code, CompatErrorCode::BadRequest);
            let response = error_response(ProtocolScope::Tempo, error);
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("Tempo public error body");
            let body: serde_json::Value = serde_json::from_slice(&body).expect("Tempo error JSON");
            assert_eq!(body["softprobe_code"], "bad_request");
        }
    }
}

#[tokio::test]
async fn tempo_phase3_auth_middleware_is_required_on_all_get_routes() {
    if skip_if_sandbox_cannot_bind_test_port(
        "tempo_phase3_auth_middleware_is_required_on_all_get_routes",
    ) {
        return;
    }
    let paths = [
        "/api/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/api/v2/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/api/search",
        "/api/search/tags",
        "/api/search/tag/service.name/values",
    ];
    let missing_temp = TempDir::new().expect("missing-auth temp");
    let (missing_router, _state, _mock) = crate::compat_support::auth::authenticated_router(
        std::sync::Arc::new(file_backed_test_config(&missing_temp)),
        "tenant-auth",
        true,
    )
    .await;
    for path in paths {
        let response = missing_router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "missing {path}"
        );
    }

    let invalid_temp = TempDir::new().expect("invalid-auth temp");
    let (invalid_router, _state, _mock) = crate::compat_support::auth::authenticated_router(
        std::sync::Arc::new(file_backed_test_config(&invalid_temp)),
        "tenant-auth",
        false,
    )
    .await;
    for path in paths {
        let response = invalid_router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .header("Authorization", "Bearer invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "invalid {path}");
    }
}

#[tokio::test]
async fn tempo_phase3_same_trace_id_isolated_across_all_routes() {
    if skip_if_sandbox_cannot_bind_test_port(
        "tempo_phase3_same_trace_id_isolated_across_all_routes",
    ) {
        return;
    }
    let fixture = fixture();
    let temp_a = TempDir::new().expect("tenant A temp");
    let temp_b = TempDir::new().expect("tenant B temp");
    let (router_a, state_a, _mock_a) = crate::compat_support::auth::authenticated_router(
        std::sync::Arc::new(file_backed_test_config(&temp_a)),
        "tenant-a",
        true,
    )
    .await;
    let (router_b, state_b, _mock_b) = crate::compat_support::auth::authenticated_router(
        std::sync::Arc::new(file_backed_test_config(&temp_b)),
        "tenant-b",
        true,
    )
    .await;
    ingest_records(&router_a, &fixture.records[..4], Some("tenant-a-key")).await;
    let mut tenant_b_records = fixture.records[..4].to_vec();
    for record in &mut tenant_b_records {
        record.attributes.remove("peer.service");
    }
    tenant_b_records[1].name = "tenant-b-query".into();
    tenant_b_records[3]
        .attributes
        .insert("tenant.only".into(), "tenant-b".into());
    ingest_records(&router_b, &tenant_b_records, Some("tenant-b-key")).await;
    flush_traces(&state_a, "tenant-a").await;
    flush_traces(&state_b, "tenant-b").await;

    let v1 = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-v1-trace-fidelity")
        .unwrap();
    let (status_a, body_a) =
        query_case_with_scope(&router_a, v1, Some("tenant-a-key"), Some("tenant-a")).await;
    let (status_b, body_b) =
        query_case_with_scope(&router_b, v1, Some("tenant-b-key"), Some("tenant-b")).await;
    assert_eq!(status_a, StatusCode::OK);
    assert_eq!(status_b, StatusCode::OK);
    assert_eq!(crate::compat_support::tempo::v1_spans(&body_a).len(), 4);
    assert_eq!(crate::compat_support::tempo::v1_spans(&body_b).len(), 4);
    assert_eq!(
        crate::compat_support::tempo::v1_spans(&body_a)[0]["spanId"],
        "AAAAAAAAAAE="
    );
    assert_eq!(
        crate::compat_support::tempo::v1_spans(&body_b)[0]["spanId"],
        "AAAAAAAAAAE="
    );
    assert!(body_a.to_string().contains("query"));
    assert!(!body_a.to_string().contains("tenant-b-query"));
    assert!(body_b.to_string().contains("tenant-b-query"));
    assert!(!body_b.to_string().contains("\"query\""));

    let v2 = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-v2-trace-fidelity")
        .unwrap();
    let (status_a, body_a) =
        query_case_with_scope(&router_a, v2, Some("tenant-a-key"), Some("tenant-a")).await;
    let (status_b, body_b) =
        query_case_with_scope(&router_b, v2, Some("tenant-b-key"), Some("tenant-b")).await;
    assert_eq!(status_a, StatusCode::OK);
    assert_eq!(status_b, StatusCode::OK);
    assert_eq!(crate::compat_support::tempo::v2_spans(&body_a).len(), 4);
    assert_eq!(crate::compat_support::tempo::v2_spans(&body_b).len(), 4);

    let name = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-search-name-filter")
        .unwrap();
    let (status_a, body_a) =
        query_case_with_scope(&router_a, name, Some("tenant-a-key"), Some("tenant-a")).await;
    let (status_b, body_b) =
        query_case_with_scope(&router_b, name, Some("tenant-b-key"), Some("tenant-b")).await;
    assert_eq!(
        (status_a, body_a["traces"].as_array().unwrap().len()),
        (StatusCode::OK, 1)
    );
    assert_eq!(
        (status_b, body_b["traces"].as_array().unwrap().len()),
        (StatusCode::OK, 0)
    );

    let tag_names = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-tag-names")
        .unwrap();
    let (status_a, body_a) =
        query_case_with_scope(&router_a, tag_names, Some("tenant-a-key"), Some("tenant-a")).await;
    let (status_b, body_b) =
        query_case_with_scope(&router_b, tag_names, Some("tenant-b-key"), Some("tenant-b")).await;
    assert_eq!(status_a, StatusCode::OK);
    assert_eq!(status_b, StatusCode::OK);
    assert!(body_a["tagNames"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tag| tag == "peer.service"));
    assert!(!body_b["tagNames"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tag| tag == "peer.service"));
    assert!(body_b["tagNames"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tag| tag == "tenant.only"));

    let tag_values = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-tag-values-peer-service")
        .unwrap();
    let (status_a, body_a) = query_case_with_scope(
        &router_a,
        tag_values,
        Some("tenant-a-key"),
        Some("tenant-a"),
    )
    .await;
    let (status_b, body_b) = query_case_with_scope(
        &router_b,
        tag_values,
        Some("tenant-b-key"),
        Some("tenant-b"),
    )
    .await;
    assert_eq!(status_a, StatusCode::OK);
    assert_eq!(status_b, StatusCode::OK);
    assert_eq!(body_a["tagValues"], serde_json::json!(["postgres"]));
    assert_eq!(body_b["tagValues"], serde_json::json!([]));
}

#[tokio::test]
async fn tempo_phase3_spoofed_scope_header_is_forbidden_on_all_routes() {
    if skip_if_sandbox_cannot_bind_test_port(
        "tempo_phase3_spoofed_scope_header_is_forbidden_on_all_routes",
    ) {
        return;
    }
    let temp = TempDir::new().expect("spoofed scope temp");
    let (router, _state, _mock) = crate::compat_support::auth::authenticated_router(
        std::sync::Arc::new(file_backed_test_config(&temp)),
        "tenant-auth",
        true,
    )
    .await;
    for path in [
        "/api/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/api/v2/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/api/search",
        "/api/search/tags",
        "/api/search/tag/service.name/values",
    ] {
        let (status, body) = query_path_with_scope(
            &router,
            path,
            Some("tenant-auth-key"),
            Some("spoofed-tenant"),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}: {body}");
        assert_eq!(body["softprobe_code"], "forbidden", "{path}: {body}");
    }
}

#[tokio::test]
async fn tempo_phase3_parameter_matrix_preserves_authenticated_tenant_and_rejects_unknowns() {
    if skip_if_sandbox_cannot_bind_test_port(
        "tempo_phase3_parameter_matrix_preserves_authenticated_tenant_and_rejects_unknowns",
    ) {
        return;
    }
    let fixture = fixture();
    let temp = TempDir::new().expect("parameter matrix temp");
    let (router, state, _mock) = crate::compat_support::auth::authenticated_router(
        std::sync::Arc::new(file_backed_test_config(&temp)),
        "tenant-auth",
        true,
    )
    .await;
    ingest_records(&router, &fixture.records, Some("tenant-auth-key")).await;
    flush_traces(&state, "tenant-auth").await;

    for TempoParameterMatrixCase { id, path } in TEMPO_PARAMETER_MATRIX {
        let (baseline_status, baseline_body) =
            query_path_with_scope(&router, path, Some("tenant-auth-key"), Some("tenant-auth"))
                .await;
        assert_eq!(baseline_status, StatusCode::OK, "{id}: baseline {path}");

        let tenant_id_path = format!("{path}?tenant_id=attacker-tenant");
        let (tenant_id_status, tenant_id_body) = query_path_with_scope(
            &router,
            &tenant_id_path,
            Some("tenant-auth-key"),
            Some("tenant-auth"),
        )
        .await;
        assert_eq!(tenant_id_status, baseline_status, "{id}: tenant_id status");
        assert_eq!(
            tenant_id_body, baseline_body,
            "{id}: tenant_id changed tenant"
        );

        let unknown_path = format!("{path}?unknown_parameter=1");
        let (unknown_status, unknown_body) = query_path_with_scope(
            &router,
            &unknown_path,
            Some("tenant-auth-key"),
            Some("tenant-auth"),
        )
        .await;
        assert_eq!(
            unknown_status,
            StatusCode::NOT_IMPLEMENTED,
            "{id}: unknown parameter status"
        );
        assert_eq!(
            unknown_body["softprobe_code"], "unsupported_feature",
            "{id}: unknown parameter code"
        );
    }
}

#[cfg(feature = "integration-e2e")]
#[test]
fn tempo_oracle_config_searches_historical_fixture_from_live_store() {
    let config_text = include_str!("../support/tempo.rs");
    let search_config = config_text
        .split_once("query_frontend:\n  search:\n")
        .map(|(_, config)| config)
        .expect("Tempo oracle query_frontend.search config");
    assert!(search_config.contains("query_backend_after: 87600h"));
    assert!(search_config.contains("query_ingesters_until: 87600h"));
    assert!(config_text.contains("ingestion_time_range_slack: 87600h"));
    assert!(config_text.contains("max_block_duration: 1s"));
    assert!(config_text.contains("complete_block_timeout: 1s"));
}

#[cfg(feature = "integration-e2e")]
#[test]
fn tempo_normalization_ignores_reference_grouping_and_otlp_defaults() {
    let lake = serde_json::json!({
        "batches": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]},
            "scopeSpans": [{
                "scope": {"attributes": [], "name": "scope", "version": "1.0"},
                "spans": [{
                    "traceId": "trace",
                    "spanId": "span",
                    "startTimeUnixNano": 1,
                    "links": [{
                        "traceId": "linked-trace",
                        "spanId": "linked-span",
                        "attributes": [
                            {"key": "link.source", "value": {"stringValue": "checkout"}},
                            {"key": "link.kind", "value": {"stringValue": "async"}}
                        ],
                        "flags": 0,
                        "traceState": ""
                    }]
                }, {
                    "traceId": "trace",
                    "spanId": "span-2",
                    "startTimeUnixNano": 2
                }]
            }]
        }]
    });
    let oracle = serde_json::json!({
        "batches": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]},
            "scopeSpans": [{
                "scope": {"name": "scope", "version": "1.0"},
                "spans": [{
                    "traceId": "trace",
                    "spanId": "span",
                    "startTimeUnixNano": "1",
                    "links": [{
                        "traceId": "linked-trace",
                        "spanId": "linked-span",
                        "attributes": [
                            {"key": "link.kind", "value": {"stringValue": "async"}},
                            {"key": "link.source", "value": {"stringValue": "checkout"}}
                        ]
                    }]
                }]
            }]
        }, {
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]},
            "scopeSpans": [{
                "scope": {"name": "scope", "version": "1.0"},
                "spans": [{
                    "traceId": "trace",
                    "spanId": "span-2",
                    "startTimeUnixNano": "2"
                }]
            }]
        }]
    });

    assert_eq!(
        normalize_tempo_response(lake),
        normalize_tempo_response(oracle)
    );
}

#[cfg(feature = "integration-e2e")]
#[test]
fn tempo_normalization_keeps_trace_fidelity_but_ignores_search_summary_details() {
    let lake_search = serde_json::json!({
        "traces": [{
            "traceID": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "rootServiceName": "api",
            "rootTraceName": "checkout",
            "startTimeUnixNano": "1700000000123456789",
            "durationMs": 2000
        }]
    });
    let tempo_search = serde_json::json!({
        "metrics": {"completedJobs": 1, "inspectedBytes": "19466", "totalJobs": 1},
        "traces": [{
            "traceID": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "rootServiceName": "api",
            "rootTraceName": "checkout",
            "startTimeUnixNano": "1700000000123456789",
            "durationMs": 2000,
            "serviceStats": {"api": {"spanCount": 3, "errorCount": 1}},
            "spanSet": {"matched": 3, "spans": [{"spanID": "0000000000000001"}]},
            "spanSets": [{"matched": 3, "spans": [{"spanID": "0000000000000001"}]}]
        }]
    });

    assert_eq!(
        normalize_tempo_response(lake_search),
        normalize_tempo_response(tempo_search)
    );

    let trace = serde_json::json!({
        "trace": {"resourceSpans": [{"scopeSpans": [{"spans": [{
            "traceId": "qqqqqqqqqqqqqqqqqqqqqg==",
            "spanId": "AAAAAAAAAAI=",
            "parentSpanId": "AAAAAAAAAAE=",
            "events": [{"name": "exception"}],
            "links": [{"traceId": "u7u7u7u7u7u7u7u7u7u7uw==", "spanId": "AAAAAAAAAAM="}]
        }]}]}]}
    });
    let normalized_trace = normalize_tempo_response(trace);
    let span = &normalized_trace["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
    assert_eq!(span["parentSpanId"], "AAAAAAAAAAE=");
    assert_eq!(span["events"][0]["name"], "exception");
    assert_eq!(span["links"][0]["spanId"], "AAAAAAAAAAM=");
}

#[cfg(feature = "integration-e2e")]
#[tokio::test]
#[ignore = "requires the pinned Tempo 2.6.1 oracle; run the compatibility lane with --ignored"]
async fn tempo_phase3_oracle_readiness_includes_resource_selector_search() {
    require_docker();
    let fixture = fixture();
    let resource_selector = fixture
        .cases
        .iter()
        .find(|case| case.id == "tempo-search-resource-selector")
        .expect("Tempo resource selector differential case");
    let oracle = start_tempo_oracle(&fixture.records, resource_selector);

    let body = query_tempo_oracle(&oracle.base, resource_selector);
    assert_eq!(
        body["traces"].as_array().map(Vec::len),
        Some(1),
        "resource-selector search must be indexed before the oracle is returned"
    );
}

#[cfg(feature = "integration-e2e")]
#[tokio::test]
#[ignore = "requires the pinned Tempo 2.6.1 oracle; run the compatibility lane with --ignored"]
async fn tempo_phase3_differential_vs_pinned_tempo() {
    require_docker();
    let fixture = fixture();
    let selection = parse_case_selection(
        "Tempo",
        std::env::var("COMPAT_CASE_IDS").ok().as_deref(),
        std::env::var("COMPAT_CASE_ID").ok().as_deref(),
    )
    .unwrap_or_else(|error| panic!("invalid Tempo differential case selection: {error}"));
    let selected_cases = select_cases(
        "tempo",
        &fixture.cases,
        selection.as_ref(),
        |case| case.differential,
        |case| {
            descriptor_for_case(
                "tempo",
                &case.id,
                "GET",
                &case.path,
                case.params.clone(),
                case.differential,
            )
        },
    )
    .unwrap_or_else(|error| panic!("invalid Tempo differential case selection: {error}"));
    assert!(
        !selected_cases.is_empty(),
        "Tempo differential selection resolved to no cases"
    );
    // Under conformance, receipts must carry manifest-static canonical
    // requests; swap runtime descriptors for manifest ones.
    let manifest_descriptors = std::env::var("COMPAT_CASE_JSON")
        .ok()
        .and_then(|path| crate::compat_support::conformance::load_manifest_descriptors(&path).ok())
        .unwrap_or_default();
    let selected_cases: Vec<
        crate::compat_support::conformance::SelectedCase<crate::compat_support::tempo::TempoCase>,
    > = selected_cases
        .iter()
        .map(|selected| {
            let descriptor = manifest_descriptors
                .iter()
                .find(|(id, runner, _)| {
                    Some(id.as_str()) == Some(selected.descriptor.case_id.as_str())
                        || runner.as_deref() == Some(selected.descriptor.case_id.as_str())
                })
                .map(|(_, _, descriptor)| descriptor.clone())
                .unwrap_or_else(|| selected.descriptor.clone());
            crate::compat_support::conformance::SelectedCase {
                case: selected.case,
                descriptor,
            }
        })
        .collect();
    let descriptors = selected_cases
        .iter()
        .map(|selected| selected.descriptor.clone())
        .collect::<Vec<_>>();
    let expected_case_ids = descriptors
        .iter()
        .map(|descriptor| descriptor.case_id.clone())
        .collect::<BTreeSet<_>>();
    let mut recorder = CompatExecutionRecorder::new("tempo", &descriptors, None)
        .expect("create Tempo execution receipt");
    let readiness_case = selected_cases
        .iter()
        .find(|selected| selected.case.path == "/api/search")
        .or_else(|| selected_cases.first())
        .expect("selected differential case");
    let oracle = start_tempo_oracle(&fixture.records, readiness_case.case);
    let (router, state, _temp) = build_tempo_router().await;
    ingest_records(&router, &fixture.records, None).await;
    flush_traces(&state, "local-sqlite-tenant").await;

    let mut executed = BTreeSet::new();
    let mut executed_fixture_ids = BTreeSet::new();
    for selected in selected_cases {
        let case = selected.case;
        let descriptor = &selected.descriptor;
        executed.insert(descriptor.case_id.clone());
        executed_fixture_ids.insert(case.id.clone());
        let oracle_body = query_tempo_oracle(&oracle.base, case);
        let (status, lake_body) = query_case(&router, case, None).await;
        let lake_normalized = normalize_tempo_response(lake_body.clone());
        let oracle_normalized = normalize_tempo_response(oracle_body.clone());
        if status != StatusCode::OK || lake_normalized != oracle_normalized {
            let artifacts = write_failure_artifacts(case, Some(&lake_body), Some(&oracle_body))
                .expect("write Tempo failure artifacts");
            recorder
                .record_case(descriptor, "failure", "normalized_mismatch")
                .expect("record Tempo execution");
            recorder
                .finish("failure", "normalized_mismatch")
                .expect("finish Tempo execution receipt");
            panic!(
                "Tempo differential mismatch for {}; artifacts at {}",
                case.id,
                artifacts.display()
            );
        }
        write_failure_artifacts(case, Some(&lake_body), Some(&oracle_body))
            .expect("write Tempo differential artifacts");
        recorder
            .record_case(descriptor, "pass", "matched")
            .expect("record Tempo execution");
    }

    recorder
        .finish("pass", "matched")
        .expect("finish Tempo execution receipt");

    if let Some(selection) = selection {
        assert_eq!(
            executed, expected_case_ids,
            "Tempo differential did not execute every selected case"
        );
        assert_eq!(
            selection, executed_fixture_ids,
            "Tempo selector must cover every fixture case it executed"
        );
    } else {
        assert!(
            executed.len() >= 3,
            "expected broad Tempo differential coverage"
        );
    }
}
