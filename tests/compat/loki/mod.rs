//! Loki Phase 0/2 compatibility contracts and evidence-backed cases.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::compat::envelopes::{error_envelope, success_envelope_minimal};
use softprobe_runtime::compat::errors::{CompatError, CompatErrorCode};
use softprobe_runtime::compat::tenant::{ProtocolScope, QueryLimits, TenantContext};
use std::collections::{BTreeMap, BTreeSet};
use tempfile::TempDir;
use tower::ServiceExt;

#[cfg(feature = "integration-e2e")]
use crate::compat_support::conformance::CompatExecutionRecorder;
#[cfg(feature = "integration-e2e")]
use crate::compat_support::conformance::{descriptor_for_case, select_cases};
use crate::compat_support::conformance::{parse_case_selection, select_differential_cases};

use crate::compat_support::loki::{
    assert_case_contract, build_loki_router, fixture, flush_logs, ingest_records,
    ingest_records_with_bearer, query_case, query_case_bearer, reference_image_from_manifest,
    LokiCase, LokiExpectation, PHASE2_EPOCH_NS,
};
use crate::util::config::file_backed_test_config;

#[cfg(feature = "integration-e2e")]
use crate::compat_support::loki::{
    fixture_for_now, normalize_loki_response, query_loki_oracle, require_docker, start_loki_oracle,
    write_failure_artifacts, LokiRecord,
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

#[test]
fn loki_scope_header_must_match_tenant() {
    let err = TenantContext::from_authenticated(
        TenantInfo {
            tenant_id: "tenant-a".into(),
            bucket_name: "b".into(),
            dataset_id: "d".into(),
        },
        ProtocolScope::Loki,
        Some("tenant-b"),
        QueryLimits::default(),
    )
    .unwrap_err();
    assert_eq!(err.code, CompatErrorCode::Forbidden);
}

#[test]
fn loki_matching_scope_header_ok() {
    TenantContext::from_authenticated(
        TenantInfo {
            tenant_id: "tenant-a".into(),
            bucket_name: "b".into(),
            dataset_id: "d".into(),
        },
        ProtocolScope::Loki,
        Some("tenant-a"),
        QueryLimits::default(),
    )
    .expect("match");
}

#[test]
fn loki_error_fixture_matches_envelope_helper() {
    let expected = load_fixture("loki_error_unsupported.json");
    let actual = error_envelope(ProtocolScope::Loki, &CompatError::unsupported("loki_api"));
    assert_eq!(actual, expected);
}

#[test]
fn loki_success_minimal_fixture_matches_helper() {
    let expected = load_fixture("loki_success_minimal.json");
    assert_eq!(success_envelope_minimal(ProtocolScope::Loki), expected);
}

#[test]
fn loki_phase2_fixture_reference_image_tracks_immutable_manifest_pin() {
    let fixture = fixture();
    assert_eq!(
        fixture.evidence.reference_image,
        reference_image_from_manifest()
    );
    assert!(
        fixture.evidence.reference_image.contains("@sha256:"),
        "Loki evidence must identify the immutable manifest reference"
    );
}

#[test]
fn loki_phase2_fixture_has_issue_evidence_and_full_get_matrix() {
    let fixture = fixture();
    assert_eq!(fixture.evidence.issue, "#29");
    assert_eq!(fixture.evidence.phase, "Phase 2");
    assert_eq!(
        fixture.evidence.reference_manifest,
        "docs/compat/references.v0.yaml"
    );
    assert!(fixture
        .evidence
        .normalization
        .contains("normalize_loki_response"));
    assert_eq!(fixture.capability.protocol, "loki");
    assert_eq!(fixture.capability.phase, "phase_2");
    assert_eq!(
        fixture.capability.ordering_policy,
        "timestamp_asc,stream_labels_asc,line_asc,structured_metadata_asc; direction reverses the complete order"
    );
    assert_eq!(
        fixture.capability.supported_endpoints.len(),
        5,
        "all five GET endpoints must be capability-declared"
    );
    for feature in [
        "stream_selectors",
        "line_filters",
        "json_and_logfmt",
        "structured_metadata",
        "post_filter_limit",
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
        "interval_sampling",
        "step_sampling",
        "unwrap",
        "metric_aggregations",
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

    let endpoints = fixture
        .cases
        .iter()
        .map(|case| case.path.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        endpoints,
        BTreeSet::from([
            "/loki/api/v1/label/service_name/values",
            "/loki/api/v1/labels",
            "/loki/api/v1/query",
            "/loki/api/v1/query_range",
            "/loki/api/v1/series",
        ])
    );

    let mut ids = BTreeSet::new();
    assert!(
        fixture.cases.iter().any(|case| case.differential),
        "fixture must declare differential cases"
    );
    for case in fixture.cases {
        assert!(ids.insert(case.id.clone()), "duplicate case ID {}", case.id);
        assert!(!case.id.is_empty());
        assert!(case.expect.status > 0);
    }
}

#[test]
fn loki_phase2_fixture_timestamps_are_after_schema_start() {
    let fixture = fixture();

    for record in &fixture.records {
        let timestamp = record.timestamp();
        assert!(
            timestamp >= PHASE2_EPOCH_NS,
            "fixture record timestamp predates the deterministic epoch: {timestamp}"
        );
    }
    for case in &fixture.cases {
        for bound in ["start", "end", "time"] {
            if let Some(value) = case.params.get(bound) {
                let timestamp = value
                    .parse::<i64>()
                    .unwrap_or_else(|_| panic!("case {} has invalid {bound}: {value}", case.id));
                assert!(
                    timestamp >= PHASE2_EPOCH_NS,
                    "case {} {bound} predates the deterministic epoch: {timestamp}",
                    case.id
                );
            }
        }
    }
}

#[cfg(test)]
mod selector_tests {
    use super::*;

    #[test]
    fn selector_parser_combines_case_ids_and_ignores_suite_sentinel() {
        let selected = parse_case_selection(
            "Loki",
            Some("loki-label-names-discovery,loki-series-discovery"),
            Some("__suite__"),
        )
        .expect("selector");

        assert_eq!(
            selected,
            Some(BTreeSet::from([
                "loki-label-names-discovery".into(),
                "loki-series-discovery".into(),
            ]))
        );
    }

    #[test]
    fn selected_cases_filter_only_requested_differential_fixture_cases() {
        let fixture = fixture();
        let selection = BTreeSet::from([
            "loki-label-names-discovery".into(),
            "loki-series-discovery".into(),
        ]);

        let cases = select_differential_cases("Loki", &fixture.cases, Some(&selection), |case| {
            case.differential.then_some(case.id.clone())
        })
        .expect("selected cases");
        assert_eq!(
            cases
                .iter()
                .map(|case| case.id.as_str())
                .collect::<Vec<_>>(),
            vec!["loki-label-names-discovery", "loki-series-discovery"]
        );
    }

    #[test]
    fn selector_rejects_unknown_and_non_differential_case_ids() {
        let fixture = fixture();

        for case_id in ["missing-loki-case", "loki-query-range-since-interval-step"] {
            let selection = BTreeSet::from([case_id.to_string()]);
            let error =
                select_differential_cases("Loki", &fixture.cases, Some(&selection), |case| {
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
        let cases = select_differential_cases("Loki", &fixture.cases, None, |case| {
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
async fn loki_phase2_contract_cases_cover_envelopes_results_and_boundaries() {
    let fixture = fixture();
    let (router, state, _temp) = build_loki_router().await;
    ingest_records(&router, &fixture.records, None).await;
    flush_logs(&state, "local-sqlite-tenant").await;

    for case in &fixture.cases {
        let (status, body) = query_case(&router, case, None).await;
        assert_case_contract(case, status, &body);
    }

    let forward = fixture
        .cases
        .iter()
        .find(|case| case.id == "loki-query-forward-duplicate-nanoseconds")
        .expect("duplicate timestamp case");
    let (_, body) = query_case(&router, forward, None).await;
    // Pinned Loki 3.1.1 splits parsed pipelines into one stream per distinct
    // label set (parsed fields + structured metadata join the stream object)
    // and keeps insertion order for equal timestamps; mirror that exactly.
    let streams = body["data"]["result"].as_array().expect("streams");
    assert_eq!(streams.len(), 2);
    let expected_timestamp = (PHASE2_EPOCH_NS + 1).to_string();
    assert_eq!(streams[0]["stream"]["request_id"], "r1");
    assert_eq!(streams[1]["stream"]["request_id"], "r2");
    for stream in streams {
        assert_eq!(stream["values"][0][0], expected_timestamp);
        assert_eq!(stream["values"][0].as_array().unwrap().len(), 2);
    }
    assert_eq!(
        streams[0]["values"][0][1], fixture.records[0].line,
        "equal timestamps keep insertion order"
    );
    assert_eq!(streams[1]["values"][0][1], fixture.records[1].line);
}

#[tokio::test]
async fn loki_phase2_tenant_isolation_keeps_streams_and_metadata_scoped() {
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
    ingest_records_with_bearer(
        &router_a,
        &fixture.records[0..2],
        None,
        Some("tenant-a-key"),
    )
    .await;
    ingest_records_with_bearer(
        &router_b,
        &fixture.records[3..4],
        None,
        Some("tenant-b-key"),
    )
    .await;
    flush_logs(&state_a, "tenant-a").await;
    flush_logs(&state_b, "tenant-b").await;

    let case = LokiCase {
        id: "tenant-isolation-query".into(),
        path: "/loki/api/v1/query_range".into(),
        params: BTreeMap::from([
            (
                "query".into(),
                "{service_name=~\"checkout|payments\"}".into(),
            ),
            ("start".into(), "1786827600000000001".into()),
            ("end".into(), "1786827602000000001".into()),
            ("direction".into(), "forward".into()),
        ]),
        expect: LokiExpectation {
            status: 200,
            envelope: "success".into(),
            result_type: Some("streams".into()),
            data_shape: Some("streams".into()),
            softprobe_code: None,
            entry_lines: vec![],
            entry_count: None,
            values: vec![],
        },
        differential: false,
    };
    let (status_a, body_a) = query_case_bearer(&router_a, &case, "tenant-a-key").await;
    let (status_b, body_b) = query_case_bearer(&router_b, &case, "tenant-b-key").await;
    assert_eq!(status_a, StatusCode::OK);
    assert_eq!(status_b, StatusCode::OK);
    // Pinned Loki 3.1.1 surfaces structured metadata (request_id/user_id) in
    // the stream object and splits streams per distinct metadata set; tenant A
    // therefore sees its two checkout streams and never tenant B's payments.
    let streams_a = body_a["data"]["result"].as_array().unwrap();
    assert_eq!(streams_a.len(), 2);
    for stream in streams_a {
        assert_eq!(stream["stream"]["service_name"], "checkout");
        assert_eq!(stream["stream"]["deployment_environment"], "prod");
    }
    let mut request_ids: Vec<&str> = streams_a
        .iter()
        .map(|stream| stream["stream"]["request_id"].as_str().unwrap())
        .collect();
    request_ids.sort_unstable();
    assert_eq!(request_ids, vec!["r1", "r2"]);
    let streams_b = body_b["data"]["result"].as_array().unwrap();
    assert_eq!(streams_b.len(), 1);
    assert_eq!(streams_b[0]["stream"]["service_name"], "payments");
    assert_eq!(streams_b[0]["stream"]["deployment_environment"], "staging");
    assert_eq!(streams_b[0]["stream"]["request_id"], "r4");
}

#[tokio::test]
async fn loki_phase2_missing_and_invalid_auth_are_denied_on_all_get_endpoints() {
    let paths = [
        "/loki/api/v1/query",
        "/loki/api/v1/query_range",
        "/loki/api/v1/labels",
        "/loki/api/v1/label/service_name/values",
        "/loki/api/v1/series",
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

#[cfg(feature = "integration-e2e")]
#[tokio::test]
#[ignore = "requires the pinned Loki oracle; run the compatibility lane with --ignored"]
async fn loki_phase2_pinned_fixture_first_query_is_nonempty_and_schema_compatible() {
    require_docker();
    // Under the conformance harness the manifest carries static timestamps;
    // using them keeps receipt canonical requests comparable byte-for-byte.
    let fixture = if std::env::var_os("COMPAT_CASE_JSON").is_some() {
        fixture()
    } else {
        fixture_for_now()
    };
    let first_case = fixture
        .cases
        .iter()
        .find(|case| case.differential)
        .expect("first differential case");
    let schema_start_ns = fixture
        .records
        .iter()
        .map(LokiRecord::timestamp)
        .min()
        .expect("shifted fixture records");

    let oracle = start_loki_oracle(&fixture.records, first_case);
    let body = query_loki_oracle(&oracle.base, first_case);
    assert_eq!(body["status"], "success", "first Loki fixture query");
    let results = body["data"]["result"]
        .as_array()
        .expect("first Loki query result array");
    assert!(
        !results.is_empty(),
        "first Loki fixture query returned no streams"
    );
    let values = results
        .iter()
        .flat_map(|stream| stream["values"].as_array())
        .collect::<Vec<_>>();
    assert!(
        !values.is_empty(),
        "first Loki fixture query returned no entries"
    );
    for value in values {
        let timestamp = value[0]
            .as_str()
            .expect("Loki result timestamp is a string")
            .parse::<i64>()
            .expect("Loki result timestamp is nanoseconds");
        assert!(
            timestamp >= schema_start_ns,
            "Loki result timestamp predates the local schema start: {timestamp}"
        );
    }
}

#[cfg(feature = "integration-e2e")]
#[tokio::test]
#[ignore = "requires the pinned Loki oracle; run the compatibility lane with --ignored"]
async fn loki_phase2_differential_vs_pinned_loki() {
    require_docker();
    // Under the conformance harness the manifest carries static timestamps;
    // using them keeps receipt canonical requests comparable byte-for-byte.
    let fixture = if std::env::var_os("COMPAT_CASE_JSON").is_some() {
        fixture()
    } else {
        fixture_for_now()
    };
    let selection = parse_case_selection(
        "Loki",
        std::env::var("COMPAT_CASE_IDS").ok().as_deref(),
        std::env::var("COMPAT_CASE_ID").ok().as_deref(),
    )
    .unwrap_or_else(|error| panic!("invalid Loki differential case selection: {error}"));
    if std::env::var_os("SELECTION_DEBUG").is_some() {
        eprintln!(
            "SELECTION_DEBUG loki raw_ids={:?} resolved={} compat_keys={:?}",
            std::env::var("COMPAT_CASE_IDS").unwrap_or_default(),
            selection.as_ref().map(|s| s.len()).unwrap_or(0),
            std::env::vars()
                .filter(|(k, _)| k.contains("COMPAT")
                    || k.contains("RUN_ID")
                    || k.contains("SOFTPROBE"))
                .collect::<Vec<_>>()
        );
    }
    let selected_cases = select_cases(
        "loki",
        &fixture.cases,
        selection.as_ref(),
        |case| case.differential,
        |case| {
            descriptor_for_case(
                "loki",
                &case.id,
                "GET",
                &case.path,
                case.params.clone(),
                case.differential,
            )
        },
    )
    .unwrap_or_else(|error| panic!("invalid Loki differential case selection: {error}"));
    assert!(
        !selected_cases.is_empty(),
        "Loki differential selection resolved to no cases"
    );
    // Under conformance, receipts must carry manifest-static canonical
    // requests; swap runtime (shifted) descriptors for manifest ones.
    let manifest_descriptors = std::env::var("COMPAT_CASE_JSON")
        .ok()
        .and_then(|path| crate::compat_support::conformance::load_manifest_descriptors(&path).ok())
        .unwrap_or_default();
    let selected_cases: Vec<crate::compat_support::conformance::SelectedCase<LokiCase>> =
        selected_cases
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
    let mut recorder = CompatExecutionRecorder::new("loki", &descriptors, None)
        .expect("create Loki execution receipt");

    // Under the conformance harness the receipt must carry manifest-static
    // canonical requests, but real Loki cannot serve week-old samples, so
    // execution uses a time-shifted copy of the fixture while canonical
    // descriptors stay static.
    let exec_fixture = if std::env::var_os("COMPAT_CASE_JSON").is_some() {
        let delta = crate::compat_support::loki::system_time_now_ns()
            - crate::compat_support::loki::FIXTURE_LAG_NS
            - PHASE2_EPOCH_NS;
        Some(fixture.shifted_by(delta))
    } else {
        None
    };
    let readiness_case = selected_cases
        .iter()
        .find(|selected| selected.case.path == "/loki/api/v1/query_range")
        .or_else(|| selected_cases.first())
        .expect("selected differential case");
    let exec_records = exec_fixture
        .as_ref()
        .map(|f| f.records.as_slice())
        .unwrap_or(fixture.records.as_slice());
    let readiness_exec = exec_fixture
        .as_ref()
        .and_then(|f| {
            f.cases
                .iter()
                .find(|candidate| candidate.id == readiness_case.case.id)
        })
        .unwrap_or(readiness_case.case);
    let oracle = start_loki_oracle(exec_records, readiness_exec);
    let (router, state, _temp) = build_loki_router().await;
    ingest_records(&router, exec_records, None).await;
    flush_logs(&state, "local-sqlite-tenant").await;

    let mut executed = BTreeSet::new();
    for selected in selected_cases {
        let case = selected.case;
        let descriptor = &selected.descriptor;
        executed.insert(descriptor.case_id.clone());
        let exec_case = exec_fixture
            .as_ref()
            .and_then(|f| f.cases.iter().find(|candidate| candidate.id == case.id))
            .cloned()
            .unwrap_or_else(|| case.clone());
        let oracle_body = query_loki_oracle(&oracle.base, &exec_case);
        let (status, lake_body) = query_case(&router, &exec_case, None).await;
        if status != StatusCode::OK || lake_body["status"] != "success" {
            let artifacts = write_failure_artifacts(case, Some(&lake_body), Some(&oracle_body))
                .expect("write Loki failure artifacts");
            recorder
                .record_case(descriptor, "failure", "softprobe_http_or_envelope_failure")
                .expect("record Loki execution");
            recorder
                .finish("failure", "softprobe_http_or_envelope_failure")
                .expect("finish Loki execution receipt");
            panic!(
                "Loki differential HTTP failure for {}; artifacts at {}",
                case.id,
                artifacts.display()
            );
        }
        if oracle_body["status"] != "success" {
            let artifacts = write_failure_artifacts(case, Some(&lake_body), Some(&oracle_body))
                .expect("write Loki failure artifacts");
            recorder
                .record_case(descriptor, "failure", "oracle_failure")
                .expect("record Loki execution");
            recorder
                .finish("failure", "oracle_failure")
                .expect("finish Loki execution receipt");
            panic!(
                "Loki oracle failure for {}; artifacts at {}",
                case.id,
                artifacts.display()
            );
        }
        let lake_normalized = normalize_loki_response(lake_body.clone());
        let oracle_normalized = normalize_loki_response(oracle_body.clone());
        if lake_normalized != oracle_normalized {
            let artifacts = write_failure_artifacts(case, Some(&lake_body), Some(&oracle_body))
                .expect("write Loki failure artifacts");
            recorder
                .record_case(descriptor, "failure", "normalized_mismatch")
                .expect("record Loki execution");
            recorder
                .finish("failure", "normalized_mismatch")
                .expect("finish Loki execution receipt");
            panic!(
                "Loki differential mismatch for {}; artifacts at {}",
                case.id,
                artifacts.display()
            );
        }
        write_failure_artifacts(case, Some(&lake_body), Some(&oracle_body))
            .expect("write Loki execution artifacts");
        recorder
            .record_case(descriptor, "pass", "matched")
            .expect("record Loki execution");
    }

    recorder
        .finish("pass", "matched")
        .expect("finish Loki execution receipt");

    if let Some(selection) = selection {
        assert_eq!(
            executed, expected_case_ids,
            "Loki differential did not execute every selected case"
        );
        assert_eq!(
            selection, executed,
            "Loki selector IDs must match manifest case IDs in the receipt"
        );
    } else {
        assert!(
            executed.len() >= 7,
            "expected broad Loki differential coverage"
        );
    }
}
