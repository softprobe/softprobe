//! Compatibility Phase 0 contract suite (auth, manifests, isolation fixtures).

#[path = "util/config.rs"]
mod config;

use arrow::array::{Array, Float64Array, ListArray, UInt64Array};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::{create_router, ControlPlaneRuntime};
use softprobe_runtime::authn::Resolver;
use softprobe_runtime::compat::capability::{parse_capability_yaml, EMBEDDED_CAPABILITY_V0};
use softprobe_runtime::compat::errors::CompatErrorCode;
use softprobe_runtime::compat::stubs::declared_compat_probe_paths;
use softprobe_runtime::models::{Metric, SummaryQuantile};
use softprobe_runtime::runtime_api::{runtime_auth_middleware, runtime_control_routes};
use softprobe_runtime::storage::schema::tables::OtlpMetricsTable;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn authenticated_router(
    auth_success: bool,
    tenant_id: &str,
) -> (Router, MockServer, TempDir) {
    let mock = MockServer::start().await;
    let body = if auth_success {
        serde_json::json!({
            "success": true,
            "data": { "tenantId": tenant_id, "resources": [] }
        })
    } else {
        serde_json::json!({ "success": false })
    };
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&mock)
        .await;

    let temp = TempDir::new().expect("temp");
    let control = ControlPlaneRuntime {
        resolver: Resolver::new(format!("{}/", mock.uri()), Duration::from_secs(60)),
    };
    let (router, state) = create_router(
        Arc::new(config::file_backed_test_config(&temp)),
        post(ingest_traces),
        Some(control),
    )
    .await
    .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn_with_state(state, runtime_auth_middleware));
    (router, mock, temp)
}

#[test]
fn capability_manifest_parses_and_pins_unsupported_feature() {
    let m = parse_capability_yaml(EMBEDDED_CAPABILITY_V0).expect("parse");
    assert_eq!(m.version, "compat.v0");
    assert_eq!(m.errors.unsupported_feature.code, "unsupported_feature");
    assert_eq!(m.errors.unsupported_feature.http_status, 501);
    assert!(m.otlp_write_canonical);
}

#[test]
fn every_declared_compat_route_has_isolation_probe() {
    let probes = declared_compat_probe_paths();
    assert!(
        probes.len() >= 16,
        "expected full matrix probe list, got {}",
        probes.len()
    );
    for (method, path) in probes {
        assert!(!method.is_empty());
        assert!(path.starts_with('/'));
    }
}

#[test]
fn histogram_metric_arrow_round_trip_preserves_buckets() {
    let mut attrs = HashMap::new();
    attrs.insert("http.route".into(), "/api".into());
    let mut resource = HashMap::new();
    resource.insert("service.name".into(), "api".into());

    let metric = Metric {
        metric_name: "http.server.duration".into(),
        description: "latency".into(),
        unit: "ms".into(),
        metric_type: "histogram".into(),
        timestamp: chrono::Utc::now(),
        value: 42.0,
        attributes: attrs,
        resource_attributes: resource,
        count: Some(7),
        sum: Some(42.0),
        bucket_counts: Some(vec![1, 2, 4]),
        explicit_bounds: Some(vec![5.0, 10.0]),
        quantiles: None,
        aggregation_temporality: Some("CUMULATIVE".into()),
        exemplars_json: Some(r#"[{"value":1.0}]"#.into()),
    };

    let schema = OtlpMetricsTable::schema();
    let batch = Metric::to_record_batch(&[metric], &schema).expect("batch");
    assert_eq!(batch.num_rows(), 1);

    let count_idx = schema.index_of("count").unwrap();
    let counts = batch
        .column(count_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 7);

    let buckets_idx = schema.index_of("bucket_counts").unwrap();
    let buckets = batch
        .column(buckets_idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = buckets.value(0);
    let values = values.as_any().downcast_ref::<UInt64Array>().unwrap();
    assert_eq!(values.values(), &[1, 2, 4]);

    let bounds_idx = schema.index_of("explicit_bounds").unwrap();
    let bounds = batch
        .column(bounds_idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let bvals = bounds.value(0);
    let bvals = bvals.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(bvals.values(), &[5.0, 10.0]);
}

#[test]
fn summary_metric_arrow_round_trip_preserves_quantiles() {
    let metric = Metric {
        metric_name: "rpc.latency".into(),
        description: "".into(),
        unit: "ms".into(),
        metric_type: "summary".into(),
        timestamp: chrono::Utc::now(),
        value: 100.0,
        quantiles: Some(vec![
            SummaryQuantile {
                quantile: 0.5,
                value: 10.0,
            },
            SummaryQuantile {
                quantile: 0.99,
                value: 50.0,
            },
        ]),
        count: Some(20),
        sum: Some(100.0),
        ..Default::default()
    };
    let schema = OtlpMetricsTable::schema();
    let batch = Metric::to_record_batch(&[metric], &schema).expect("batch");
    let q_idx = schema.index_of("quantiles").unwrap();
    let list = batch
        .column(q_idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value(0).len(), 2);
}

#[tokio::test]
async fn compat_routes_deny_missing_and_invalid_bearer() {
    let (router, _mock, _temp) = authenticated_router(true, "tenant-compat").await;

    let missing = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/query")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let (fail_router, _mock2, _temp2) = authenticated_router(false, "tenant-compat").await;
    let invalid = fail_router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/loki/api/v1/labels")
                .header("Authorization", "Bearer bad-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn compat_routes_authenticated_return_expected_status() {
    let (router, _mock, _temp) = authenticated_router(true, "tenant-compat").await;

    for (method, path) in declared_compat_probe_paths() {
        let req = Request::builder()
            .method(*method)
            .uri(*path)
            .header("Authorization", "Bearer good-key")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        let path = *path;
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        if path.starts_with("/api/v1/") {
            // Phase 1: discovery succeeds (empty lake → empty data).
            // query / query_range without params → bad_data (400).
            if path == "/api/v1/labels"
                || path.starts_with("/api/v1/label/")
                || path == "/api/v1/series"
                || path == "/api/v1/metadata"
            {
                assert_eq!(status, StatusCode::OK, "{method} {path}: {json}");
                assert_eq!(json["status"], "success", "{method} {path}: {json}");
                if path == "/api/v1/labels"
                    || path.starts_with("/api/v1/label/")
                    || path == "/api/v1/series"
                {
                    assert!(
                        json["data"].is_array(),
                        "{method} {path}: expected array data {json}"
                    );
                    if path == "/api/v1/labels" || path.starts_with("/api/v1/label/") {
                        assert_eq!(
                            json["data"].as_array().unwrap().len(),
                            0,
                            "{method} {path}: empty lake labels should be []"
                        );
                    }
                } else if path == "/api/v1/metadata" {
                    assert!(
                        json["data"].is_object(),
                        "{method} {path}: expected object data {json}"
                    );
                }
            } else {
                // query / query_range without `query` → bad_data
                assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}: {json}");
                assert_eq!(json["status"], "error", "{method} {path}: {json}");
                assert_eq!(json["errorType"], "bad_data", "{method} {path}: {json}");
            }
        } else if path.starts_with("/loki/") {
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{method} {path}");
            assert_eq!(json["status"], "error", "{method} {path}: {json}");
            assert!(
                json["error"]
                    .as_str()
                    .unwrap_or("")
                    .starts_with("unsupported_feature:"),
                "{method} {path}: {json}"
            );
        } else {
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{method} {path}");
            assert_eq!(
                json["softprobe_code"], "unsupported_feature",
                "{method} {path}: {json}"
            );
            assert!(
                json["message"]
                    .as_str()
                    .unwrap_or("")
                    .starts_with("unsupported_feature:"),
                "{method} {path}: {json}"
            );
        }
    }
}

#[tokio::test]
async fn loki_mismatched_scope_header_is_forbidden() {
    let (router, _mock, _temp) = authenticated_router(true, "tenant-compat").await;
    let resp = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/loki/api/v1/query")
                .header("Authorization", "Bearer good-key")
                .header("X-Scope-OrgID", "other-tenant")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["status"], "error");
    assert!(
        json["error"]
            .as_str()
            .unwrap_or("")
            .starts_with("forbidden:"),
        "{json}"
    );
}

#[tokio::test]
async fn tempo_mismatched_scope_header_is_forbidden() {
    let (router, _mock, _temp) = authenticated_router(true, "tenant-compat").await;
    let resp = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/traces/abc")
                .header("Authorization", "Bearer good-key")
                .header("X-Scope-OrgID", "spoof")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["softprobe_code"], "forbidden");
    assert!(
        json["message"]
            .as_str()
            .unwrap_or("")
            .starts_with("forbidden:"),
        "{json}"
    );
}

#[tokio::test]
async fn compat_query_tenant_id_param_does_not_override_auth() {
    // Negative isolation: query-string tenant_id must not change authenticated scope.
    let (router, _mock, _temp) = authenticated_router(true, "tenant-compat").await;
    let resp = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/query?query=up&tenant_id=attacker")
                .header("Authorization", "Bearer good-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Authenticated for tenant-compat; empty lake → success with empty vector.
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["resultType"], "vector");
}

#[tokio::test]
async fn compat_query_tenant_id_body_does_not_override_auth() {
    // Negative isolation: form body tenant_id must not change authenticated scope.
    let (router, _mock, _temp) = authenticated_router(true, "tenant-compat").await;
    let resp = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("Authorization", "Bearer good-key")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=up&tenant_id=attacker"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["resultType"], "vector");
}

#[tokio::test]
async fn prometheus_labels_empty_lake_success() {
    let (router, _mock, _temp) = authenticated_router(true, "tenant-compat").await;
    let resp = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/labels")
                .header("Authorization", "Bearer good-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"], serde_json::json!([]));
}

#[test]
fn auth_contract_fixtures_document_expected_statuses() {
    let dir = format!("{}/tests/compat/fixtures", env!("CARGO_MANIFEST_DIR"));
    for name in [
        "auth_missing_bearer.json",
        "auth_forbidden.json",
        "auth_scope_mismatch.json",
    ] {
        let raw = std::fs::read_to_string(format!("{dir}/{name}")).expect(name);
        let v: serde_json::Value = serde_json::from_str(&raw).expect(name);
        let status = v["expect"]["http_status"].as_u64().expect("http_status");
        assert!(
            status == 401 || status == 403,
            "{name} unexpected status {status}"
        );
    }
}

#[test]
fn unsupported_error_code_stable() {
    let err = softprobe_runtime::compat::errors::CompatError::unsupported("x");
    assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    assert_eq!(err.code.as_str(), "unsupported_feature");
}
