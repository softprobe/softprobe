//! Grafana Prometheus datasource smoke (#27 Prom-only).
//!
//! Exercises the native Prom HTTP contract Grafana uses (Bearer auth, discovery,
//! instant/range, POST form) without starting a Grafana container.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::{create_router, ControlPlaneRuntime};
use softprobe_runtime::authn::Resolver;
use softprobe_runtime::runtime_api::{runtime_auth_middleware, runtime_control_routes};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::compat_support::prometheus::{
    encode_query_pairs, gauge_otlp, get_json_bearer, post_form_json_bearer,
};
use crate::util::config::file_backed_test_config;

async fn authenticated_router(tenant_id: &str) -> (Router, MockServer, TempDir) {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "data": { "tenantId": tenant_id, "resources": [] }
        })))
        .mount(&mock)
        .await;

    let temp = TempDir::new().expect("temp");
    let control = ControlPlaneRuntime {
        resolver: Resolver::new(format!("{}/", mock.uri()), Duration::from_secs(60)),
    };
    let (router, state) = create_router(
        Arc::new(file_backed_test_config(&temp)),
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

#[tokio::test]
async fn grafana_prom_sequence_with_bearer() {
    let (router, _mock, _temp) = authenticated_router("grafana-prom-tenant").await;
    let bearer = "good-key";

    // Health-shaped probe without auth must fail.
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/query?query=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let ts_nano = 1_700_000_000_000_000_000u64;
    let body = gauge_otlp("http.requests", "checkout", 42.0, ts_nano);
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/metrics")
                .header("content-type", "application/x-protobuf")
                .header("Authorization", format!("Bearer {bearer}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "OTLP ingest under Bearer must succeed"
    );

    let (status, labels) = get_json_bearer(&router, "/api/v1/labels", bearer).await;
    assert_eq!(status, StatusCode::OK, "{labels}");
    assert_eq!(labels["status"], "success");
    assert!(labels["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v.as_str() == Some("job")));

    let values_q = encode_query_pairs(&[("match[]", r#"http_requests{job="checkout"}"#)]);
    let (status, values) = get_json_bearer(
        &router,
        &format!("/api/v1/label/job/values?{values_q}"),
        bearer,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{values}");
    assert!(values["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v.as_str() == Some("checkout")));

    let series_q = encode_query_pairs(&[("match[]", r#"http_requests{job="checkout"}"#)]);
    let (status, series) =
        get_json_bearer(&router, &format!("/api/v1/series?{series_q}"), bearer).await;
    assert_eq!(status, StatusCode::OK, "{series}");
    assert!(!series["data"].as_array().unwrap().is_empty());

    let (status, meta) =
        get_json_bearer(&router, "/api/v1/metadata?metric=http_requests", bearer).await;
    assert_eq!(status, StatusCode::OK, "{meta}");
    assert!(meta["data"]
        .as_object()
        .unwrap()
        .contains_key("http_requests"));

    let eval_s = (ts_nano / 1_000_000_000) as i64;
    let time = eval_s.to_string();
    let query_form = encode_query_pairs(&[
        ("query", r#"http_requests{job="checkout"}"#),
        ("time", time.as_str()),
    ]);
    let (status, query) =
        get_json_bearer(&router, &format!("/api/v1/query?{query_form}"), bearer).await;
    assert_eq!(status, StatusCode::OK, "{query}");
    assert_eq!(query["status"], "success");
    assert_eq!(query["data"]["resultType"], "vector");
    assert_eq!(query["data"]["result"].as_array().unwrap().len(), 1);

    let start = (eval_s - 60).to_string();
    let end = time.clone();
    let range_form = encode_query_pairs(&[
        ("query", r#"http_requests{job="checkout"}"#),
        ("start", start.as_str()),
        ("end", end.as_str()),
        ("step", "30"),
    ]);
    let (status, range) = get_json_bearer(
        &router,
        &format!("/api/v1/query_range?{range_form}"),
        bearer,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{range}");
    assert_eq!(range["status"], "success");
    assert_eq!(range["data"]["resultType"], "matrix");

    // Grafana often POSTs form-encoded queries (httpMethod: POST in provisioning).
    let (status, post_q) =
        post_form_json_bearer(&router, "/api/v1/query", &query_form, bearer).await;
    assert_eq!(status, StatusCode::OK, "{post_q}");
    assert_eq!(post_q["status"], "success");
    assert_eq!(post_q["data"]["result"], query["data"]["result"]);

    let (status, post_range) =
        post_form_json_bearer(&router, "/api/v1/query_range", &range_form, bearer).await;
    assert_eq!(status, StatusCode::OK, "{post_range}");
    assert_eq!(post_range["status"], "success");
    assert_eq!(post_range["data"]["resultType"], "matrix");

    let rate_form = encode_query_pairs(&[
        ("query", r#"rate(http_requests{job="checkout"}[5m])"#),
        ("time", time.as_str()),
    ]);
    let (status, rate_body) =
        post_form_json_bearer(&router, "/api/v1/query", &rate_form, bearer).await;
    assert_eq!(status, StatusCode::OK, "{rate_body}");
    assert_eq!(rate_body["status"], "success");
    assert_eq!(rate_body["data"]["resultType"], "vector");

    let bad = encode_query_pairs(&[("query", "histogram_quantile(0.9, rate(x[5m]))")]);
    let (status, err) = get_json_bearer(&router, &format!("/api/v1/query?{bad}"), bearer).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{err}");
    assert_eq!(err["status"], "error");
    assert!(err["error"]
        .as_str()
        .unwrap_or("")
        .contains("unsupported_feature"));
}
