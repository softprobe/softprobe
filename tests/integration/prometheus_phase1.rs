//! Phase 1 Prometheus: ingest metrics then hit discovery + query APIs.

use axum::http::StatusCode;
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::sync::Arc;
use tempfile::TempDir;

use crate::compat_support::prometheus::{encode_query_pairs, gauge_otlp, get_json, ingest_metrics};
use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant;

async fn build_tenant_router() -> (Router, TempDir) {
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_local_sqlite_tenant));
    (router, temp)
}

fn encode_query(params: &[(&str, &str)]) -> String {
    encode_query_pairs(params)
}

#[tokio::test]
async fn ingest_then_labels_series_and_query() {
    let (router, _temp) = build_tenant_router().await;
    // Fixed timestamp so lookback covers the sample.
    let ts_nano = 1_700_000_000_000_000_000u64;
    let body = gauge_otlp("http.requests", "checkout", 42.0, ts_nano);
    ingest_metrics(&router, body).await;

    let (status, labels) = get_json(&router, "/api/v1/labels").await;
    assert_eq!(status, StatusCode::OK, "{labels}");
    assert_eq!(labels["status"], "success");
    let names = labels["data"].as_array().expect("labels array");
    assert!(
        names.iter().any(|v| v.as_str() == Some("__name__")),
        "labels={labels}"
    );
    assert!(
        names.iter().any(|v| v.as_str() == Some("job")),
        "labels={labels}"
    );

    let values_q = encode_query(&[("match[]", r#"http_requests{job="checkout"}"#)]);
    let (status, values) = get_json(&router, &format!("/api/v1/label/job/values?{values_q}")).await;
    assert_eq!(status, StatusCode::OK, "{values}");
    assert_eq!(values["status"], "success");
    assert!(
        values["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str() == Some("checkout")),
        "values={values}"
    );

    let (status, meta) = get_json(&router, "/api/v1/metadata?metric=http_requests").await;
    assert_eq!(status, StatusCode::OK, "{meta}");
    assert_eq!(meta["status"], "success");
    assert!(
        meta["data"]
            .as_object()
            .map(|o| o.contains_key("http_requests"))
            .unwrap_or(false),
        "metadata must key by projected Prometheus name http_requests, got {meta}"
    );
    let entry = &meta["data"]["http_requests"][0];
    assert_eq!(
        entry["type"], "gauge",
        "metadata type must use Prometheus vocabulary, got {entry}"
    );

    let series_q = encode_query(&[("match[]", r#"http_requests{job="checkout"}"#)]);
    let (status, series) = get_json(&router, &format!("/api/v1/series?{series_q}")).await;
    assert_eq!(status, StatusCode::OK, "{series}");
    assert_eq!(series["status"], "success");
    let arr = series["data"].as_array().expect("series array");
    assert!(!arr.is_empty(), "series={series}");
    assert_eq!(arr[0]["__name__"], "http_requests");
    assert_eq!(arr[0]["job"], "checkout");

    let eval_s = (ts_nano / 1_000_000_000) as i64;
    let query_q = encode_query(&[
        ("query", r#"http_requests{job="checkout"}"#),
        ("time", &eval_s.to_string()),
    ]);
    let (status, query) = get_json(&router, &format!("/api/v1/query?{query_q}")).await;
    assert_eq!(status, StatusCode::OK, "{query}");
    assert_eq!(query["status"], "success");
    assert_eq!(query["data"]["resultType"], "vector");
    let result = query["data"]["result"].as_array().expect("result");
    assert_eq!(result.len(), 1, "query={query}");
    assert_eq!(result[0]["value"][1], "42.0");
}

#[tokio::test]
async fn two_tenant_prometheus_isolation() {
    // File-backed sqlite catalogs share one process-default DuckLake scope when there is no
    // postgres registry. Use two AppStates (separate TempDirs) to prove Prom discovery is
    // bound to the tenant engine/lake — not a global metrics table.
    let (router_a, _temp_a) = build_tenant_router().await;
    let (router_b, _temp_b) = build_tenant_router().await;
    let ts_nano = 1_700_000_000_000_000_000u64;
    ingest_metrics(
        &router_a,
        gauge_otlp("http.requests", "tenant-a-job", 1.0, ts_nano),
    )
    .await;
    ingest_metrics(
        &router_b,
        gauge_otlp("http.requests", "tenant-b-job", 2.0, ts_nano),
    )
    .await;

    let values_q = encode_query(&[("match[]", "http_requests")]);
    let (status, values_a) =
        get_json(&router_a, &format!("/api/v1/label/job/values?{values_q}")).await;
    assert_eq!(status, StatusCode::OK, "{values_a}");
    let va = values_a["data"].as_array().unwrap();
    assert!(
        va.iter().any(|v| v.as_str() == Some("tenant-a-job")),
        "values_a={values_a}"
    );
    assert!(
        !va.iter().any(|v| v.as_str() == Some("tenant-b-job")),
        "router_a must not see router_b metrics: {values_a}"
    );

    let (status, values_b) =
        get_json(&router_b, &format!("/api/v1/label/job/values?{values_q}")).await;
    assert_eq!(status, StatusCode::OK, "{values_b}");
    let vb = values_b["data"].as_array().unwrap();
    assert!(
        vb.iter().any(|v| v.as_str() == Some("tenant-b-job")),
        "values_b={values_b}"
    );
    assert!(
        !vb.iter().any(|v| v.as_str() == Some("tenant-a-job")),
        "router_b must not see router_a metrics: {values_b}"
    );
}

#[tokio::test]
async fn unsupported_promql_returns_501() {
    let (router, _temp) = build_tenant_router().await;
    let q = encode_query(&[("query", "histogram_quantile(0.9, rate(x[5m]))")]);
    let (status, body) = get_json(&router, &format!("/api/v1/query?{q}")).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    assert_eq!(body["status"], "error");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("unsupported_feature"),
        "{body}"
    );
}

#[tokio::test]
async fn invalid_matcher_regex_is_bad_data() {
    let (router, _temp) = build_tenant_router().await;
    let ts_nano = 1_700_000_000_000_000_000u64;
    ingest_metrics(
        &router,
        gauge_otlp("http.requests", "checkout", 1.0, ts_nano),
    )
    .await;
    let series_q = encode_query(&[("match[]", r#"http_requests{job=~"(unclosed"}"#)]);
    let (status, body) = get_json(&router, &format!("/api/v1/series?{series_q}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
}

#[tokio::test]
async fn query_range_window_too_large_is_limit_exceeded() {
    let (router, _temp) = build_tenant_router().await;
    let q = encode_query(&[
        ("query", "up"),
        ("start", "0"),
        ("end", "9999999999"),
        ("step", "60"),
    ]);
    let (status, body) = get_json(&router, &format!("/api/v1/query_range?{q}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("max_query_range_seconds"),
        "{body}"
    );
}
