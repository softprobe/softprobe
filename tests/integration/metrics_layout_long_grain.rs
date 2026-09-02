//! AC-Q2 / AC-Q5: backdated fixtures → SQL materialize 1h/collapse → non-empty Prom.
//!
//! Catalog rows alone are not enough: PromQL lookback must cover 1h grain, and
//! `sum by (job) (rate(...[5m]))` over ≥2h must evaluate against collapse samples.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::config::Config;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::compat_support::prometheus::{
    encode_query_pairs, gauge_labeled_otlp, gauge_series_otlp, get_json, ingest_metrics,
};
use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant;

/// Same EVAL_END as the metrics-layout harness (800s past an hour boundary).
const EVAL_END_S: i64 = 1_700_000_000;
const TALL_DAYS: i64 = 3;
const COLLAPSE_JOBS: i64 = 3;

async fn build_router(config: Config) -> Router {
    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_local_sqlite_tenant))
}

async fn post_sql(router: &Router, sql: &str) -> StatusCode {
    let body = serde_json::json!({ "sql": sql });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    resp.status()
}

fn hourly_samples(end_s: i64, days: i64, base: f64) -> Vec<(u64, f64)> {
    let n = days * 24;
    (0..n)
        .map(|i| {
            let ts = (end_s - (n - 1 - i) * 3600) as u64 * 1_000_000_000;
            (ts, base + i as f64)
        })
        .collect()
}

async fn materialize_1h_and_collapse(router: &Router) {
    // Match harness / compaction SQL (closed hours already << now() for EVAL_END).
    let steps = [
        r#"
INSERT INTO softprobe.metric_samples_1h
  (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)
SELECT
  series_id,
  time_bucket(INTERVAL '1 hour', timestamp) AS window_ts,
  CAST(time_bucket(INTERVAL '1 hour', timestamp) AS DATE) AS record_date,
  count(*)::UBIGINT AS count,
  sum(value) AS sum,
  min(value) AS min,
  max(value) AS max,
  arg_max(value, timestamp) AS last,
  max(timestamp) AS last_ts
FROM softprobe.metric_samples
WHERE timestamp < now() - INTERVAL '24 hours'
  AND time_bucket(INTERVAL '1 hour', timestamp) >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM softprobe.metric_samples_1h)
GROUP BY series_id, time_bucket(INTERVAL '1 hour', timestamp)
"#,
        r#"
INSERT INTO softprobe.metric_collapse_job_1h
  (metric_name, job, window_ts, record_date, count, sum, min, max, last)
SELECT
  s.metric_name,
  p.label_value AS job,
  h.window_ts,
  h.record_date,
  sum(h.count)::UBIGINT AS count,
  sum(h.sum) AS sum,
  min(h.min) AS min,
  max(h.max) AS max,
  sum(h.last) AS last
FROM softprobe.metric_samples_1h h
JOIN softprobe.metric_series s
  ON h.series_id = s.series_id AND h.record_date = s.record_date
JOIN softprobe.metric_postings p
  ON p.series_id = h.series_id AND p.record_date = h.record_date
 AND p.label_name = 'job'
WHERE h.window_ts >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM softprobe.metric_collapse_job_1h)
GROUP BY s.metric_name, p.label_value, h.window_ts, h.record_date
"#,
    ];
    for sql in steps {
        let status = post_sql(router, sql.trim()).await;
        assert!(
            status.is_success(),
            "materialize step failed: http={status}"
        );
    }
}

#[tokio::test]
async fn long_grain_prom_1h_and_collapse_nonempty_after_materialize() {
    let temp = TempDir::new().unwrap();
    let config = file_backed_test_config(&temp);
    let router = build_router(config).await;

    let tall = hourly_samples(EVAL_END_S, TALL_DAYS, 1.0);
    ingest_metrics(&router, gauge_series_otlp("layout_tall", "tall", &tall)).await;

    for j in 0..COLLAPSE_JOBS {
        let job = format!("job{j}");
        let samples = hourly_samples(EVAL_END_S, TALL_DAYS, 10.0 + j as f64);
        ingest_metrics(
            &router,
            gauge_labeled_otlp(
                "layout_http",
                &[
                    ("service.name".into(), job.clone()),
                    ("service.instance.id".into(), format!("i{j}")),
                ],
                &samples,
            ),
        )
        .await;
    }

    materialize_1h_and_collapse(&router).await;

    let end = EVAL_END_S as f64;
    let start = end - (TALL_DAYS as f64) * 86400.0;
    let q = encode_query_pairs(&[
        ("query", "layout_tall"),
        ("start", &start.to_string()),
        ("end", &end.to_string()),
        ("step", "1h"),
    ]);
    let (status, body) = get_json(&router, &format!("/api/v1/query_range?{q}")).await;
    assert_eq!(status, StatusCode::OK, "Q2 http: {body}");
    let result = body["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(result.len(), 1, "AC-Q2: expected 1 series, body={body}");
    let points = result[0]["values"].as_array().map(|v| v.len()).unwrap_or(0);
    let want_pts = (TALL_DAYS * 24 - 2) as usize;
    assert!(
        points >= want_pts,
        "AC-Q2: expected ≥{want_pts} 1h points, got {points}; body={body}"
    );

    let cq = encode_query_pairs(&[
        ("query", "sum by (job) (rate(layout_http[5m]))"),
        ("start", &start.to_string()),
        ("end", &end.to_string()),
        ("step", "1h"),
    ]);
    let (status2, body2) = get_json(&router, &format!("/api/v1/query_range?{cq}")).await;
    assert_eq!(status2, StatusCode::OK, "Q5 http: {body2}");
    let result2 = body2["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        result2.len(),
        COLLAPSE_JOBS as usize,
        "AC-Q5: series must equal J; body={body2}"
    );
}
