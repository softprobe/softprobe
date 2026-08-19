//! T-Q8 / AC-Q8 + T-Q0 / AC-Q0: GOLD overview query_range on F-gold + ingest heartbeat.
//!
//! Design: `docs/metrics-timeseries-layout.md` §10.2 (15 exprs), §10.1 F-gold, AC-Q0 sender.
//! p95 ≤5s ×5 repeats stays open for the release_full perf harness (partial until then).

use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::compat::prometheus::GOLD_OVERVIEW_EXPRS;
use softprobe_runtime::config::Config;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::sync::Arc;
use tempfile::TempDir;

use crate::compat_support::prometheus::{
    encode_query_pairs, gauge_labeled_otlp, gauge_series_otlp, get_json, histogram_series_otlp,
    ingest_metrics, sum_labeled_otlp, sum_series_otlp,
};
use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant;

/// Fixed timeline (unix seconds) for F-gold samples + 30m query_range window.
const EVAL_END_S: i64 = 1_700_000_600;
const STEP_S: i64 = 60;
const SAMPLE_POINTS: i64 = 12; // ~11 minutes of 60s scrapes (covers rate[5m])

fn attach(metadata_path: &str, data_path: &str) -> duckdb::Connection {
    let connection = duckdb::Connection::open_in_memory().expect("duckdb");
    connection
        .execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
        .expect("extensions");
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS softprobe \
             (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, \
              DATA_INLINING_ROW_LIMIT 0);",
            metadata_path.replace('\'', "''"),
            data_path.replace('\'', "''"),
        ))
        .expect("attach");
    connection
}

fn heartbeat_sample_count(conn: &duckdb::Connection) -> i64 {
    conn.query_row(
        "SELECT count(*) \
         FROM softprobe.metric_samples sm \
         JOIN softprobe.metric_series s \
           ON sm.series_id = s.series_id AND sm.record_date = s.record_date \
         WHERE s.metric_name = 'layout_ingest_heartbeat'",
        [],
        |r| r.get(0),
    )
    .unwrap_or(0)
}

async fn build_router(config: Config) -> Router {
    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_local_sqlite_tenant))
}

fn rising_counter_samples(end_s: i64) -> Vec<(u64, f64)> {
    (0..SAMPLE_POINTS)
        .map(|i| {
            let ts = (end_s - (SAMPLE_POINTS - 1 - i) * STEP_S) as u64 * 1_000_000_000;
            (ts, 10.0 + i as f64 * 5.0)
        })
        .collect()
}

fn rising_hist_samples(end_s: i64) -> Vec<(u64, u64, f64)> {
    (0..SAMPLE_POINTS)
        .map(|i| {
            let ts = (end_s - (SAMPLE_POINTS - 1 - i) * STEP_S) as u64 * 1_000_000_000;
            let count = 10 + i as u64 * 5;
            (ts, count, count as f64 * 2.0)
        })
        .collect()
}

fn gauge_samples(end_s: i64, base: f64) -> Vec<(u64, f64)> {
    (0..SAMPLE_POINTS)
        .map(|i| {
            let ts = (end_s - (SAMPLE_POINTS - 1 - i) * STEP_S) as u64 * 1_000_000_000;
            (ts, base + i as f64 * 0.1)
        })
        .collect()
}

/// Seed exact series names/labels needed by the 15 GOLD exprs (F-gold).
async fn seed_f_gold(router: &Router) {
    let counters = rising_counter_samples(EVAL_END_S);
    let hists = rising_hist_samples(EVAL_END_S);
    let gauges = gauge_samples(EVAL_END_S, 40.0);

    // Classic histogram `_count` panels.
    for (name, job) in [
        ("http.server.request.duration", "frontend"),
        ("http.server.request.duration", "checkout"),
        ("rpc.server.call.duration", "productcatalog"),
        ("http.client.request.duration", "frontend"),
        ("demo.cart.add.item.latency", "cart"),
    ] {
        ingest_metrics(router, histogram_series_otlp(name, job, &[], &hists)).await;
    }

    // Sum / counter panels (job or category labels).
    for job in ["frontend", "checkout"] {
        ingest_metrics(
            router,
            sum_series_otlp("traces.span.metrics.calls", job, &counters),
        )
        .await;
        ingest_metrics(
            router,
            sum_series_otlp("demo.payment.transactions", job, &counters),
        )
        .await;
        ingest_metrics(
            router,
            sum_series_otlp("demo.shipping.items.shipped", job, &counters),
        )
        .await;
        ingest_metrics(
            router,
            sum_series_otlp("demo.exchange.conversions.counter", job, &counters),
        )
        .await;
        ingest_metrics(router, sum_series_otlp("quotes", job, &counters)).await;
        ingest_metrics(router, sum_series_otlp("k6_iterations", job, &counters)).await;
    }

    for category in ["binoculars", "telescopes"] {
        ingest_metrics(
            router,
            sum_labeled_otlp(
                "demo.ad.served.total",
                &[
                    ("service.name".into(), "adservice".into()),
                    ("category".into(), category.into()),
                ],
                &counters,
            ),
        )
        .await;
    }

    ingest_metrics(
        router,
        gauge_series_otlp("k6_vus", "loadgenerator", &gauges),
    )
    .await;
    ingest_metrics(
        router,
        sum_series_otlp("k6_http_req_failed_total", "loadgenerator", &counters),
    )
    .await;

    for (container, cpu, mem) in [
        ("frontend", 0.35, 42.0),
        ("checkout", 0.55, 61.0),
        ("adservice", 0.22, 33.0),
    ] {
        let cpu_samples = gauge_samples(EVAL_END_S, cpu);
        let mem_samples = gauge_samples(EVAL_END_S, mem);
        let labels = [
            ("service.name".into(), "infra".into()),
            ("container_name".into(), container.into()),
        ];
        ingest_metrics(
            router,
            gauge_labeled_otlp("container_cpu_utilization", &labels, &cpu_samples),
        )
        .await;
        ingest_metrics(
            router,
            gauge_labeled_otlp("container_memory_percent", &labels, &mem_samples),
        )
        .await;
    }
}

async fn ingest_heartbeat(router: &Router, seq: i64) {
    let ts = (EVAL_END_S + seq) as u64 * 1_000_000_000;
    ingest_metrics(
        router,
        gauge_series_otlp(
            "layout_ingest_heartbeat",
            "layout-sender",
            &[(ts, seq as f64)],
        ),
    )
    .await;
}

/// T-Q8 / AC-Q8: each GOLD expr `query_range` 30m → HTTP 200, status=success.
/// T-Q0 / AC-Q0: heartbeat sample row count increases while those queries run.
#[tokio::test]
async fn gold_overview_query_range_30m_succeeds_with_heartbeat() {
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let router = build_router(config).await;

    seed_f_gold(&router).await;

    // Baseline heartbeat count (may be 0); then keep sender alive during queries.
    let conn_before = attach(&metadata_path, &data_path);
    let before = heartbeat_sample_count(&conn_before);
    drop(conn_before);

    ingest_heartbeat(&router, 1).await;

    let start = (EVAL_END_S - 30 * 60).to_string();
    let end = EVAL_END_S.to_string();
    assert_eq!(GOLD_OVERVIEW_EXPRS.len(), 15);

    for (i, expr) in GOLD_OVERVIEW_EXPRS.iter().enumerate() {
        // AC-Q0: sender keeps writing during the measure window.
        ingest_heartbeat(&router, 2 + i as i64).await;

        let q = encode_query_pairs(&[
            ("query", expr),
            ("start", start.as_str()),
            ("end", end.as_str()),
            ("step", "15"),
        ]);
        let (status, body) = get_json(&router, &format!("/api/v1/query_range?{q}")).await;
        assert_eq!(
            status,
            axum::http::StatusCode::OK,
            "AC-Q8 HTTP for `{expr}`: {body}"
        );
        assert_eq!(
            body["status"], "success",
            "AC-Q8 status for `{expr}`: {body}"
        );
        assert_eq!(
            body["data"]["resultType"], "matrix",
            "AC-Q8 resultType for `{expr}`: {body}"
        );
    }

    let conn_after = attach(&metadata_path, &data_path);
    let after = heartbeat_sample_count(&conn_after);
    assert!(
        after >= before + 1,
        "AC-Q0 / T-Q0: layout_ingest_heartbeat row count must increase during queries \
         (before={before}, after={after})"
    );
}
