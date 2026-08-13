//! Mini differential: Softprobe Prometheus API vs pinned prom/prometheus:v2.54.1.
//!
//! Requires Docker. Run via `make test-prom-diff`.

use axum::http::StatusCode;
use softprobe_runtime::compat::prometheus::diff_normalize::normalize_prom_response;
use std::path::PathBuf;

use crate::compat_support::prometheus::{
    encode_query_owned, gauge_series_otlp, get_json, ingest_metrics, sum_series_otlp,
};
use crate::compat_support::prometheus_oracle::{
    build_tenant_router, query_prom_oracle, require_docker, start_prometheus_with_openmetrics,
    EVAL_BASE_SECS,
};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compat/prometheus/diff")
}

fn ns(sec: u64) -> u64 {
    sec * 1_000_000_000
}

#[tokio::test]
#[ignore = "docker oracle; run via make test-prom-diff"]
async fn mini_diff_vs_pinned_prometheus() {
    require_docker();

    let dir = fixture_dir();
    let samples = dir.join("samples.openmetrics");
    let cases_path = dir.join("cases.json");
    assert!(samples.is_file(), "missing {samples:?}");
    assert!(cases_path.is_file(), "missing {cases_path:?}");

    let om = std::fs::read_to_string(&samples).expect("read openmetrics");
    let oracle = start_prometheus_with_openmetrics(&om, "thelake-prom-diff");

    // Lake with equivalent samples.
    let (router, _temp) = build_tenant_router().await;
    ingest_metrics(
        &router,
        gauge_series_otlp(
            "http_requests",
            "checkout",
            &[
                (ns(EVAL_BASE_SECS), 10.0),
                (ns(EVAL_BASE_SECS + 60), 20.0),
                (ns(EVAL_BASE_SECS + 120), 40.0),
            ],
        ),
    )
    .await;
    ingest_metrics(
        &router,
        gauge_series_otlp(
            "http_requests",
            "api",
            &[
                (ns(EVAL_BASE_SECS), 5.0),
                (ns(EVAL_BASE_SECS + 60), 15.0),
                (ns(EVAL_BASE_SECS + 120), 35.0),
            ],
        ),
    )
    .await;
    ingest_metrics(
        &router,
        sum_series_otlp(
            "demo_counter",
            "checkout",
            &[
                (ns(EVAL_BASE_SECS), 100.0),
                (ns(EVAL_BASE_SECS + 60), 160.0),
                (ns(EVAL_BASE_SECS + 120), 220.0),
            ],
        ),
    )
    .await;

    let cases: Vec<serde_json::Value> =
        serde_json::from_str(&std::fs::read_to_string(&cases_path).unwrap()).unwrap();

    for case in cases {
        let id = case["id"].as_str().unwrap();
        let path = case["path"].as_str().unwrap();
        let params_obj = case["params"].as_object().unwrap();
        let params: Vec<(String, String)> = params_obj
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
            .collect();

        let oracle_raw = query_prom_oracle(&oracle.base, path, &params);
        assert_eq!(oracle_raw["status"], "success", "oracle {id}: {oracle_raw}");

        let qs = encode_query_owned(&params);
        let (status, lake_raw) = get_json(&router, &format!("{path}?{qs}")).await;
        assert_eq!(status, StatusCode::OK, "lake {id}: {lake_raw}");
        assert_eq!(lake_raw["status"], "success", "lake {id}: {lake_raw}");

        let oracle_n = normalize_prom_response(oracle_raw);
        let lake_n = normalize_prom_response(lake_raw);
        assert_eq!(
            lake_n["data"]["resultType"], oracle_n["data"]["resultType"],
            "case {id} resultType mismatch\nlake={lake_n}\noracle={oracle_n}"
        );
        assert_eq!(
            lake_n["data"]["result"], oracle_n["data"]["result"],
            "case {id} result mismatch\nlake={lake_n}\noracle={oracle_n}"
        );
    }
}
