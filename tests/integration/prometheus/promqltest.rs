//! Prometheus PromQL: lake HTTP API contracts + curated upstream promqltest vs pinned Prometheus.
//!
//! Curated corpus (`#[ignore]`) requires Docker — run via `make test-promqltest`.

use axum::http::StatusCode;
use axum::Router;
use softprobe_runtime::compat::prometheus::diff_normalize::normalize_prom_response;
use softprobe_runtime::compat::promql::parse_promql;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

use crate::compat_support::prometheus::{
    encode_query_owned, encode_query_pairs, gauge_labeled_otlp, gauge_otlp,
    gauge_series_otlp_with_flags, get_json, ingest_metrics, post_form_json, post_form_json_as,
    sum_labeled_otlp,
};
use crate::compat_support::prometheus_oracle::{
    build_tenant_router, query_prom_oracle, require_docker, start_prometheus_with_openmetrics,
    PromOracle, EVAL_BASE_MS,
};
use crate::compat_support::promqltest::{
    parse_promqltest, series_samples, to_openmetrics, Command as PtCmd, SeriesSpec,
};

fn encode_query(params: &[(&str, &str)]) -> String {
    encode_query_pairs(params)
}

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compat/prometheus/promqltest/curated")
}

/// `# softprobe: counter` in the file header means ingest as cumulative sum.
fn as_counter_from_text(text: &str) -> bool {
    text.lines().take(20).any(|l| {
        let t = l.trim_start().trim_start_matches('#').trim();
        t == "softprobe: counter" || t.starts_with("softprobe: as_counter")
    })
}

async fn ingest_series_to_lake(
    router: &Router,
    interval: Duration,
    series: &[SeriesSpec],
    as_counter: bool,
) {
    for spec in series {
        let samples = series_samples(interval, &spec.values, EVAL_BASE_MS);
        let ns_samples: Vec<(u64, f64)> = samples
            .into_iter()
            .filter(|(_, v)| !v.is_nan())
            .map(|(ms, v)| (ms as u64 * 1_000_000, v))
            .collect();
        if ns_samples.is_empty() {
            continue;
        }
        let labels: Vec<(String, String)> = spec
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let body = if as_counter {
            sum_labeled_otlp(&spec.name, &labels, &ns_samples)
        } else {
            gauge_labeled_otlp(&spec.name, &labels, &ns_samples)
        };
        ingest_metrics(router, body).await;
    }
}

fn query_supported(query: &str) -> bool {
    parse_promql(query).is_ok()
}

async fn ensure_backends(
    oracle: &mut Option<PromOracle>,
    lake: &mut Option<(Router, TempDir)>,
    loaded: &[(SeriesSpec, Duration)],
    as_counter: bool,
) {
    if oracle.is_some() && lake.is_some() {
        return;
    }
    let om = if loaded.is_empty() {
        "# EOF\n".to_string()
    } else {
        to_openmetrics(loaded, as_counter, EVAL_BASE_MS)
    };
    *oracle = Some(start_prometheus_with_openmetrics(&om, "thelake-promqltest"));
    let (router, temp) = build_tenant_router().await;
    if !loaded.is_empty() {
        let mut by_interval: Vec<(Duration, Vec<SeriesSpec>)> = Vec::new();
        for (spec, iv) in loaded {
            if let Some((_, specs)) = by_interval.iter_mut().find(|(d, _)| *d == *iv) {
                specs.push(spec.clone());
            } else {
                by_interval.push((*iv, vec![spec.clone()]));
            }
        }
        for (iv, specs) in by_interval {
            ingest_series_to_lake(&router, iv, &specs, as_counter).await;
        }
    }
    *lake = Some((router, temp));
}

async fn run_curated_file(path: &Path, as_counter: bool) {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let cmds = parse_promqltest(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"));
    let file = path.file_name().unwrap().to_string_lossy();

    let mut loaded: Vec<(SeriesSpec, Duration)> = Vec::new();
    let mut oracle: Option<PromOracle> = None;
    let mut lake: Option<(Router, TempDir)> = None;
    let mut ran = 0usize;

    for cmd in cmds {
        match cmd {
            PtCmd::Clear => {
                loaded.clear();
                oracle = None;
                lake = None;
            }
            PtCmd::Load { interval, series } => {
                for s in series {
                    loaded.push((s, interval));
                }
                // Rebuild backends after each load (merge semantics for consecutive loads).
                oracle = None;
                lake = None;
                ensure_backends(&mut oracle, &mut lake, &loaded, as_counter).await;
            }
            PtCmd::EvalInstant {
                at, query, line, ..
            } => {
                assert!(
                    query_supported(&query),
                    "curated fixture must only use supported PromQL [{file}:{line}] {query}"
                );
                ensure_backends(&mut oracle, &mut lake, &loaded, as_counter).await;
                let (oracle_h, router_pair) = (
                    oracle.as_ref().expect("oracle"),
                    lake.as_ref().expect("lake"),
                );
                let time_s = (EVAL_BASE_MS as f64 / 1000.0) + at.as_secs_f64();
                let params = vec![
                    ("query".into(), query.clone()),
                    ("time".into(), format!("{time_s}")),
                ];
                let oracle_raw = query_prom_oracle(&oracle_h.base, "/api/v1/query", &params);
                assert_eq!(
                    oracle_raw["status"], "success",
                    "oracle {file}:{line}: {oracle_raw}"
                );
                let qs = encode_query_owned(&params);
                let (status, lake_raw) =
                    get_json(&router_pair.0, &format!("/api/v1/query?{qs}")).await;
                assert_eq!(status, StatusCode::OK, "lake {file}:{line}: {lake_raw}");
                assert_eq!(
                    lake_raw["status"], "success",
                    "lake {file}:{line}: {lake_raw}"
                );
                let oracle_n = normalize_prom_response(oracle_raw);
                let lake_n = normalize_prom_response(lake_raw);
                assert_eq!(
                    lake_n["data"]["resultType"], oracle_n["data"]["resultType"],
                    "{file}:{line} resultType\nquery={query}\nlake={lake_n}\noracle={oracle_n}"
                );
                assert_eq!(
                    lake_n["data"]["result"], oracle_n["data"]["result"],
                    "{file}:{line} result\nquery={query}\nlake={lake_n}\noracle={oracle_n}"
                );
                ran += 1;
            }
            PtCmd::EvalRange {
                from,
                to,
                step,
                query,
                line,
                ..
            } => {
                assert!(
                    query_supported(&query),
                    "curated fixture must only use supported PromQL [{file}:{line}] {query}"
                );
                ensure_backends(&mut oracle, &mut lake, &loaded, as_counter).await;
                let (oracle_h, router_pair) = (
                    oracle.as_ref().expect("oracle"),
                    lake.as_ref().expect("lake"),
                );
                let base_s = EVAL_BASE_MS as f64 / 1000.0;
                let params = vec![
                    ("query".into(), query.clone()),
                    ("start".into(), format!("{}", base_s + from.as_secs_f64())),
                    ("end".into(), format!("{}", base_s + to.as_secs_f64())),
                    ("step".into(), format!("{}", step.as_secs_f64())),
                ];
                let oracle_raw = query_prom_oracle(&oracle_h.base, "/api/v1/query_range", &params);
                assert_eq!(
                    oracle_raw["status"], "success",
                    "oracle {file}:{line}: {oracle_raw}"
                );
                let qs = encode_query_owned(&params);
                let (status, lake_raw) =
                    get_json(&router_pair.0, &format!("/api/v1/query_range?{qs}")).await;
                assert_eq!(status, StatusCode::OK, "lake {file}:{line}: {lake_raw}");
                assert_eq!(
                    lake_raw["status"], "success",
                    "lake {file}:{line}: {lake_raw}"
                );
                let oracle_n = normalize_prom_response(oracle_raw);
                let lake_n = normalize_prom_response(lake_raw);
                assert_eq!(
                    lake_n["data"]["resultType"], oracle_n["data"]["resultType"],
                    "{file}:{line} range resultType\nquery={query}\nlake={lake_n}\noracle={oracle_n}"
                );
                assert_eq!(
                    lake_n["data"]["result"], oracle_n["data"]["result"],
                    "{file}:{line} range result\nquery={query}\nlake={lake_n}\noracle={oracle_n}"
                );
                ran += 1;
            }
        }
    }

    eprintln!("promqltest {file}: ran={ran}");
    assert!(ran > 0, "{file}: expected at least one eval to run");
}

#[tokio::test]
#[ignore = "docker oracle; run via make test-promqltest"]
async fn curated_promqltest_vs_pinned_prometheus() {
    require_docker();
    let dir = corpus_dir();
    assert!(dir.is_dir(), "missing curated corpus {dir:?}");

    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read curated dir: {e}"))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("test"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "curated corpus is empty under {dir:?}");

    for path in files {
        let text = std::fs::read_to_string(&path).expect("read curated");
        let as_counter = as_counter_from_text(&text);
        run_curated_file(&path, as_counter).await;
    }
}

// --- Lake Prometheus HTTP API contracts (no Docker oracle) ---

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
async fn post_form_query_matches_get() {
    let (router, _temp) = build_tenant_router().await;
    let ts_nano = 1_700_000_000_000_000_000u64;
    ingest_metrics(
        &router,
        gauge_otlp("http.requests", "checkout", 42.0, ts_nano),
    )
    .await;
    let eval_s = (ts_nano / 1_000_000_000) as i64;
    let time = eval_s.to_string();
    let params = [
        ("query", r#"http_requests{job="checkout"}"#),
        ("time", time.as_str()),
    ];
    let form = encode_query(&params);
    let (get_status, get_body) = get_json(&router, &format!("/api/v1/query?{form}")).await;
    let (post_status, post_body) = post_form_json(&router, "/api/v1/query", &form).await;
    assert_eq!(get_status, post_status, "get={get_body} post={post_body}");
    assert_eq!(get_body, post_body);

    let start = (eval_s - 60).to_string();
    let end = eval_s.to_string();
    let range_params = [
        ("query", r#"http_requests{job="checkout"}"#),
        ("start", start.as_str()),
        ("end", end.as_str()),
        ("step", "30"),
    ];
    let range_form = encode_query(&range_params);
    let (get_status, get_body) =
        get_json(&router, &format!("/api/v1/query_range?{range_form}")).await;
    let (post_status, post_body) =
        post_form_json(&router, "/api/v1/query_range", &range_form).await;
    assert_eq!(get_status, post_status, "get={get_body} post={post_body}");
    assert_eq!(get_body, post_body);
}

#[tokio::test]
async fn post_query_without_form_content_type_ignores_body() {
    let (router, _temp) = build_tenant_router().await;
    let form = encode_query(&[("query", "up")]);
    let (status, body) = post_form_json_as(&router, "/api/v1/query", &form, None, false).await;
    // Body ignored → missing query → bad_data (not success with "up").
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
}

#[tokio::test]
async fn otlp_no_recorded_value_omitted_from_instant_query() {
    let (router, _temp) = build_tenant_router().await;
    let ts_nano = 1_700_000_000_000_000_000u64;
    // flags=1 → DATA_POINT_FLAGS_NO_RECORDED_VALUE (stale/NaN).
    let body = gauge_series_otlp_with_flags(
        "http.requests",
        "checkout",
        &[(ts_nano - 60_000_000_000, 42.0, 0), (ts_nano, 0.0, 1)],
    );
    ingest_metrics(&router, body).await;
    let eval_s = (ts_nano / 1_000_000_000) as i64;
    let time = eval_s.to_string();
    let q = encode_query(&[
        ("query", r#"http_requests{job="checkout"}"#),
        ("time", time.as_str()),
    ]);
    let (status, body) = get_json(&router, &format!("/api/v1/query?{q}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "success");
    let result = body["data"]["result"].as_array().expect("result");
    assert!(
        result.is_empty(),
        "stale/NaN latest sample must omit series, got {body}"
    );
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
async fn query_range_long_windows_not_range_rejected() {
    // AC-W1 / AC-W2 / AC-W6: 30d / 180d / 365d must not fail with range exceeds.
    let (router, _temp) = build_tenant_router().await;
    let end: i64 = 1_700_000_000;
    for days in [30i64, 180, 365] {
        let start = end - days * 86_400;
        let q = encode_query(&[
            ("query", "up"),
            ("start", &start.to_string()),
            ("end", &end.to_string()),
            ("step", "3600"),
        ]);
        let (status, body) = get_json(&router, &format!("/api/v1/query_range?{q}")).await;
        assert_eq!(status, StatusCode::OK, "days={days} body={body}");
        assert_eq!(body["status"], "success", "days={days} body={body}");
        let err = body["error"].as_str().unwrap_or("");
        assert!(
            !err.contains("max_query_range_seconds") && !err.contains("range exceeds"),
            "days={days} must not range-reject: {body}"
        );
    }
}
