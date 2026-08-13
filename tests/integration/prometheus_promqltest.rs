//! Curated upstream promqltest corpus vs pinned Prometheus (option A).
//!
//! Requires Docker. Run via `make test-promqltest`.

use axum::http::StatusCode;
use axum::Router;
use softprobe_runtime::compat::prometheus::diff_normalize::normalize_prom_response;
use softprobe_runtime::compat::promql::parse_promql;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

use crate::compat_support::prometheus::{
    encode_query_owned, gauge_labeled_otlp, get_json, ingest_metrics, sum_labeled_otlp,
};
use crate::compat_support::prometheus_oracle::{
    build_tenant_router, query_prom_oracle, require_docker, start_prometheus_with_openmetrics,
    PromOracle, EVAL_BASE_MS,
};
use crate::compat_support::promqltest::{
    parse_promqltest, series_samples, to_openmetrics, Command as PtCmd, SeriesSpec,
};

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
