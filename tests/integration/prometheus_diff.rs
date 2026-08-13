//! Mini differential: Softprobe Prometheus API vs pinned prom/prometheus:v2.54.1.
//!
//! Requires Docker. Run via `make test-prom-diff`.

use axum::http::StatusCode;
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::compat::prometheus::diff_normalize::normalize_prom_response;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use crate::compat_support::prometheus::{
    encode_query_owned, gauge_series_otlp, get_json, ingest_metrics, sum_series_otlp,
};
use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant;

const PROM_IMAGE: &str = "prom/prometheus:v2.54.1";
const EVAL_BASE: u64 = 1_700_000_000;

fn encode_query(params: &[(String, String)]) -> String {
    encode_query_owned(params)
}

fn docker_available() -> bool {
    Command::new("docker")
        .args(["info"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn wait_http_ok(url: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        let ok = Command::new("curl")
            .args(["-sf", "-o", "/dev/null", "-w", "%{http_code}", url])
            .output()
            .ok()
            .and_then(|o| {
                if !o.status.success() {
                    return None;
                }
                String::from_utf8(o.stdout)
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok())
            })
            .map(|code| code > 0 && code < 500)
            .unwrap_or(false);
        if ok {
            return;
        }
        if start.elapsed() > timeout {
            panic!("timeout waiting for {url}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn query_prom_oracle(base: &str, path: &str, params: &[(String, String)]) -> serde_json::Value {
    let url = format!("{base}{path}?{}", encode_query(params));
    let out = Command::new("curl")
        .args(["-sf", &url])
        .output()
        .unwrap_or_else(|e| panic!("oracle curl {url}: {e}"));
    assert!(
        out.status.success(),
        "oracle curl failed {}: {}",
        url,
        String::from_utf8_lossy(&out.stderr)
    );
    let body = String::from_utf8(out.stdout).expect("oracle body utf8");
    serde_json::from_str(&body).unwrap_or_else(|e| panic!("oracle json: {e} body={body}"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compat/prometheus/diff")
}

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

fn ns(sec: u64) -> u64 {
    sec * 1_000_000_000
}

#[tokio::test]
#[ignore = "docker oracle; run via make test-prom-diff"]
async fn mini_diff_vs_pinned_prometheus() {
    if !docker_available() {
        eprintln!("skip prometheus mini-diff: docker not available");
        return;
    }

    let dir = fixture_dir();
    let samples = dir.join("samples.openmetrics");
    let cases_path = dir.join("cases.json");
    assert!(samples.is_file(), "missing {samples:?}");
    assert!(cases_path.is_file(), "missing {cases_path:?}");

    let work = TempDir::new().expect("work");
    let work_path = work.path();
    // Container user must be able to write TSDB output into the bind mount.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(work_path).unwrap().permissions();
        perms.set_mode(0o777);
        std::fs::set_permissions(work_path, perms).unwrap();
    }
    std::fs::copy(&samples, work_path.join("samples.openmetrics")).unwrap();
    std::fs::create_dir_all(work_path.join("tsdb")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(work_path.join("tsdb"))
            .unwrap()
            .permissions();
        perms.set_mode(0o777);
        std::fs::set_permissions(work_path.join("tsdb"), perms).unwrap();
    }

    // Build TSDB blocks from OpenMetrics using the pinned image's promtool.
    let status = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-u",
            "0",
            "-v",
            &format!("{}:/data", work_path.display()),
            "--entrypoint",
            "promtool",
            PROM_IMAGE,
            "tsdb",
            "create-blocks-from",
            "openmetrics",
            "/data/samples.openmetrics",
            "/data/tsdb",
        ])
        .status()
        .expect("docker promtool");
    assert!(status.success(), "promtool tsdb create-blocks-from failed");

    let container = format!("thelake-prom-diff-{}", std::process::id());
    let _ = Command::new("docker")
        .args(["rm", "-f", &container])
        .status();
    let run = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &container,
            "-p",
            "19090:9090",
            "-v",
            &format!("{}:/prometheus", work_path.join("tsdb").display()),
            PROM_IMAGE,
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.path=/prometheus",
            "--web.enable-lifecycle",
        ])
        .status()
        .expect("docker run prometheus");
    assert!(run.success(), "failed to start prometheus container");

    struct Guard(String);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = Command::new("docker").args(["rm", "-f", &self.0]).status();
        }
    }
    let _guard = Guard(container.clone());

    wait_http_ok("http://127.0.0.1:19090/-/ready", Duration::from_secs(30));
    let oracle = "http://127.0.0.1:19090";

    // Lake with equivalent samples.
    let (router, _temp) = build_tenant_router().await;
    ingest_metrics(
        &router,
        gauge_series_otlp(
            "http_requests",
            "checkout",
            &[
                (ns(EVAL_BASE), 10.0),
                (ns(EVAL_BASE + 60), 20.0),
                (ns(EVAL_BASE + 120), 40.0),
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
                (ns(EVAL_BASE), 5.0),
                (ns(EVAL_BASE + 60), 15.0),
                (ns(EVAL_BASE + 120), 35.0),
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
                (ns(EVAL_BASE), 100.0),
                (ns(EVAL_BASE + 60), 160.0),
                (ns(EVAL_BASE + 120), 220.0),
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

        let oracle_raw = query_prom_oracle(oracle, path, &params);
        assert_eq!(oracle_raw["status"], "success", "oracle {id}: {oracle_raw}");

        let qs = encode_query(&params);
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
