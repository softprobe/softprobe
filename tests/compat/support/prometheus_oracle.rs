//! Shared pinned-Prometheus Docker oracle for differential compat tests.

use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use crate::compat_support::prometheus::encode_query_owned;
use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant;

/// Pinned image used by mini-diff and curated promqltest.
pub const PROM_IMAGE: &str = "prom/prometheus:v2.54.1";

/// Shared timeline base (unix seconds) for OpenMetrics / lake sample alignment.
pub const EVAL_BASE_SECS: u64 = 1_700_000_000;

/// Shared timeline base (unix milliseconds).
pub const EVAL_BASE_MS: i64 = (EVAL_BASE_SECS as i64) * 1_000;

pub fn require_docker() {
    let ok = Command::new("docker")
        .args(["info"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    assert!(
        ok,
        "Docker is required for Prometheus differential gates (make test-prom-diff / test-promqltest)"
    );
}

pub fn wait_http_ok(url: &str, timeout: Duration) {
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

pub fn query_prom_oracle(base: &str, path: &str, params: &[(String, String)]) -> serde_json::Value {
    let url = format!("{base}{path}?{}", encode_query_owned(params));
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

pub async fn build_tenant_router() -> (Router, TempDir) {
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

fn chmod_777(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o777);
        std::fs::set_permissions(path, perms).unwrap();
    }
}

struct PromGuard {
    name: String,
}

impl Drop for PromGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .status();
    }
}

/// Live Prometheus serving OpenMetrics-backed TSDB blocks (ephemeral host port).
pub struct PromOracle {
    _guard: PromGuard,
    _work: TempDir,
    pub base: String,
}

/// Start pinned Prometheus from OpenMetrics text. Uses Docker-assigned host port.
pub fn start_prometheus_with_openmetrics(om: &str, container_prefix: &str) -> PromOracle {
    let work = TempDir::new().expect("prom work");
    let work_path = work.path();
    chmod_777(work_path);
    std::fs::write(work_path.join("samples.openmetrics"), om).unwrap();
    let tsdb = work_path.join("tsdb");
    std::fs::create_dir_all(&tsdb).unwrap();
    chmod_777(&tsdb);

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

    let container = format!(
        "{container_prefix}-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    );
    let _ = Command::new("docker")
        .args(["rm", "-f", &container])
        .status();
    // Ephemeral host port avoids collisions when Load recreates the oracle.
    let run = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &container,
            "-p",
            "127.0.0.1::9090",
            "-v",
            &format!("{}:/prometheus", tsdb.display()),
            PROM_IMAGE,
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.path=/prometheus",
            "--web.enable-lifecycle",
            "--storage.tsdb.retention.time=99999d",
            "--query.lookback-delta=5m",
        ])
        .status()
        .expect("docker run prometheus");
    assert!(run.success(), "failed to start prometheus container");

    let port_out = Command::new("docker")
        .args(["port", &container, "9090"])
        .output()
        .expect("docker port");
    assert!(
        port_out.status.success(),
        "docker port failed: {}",
        String::from_utf8_lossy(&port_out.stderr)
    );
    let mapping = String::from_utf8_lossy(&port_out.stdout);
    // e.g. "127.0.0.1:32768"
    let host_port = mapping
        .lines()
        .next()
        .and_then(|line| line.rsplit(':').next())
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| panic!("unexpected docker port output: {mapping}"));

    let base = format!("http://127.0.0.1:{host_port}");
    wait_http_ok(&format!("{base}/-/ready"), Duration::from_secs(45));
    PromOracle {
        _guard: PromGuard { name: container },
        _work: work,
        base,
    }
}
