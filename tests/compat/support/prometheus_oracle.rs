//! Shared pinned-Prometheus Docker oracle for differential compat tests.

use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::AppState;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use crate::compat_support::lifecycle;
use crate::compat_support::prometheus::encode_query_owned;
use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant;

/// Shared timeline base (unix seconds) for OpenMetrics / lake sample alignment.
pub const EVAL_BASE_SECS: u64 = 1_700_000_000;

/// Shared timeline base (unix milliseconds).
pub const EVAL_BASE_MS: i64 = (EVAL_BASE_SECS as i64) * 1_000;

fn prometheus_manifest_reference_image() -> String {
    let manifest: serde_yaml::Value =
        serde_yaml::from_str(include_str!("../../../docs/compat/references.v0.yaml"))
            .expect("references.v0.yaml parses");
    let reference = &manifest["references"]["prometheus"];
    let image = reference["image"]
        .as_str()
        .expect("Prometheus reference image is declared");
    let digest = reference["digest"]
        .as_str()
        .expect("Prometheus reference digest is declared");
    let digest_hex = digest
        .strip_prefix("sha256:")
        .expect("Prometheus reference digest uses sha256")
        .to_ascii_lowercase();
    assert_eq!(
        digest_hex.len(),
        64,
        "Prometheus reference digest must contain 64 hexadecimal characters"
    );
    assert!(
        digest_hex.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "Prometheus reference digest must contain only hexadecimal characters"
    );
    format!("{image}@sha256:{digest_hex}")
}

pub fn prometheus_reference_image() -> String {
    std::env::var("PROMETHEUS_REFERENCE_IMAGE")
        .ok()
        .filter(|image| !image.is_empty())
        .unwrap_or_else(prometheus_manifest_reference_image)
}

pub fn require_docker() {
    lifecycle::require_docker(
        "Docker is required for Prometheus differential gates (make test-prom-diff / test-promqltest)",
    );
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

pub async fn build_tenant_router_with_state() -> (Router, AppState, TempDir) {
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn(inject_local_sqlite_tenant));
    (router, state, temp)
}

pub async fn build_tenant_router() -> (Router, TempDir) {
    let (router, _state, temp) = build_tenant_router_with_state().await;
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

/// Live Prometheus serving OpenMetrics-backed TSDB blocks (ephemeral host port).
pub struct PromOracle {
    _service: lifecycle::ReferenceService,
    _work: TempDir,
    pub base: String,
}

/// Start pinned Prometheus from OpenMetrics text. Uses Docker-assigned host port.
pub fn start_prometheus_with_openmetrics(om: &str, container_prefix: &str) -> PromOracle {
    let image = prometheus_reference_image();
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
            image.as_str(),
            "tsdb",
            "create-blocks-from",
            "openmetrics",
            "/data/samples.openmetrics",
            "/data/tsdb",
        ])
        .status()
        .expect("docker promtool");
    assert!(status.success(), "promtool tsdb create-blocks-from failed");

    // Ephemeral host port avoids collisions when Load recreates the oracle.
    let service = lifecycle::start_reference_service(
        container_prefix,
        &image,
        &[
            "-p".into(),
            "127.0.0.1::9090".into(),
            "-v".into(),
            format!("{}:/prometheus", tsdb.display()),
        ],
        &[
            "--config.file=/etc/prometheus/prometheus.yml".into(),
            "--storage.tsdb.path=/prometheus".into(),
            "--web.enable-lifecycle".into(),
            "--storage.tsdb.retention.time=99999d".into(),
            "--query.lookback-delta=5m".into(),
        ],
        "9090",
        &[],
        "/-/ready",
        Duration::from_secs(45),
        "Docker is required for Prometheus differential gates (make test-prom-diff / test-promqltest)",
    );

    service.wait_queryable(
        &format!("{}/api/v1/query?query=up", service.base),
        Duration::from_secs(45),
    );
    let base = service.base.clone();

    PromOracle {
        _service: service,
        _work: work,
        base,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};

    fn reference_image_env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock")
    }

    struct ReferenceImageEnvGuard(Option<OsString>);

    impl Drop for ReferenceImageEnvGuard {
        fn drop(&mut self) {
            match self.0.take() {
                Some(value) => std::env::set_var("PROMETHEUS_REFERENCE_IMAGE", value),
                None => std::env::remove_var("PROMETHEUS_REFERENCE_IMAGE"),
            }
        }
    }

    fn set_reference_image_env(value: Option<&str>) -> ReferenceImageEnvGuard {
        let previous = std::env::var_os("PROMETHEUS_REFERENCE_IMAGE");
        match value {
            Some(value) => std::env::set_var("PROMETHEUS_REFERENCE_IMAGE", value),
            None => std::env::remove_var("PROMETHEUS_REFERENCE_IMAGE"),
        }
        ReferenceImageEnvGuard(previous)
    }

    #[test]
    fn reference_image_defaults_to_canonical_manifest_image() {
        let _lock = reference_image_env_lock();
        {
            let _env = set_reference_image_env(None);
            let manifest: serde_yaml::Value =
                serde_yaml::from_str(include_str!("../../../docs/compat/references.v0.yaml"))
                    .expect("references.v0.yaml parses");
            let reference = &manifest["references"]["prometheus"];
            let expected = format!(
                "{}@{}",
                reference["image"].as_str().expect("Prometheus image"),
                reference["digest"].as_str().expect("Prometheus digest")
            );
            assert_eq!(prometheus_reference_image(), expected);
        }
        let _env = set_reference_image_env(Some(""));
        assert_eq!(
            prometheus_reference_image(),
            prometheus_manifest_reference_image()
        );
    }

    #[test]
    fn reference_image_uses_non_empty_environment_override() {
        let _lock = reference_image_env_lock();
        let _env = set_reference_image_env(Some("prom/prometheus:v2.55.0"));

        assert_eq!(
            prometheus_reference_image(),
            "prom/prometheus:v2.55.0".to_string()
        );
    }
}
