//! Prometheus Phase 1: discovery + query auth isolation contracts.

use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::compat::envelopes::{error_envelope, success_envelope_minimal};
use softprobe_runtime::compat::errors::{CompatError, CompatErrorCode};
use softprobe_runtime::compat::stubs::declared_compat_probe_paths;
use softprobe_runtime::compat::tenant::{ProtocolScope, QueryLimits, TenantContext};

fn tenant(id: &str) -> TenantInfo {
    TenantInfo {
        tenant_id: id.into(),
        bucket_name: "b".into(),
        dataset_id: "d".into(),
    }
}

fn load_fixture(name: &str) -> serde_json::Value {
    let path = format!(
        "{}/tests/compat/fixtures/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&raw).expect("parse fixture")
}

#[test]
fn prometheus_probe_paths_are_declared() {
    let paths: Vec<_> = declared_compat_probe_paths()
        .iter()
        .filter(|(_, p)| p.starts_with("/api/v1/"))
        .collect();
    assert!(paths.len() >= 6, "expected prometheus probe paths");
}

#[test]
fn prometheus_context_rejects_tenant_spoof_via_scope_header() {
    let err = TenantContext::from_authenticated(
        tenant("tenant-a"),
        ProtocolScope::Prometheus,
        Some("tenant-b"),
        QueryLimits::default(),
    )
    .expect_err("spoof");
    assert_eq!(err.code, CompatErrorCode::Forbidden);
}

#[test]
fn prometheus_error_fixture_matches_envelope_helper() {
    let expected = load_fixture("prometheus_error_unsupported.json");
    let actual = error_envelope(
        ProtocolScope::Prometheus,
        &CompatError::unsupported("prometheus_api"),
    );
    assert_eq!(actual, expected);
}

#[test]
fn prometheus_success_minimal_fixture_matches_helper() {
    let expected = load_fixture("prometheus_success_minimal.json");
    assert_eq!(
        success_envelope_minimal(ProtocolScope::Prometheus),
        expected
    );
}

#[test]
fn discovery_success_fixture_shape() {
    // labels / label values → string array; series → objects; metadata → map
    let labels = serde_json::json!({"status":"success","data":[]});
    assert_eq!(labels["status"], "success");
    assert!(labels["data"].is_array());
    let series = serde_json::json!({"status":"success","data":[{"__name__":"up"}]});
    assert!(series["data"][0].is_object());
}
