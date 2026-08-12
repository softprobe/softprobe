//! Prometheus Phase 0: auth isolation + capability contract (stubs only).

use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::compat::errors::CompatErrorCode;
use softprobe_runtime::compat::stubs::declared_compat_probe_paths;
use softprobe_runtime::compat::tenant::{ProtocolScope, QueryLimits, TenantContext};

fn tenant(id: &str) -> TenantInfo {
    TenantInfo {
        tenant_id: id.into(),
        bucket_name: "b".into(),
        dataset_id: "d".into(),
    }
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
    // Prometheus does not use X-Scope-OrgID, but TenantContext still rejects spoofing
    // if a mismatched scope is supplied by a shared helper.
    let err = TenantContext::from_authenticated(
        tenant("tenant-a"),
        ProtocolScope::Prometheus,
        Some("tenant-b"),
        QueryLimits::default(),
    )
    .expect_err("spoof");
    assert_eq!(err.code, CompatErrorCode::Forbidden);
}
