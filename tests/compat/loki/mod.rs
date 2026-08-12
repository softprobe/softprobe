//! Loki Phase 0: scope-header isolation contracts.

use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::compat::errors::CompatErrorCode;
use softprobe_runtime::compat::tenant::{ProtocolScope, QueryLimits, TenantContext};

#[test]
fn loki_scope_header_must_match_tenant() {
    let err = TenantContext::from_authenticated(
        TenantInfo {
            tenant_id: "tenant-a".into(),
            bucket_name: "b".into(),
            dataset_id: "d".into(),
        },
        ProtocolScope::Loki,
        Some("tenant-b"),
        QueryLimits::default(),
    )
    .unwrap_err();
    assert_eq!(err.code, CompatErrorCode::Forbidden);
}

#[test]
fn loki_matching_scope_header_ok() {
    TenantContext::from_authenticated(
        TenantInfo {
            tenant_id: "tenant-a".into(),
            bucket_name: "b".into(),
            dataset_id: "d".into(),
        },
        ProtocolScope::Loki,
        Some("tenant-a"),
        QueryLimits::default(),
    )
    .expect("match");
}
