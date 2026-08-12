//! Tempo Phase 0: tenant header isolation contracts.

use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::compat::errors::CompatErrorCode;
use softprobe_runtime::compat::tenant::{ProtocolScope, QueryLimits, TenantContext};

#[test]
fn tempo_scope_header_must_match_tenant() {
    let err = TenantContext::from_authenticated(
        TenantInfo {
            tenant_id: "tenant-a".into(),
            bucket_name: "b".into(),
            dataset_id: "d".into(),
        },
        ProtocolScope::Tempo,
        Some("other"),
        QueryLimits::default(),
    )
    .unwrap_err();
    assert_eq!(err.code, CompatErrorCode::Forbidden);
}
