//! Tempo Phase 0: tenant header isolation contracts + envelope fixtures.

use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::compat::envelopes::{error_envelope, success_envelope_minimal};
use softprobe_runtime::compat::errors::{CompatError, CompatErrorCode};
use softprobe_runtime::compat::tenant::{ProtocolScope, QueryLimits, TenantContext};

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

#[test]
fn tempo_error_fixture_matches_envelope_helper() {
    let expected = load_fixture("tempo_error_unsupported.json");
    let actual = error_envelope(ProtocolScope::Tempo, &CompatError::unsupported("tempo_api"));
    assert_eq!(actual, expected);
}

#[test]
fn tempo_success_minimal_fixture_matches_helper() {
    let expected = load_fixture("tempo_success_minimal.json");
    assert_eq!(success_envelope_minimal(ProtocolScope::Tempo), expected);
}
