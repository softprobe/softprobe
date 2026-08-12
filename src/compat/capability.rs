use crate::compat::errors::{CompatError, CompatErrorCode};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CapabilityManifest {
    pub version: String,
    pub otlp_write_canonical: bool,
    pub protocols: serde_yaml::Value,
    pub errors: CapabilityErrors,
    pub limits: CapabilityLimits,
    pub auth: CapabilityAuth,
    #[serde(default)]
    pub storage_fidelity: serde_yaml::Value,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CapabilityErrors {
    pub unsupported_feature: ErrorSpec,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ErrorSpec {
    pub code: String,
    pub http_status: u16,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CapabilityLimits {
    pub max_query_range_seconds: u64,
    pub max_series: usize,
    pub max_response_bytes: usize,
    pub max_labels_per_series: usize,
    pub query_timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CapabilityAuth {
    pub bearer: String,
    pub loki_scope_header: String,
    pub tempo_scope_header: String,
    pub scope_header_must_match_tenant: bool,
}

pub fn load_capability_v0(path: impl AsRef<Path>) -> Result<CapabilityManifest, CompatError> {
    let text = std::fs::read_to_string(path.as_ref()).map_err(|e| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("failed to read capability manifest: {e}"),
        )
    })?;
    parse_capability_yaml(&text)
}

pub fn parse_capability_yaml(text: &str) -> Result<CapabilityManifest, CompatError> {
    let manifest: CapabilityManifest = serde_yaml::from_str(text).map_err(|e| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("invalid capability manifest: {e}"),
        )
    })?;
    if manifest.version != "compat.v0" {
        return Err(CompatError::new(
            CompatErrorCode::BadRequest,
            format!("unsupported capability version '{}'", manifest.version),
        ));
    }
    if manifest.errors.unsupported_feature.code != "unsupported_feature" {
        return Err(CompatError::new(
            CompatErrorCode::BadRequest,
            "unsupported_feature error code must be 'unsupported_feature'",
        ));
    }
    if !manifest.otlp_write_canonical {
        return Err(CompatError::new(
            CompatErrorCode::BadRequest,
            "otlp_write_canonical must be true",
        ));
    }
    Ok(manifest)
}

/// Embedded Phase 0 manifest text (keeps unit tests independent of CWD).
pub const EMBEDDED_CAPABILITY_V0: &str = include_str!("../../docs/compat/capability.v0.yaml");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_embedded_capability_manifest() {
        let m = parse_capability_yaml(EMBEDDED_CAPABILITY_V0).expect("parse");
        assert_eq!(m.version, "compat.v0");
        assert!(m.otlp_write_canonical);
        assert_eq!(m.errors.unsupported_feature.http_status, 501);
        assert_eq!(m.limits.max_labels_per_series, 40);
        assert!(m.auth.scope_header_must_match_tenant);
    }
}
