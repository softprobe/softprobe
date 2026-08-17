//! Authenticated tenant context for compatibility (and future adapter) handlers.

use crate::authn::TenantInfo;
use crate::compat::capability::{parse_capability_yaml, CapabilityLimits, EMBEDDED_CAPABILITY_V0};
use crate::compat::errors::{CompatError, CompatErrorCode};
use std::time::{Duration, Instant};

pub const LOKI_SCOPE_HEADER: &str = "x-scope-orgid";
pub const TEMPO_SCOPE_HEADER: &str = "x-scope-orgid";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolScope {
    Prometheus,
    Loki,
    Tempo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLimits {
    /// Softprobe Prom range ceiling. `0` = unlimited (AC-W1 / §9.2); retention TTL
    /// bounds readable history, not this field.
    pub max_query_range_seconds: u64,
    pub max_series: usize,
    pub max_response_bytes: usize,
    pub max_labels_per_series: usize,
    pub query_timeout: Duration,
}

impl From<&CapabilityLimits> for QueryLimits {
    fn from(limits: &CapabilityLimits) -> Self {
        Self {
            max_query_range_seconds: limits.max_query_range_seconds,
            max_series: limits.max_series,
            max_response_bytes: limits.max_response_bytes,
            max_labels_per_series: limits.max_labels_per_series,
            query_timeout: Duration::from_secs(limits.query_timeout_seconds),
        }
    }
}

impl Default for QueryLimits {
    fn default() -> Self {
        // Single source of truth: embedded docs/compat/capability.v0.yaml
        let manifest = parse_capability_yaml(EMBEDDED_CAPABILITY_V0)
            .expect("embedded capability.v0.yaml must parse");
        QueryLimits::from(&manifest.limits)
    }
}

impl QueryLimits {
    /// Shared start/end window checks for Prom discovery + query handlers and backends.
    pub fn validate_time_range_ms(
        &self,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
    ) -> Result<(), CompatError> {
        if let (Some(start), Some(end)) = (start_ms, end_ms) {
            if end < start {
                return Err(CompatError::new(
                    CompatErrorCode::BadRequest,
                    "end must be >= start",
                ));
            }
            // Use i128 so extreme timestamps (e.g. from 1e30 params) cannot overflow i64
            // and bypass max_query_range_seconds via wraparound.
            let range_ms = i128::from(end).saturating_sub(i128::from(start));
            let range_secs = if range_ms <= 0 {
                0u64
            } else {
                let secs = range_ms / 1000;
                if secs > i128::from(u64::MAX) {
                    u64::MAX
                } else {
                    secs as u64
                }
            };
            // 0 = unlimited (§9.2 / AC-W1): do not reject on window length.
            if self.max_query_range_seconds > 0 && range_secs > self.max_query_range_seconds {
                return Err(CompatError::new(
                    CompatErrorCode::LimitExceeded,
                    format!(
                        "query range {range_secs}s exceeds max_query_range_seconds {}",
                        self.max_query_range_seconds
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// Tenant-bound request context. Handlers must not accept tenant ids from
/// query parameters or bodies — only from this type.
#[derive(Debug, Clone)]
pub struct TenantContext {
    pub tenant: TenantInfo,
    pub protocol: ProtocolScope,
    /// Validated protocol scope header value when the client sent one.
    pub scope_header: Option<String>,
    pub limits: QueryLimits,
    pub deadline: Instant,
}

impl TenantContext {
    pub fn from_authenticated(
        tenant: TenantInfo,
        protocol: ProtocolScope,
        scope_header: Option<&str>,
        limits: QueryLimits,
    ) -> Result<Self, CompatError> {
        if tenant.tenant_id.trim().is_empty() {
            return Err(CompatError::new(
                CompatErrorCode::Forbidden,
                "authenticated tenant id is empty",
            ));
        }

        let scope_header = match scope_header.map(str::trim).filter(|s| !s.is_empty()) {
            Some(raw) if raw != tenant.tenant_id => {
                return Err(CompatError::new(
                    CompatErrorCode::Forbidden,
                    format!(
                        "scope header '{raw}' does not match authenticated tenant '{}'",
                        tenant.tenant_id
                    ),
                ));
            }
            Some(raw) => Some(raw.to_string()),
            None => None,
        };

        let deadline = Instant::now() + limits.query_timeout;
        Ok(Self {
            tenant,
            protocol,
            scope_header,
            limits,
            deadline,
        })
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant.tenant_id
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

/// Extract Loki/Tempo scope header value from a header map (case-insensitive name).
pub fn scope_header_value<'a>(headers: &'a http::HeaderMap, header_name: &str) -> Option<&'a str> {
    headers
        .get(header_name)
        .or_else(|| headers.get(LOKI_SCOPE_HEADER))
        .and_then(|v| v.to_str().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(id: &str) -> TenantInfo {
        TenantInfo {
            tenant_id: id.to_string(),
            bucket_name: "bucket".to_string(),
            dataset_id: "dataset".to_string(),
        }
    }

    #[test]
    fn builds_context_without_scope_header() {
        let ctx = TenantContext::from_authenticated(
            tenant("t1"),
            ProtocolScope::Prometheus,
            None,
            QueryLimits::default(),
        )
        .expect("context");
        assert_eq!(ctx.tenant_id(), "t1");
        assert!(ctx.scope_header.is_none());
    }

    #[test]
    fn accepts_matching_scope_header() {
        let ctx = TenantContext::from_authenticated(
            tenant("t1"),
            ProtocolScope::Loki,
            Some("t1"),
            QueryLimits::default(),
        )
        .expect("context");
        assert_eq!(ctx.scope_header.as_deref(), Some("t1"));
    }

    #[test]
    fn rejects_mismatched_scope_header() {
        let err = TenantContext::from_authenticated(
            tenant("t1"),
            ProtocolScope::Loki,
            Some("other"),
            QueryLimits::default(),
        )
        .expect_err("mismatch");
        assert_eq!(err.code, CompatErrorCode::Forbidden);
    }

    #[test]
    fn rejects_empty_tenant() {
        let err = TenantContext::from_authenticated(
            tenant("  "),
            ProtocolScope::Tempo,
            None,
            QueryLimits::default(),
        )
        .expect_err("empty");
        assert_eq!(err.code, CompatErrorCode::Forbidden);
    }

    #[test]
    fn query_limits_match_embedded_capability_manifest() {
        let limits = QueryLimits::default();
        let manifest = parse_capability_yaml(EMBEDDED_CAPABILITY_V0).unwrap();
        assert_eq!(limits, QueryLimits::from(&manifest.limits));
        assert_eq!(limits.max_labels_per_series, 40);
        assert_eq!(limits.query_timeout, Duration::from_secs(30));
        assert_eq!(
            limits.max_query_range_seconds, 0,
            "AC-W1: embedded capability must set unlimited (0)"
        );
    }

    /// T-W1 / AC-W1: max_query_range_seconds = 0 → no Softprobe length reject.
    #[test]
    fn max_query_range_is_unlimited() {
        let limits = QueryLimits::default();
        assert_eq!(limits.max_query_range_seconds, 0);
        let day_ms = 86_400_000i64;
        // 180d and 365d must not fail with range exceeds.
        limits
            .validate_time_range_ms(Some(0), Some(180 * day_ms))
            .expect("180d must be accepted");
        limits
            .validate_time_range_ms(Some(0), Some(365 * day_ms))
            .expect("365d must be accepted");
        // Extreme span previously overflowed; with unlimited it must still be Ok
        // (only end < start is rejected).
        limits
            .validate_time_range_ms(Some(i64::MIN / 2), Some(i64::MAX / 2))
            .expect("unlimited must accept extreme span");
    }

    #[test]
    fn validate_time_range_rejects_when_cap_configured() {
        let mut limits = QueryLimits::default();
        limits.max_query_range_seconds = 86_400;
        let err = limits
            .validate_time_range_ms(Some(0), Some(200_000_000))
            .expect_err("must reject when cap > 0");
        assert_eq!(err.code, CompatErrorCode::LimitExceeded);
        assert!(err.message.contains("max_query_range_seconds"));
    }

    #[test]
    fn validate_time_range_still_rejects_end_before_start() {
        let limits = QueryLimits::default();
        let err = limits
            .validate_time_range_ms(Some(1000), Some(0))
            .expect_err("end < start");
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }
}
