use crate::compat::errors::CompatError;
use crate::compat::tenant::TenantContext;
use async_trait::async_trait;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricSeries {
    pub labels: BTreeMap<String, String>,
    pub samples: Vec<Sample>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsQueryRequest {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    /// Opaque matcher string until Phase 1 lowers PromQL selectors.
    pub matchers: String,
}

#[async_trait]
pub trait MetricsQueryBackend: Send + Sync {
    async fn query_range(
        &self,
        ctx: &TenantContext,
        request: MetricsQueryRequest,
    ) -> Result<Vec<MetricSeries>, CompatError>;

    async fn label_names(
        &self,
        ctx: &TenantContext,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
    ) -> Result<Vec<String>, CompatError>;

    async fn label_values(
        &self,
        ctx: &TenantContext,
        name: &str,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
    ) -> Result<Vec<String>, CompatError>;
}

/// Phase 0 placeholder: every method returns `unsupported_feature`.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnsupportedMetricsBackend;

#[async_trait]
impl MetricsQueryBackend for UnsupportedMetricsBackend {
    async fn query_range(
        &self,
        _ctx: &TenantContext,
        _request: MetricsQueryRequest,
    ) -> Result<Vec<MetricSeries>, CompatError> {
        Err(CompatError::unsupported("metrics_query_backend"))
    }

    async fn label_names(
        &self,
        _ctx: &TenantContext,
        _start_ms: Option<i64>,
        _end_ms: Option<i64>,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("metrics_label_names"))
    }

    async fn label_values(
        &self,
        _ctx: &TenantContext,
        _name: &str,
        _start_ms: Option<i64>,
        _end_ms: Option<i64>,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("metrics_label_values"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authn::TenantInfo;
    use crate::compat::errors::CompatErrorCode;
    use crate::compat::tenant::{ProtocolScope, QueryLimits};

    #[tokio::test]
    async fn unsupported_backend_returns_stable_error() {
        let ctx = TenantContext::from_authenticated(
            TenantInfo {
                tenant_id: "t".into(),
                bucket_name: "b".into(),
                dataset_id: "d".into(),
            },
            ProtocolScope::Prometheus,
            None,
            QueryLimits::default(),
        )
        .unwrap();
        let err = UnsupportedMetricsBackend
            .query_range(
                &ctx,
                MetricsQueryRequest {
                    start_ms: None,
                    end_ms: None,
                    matchers: String::new(),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }
}
