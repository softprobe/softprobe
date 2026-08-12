use crate::compat::errors::CompatError;
use crate::compat::tenant::TenantContext;
use async_trait::async_trait;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct LogHit {
    pub timestamp_ns: i64,
    pub line: String,
    pub labels: BTreeMap<String, String>,
    pub structured_metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsQueryRequest {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub selector: String,
    pub limit: usize,
}

#[async_trait]
pub trait LogsQueryBackend: Send + Sync {
    async fn query_range(
        &self,
        ctx: &TenantContext,
        request: LogsQueryRequest,
    ) -> Result<Vec<LogHit>, CompatError>;

    async fn label_names(&self, ctx: &TenantContext) -> Result<Vec<String>, CompatError>;

    async fn label_values(
        &self,
        ctx: &TenantContext,
        name: &str,
    ) -> Result<Vec<String>, CompatError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnsupportedLogsBackend;

#[async_trait]
impl LogsQueryBackend for UnsupportedLogsBackend {
    async fn query_range(
        &self,
        _ctx: &TenantContext,
        _request: LogsQueryRequest,
    ) -> Result<Vec<LogHit>, CompatError> {
        Err(CompatError::unsupported("logs_query_backend"))
    }

    async fn label_names(&self, _ctx: &TenantContext) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("logs_label_names"))
    }

    async fn label_values(
        &self,
        _ctx: &TenantContext,
        _name: &str,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("logs_label_values"))
    }
}
