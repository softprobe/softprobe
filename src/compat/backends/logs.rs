use crate::compat::backends::metrics::LabelMatcher;
use crate::compat::errors::CompatError;
use crate::compat::tenant::TenantContext;
use async_trait::async_trait;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct LogHit {
    /// Loki contract: nanoseconds since the Unix epoch, preserved end to end.
    pub timestamp_ns: i64,
    pub line: String,
    pub labels: BTreeMap<String, String>,
    pub structured_metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsQueryRequest {
    /// Loki contract: nanoseconds since the Unix epoch; start is inclusive.
    pub start_ns: Option<i64>,
    /// Loki contract: nanoseconds since the Unix epoch; end is exclusive.
    pub end_ns: Option<i64>,
    pub matchers: Vec<LabelMatcher>,
    pub line_filters: Vec<LogLineFilter>,
    pub parser: Option<LogParser>,
    pub parsed_filters: Vec<LabelMatcher>,
    pub unwrap: Option<String>,
    pub limit: usize,
    pub direction: LogDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogLineFilter {
    Contains(String),
    NotContains(String),
    Regex(String),
    NotRegex(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogParser {
    Json,
    Logfmt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsDiscoveryRequest {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub matchers: Vec<Vec<LabelMatcher>>,
}

#[async_trait]
pub trait LogsQueryBackend: Send + Sync {
    async fn query_range(
        &self,
        ctx: &TenantContext,
        request: LogsQueryRequest,
    ) -> Result<Vec<LogHit>, CompatError>;

    async fn label_names(
        &self,
        ctx: &TenantContext,
        request: LogsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError>;

    async fn label_values(
        &self,
        ctx: &TenantContext,
        name: &str,
        request: LogsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError>;

    async fn series(
        &self,
        ctx: &TenantContext,
        request: LogsDiscoveryRequest,
    ) -> Result<Vec<BTreeMap<String, String>>, CompatError>;
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

    async fn label_names(
        &self,
        _ctx: &TenantContext,
        _request: LogsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("logs_label_names"))
    }

    async fn label_values(
        &self,
        _ctx: &TenantContext,
        _name: &str,
        _request: LogsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("logs_label_values"))
    }

    async fn series(
        &self,
        _ctx: &TenantContext,
        _request: LogsDiscoveryRequest,
    ) -> Result<Vec<BTreeMap<String, String>>, CompatError> {
        Err(CompatError::unsupported("logs_series"))
    }
}
