use crate::compat::errors::CompatError;
use crate::compat::tenant::TenantContext;
use async_trait::async_trait;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct TraceSearchHit {
    pub trace_id: String,
    pub root_service_name: Option<String>,
    pub root_trace_name: Option<String>,
    pub start_time_unix_nano: i64,
    pub duration_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSearchRequest {
    pub tags: BTreeMap<String, String>,
    pub min_duration_ms: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit: usize,
}

#[async_trait]
pub trait TraceQueryBackend: Send + Sync {
    async fn get_trace(
        &self,
        ctx: &TenantContext,
        trace_id: &str,
    ) -> Result<Option<serde_json::Value>, CompatError>;

    async fn search(
        &self,
        ctx: &TenantContext,
        request: TraceSearchRequest,
    ) -> Result<Vec<TraceSearchHit>, CompatError>;

    async fn search_tags(&self, ctx: &TenantContext) -> Result<Vec<String>, CompatError>;

    async fn search_tag_values(
        &self,
        ctx: &TenantContext,
        tag: &str,
    ) -> Result<Vec<String>, CompatError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnsupportedTraceBackend;

#[async_trait]
impl TraceQueryBackend for UnsupportedTraceBackend {
    async fn get_trace(
        &self,
        _ctx: &TenantContext,
        _trace_id: &str,
    ) -> Result<Option<serde_json::Value>, CompatError> {
        Err(CompatError::unsupported("trace_get"))
    }

    async fn search(
        &self,
        _ctx: &TenantContext,
        _request: TraceSearchRequest,
    ) -> Result<Vec<TraceSearchHit>, CompatError> {
        Err(CompatError::unsupported("trace_search"))
    }

    async fn search_tags(&self, _ctx: &TenantContext) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("trace_search_tags"))
    }

    async fn search_tag_values(
        &self,
        _ctx: &TenantContext,
        _tag: &str,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("trace_search_tag_values"))
    }
}
