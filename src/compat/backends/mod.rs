//! Typed query backends shared by protocol adapters.

pub mod ducklake_logs;
pub mod ducklake_metrics;
pub mod ducklake_traces;
pub mod grain;
pub mod logs;
pub mod metrics;
pub mod postings_resolve;
pub mod prom_labels;
pub mod traces;

pub use ducklake_metrics::DuckLakeMetricsBackend;
pub use ducklake_traces::DuckLakeTraceBackend;
pub use logs::{LogHit, LogsQueryBackend, LogsQueryRequest};
pub use metrics::{
    LabelMatcher, MatcherOp, MetricMetadata, MetricSeries, MetricsDiscoveryRequest,
    MetricsQueryBackend, MetricsQueryRequest, Sample, UnsupportedMetricsBackend,
};
pub use traces::{TraceQueryBackend, TraceSearchHit, TraceSearchRequest};
