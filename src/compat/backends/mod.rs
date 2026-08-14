//! Typed query backends shared by protocol adapters.

pub mod ducklake_metrics;
pub mod logs;
pub mod metrics;
pub mod prom_labels;
pub mod traces;

pub use ducklake_metrics::DuckLakeMetricsBackend;
pub use logs::{LogHit, LogsQueryBackend, LogsQueryRequest};
pub use metrics::{
    LabelMatcher, MatcherOp, MetricMetadata, MetricSeries, MetricsDiscoveryRequest,
    MetricsQueryBackend, MetricsQueryRequest, Sample, UnsupportedMetricsBackend,
};
pub use traces::{TraceQueryBackend, TraceSearchHit, TraceSearchRequest};
