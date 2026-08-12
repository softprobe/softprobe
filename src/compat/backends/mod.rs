//! Typed query backends shared by protocol adapters.

pub mod logs;
pub mod metrics;
pub mod traces;

pub use logs::{LogHit, LogsQueryBackend, LogsQueryRequest};
pub use metrics::{MetricSeries, MetricsQueryBackend, MetricsQueryRequest, Sample};
pub use traces::{TraceQueryBackend, TraceSearchHit, TraceSearchRequest};
