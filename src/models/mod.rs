pub mod log;
pub mod metric;
pub mod score;
pub mod score_config;
/// Domain models - single source of truth for data structures
/// These models are used across all layers: ingestion, buffering, storage, and querying
pub mod span;

pub use log::Log;
pub use metric::{Metric, SummaryQuantile, UNSUPPORTED_EXPONENTIAL_HISTOGRAM};
pub use score::{Score, ScoreDataType, ScoreSource};
pub use score_config::ScoreConfig;
pub use span::{Span, SpanEvent};
