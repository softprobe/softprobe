pub mod log;
pub mod metric;
pub mod score;
/// Domain models - single source of truth for data structures
/// These models are used across all layers: ingestion, buffering, storage, and querying
pub mod span;

pub use log::Log;
pub use metric::Metric;
pub use score::{Score, ScoreDataType, ScoreSource};
pub use span::{Span, SpanEvent};
