/// Domain models - single source of truth for data structures
/// These models are used across all layers: ingestion, buffering, storage, and querying

pub mod span;
pub mod log;

pub use span::{Span, SpanEvent};
pub use log::Log;
