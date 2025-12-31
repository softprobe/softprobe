use crate::storage::buffer::Bufferable;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Span domain model - unified representation across all layers
/// Used for: OTLP ingestion → buffering → Iceberg storage → query results → JSON responses
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Span {
    // Primary identifiers
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,

    // Application context
    pub app_id: String,
    pub organization_id: Option<String>,
    pub tenant_id: Option<String>,

    // Span metadata
    pub message_type: String,
    pub span_kind: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub end_timestamp: Option<chrono::DateTime<chrono::Utc>>,

    // Complex data
    pub attributes: HashMap<String, String>,
    pub events: Vec<SpanEvent>,

    // Status
    pub status_code: Option<String>,
    pub status_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub attributes: HashMap<String, String>,
}

impl Bufferable for Span {
    fn partition_key(&self) -> chrono::NaiveDate {
        self.timestamp.date_naive()
    }

    fn grouping_key(&self) -> String {
        // Extract session_id from attributes, fallback to trace_id
        self.attributes
            .get("sp.session.id")
            .cloned()
            .unwrap_or_else(|| self.trace_id.clone())
    }

    fn compare_for_sort(&self, other: &Self) -> Ordering {
        self.trace_id.cmp(&other.trace_id)
            .then_with(|| self.timestamp.cmp(&other.timestamp))
    }

    fn timestamp(&self) -> chrono::DateTime<chrono::Utc> {
        self.timestamp
    }
}
