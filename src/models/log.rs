use crate::storage::buffer::Bufferable;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Log domain model - unified representation across all layers
/// Used for: OTLP ingestion → buffering → Iceberg storage → query results → JSON responses
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Log {
    // Session context
    pub session_id: Option<String>,

    // Timestamps
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub observed_timestamp: Option<chrono::DateTime<chrono::Utc>>,

    // Severity
    pub severity_number: i32,
    pub severity_text: String,

    // Log content
    pub body: String,

    // Attributes
    pub attributes: HashMap<String, String>,
    pub resource_attributes: HashMap<String, String>,

    // Trace correlation
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

impl Bufferable for Log {
    fn partition_key(&self) -> chrono::NaiveDate {
        self.timestamp.date_naive()
    }

    fn grouping_key(&self) -> String {
        self.session_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string())
    }

    fn compare_for_sort(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
            .then_with(|| self.severity_number.cmp(&other.severity_number))
    }

    fn timestamp(&self) -> chrono::DateTime<chrono::Utc> {
        self.timestamp
    }
}
