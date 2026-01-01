use crate::storage::buffer::Bufferable;
use std::cmp::Ordering;
use std::collections::HashMap;
use anyhow::Result;

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

impl Log {
    /// Convert a batch of Logs to Arrow RecordBatch for Iceberg storage
    pub fn to_record_batch(
        logs: &[Log],
        iceberg_schema: &iceberg::spec::Schema,
    ) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        crate::storage::iceberg::arrow::logs_to_record_batch(logs, iceberg_schema)
    }

    /// Create a Log from an OTLP LogRecord and resource attributes
    pub fn from_otlp(
        log_record: opentelemetry_proto::tonic::logs::v1::LogRecord,
        resource_attributes: &HashMap<String, String>,
    ) -> Result<Self> {
        // Extract session_id from log record or resource attributes
        let session_id = Self::extract_session_id(&log_record, resource_attributes);

        // Extract timestamp (nanoseconds since epoch)
        let timestamp = if log_record.time_unix_nano > 0 {
            chrono::DateTime::from_timestamp_nanos(log_record.time_unix_nano as i64)
        } else {
            chrono::Utc::now()
        };

        // Extract observed timestamp
        let observed_timestamp = if log_record.observed_time_unix_nano > 0 {
            Some(chrono::DateTime::from_timestamp_nanos(log_record.observed_time_unix_nano as i64))
        } else {
            None
        };

        // Extract severity
        let severity_text = if !log_record.severity_text.is_empty() {
            log_record.severity_text.clone()
        } else {
            format!("SEVERITY_{:?}", log_record.severity_number())
        };

        // Extract body
        let body = Self::extract_log_body(&log_record).unwrap_or_default();

        // Extract log record attributes
        let mut attributes = HashMap::new();
        for attr in &log_record.attributes {
            if let Some(value) = &attr.value {
                let value_str = match value.value.as_ref() {
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                    _ => continue,
                };
                attributes.insert(attr.key.clone(), value_str);
            }
        }

        // Extract trace context
        let trace_id = if !log_record.trace_id.is_empty() {
            Some(hex::encode(&log_record.trace_id))
        } else {
            None
        };

        let span_id = if !log_record.span_id.is_empty() {
            Some(hex::encode(&log_record.span_id))
        } else {
            None
        };

        Ok(Log {
            session_id,
            timestamp,
            observed_timestamp,
            severity_number: log_record.severity_number,
            severity_text,
            body,
            attributes,
            resource_attributes: resource_attributes.clone(),
            trace_id,
            span_id,
        })
    }

    /// Extract resource attributes from OTLP ResourceLogs
    pub fn extract_resource_attributes(
        resource_logs: &opentelemetry_proto::tonic::logs::v1::ResourceLogs,
    ) -> HashMap<String, String> {
        let mut attributes = HashMap::new();

        if let Some(resource) = &resource_logs.resource {
            for attr in &resource.attributes {
                if let Some(value) = &attr.value {
                    let value_str = match value.value.as_ref() {
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                        _ => continue,
                    };
                    attributes.insert(attr.key.clone(), value_str);
                }
            }
        }

        attributes
    }

    /// Extract session_id from log record or resource attributes
    fn extract_session_id(
        log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord,
        resource_attributes: &HashMap<String, String>,
    ) -> Option<String> {
        // Check log record attributes first
        for attr in &log_record.attributes {
            if attr.key == "session.id" || attr.key == "session_id" {
                if let Some(value) = &attr.value {
                    if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) = &value.value {
                        return Some(s.clone());
                    }
                }
            }
        }

        // Fallback to resource attributes
        resource_attributes.get("session.id")
            .or_else(|| resource_attributes.get("session_id"))
            .cloned()
    }

    /// Extract log body as string
    fn extract_log_body(
        log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord,
    ) -> Option<String> {
        log_record.body.as_ref().and_then(|body| {
            match body.value.as_ref() {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => Some(s.clone()),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => Some(i.to_string()),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => Some(d.to_string()),
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => Some(b.to_string()),
                _ => None,
            }
        })
    }
}
