use crate::storage::buffer::Bufferable;
use anyhow::Result;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Log domain model - unified representation across all layers
/// Used for: OTLP ingestion → buffering → Iceberg storage → query results → JSON responses
///
/// This struct EXACTLY matches the Iceberg schema defined in src/storage/iceberg/tables.rs
/// Field order matches Iceberg field IDs for consistency
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Log {
    // Field 1: session_id (OPTIONAL in Iceberg)
    // Extracted from log attributes (session.id, session_id, sp.session.id) or resource attributes
    pub session_id: Option<String>,

    // Field 2-3: Timestamps
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub observed_timestamp: Option<chrono::DateTime<chrono::Utc>>,

    // Field 4-5: Severity
    pub severity_number: i32,
    pub severity_text: String,

    // Field 6: Log content
    pub body: String,

    // Field 7: Attributes MAP<STRING, STRING>
    // Log-level attributes from OTLP LogRecord
    pub attributes: HashMap<String, String>,

    // Field 8: Resource attributes MAP<STRING, STRING>
    // Service-level attributes (service.name, host.name, etc.)
    pub resource_attributes: HashMap<String, String>,

    // Field 9-10: Trace correlation
    // Links logs to traces for distributed tracing
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    // Field 15: record_date (partition key - computed, not stored in struct)
    // Derived from timestamp at write time in arrow.rs
}

impl Bufferable for Log {
    fn partition_key(&self) -> chrono::NaiveDate {
        self.timestamp.date_naive()
    }

    fn grouping_key(&self) -> String {
        // Use session_id if available, otherwise "unknown"
        self.session_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string())
    }

    fn compare_for_sort(&self, other: &Self) -> Ordering {
        // Sort by session_id first, then timestamp
        // This matches Iceberg sort order (field 1, 2)
        self.session_id
            .cmp(&other.session_id)
            .then_with(|| self.timestamp.cmp(&other.timestamp))
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
            Some(chrono::DateTime::from_timestamp_nanos(
                log_record.observed_time_unix_nano as i64,
            ))
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
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                        i.to_string()
                    }
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d),
                    ) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(
                        b,
                    )) => b.to_string(),
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
                        Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                s,
                            ),
                        ) => s.clone(),
                        Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i),
                        ) => i.to_string(),
                        Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(
                                d,
                            ),
                        ) => d.to_string(),
                        Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b),
                        ) => b.to_string(),
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
            if attr.key == "session.id" || attr.key == "session_id" || attr.key == "sp.session.id" {
                if let Some(value) = &attr.value {
                    if let Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) = &value.value
                    {
                        return Some(s.clone());
                    }
                }
            }
        }

        // Fallback to resource attributes
        resource_attributes
            .get("session.id")
            .or_else(|| resource_attributes.get("session_id"))
            .or_else(|| resource_attributes.get("sp.session.id"))
            .cloned()
    }

    /// Extract log body as string
    fn extract_log_body(
        log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord,
    ) -> Option<String> {
        log_record
            .body
            .as_ref()
            .and_then(|body| match body.value.as_ref() {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                    Some(s.clone())
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                    Some(i.to_string())
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => {
                    Some(d.to_string())
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => {
                    Some(b.to_string())
                }
                _ => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::Log;
    use crate::storage::buffer::Bufferable;
    use chrono::{TimeZone, Utc};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs};
    use std::collections::HashMap;
    use std::cmp::Ordering;

    fn sample_log(ts_nano: i64) -> Log {
        Log {
            session_id: Some("sid".into()),
            timestamp: chrono::DateTime::from_timestamp_nanos(ts_nano),
            observed_timestamp: None,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: "hello".into(),
            attributes: HashMap::new(),
            resource_attributes: [("service.name".into(), "api".into())]
                .into_iter()
                .collect(),
            trace_id: None,
            span_id: None,
        }
    }

    #[test]
    fn bufferable_partition_and_grouping() {
        let t = Utc.with_ymd_and_hms(2024, 1, 2, 3, 4, 5).unwrap();
        let mut l = sample_log(t.timestamp_nanos_opt().unwrap());
        l.session_id = Some("a".into());
        assert_eq!(l.partition_key(), t.date_naive());
        assert_eq!(l.grouping_key(), "a");
        l.session_id = None;
        assert_eq!(l.grouping_key(), "unknown");
    }

    #[test]
    fn bufferable_compare_orders_by_session_then_time() {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 1).unwrap();
        let mut a = sample_log(t0.timestamp_nanos_opt().unwrap());
        a.session_id = Some("b".into());
        let mut b = sample_log(t1.timestamp_nanos_opt().unwrap());
        b.session_id = Some("a".into());
        assert_eq!(a.compare_for_sort(&b), Ordering::Greater);
    }

    #[test]
    fn extract_resource_attributes_empty_and_nonempty() {
        let empty = ResourceLogs {
            resource: None,
            ..Default::default()
        };
        assert!(Log::extract_resource_attributes(&empty).is_empty());
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::resource::v1::Resource;
        let with = ResourceLogs {
            resource: Some(Resource {
                attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "k".into(),
                    value: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "v".into(),
                            ),
                        ),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            ..Default::default()
        };
        let m = Log::extract_resource_attributes(&with);
        assert_eq!(m.get("k"), Some(&"v".to_string()));
    }

    #[test]
    fn from_otlp_minimal_string_body() {
        let lr = LogRecord {
            time_unix_nano: 1,
            body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "body".into(),
                    ),
                ),
            }),
            ..Default::default()
        };
        let ra = HashMap::new();
        let log = Log::from_otlp(lr, &ra).expect("from_otlp");
        assert_eq!(log.body, "body");
    }
}
