use crate::storage::buffer::Bufferable;
use anyhow::Result;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Span domain model - unified representation across all layers
/// Used for: OTLP ingestion → buffering → Iceberg storage → query results → JSON responses
///
/// This struct EXACTLY matches the Iceberg schema defined in src/storage/iceberg/tables.rs
/// Field order matches Iceberg field IDs for consistency
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Span {
    // Field 1: session_id (REQUIRED in Iceberg)
    // Extracted from sp.session.id attribute or defaults to trace_id
    pub session_id: String,

    // Field 2-4: Primary identifiers
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,

    // Field 5-7: Application context
    pub app_id: String,
    pub organization_id: Option<String>,
    pub tenant_id: Option<String>,

    // Field 8-11: Span metadata
    pub message_type: String,
    pub span_kind: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub end_timestamp: Option<chrono::DateTime<chrono::Utc>>,

    // Field 12: Attributes MAP<STRING, STRING>
    // Includes user-provided sp.* business attributes for search
    pub attributes: HashMap<String, String>,

    // Field 13: Events ARRAY<STRUCT<name, timestamp, attributes>>
    // Contains http.request and http.response events with full bodies
    pub events: Vec<SpanEvent>,

    // Field 14-15: Status
    pub status_code: Option<String>,
    pub status_message: Option<String>,

    // Field 25-31: HTTP data (extracted from span events)
    // These are populated by extract_http_data_from_events() method
    // Stored separately from events for columnar I/O efficiency (per ADR-003)
    pub http_request_method: Option<String>,
    pub http_request_path: Option<String>,
    pub http_request_headers: Option<String>,
    pub http_request_body: Option<String>,
    pub http_response_status_code: Option<i32>,
    pub http_response_headers: Option<String>,
    pub http_response_body: Option<String>,
    // Field 32: record_date (partition key - computed, not stored in struct)
    // Derived from timestamp at write time in arrow.rs
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
        // Use explicit session_id field (already populated from sp.session.id or trace_id)
        self.session_id.clone()
    }

    fn compare_for_sort(&self, other: &Self) -> Ordering {
        // Sort by session_id first, then trace_id, then timestamp
        // This matches Iceberg sort order (field 1, 2, 10)
        self.session_id
            .cmp(&other.session_id)
            .then_with(|| self.trace_id.cmp(&other.trace_id))
            .then_with(|| self.timestamp.cmp(&other.timestamp))
    }

    fn timestamp(&self) -> chrono::DateTime<chrono::Utc> {
        self.timestamp
    }
}

impl Span {
    /// Convert a batch of Spans to Arrow RecordBatch for Iceberg storage
    ///
    /// This is a batch operation delegated to the arrow module since RecordBatch
    /// creation requires schema context and columnar array building
    pub fn to_record_batch(
        spans: &[Span],
        iceberg_schema: &iceberg::spec::Schema,
    ) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        crate::storage::iceberg::arrow::spans_to_record_batch(spans, iceberg_schema)
    }

    /// Create a Span from an OTLP span and resource attributes
    pub fn from_otlp(
        otlp_span: opentelemetry_proto::tonic::trace::v1::Span,
        resource_attributes: &HashMap<String, String>,
    ) -> Result<Self> {
        // Extract span attributes
        let mut attributes = HashMap::new();
        for attr in &otlp_span.attributes {
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

        // Extract events
        let events = otlp_span.events.iter().map(|event| {
            let mut event_attributes = HashMap::new();
            for attr in &event.attributes {
                if let Some(value) = &attr.value {
                    let value_str = match value.value.as_ref() {
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                        _ => return None,
                    };
                    event_attributes.insert(attr.key.clone(), value_str);
                }
            }

            let event_timestamp = if event.time_unix_nano > 0 {
                chrono::DateTime::from_timestamp(
                    (event.time_unix_nano / 1_000_000_000) as i64,
                    (event.time_unix_nano % 1_000_000_000) as u32
                ).unwrap_or_else(|| chrono::Utc::now())
            } else {
                chrono::Utc::now()
            };

            Some(SpanEvent {
                name: event.name.clone(),
                timestamp: event_timestamp,
                attributes: event_attributes,
            })
        }).flatten().collect();

        // Convert timestamps
        let timestamp = if otlp_span.start_time_unix_nano > 0 {
            chrono::DateTime::from_timestamp(
                (otlp_span.start_time_unix_nano / 1_000_000_000) as i64,
                (otlp_span.start_time_unix_nano % 1_000_000_000) as u32,
            )
            .unwrap_or_else(|| chrono::Utc::now())
        } else {
            chrono::Utc::now()
        };

        let end_timestamp = if otlp_span.end_time_unix_nano > 0 {
            Some(
                chrono::DateTime::from_timestamp(
                    (otlp_span.end_time_unix_nano / 1_000_000_000) as i64,
                    (otlp_span.end_time_unix_nano % 1_000_000_000) as u32,
                )
                .unwrap_or_else(|| chrono::Utc::now()),
            )
        } else {
            None
        };

        // Extract app_id from resource attributes
        let app_id = resource_attributes
            .get("sp.app.id")
            .or_else(|| resource_attributes.get("service.name"))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        let trace_id = hex::encode(&otlp_span.trace_id);

        // Extract session_id from attributes (sp.session.id) or default to trace_id
        // This must be done before creating the span since we need to look at attributes
        let session_id = attributes
            .get("sp.session.id")
            .cloned()
            .unwrap_or_else(|| trace_id.clone());

        let mut span = Self {
            session_id,
            trace_id,
            span_id: hex::encode(&otlp_span.span_id),
            parent_span_id: if otlp_span.parent_span_id.is_empty() {
                None
            } else {
                Some(hex::encode(&otlp_span.parent_span_id))
            },
            app_id,
            organization_id: resource_attributes.get("sp.organization.id").cloned(),
            tenant_id: resource_attributes.get("sp.tenant.id").cloned(),
            message_type: otlp_span.name.clone(),
            span_kind: Some(format!("{:?}", otlp_span.kind())),
            timestamp,
            end_timestamp,
            attributes,
            events,
            // HTTP fields will be populated by extract_http_data_from_events()
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: otlp_span
                .status
                .as_ref()
                .map(|s| format!("{:?}", s.code()).to_uppercase()),
            status_message: otlp_span.status.as_ref().and_then(|s| {
                if s.message.is_empty() {
                    None
                } else {
                    Some(s.message.clone())
                }
            }),
        };

        // Extract HTTP data from span events and attributes
        span.extract_http_data_from_events();

        Ok(span)
    }

    /// Extract resource attributes from OTLP ResourceSpans
    pub fn extract_resource_attributes(
        resource_spans: &opentelemetry_proto::tonic::trace::v1::ResourceSpans,
    ) -> HashMap<String, String> {
        let mut attributes = HashMap::new();

        if let Some(resource) = &resource_spans.resource {
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

    /// Extract HTTP data from span events
    /// Looks for 'http.request' and 'http.response' events and extracts their attributes
    fn extract_http_data_from_events(&mut self) {
        // Find http.request event
        if let Some(request_event) = self.events.iter().find(|e| e.name == "http.request") {
            self.http_request_headers = request_event
                .attributes
                .get("http.request.headers")
                .cloned();
            self.http_request_body = request_event.attributes.get("http.request.body").cloned();
        }

        // Find http.response event
        if let Some(response_event) = self.events.iter().find(|e| e.name == "http.response") {
            self.http_response_headers = response_event
                .attributes
                .get("http.response.headers")
                .cloned();
            self.http_response_body = response_event.attributes.get("http.response.body").cloned();
        }

        // Extract standard HTTP attributes from span attributes
        self.http_request_method = self.attributes.get("http.request.method").cloned();
        self.http_request_path = self
            .attributes
            .get("http.request.path")
            .or_else(|| self.attributes.get("http.target"))
            .cloned();

        // Extract response status code from span attributes
        if let Some(status_code_str) = self.attributes.get("http.response.status_code") {
            self.http_response_status_code = status_code_str.parse::<i32>().ok();
        } else if let Some(status_code_str) = self.attributes.get("http.status_code") {
            self.http_response_status_code = status_code_str.parse::<i32>().ok();
        }
    }
}
