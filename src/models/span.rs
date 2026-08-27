use anyhow::Result;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Internal JSON carriers used only while the canonical span model is persisted.
/// They keep OTLP scope/link metadata out of the user attribute namespace on read.
pub const INSTRUMENTATION_SCOPE_ATTRIBUTE: &str = "__softprobe.instrumentation_scope";
pub const LINKS_ATTRIBUTE: &str = "__softprobe.links";
/// Namespace prefix reserved for internal carriers; client telemetry must not use it.
pub const RESERVED_ATTRIBUTE_PREFIX: &str = "__softprobe.";

pub fn encode_instrumentation_scope(
    scope: &opentelemetry_proto::tonic::common::v1::InstrumentationScope,
) -> String {
    let attributes = crate::models::key_values_to_map(&scope.attributes)
        .into_iter()
        .map(|(key, value)| {
            serde_json::json!({
                "key": key,
                "value": {"stringValue": value},
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "name": scope.name,
        "version": scope.version,
        "attributes": attributes,
    })
    .to_string()
}

pub fn encode_links(links: &[opentelemetry_proto::tonic::trace::v1::span::Link]) -> String {
    serde_json::json!(links
        .iter()
        .map(|link| {
            let attributes = crate::models::key_values_to_map(&link.attributes)
                .into_iter()
                .map(|(key, value)| {
                    serde_json::json!({
                        "key": key,
                        "value": {"stringValue": value},
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "traceId": hex::encode(&link.trace_id),
                "spanId": hex::encode(&link.span_id),
                "traceState": link.trace_state,
                "attributes": attributes,
                "flags": link.flags,
            })
        })
        .collect::<Vec<_>>())
    .to_string()
}

/// Span domain model - unified representation across all layers
/// Used for: OTLP ingestion → DuckLake storage → query results → JSON responses
///
/// This struct matches the telemetry schema in `src/storage/schema/tables.rs`
/// (legacy path; Arrow/DuckLake column order).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Span {
    // Field 1: session_id (REQUIRED)
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

    // Resource attributes retained for Tempo resource projection and promotion extraction.
    pub resource_attributes: HashMap<String, String>,

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

impl Span {
    pub fn partition_key(&self) -> chrono::NaiveDate {
        self.timestamp.date_naive()
    }

    pub fn grouping_key(&self) -> String {
        self.session_id.clone()
    }

    pub fn compare_for_sort(&self, other: &Self) -> Ordering {
        self.session_id
            .cmp(&other.session_id)
            .then_with(|| self.trace_id.cmp(&other.trace_id))
            .then_with(|| self.timestamp.cmp(&other.timestamp))
    }

    /// Convert a batch of Spans to Arrow RecordBatch for DuckLake storage
    ///
    /// This is a batch operation delegated to the arrow module since RecordBatch
    /// creation requires schema context and columnar array building
    pub fn to_record_batch(
        spans: &[Span],
        schema: &arrow::datatypes::Schema,
    ) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        crate::storage::schema::arrow::spans_to_record_batch(spans, schema)
    }

    /// Create a Span from an OTLP span and resource attributes
    pub fn from_otlp(
        otlp_span: opentelemetry_proto::tonic::trace::v1::Span,
        resource_attributes: &HashMap<String, String>,
    ) -> Result<Self> {
        // Reject timestamps outside the signed-nanosecond range so they can
        // never silently persist as epoch/null downstream.
        for label in [
            ("start_time_unix_nano", otlp_span.start_time_unix_nano),
            ("end_time_unix_nano", otlp_span.end_time_unix_nano),
        ] {
            anyhow::ensure!(
                label.1 <= i64::MAX as u64,
                "span {} exceeds signed nanosecond range ({})",
                label.0,
                label.1
            );
        }
        for event in &otlp_span.events {
            anyhow::ensure!(
                event.time_unix_nano <= i64::MAX as u64,
                "span event time exceeds signed nanosecond range ({})",
                event.time_unix_nano
            );
        }
        let attributes = crate::models::key_values_to_map(&otlp_span.attributes);

        // Extract events
        let events = otlp_span
            .events
            .iter()
            .map(|event| {
                let event_timestamp = if event.time_unix_nano > 0 {
                    chrono::DateTime::from_timestamp(
                        (event.time_unix_nano / 1_000_000_000) as i64,
                        (event.time_unix_nano % 1_000_000_000) as u32,
                    )
                    .unwrap_or_else(chrono::Utc::now)
                } else {
                    chrono::Utc::now()
                };

                SpanEvent {
                    name: event.name.clone(),
                    timestamp: event_timestamp,
                    attributes: crate::models::key_values_to_map(&event.attributes),
                }
            })
            .collect();

        // Convert timestamps
        let timestamp = if otlp_span.start_time_unix_nano > 0 {
            chrono::DateTime::from_timestamp(
                (otlp_span.start_time_unix_nano / 1_000_000_000) as i64,
                (otlp_span.start_time_unix_nano % 1_000_000_000) as u32,
            )
            .unwrap_or_else(chrono::Utc::now)
        } else {
            chrono::Utc::now()
        };

        let end_timestamp = if otlp_span.end_time_unix_nano > 0 {
            Some(
                chrono::DateTime::from_timestamp(
                    (otlp_span.end_time_unix_nano / 1_000_000_000) as i64,
                    (otlp_span.end_time_unix_nano % 1_000_000_000) as u32,
                )
                .unwrap_or_else(chrono::Utc::now),
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
            resource_attributes: resource_attributes.clone(),
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
        match &resource_spans.resource {
            Some(resource) => crate::models::key_values_to_map(&resource.attributes),
            None => HashMap::new(),
        }
    }

    /// Extract HTTP data from span events
    /// Looks for `http.request` / `http.response` events and span-attribute fallbacks.
    /// Request/response bodies accept the proxy/SDK keys (`http.request.body`, `http.response.body`)
    /// and the OBI eBPF convention (`http.request.body.content`, `http.response.body.content`);
    /// when both are present, the shorter legacy keys win.
    fn extract_http_data_from_events(&mut self) {
        // Find http.request event
        if let Some(request_event) = self.events.iter().find(|e| e.name == "http.request") {
            self.http_request_headers = request_event
                .attributes
                .get("http.request.headers")
                .cloned();
            self.http_request_body = request_event
                .attributes
                .get("http.request.body")
                .or_else(|| request_event.attributes.get("http.request.body.content"))
                .cloned();
        }

        // Find http.response event
        if let Some(response_event) = self.events.iter().find(|e| e.name == "http.response") {
            self.http_response_headers = response_event
                .attributes
                .get("http.response.headers")
                .cloned();
            self.http_response_body = response_event
                .attributes
                .get("http.response.body")
                .or_else(|| response_event.attributes.get("http.response.body.content"))
                .cloned();
        }

        // Fall back to span attributes when events do not carry HTTP payload fields.
        if self.http_request_headers.is_none() {
            self.http_request_headers = self.attributes.get("http.request.headers").cloned();
        }
        if self.http_request_body.is_none() {
            self.http_request_body = self
                .attributes
                .get("http.request.body")
                .or_else(|| self.attributes.get("http.request.body.content"))
                .cloned();
        }
        if self.http_response_headers.is_none() {
            self.http_response_headers = self.attributes.get("http.response.headers").cloned();
        }
        if self.http_response_body.is_none() {
            self.http_response_body = self
                .attributes
                .get("http.response.body")
                .or_else(|| self.attributes.get("http.response.body.content"))
                .cloned();
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

#[cfg(test)]
mod tests {
    use super::{encode_instrumentation_scope, encode_links, Span, SpanEvent};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::trace::v1::span::Link;
    use std::collections::HashMap;

    fn base_span() -> Span {
        Span {
            session_id: "s".to_string(),
            trace_id: "t".to_string(),
            span_id: "sp".to_string(),
            parent_span_id: None,
            app_id: "app".to_string(),
            organization_id: None,
            tenant_id: None,
            message_type: "msg".to_string(),
            span_kind: None,
            timestamp: chrono::Utc::now(),
            end_timestamp: None,
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            events: Vec::new(),
            status_code: None,
            status_message: None,
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
        }
    }

    #[test]
    fn otlp_scope_and_link_carriers_keep_key_value_shape() {
        let scope = InstrumentationScope {
            name: "otel-rust".into(),
            version: "1.0".into(),
            attributes: vec![KeyValue {
                key: "scope.attr".into(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "yes".into(),
                        ),
                    ),
                }),
            }],
            dropped_attributes_count: 0,
        };
        let scope_json: serde_json::Value =
            serde_json::from_str(&encode_instrumentation_scope(&scope)).unwrap();
        assert_eq!(scope_json["attributes"][0]["key"], "scope.attr");

        let links_json: serde_json::Value = serde_json::from_str(&encode_links(&[Link {
            trace_id: vec![1, 2],
            span_id: vec![3, 4],
            trace_state: "vendor=value".into(),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 1,
        }]))
        .unwrap();
        assert_eq!(links_json[0]["traceState"], "vendor=value");
        assert!(links_json[0]["attributes"].is_array());
    }

    #[test]
    fn extract_http_data_falls_back_to_attributes_for_headers_and_bodies() {
        let mut span = base_span();
        span.attributes.insert(
            "http.request.headers".to_string(),
            "{\"x-a\":\"1\"}".to_string(),
        );
        span.attributes
            .insert("http.request.body".to_string(), "{\"in\":true}".to_string());
        span.attributes.insert(
            "http.response.headers".to_string(),
            "{\"content-type\":\"application/json\"}".to_string(),
        );
        span.attributes.insert(
            "http.response.body".to_string(),
            "{\"ok\":true}".to_string(),
        );

        span.extract_http_data_from_events();

        assert_eq!(
            span.http_request_headers.as_deref(),
            Some("{\"x-a\":\"1\"}")
        );
        assert_eq!(span.http_request_body.as_deref(), Some("{\"in\":true}"));
        assert_eq!(
            span.http_response_headers.as_deref(),
            Some("{\"content-type\":\"application/json\"}")
        );
        assert_eq!(span.http_response_body.as_deref(), Some("{\"ok\":true}"));
    }

    #[test]
    fn extract_http_data_prefers_event_values_over_attribute_fallback() {
        let mut span = base_span();
        span.attributes.insert(
            "http.request.body".to_string(),
            "from-attribute".to_string(),
        );
        span.attributes.insert(
            "http.response.body".to_string(),
            "from-attribute".to_string(),
        );

        let mut request_attrs = HashMap::new();
        request_attrs.insert("http.request.body".to_string(), "from-event".to_string());
        let mut response_attrs = HashMap::new();
        response_attrs.insert("http.response.body".to_string(), "from-event".to_string());
        span.events = vec![
            SpanEvent {
                name: "http.request".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: request_attrs,
            },
            SpanEvent {
                name: "http.response".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: response_attrs,
            },
        ];

        span.extract_http_data_from_events();

        assert_eq!(span.http_request_body.as_deref(), Some("from-event"));
        assert_eq!(span.http_response_body.as_deref(), Some("from-event"));
    }

    #[test]
    fn extract_http_data_accepts_obi_body_content_on_span_attributes() {
        let mut span = base_span();
        span.attributes.insert(
            "http.request.body.content".to_string(),
            "{\"obi-req\":true}".to_string(),
        );
        span.attributes.insert(
            "http.response.body.content".to_string(),
            "{\"obi-res\":1}".to_string(),
        );

        span.extract_http_data_from_events();

        assert_eq!(
            span.http_request_body.as_deref(),
            Some("{\"obi-req\":true}")
        );
        assert_eq!(span.http_response_body.as_deref(), Some("{\"obi-res\":1}"));
    }

    #[test]
    fn extract_http_data_accepts_obi_body_content_on_http_events() {
        let mut span = base_span();
        let mut request_attrs = HashMap::new();
        request_attrs.insert(
            "http.request.body.content".to_string(),
            "{\"from-obi-req\":true}".to_string(),
        );
        let mut response_attrs = HashMap::new();
        response_attrs.insert(
            "http.response.body.content".to_string(),
            "{\"from-obi-res\":2}".to_string(),
        );
        span.events = vec![
            SpanEvent {
                name: "http.request".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: request_attrs,
            },
            SpanEvent {
                name: "http.response".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: response_attrs,
            },
        ];

        span.extract_http_data_from_events();

        assert_eq!(
            span.http_request_body.as_deref(),
            Some("{\"from-obi-req\":true}")
        );
        assert_eq!(
            span.http_response_body.as_deref(),
            Some("{\"from-obi-res\":2}")
        );
    }

    #[test]
    fn extract_http_data_prefers_legacy_body_keys_when_both_conventions_present() {
        let mut span = base_span();
        span.attributes
            .insert("http.request.body".to_string(), "legacy-req".to_string());
        span.attributes.insert(
            "http.request.body.content".to_string(),
            "obi-req".to_string(),
        );
        span.attributes
            .insert("http.response.body".to_string(), "legacy-res".to_string());
        span.attributes.insert(
            "http.response.body.content".to_string(),
            "obi-res".to_string(),
        );

        span.extract_http_data_from_events();

        assert_eq!(span.http_request_body.as_deref(), Some("legacy-req"));
        assert_eq!(span.http_response_body.as_deref(), Some("legacy-res"));

        let mut span2 = base_span();
        let mut request_attrs = HashMap::new();
        request_attrs.insert("http.request.body".to_string(), "legacy-req-ev".to_string());
        request_attrs.insert(
            "http.request.body.content".to_string(),
            "obi-req-ev".to_string(),
        );
        let mut response_attrs = HashMap::new();
        response_attrs.insert(
            "http.response.body".to_string(),
            "legacy-res-ev".to_string(),
        );
        response_attrs.insert(
            "http.response.body.content".to_string(),
            "obi-res-ev".to_string(),
        );
        span2.events = vec![
            SpanEvent {
                name: "http.request".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: request_attrs,
            },
            SpanEvent {
                name: "http.response".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: response_attrs,
            },
        ];

        span2.extract_http_data_from_events();

        assert_eq!(span2.http_request_body.as_deref(), Some("legacy-req-ev"));
        assert_eq!(span2.http_response_body.as_deref(), Some("legacy-res-ev"));
    }
}
