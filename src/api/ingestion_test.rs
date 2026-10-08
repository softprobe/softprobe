use super::*;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, Span, Status};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::common::v1::{KeyValue, AnyValue, any_value};
use std::collections::HashMap;

fn create_test_span(
    _trace_id: &str,
    _span_id: &str,
    parent_span_id: Option<&str>,
    name: &str,
    start_time: u64,
    end_time: u64,
) -> Span {
    // Create fixed-length byte arrays for trace and span IDs
    let mut trace_id_bytes = vec![0u8; 16];
    let mut span_id_bytes = vec![0u8; 8];
    let mut parent_span_id_bytes = vec![0u8; 8];

    // Fill with some test data
    trace_id_bytes[0] = 0x12;
    trace_id_bytes[1] = 0x34;
    span_id_bytes[0] = 0x56;
    span_id_bytes[1] = 0x78;
    
    if parent_span_id.is_some() {
        parent_span_id_bytes[0] = 0x9a;
        parent_span_id_bytes[1] = 0xbc;
    } else {
        parent_span_id_bytes = vec![];
    }

    Span {
        trace_id: trace_id_bytes,
        span_id: span_id_bytes,
        parent_span_id: parent_span_id_bytes,
        name: name.to_string(),
        kind: 1, // SPAN_KIND_INTERNAL
        start_time_unix_nano: start_time,
        end_time_unix_nano: end_time,
        attributes: vec![
            KeyValue {
                key: "sp.session.id".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("session_123".to_string())),
                }),
            },
            KeyValue {
                key: "http.method".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("POST".to_string())),
                }),
            },
        ],
        events: vec![],
        status: Some(Status {
            code: 1, // STATUS_CODE_OK
            message: "".to_string(),
        }),
        ..Default::default()
    }
}

fn create_test_resource_attributes() -> HashMap<String, String> {
    let mut attrs = HashMap::new();
    attrs.insert("sp.app.id".to_string(), "test_app".to_string());
    attrs.insert("sp.organization.id".to_string(), "org_123".to_string());
    attrs.insert("service.name".to_string(), "test_service".to_string());
    attrs
}

#[test]
fn test_convert_otlp_span_to_span_data() {
    let span = create_test_span(
        "12345678901234567890123456789012",
        "1234567890123456",
        Some("0987654321098765"),
        "test_operation",
        1640995200000000000, // 2022-01-01 00:00:00 UTC in nanoseconds
        1640995260000000000, // 2022-01-01 00:01:00 UTC in nanoseconds
    );

    let resource_attributes = create_test_resource_attributes();
    
    let result = convert_otlp_span_to_span_data(span, &resource_attributes).unwrap();

    assert_eq!(result.trace_id, "12340000000000000000000000000000");
    assert_eq!(result.span_id, "5678000000000000");
    assert_eq!(result.parent_span_id, Some("9abc000000000000".to_string()));
    assert_eq!(result.app_id, "test_app");
    assert_eq!(result.organization_id, Some("org_123".to_string()));
    assert_eq!(result.message_type, "test_operation");
    
    // Check session_id is extracted from span attributes
    assert_eq!(result.attributes.get("sp.session.id"), Some(&"session_123".to_string()));
    assert_eq!(result.attributes.get("http.method"), Some(&"POST".to_string()));

    // Check timestamps
    assert!(result.timestamp.timestamp_nanos_opt().unwrap() > 0);
    assert!(result.end_timestamp.is_some());
}

#[test]
fn test_convert_otlp_span_fallback_to_service_name() {
    let span = create_test_span(
        "12345678901234567890123456789012",
        "1234567890123456",
        None,
        "test_operation",
        1640995200000000000,
        1640995260000000000,
    );

    let mut resource_attributes = HashMap::new();
    resource_attributes.insert("service.name".to_string(), "fallback_service".to_string());
    
    let result = convert_otlp_span_to_span_data(span, &resource_attributes).unwrap();

    assert_eq!(result.app_id, "fallback_service");
    assert_eq!(result.parent_span_id, None);
}

#[test]
fn test_convert_otlp_span_unknown_app_id() {
    let span = create_test_span(
        "12345678901234567890123456789012",
        "1234567890123456",
        None,
        "test_operation",
        1640995200000000000,
        1640995260000000000,
    );

    let resource_attributes = HashMap::new(); // No app_id or service.name
    
    let result = convert_otlp_span_to_span_data(span, &resource_attributes).unwrap();

    assert_eq!(result.app_id, "unknown");
}

#[test]
fn test_extract_resource_attributes() {
    let resource = Resource {
        attributes: vec![
            KeyValue {
                key: "sp.app.id".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("my_app".to_string())),
                }),
            },
            KeyValue {
                key: "environment".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("production".to_string())),
                }),
            },
            KeyValue {
                key: "instance.count".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(5)),
                }),
            },
        ],
        ..Default::default()
    };

    let resource_spans = ResourceSpans {
        resource: Some(resource),
        scope_spans: vec![],
        schema_url: "".to_string(),
    };

    let attributes = extract_resource_attributes(&resource_spans);

    assert_eq!(attributes.get("sp.app.id"), Some(&"my_app".to_string()));
    assert_eq!(attributes.get("environment"), Some(&"production".to_string()));
    assert_eq!(attributes.get("instance.count"), Some(&"5".to_string()));
}
