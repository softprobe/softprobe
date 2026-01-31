use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    any_value, AnyValue, InstrumentationScope, KeyValue,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    span, ResourceSpans, ScopeSpans, Span, Status,
};

pub fn create_test_otlp_request() -> ExportTraceServiceRequest {
    let span = Span {
        trace_id: vec![
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55,
            0x66, 0x77, 0x88,
        ],
        span_id: vec![0x21, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
        parent_span_id: vec![],
        name: "test_operation".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: 1_640_995_200_000_000_000,
        end_time_unix_nano: 1_640_995_260_000_000_000,
        attributes: vec![KeyValue {
            key: "sp.session.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "test_session_123".to_string(),
                )),
            }),
        }],
        events: vec![],
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        ..Default::default()
    };

    let scope = ScopeSpans {
        scope: Some(InstrumentationScope {
            name: "softprobe.test".to_string(),
            version: "1.0.0".to_string(),
            ..Default::default()
        }),
        spans: vec![span],
        schema_url: String::new(),
    };

    let resource = Resource {
        attributes: vec![KeyValue {
            key: "sp.app.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "test_application".to_string(),
                )),
            }),
        }],
        dropped_attributes_count: 0,
    };

    let resource_spans = ResourceSpans {
        resource: Some(resource),
        scope_spans: vec![scope],
        schema_url: String::new(),
    };

    ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    }
}
