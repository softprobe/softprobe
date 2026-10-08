use crate::compat::backends::traces::{TraceAttribute, TraceData, TraceSearchHit, TraceSpan};
use crate::compat::errors::CompatError;
use crate::compat::errors::CompatErrorCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub fn trace_v1_response(data: &TraceData, max_bytes: usize) -> Result<Response, CompatError> {
    response(json!({"batches": batches(data)?}), max_bytes)
}

pub fn trace_v2_response(data: &TraceData, max_bytes: usize) -> Result<Response, CompatError> {
    response(
        json!({"trace": {"resourceSpans": resource_spans(data)?}}),
        max_bytes,
    )
}

pub fn search_response(hits: &[TraceSearchHit], max_bytes: usize) -> Result<Response, CompatError> {
    let traces = hits
        .iter()
        .map(|hit| {
            json!({
                "traceID": hit.trace_id,
                "rootServiceName": hit.root_service_name,
                "rootTraceName": hit.root_trace_name,
                "startTimeUnixNano": hit.start_time_unix_nano.to_string(),
                "durationMs": hit.duration_ms,
            })
        })
        .collect::<Vec<_>>();
    response(json!({"traces": traces}), max_bytes)
}

pub fn tag_names_response(values: &[String], max_bytes: usize) -> Result<Response, CompatError> {
    response(json!({"tagNames": values}), max_bytes)
}

pub fn tag_values_response(values: &[String], max_bytes: usize) -> Result<Response, CompatError> {
    response(json!({"tagValues": values}), max_bytes)
}

fn response(body: Value, max_bytes: usize) -> Result<Response, CompatError> {
    let bytes = serde_json::to_vec(&body).map_err(|err| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("failed to encode Tempo response: {err}"),
        )
    })?;
    if bytes.len() > max_bytes {
        return Err(CompatError::new(
            CompatErrorCode::LimitExceeded,
            format!(
                "response size {} exceeds max_response_bytes {max_bytes}",
                bytes.len()
            ),
        ));
    }
    Ok(Json(body).into_response())
}

fn batches(data: &TraceData) -> Result<Vec<Value>, CompatError> {
    resource_groups(data)
        .into_iter()
        .map(|(resource_attributes, scopes)| {
            Ok(json!({
                "resource": {"attributes": attributes(&resource_attributes)},
                "scopeSpans": scope_groups(&scopes)?,
            }))
        })
        .collect()
}

fn resource_spans(data: &TraceData) -> Result<Vec<Value>, CompatError> {
    resource_groups(data)
        .into_iter()
        .map(|(resource_attributes, scopes)| {
            Ok(json!({
                "resource": {"attributes": attributes(&resource_attributes)},
                "scopeSpans": scope_groups(&scopes)?,
            }))
        })
        .collect()
}

fn resource_groups<'a>(data: &'a TraceData) -> Vec<ResourceGroup<'a>> {
    let mut groups = BTreeMap::<String, ResourceGroup<'_>>::new();
    for span in &data.spans {
        let resource_attributes = resource_attributes(span);
        let resource_key = resource_attributes
            .iter()
            .map(|attribute| format!("{}={}", attribute.key, attribute.value))
            .collect::<Vec<_>>()
            .join("\u{1f}");
        let scope_value = span
            .instrumentation_scope
            .clone()
            .unwrap_or_else(|| json!({}));
        let scope_key = serde_json::to_string(&scope_value).unwrap_or_default();
        let entry = groups
            .entry(resource_key)
            .or_insert_with(|| (resource_attributes, BTreeMap::new()));
        entry
            .1
            .entry(scope_key)
            .or_insert_with(|| (scope_value, Vec::new()))
            .1
            .push(span);
    }
    groups.into_values().collect()
}

fn scope_groups(scopes: &ScopeGroups<'_>) -> Result<Vec<Value>, CompatError> {
    scopes
        .values()
        .map(|(scope, spans)| {
            Ok(json!({
                "scope": scope,
                "spans": spans
                    .iter()
                    .map(|item| span(item))
                    .collect::<Result<Vec<_>, _>>()?
            }))
        })
        .collect()
}

type ScopeGroups<'a> = BTreeMap<String, (Value, Vec<&'a TraceSpan>)>;
type ResourceGroup<'a> = (Vec<TraceAttribute>, ScopeGroups<'a>);

fn resource_attributes(span: &TraceSpan) -> Vec<TraceAttribute> {
    let mut values = span.resource_attributes.clone();
    if let Some(service) = &span.service_name {
        if !values
            .iter()
            .any(|attribute| attribute.key == "service.name")
        {
            values.push(TraceAttribute {
                key: "service.name".into(),
                value: service.clone(),
            });
        }
    }
    values.sort_by(|left, right| left.key.cmp(&right.key));
    values
}

fn span(span: &crate::compat::backends::traces::TraceSpan) -> Result<Value, CompatError> {
    let mut value = Map::new();
    value.insert(
        "traceId".into(),
        Value::String(wire_id(&span.trace_id, 16, "traceId")?),
    );
    value.insert(
        "spanId".into(),
        Value::String(wire_id(&span.span_id, 8, "spanId")?),
    );
    if let Some(parent) = &span.parent_span_id {
        value.insert(
            "parentSpanId".into(),
            Value::String(wire_id(parent, 8, "parentSpanId")?),
        );
    }
    value.insert("name".into(), Value::String(span.name.clone()));
    if let Some(kind) = &span.kind {
        value.insert("kind".into(), Value::String(wire_span_kind(kind)));
    }
    value.insert(
        "startTimeUnixNano".into(),
        Value::String(span.start_time_unix_nano.to_string()),
    );
    if let Some(end) = span.end_time_unix_nano {
        value.insert("endTimeUnixNano".into(), Value::String(end.to_string()));
    }
    value.insert("attributes".into(), attributes(&span.attributes));
    if !span.events.is_empty() {
        value.insert(
            "events".into(),
            json!(span
                .events
                .iter()
                .map(|event| json!({
                    "name": event.name,
                    "timeUnixNano": event.timestamp_unix_nano.to_string(),
                    "attributes": attributes(&event.attributes),
                }))
                .collect::<Vec<_>>()),
        );
    }
    if !span.links.is_empty() {
        value.insert(
            "links".into(),
            Value::Array(
                span.links
                    .iter()
                    .map(wire_link)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        );
    }
    if span.status_code.is_some() || span.status_message.is_some() {
        let mut status = Map::new();
        if let Some(code) = &span.status_code {
            status.insert("code".into(), Value::String(wire_status_code(code)));
        }
        if let Some(message) = &span.status_message {
            status.insert("message".into(), Value::String(message.clone()));
        }
        value.insert("status".into(), Value::Object(status));
    }
    Ok(Value::Object(value))
}

fn wire_id(value: &str, expected_len: usize, field: &str) -> Result<String, CompatError> {
    let bytes = hex::decode(value).map_err(|_| malformed_id(field))?;
    if bytes.len() != expected_len {
        return Err(malformed_id(field));
    }
    Ok(BASE64.encode(bytes))
}

fn wire_span_kind(value: &str) -> String {
    match value.trim().to_ascii_uppercase().as_str() {
        "INTERNAL" | "SPAN_KIND_INTERNAL" => "SPAN_KIND_INTERNAL",
        "SERVER" | "SPAN_KIND_SERVER" => "SPAN_KIND_SERVER",
        "CLIENT" | "SPAN_KIND_CLIENT" => "SPAN_KIND_CLIENT",
        "PRODUCER" | "SPAN_KIND_PRODUCER" => "SPAN_KIND_PRODUCER",
        "CONSUMER" | "SPAN_KIND_CONSUMER" => "SPAN_KIND_CONSUMER",
        "UNSPECIFIED" | "SPAN_KIND_UNSPECIFIED" => "SPAN_KIND_UNSPECIFIED",
        other => other,
    }
    .to_string()
}

fn wire_status_code(value: &str) -> String {
    match value.trim().to_ascii_uppercase().as_str() {
        "OK" | "STATUS_CODE_OK" => "STATUS_CODE_OK",
        "ERROR" | "STATUS_CODE_ERROR" => "STATUS_CODE_ERROR",
        "UNSET" | "STATUS_CODE_UNSET" => "STATUS_CODE_UNSET",
        other => other,
    }
    .to_string()
}

fn wire_link(value: &Value) -> Result<Value, CompatError> {
    let Value::Object(object) = value else {
        return Err(malformed_id("link"));
    };
    let mut output = object.clone();
    let trace_id = object
        .get("traceId")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed_id("link.traceId"))?;
    let span_id = object
        .get("spanId")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed_id("link.spanId"))?;
    output.insert(
        "traceId".into(),
        Value::String(wire_id(trace_id, 16, "link.traceId")?),
    );
    output.insert(
        "spanId".into(),
        Value::String(wire_id(span_id, 8, "link.spanId")?),
    );
    Ok(Value::Object(output))
}

fn malformed_id(field: &str) -> CompatError {
    CompatError::new(
        CompatErrorCode::BadRequest,
        format!(
            "{field} must be exactly {} hexadecimal bytes",
            if field.contains("span") { 8 } else { 16 }
        ),
    )
}

fn attributes(values: &[TraceAttribute]) -> Value {
    json!(values
        .iter()
        .map(|attribute| json!({"key": attribute.key, "value": {"stringValue": attribute.value}}))
        .collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::backends::traces::{TraceEvent, TraceSpan};
    use crate::compat::errors::CompatErrorCode;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn encodes_v2_otlp_shape_with_nanoseconds_events_and_status() {
        let data = TraceData {
            spans: vec![TraceSpan {
                trace_id: "000102030405060708090a0b0c0d0e0f".into(),
                span_id: "0001020304050607".into(),
                parent_span_id: None,
                name: "root".into(),
                kind: Some("SPAN_KIND_SERVER".into()),
                start_time_unix_nano: 1_700_000_000_123_456_789,
                end_time_unix_nano: Some(1_700_000_000_223_456_789),
                attributes: vec![TraceAttribute {
                    key: "k".into(),
                    value: "v".into(),
                }],
                status_code: Some("STATUS_CODE_ERROR".into()),
                status_message: Some("boom".into()),
                events: vec![TraceEvent {
                    name: "exception".into(),
                    timestamp_unix_nano: 1_700_000_000_123_456_790,
                    attributes: vec![],
                }],
                service_name: Some("api".into()),
                resource_attributes: vec![TraceAttribute {
                    key: "deployment.environment".into(),
                    value: "prod".into(),
                }],
                instrumentation_scope: Some(json!({"name": "otel-rust", "version": "1.0"})),
                links: vec![json!({
                    "traceId": "101112131415161718191a1b1c1d1e1f",
                    "spanId": "1011121314151617"
                })],
            }],
        };
        let response = trace_v2_response(&data, 100_000).expect("response");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["startTimeUnixNano"],
            "1700000000123456789"
        );
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["events"][0]
                ["timeUnixNano"],
            "1700000000123456790"
        );
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["status"]["message"],
            "boom"
        );
        assert_eq!(
            json["trace"]["resourceSpans"][0]["resource"]["attributes"][1]["key"],
            "service.name"
        );
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["scope"]["name"],
            "otel-rust"
        );
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["links"][0]["traceId"],
            "EBESExQVFhcYGRobHB0eHw=="
        );
    }

    #[tokio::test]
    async fn encodes_ids_enums_and_distinct_resource_scope_groups() {
        let span = |trace_id: &str, span_id: &str, resource: &str, scope: &str| TraceSpan {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            parent_span_id: None,
            name: scope.into(),
            kind: Some("SERVER".into()),
            start_time_unix_nano: 10,
            end_time_unix_nano: Some(20),
            attributes: vec![],
            status_code: Some("ERROR".into()),
            status_message: None,
            events: vec![],
            service_name: Some(resource.into()),
            resource_attributes: vec![],
            instrumentation_scope: Some(json!({"name": scope})),
            links: vec![json!({
                "traceId": "000102030405060708090a0b0c0d0e0f",
                "spanId": "0001020304050607"
            })],
        };
        let data = TraceData {
            spans: vec![
                span(
                    "000102030405060708090a0b0c0d0e0f",
                    "0001020304050607",
                    "api",
                    "scope-a",
                ),
                span(
                    "101112131415161718191a1b1c1d1e1f",
                    "1011121314151617",
                    "worker",
                    "scope-b",
                ),
            ],
        };
        let response = trace_v2_response(&data, 100_000).expect("response");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["trace"]["resourceSpans"].as_array().unwrap().len(), 2);
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"],
            "AAECAwQFBgcICQoLDA0ODw=="
        );
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["spanId"],
            "AAECAwQFBgc="
        );
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["kind"],
            "SPAN_KIND_SERVER"
        );
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["status"]["code"],
            "STATUS_CODE_ERROR"
        );
        assert_eq!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["links"][0]["traceId"],
            "AAECAwQFBgcICQoLDA0ODw=="
        );
    }

    #[test]
    fn malformed_otlp_ids_return_an_explicit_error() {
        let err = wire_id("not-an-id", 16, "traceId").unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
        let err = wire_id("00010203", 16, "traceId").unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);

        let response_error = trace_v2_response(
            &TraceData {
                spans: vec![TraceSpan {
                    trace_id: "not-an-id".into(),
                    span_id: "0001020304050607".into(),
                    parent_span_id: None,
                    name: "invalid".into(),
                    kind: None,
                    start_time_unix_nano: 1,
                    end_time_unix_nano: None,
                    attributes: vec![],
                    status_code: None,
                    status_message: None,
                    events: vec![],
                    service_name: None,
                    resource_attributes: vec![],
                    instrumentation_scope: None,
                    links: vec![],
                }],
            },
            100_000,
        )
        .unwrap_err();
        assert_eq!(response_error.code, CompatErrorCode::BadRequest);
    }

    #[test]
    fn malformed_link_ids_return_an_explicit_error() {
        let err = wire_link(&json!({"traceId": "bad", "spanId": "bad"})).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }
}
