//! Shared OTLP `AnyValue` → stored-string encoding for attribute maps.
//!
//! Scalars use stable string forms. Arrays and kvlists become tagged compact
//! JSON (`sp.json:…`) so DuckLake VARIANT staging can rehydrate nested structure
//! without confusing OTLP StringValues that happen to look like JSON. Bytes are
//! stored as standard base64.
//!
//! Nested children that cannot be encoded are stored as JSON `null` (never
//! silently omitted from arrays/objects).

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use std::collections::HashMap;

/// Prefix for nested JSON stored in `HashMap<String, String>` attribute maps.
pub const NESTED_JSON_PREFIX: &str = "sp.json:";

/// Encode an OTLP AnyValue for `HashMap<String, String>` attribute maps.
pub fn any_value_to_stored_string(value: &AnyValue) -> Option<String> {
    match value.value.as_ref()? {
        any_value::Value::StringValue(s) => Some(s.clone()),
        any_value::Value::IntValue(i) => Some(i.to_string()),
        any_value::Value::DoubleValue(d) => Some(d.to_string()),
        any_value::Value::BoolValue(b) => Some(b.to_string()),
        any_value::Value::BytesValue(bytes) => Some(BASE64.encode(bytes)),
        any_value::Value::ArrayValue(_) | any_value::Value::KvlistValue(_) => {
            let json = any_value_to_json(value)?;
            Some(format!("{NESTED_JSON_PREFIX}{json}"))
        }
    }
}

/// Convert OTLP AnyValue to JSON (used for nested fidelity and exemplars).
pub fn any_value_to_json(value: &AnyValue) -> Option<JsonValue> {
    match value.value.as_ref()? {
        any_value::Value::StringValue(s) => Some(JsonValue::String(s.clone())),
        any_value::Value::BoolValue(b) => Some(JsonValue::Bool(*b)),
        any_value::Value::IntValue(i) => Some(JsonValue::Number(Number::from(*i))),
        any_value::Value::DoubleValue(d) => match Number::from_f64(*d) {
            Some(n) => Some(JsonValue::Number(n)),
            // Preserve non-finite doubles as strings rather than dropping them.
            None => Some(JsonValue::String(d.to_string())),
        },
        any_value::Value::BytesValue(bytes) => Some(JsonValue::String(BASE64.encode(bytes))),
        any_value::Value::ArrayValue(arr) => {
            let items: Vec<JsonValue> = arr
                .values
                .iter()
                .map(|v| any_value_to_json(v).unwrap_or(JsonValue::Null))
                .collect();
            Some(JsonValue::Array(items))
        }
        any_value::Value::KvlistValue(kvlist) => {
            let mut obj = JsonMap::new();
            for kv in &kvlist.values {
                if kv.key.is_empty() {
                    continue;
                }
                let v = kv
                    .value
                    .as_ref()
                    .and_then(any_value_to_json)
                    .unwrap_or(JsonValue::Null);
                obj.insert(kv.key.clone(), v);
            }
            Some(JsonValue::Object(obj))
        }
    }
}

/// Convert a list of OTLP key/values into the canonical attribute map.
pub fn key_values_to_map(attrs: &[KeyValue]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for attr in attrs {
        if attr.key.is_empty() {
            continue;
        }
        if let Some(value) = attr.value.as_ref().and_then(any_value_to_stored_string) {
            out.insert(attr.key.clone(), value);
        }
    }
    out
}

/// Strip the nested-JSON tag if present; returns `(is_tagged, payload)`.
pub fn strip_nested_json_prefix(value: &str) -> Option<&str> {
    value.strip_prefix(NESTED_JSON_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};

    fn av(v: any_value::Value) -> AnyValue {
        AnyValue { value: Some(v) }
    }

    #[test]
    fn scalars_use_stable_string_forms() {
        assert_eq!(
            any_value_to_stored_string(&av(any_value::Value::StringValue("x".into()))).as_deref(),
            Some("x")
        );
        assert_eq!(
            any_value_to_stored_string(&av(any_value::Value::IntValue(42))).as_deref(),
            Some("42")
        );
        assert_eq!(
            any_value_to_stored_string(&av(any_value::Value::BoolValue(true))).as_deref(),
            Some("true")
        );
        assert_eq!(
            any_value_to_stored_string(&av(any_value::Value::DoubleValue(1.5))).as_deref(),
            Some("1.5")
        );
    }

    #[test]
    fn arrays_and_kvlists_are_tagged_compact_json() {
        let arr = av(any_value::Value::ArrayValue(ArrayValue {
            values: vec![
                av(any_value::Value::StringValue("a".into())),
                av(any_value::Value::IntValue(1)),
            ],
        }));
        let s = any_value_to_stored_string(&arr).unwrap();
        assert!(s.starts_with(NESTED_JSON_PREFIX));
        let parsed: JsonValue =
            serde_json::from_str(strip_nested_json_prefix(&s).unwrap()).unwrap();
        assert_eq!(parsed, serde_json::json!(["a", 1]));

        let map = av(any_value::Value::KvlistValue(KeyValueList {
            values: vec![KeyValue {
                key: "k".into(),
                value: Some(av(any_value::Value::BoolValue(false))),
            }],
        }));
        let s = any_value_to_stored_string(&map).unwrap();
        let parsed: JsonValue =
            serde_json::from_str(strip_nested_json_prefix(&s).unwrap()).unwrap();
        assert_eq!(parsed, serde_json::json!({"k": false}));
    }

    #[test]
    fn string_looking_like_json_is_not_tagged() {
        let s = any_value_to_stored_string(&av(any_value::Value::StringValue(r#"{"a":1}"#.into())))
            .unwrap();
        assert_eq!(s, r#"{"a":1}"#);
        assert!(strip_nested_json_prefix(&s).is_none());
    }

    #[test]
    fn nested_empty_any_value_becomes_null_not_omitted() {
        let arr = av(any_value::Value::ArrayValue(ArrayValue {
            values: vec![AnyValue { value: None }, av(any_value::Value::IntValue(1))],
        }));
        let json = any_value_to_json(&arr).unwrap();
        assert_eq!(json, serde_json::json!([null, 1]));
    }

    #[test]
    fn bytes_are_base64() {
        let s =
            any_value_to_stored_string(&av(any_value::Value::BytesValue(vec![0, 1, 2]))).unwrap();
        assert_eq!(s, BASE64.encode([0, 1, 2]));
    }

    #[test]
    fn key_values_to_map_preserves_nested() {
        let attrs = vec![KeyValue {
            key: "tags".into(),
            value: Some(av(any_value::Value::ArrayValue(ArrayValue {
                values: vec![av(any_value::Value::StringValue("x".into()))],
            }))),
        }];
        let map = key_values_to_map(&attrs);
        assert_eq!(
            map.get("tags").map(String::as_str),
            Some(r#"sp.json:["x"]"#)
        );
    }
}
