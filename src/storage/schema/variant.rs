//! DuckLake VARIANT staging and SQL helpers for shredded attribute columns.
//!
//! Hot MAP columns are staged as JSON (Utf8) in Arrow/Parquet, then cast to
//! `VARIANT` on DuckLake CREATE/INSERT so nested fields can be shredded.

use serde_json::{Map as JsonMap, Number, Value};
use std::collections::HashMap;

/// Attribute keys encoded as JSON integers for stable VARIANT shredding.
pub const VARIANT_INT64_KEYS: &[&str] = &[
    "gen_ai.usage.input_tokens",
    "gen_ai.usage.output_tokens",
    "gen_ai.usage.total_tokens",
];

/// Attribute keys encoded as JSON floats for stable VARIANT shredding.
pub const VARIANT_FLOAT64_KEYS: &[&str] = &["sp.cost.total"];

/// Telemetry columns stored as DuckLake `VARIANT` (staged as JSON Utf8).
pub fn hot_variant_columns(table_name: &str) -> &'static [&'static str] {
    match table_name {
        "traces" => &["attributes"],
        "logs" => &["attributes", "resource_attributes"],
        "metrics" => &["attributes", "resource_attributes"],
        _ => &[],
    }
}

/// Escape a SQL string literal (single quotes only).
pub fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// VARIANT object field as VARCHAR (required for COALESCE / string filters).
pub fn variant_varchar(column: &str, key: &str) -> String {
    format!(
        "CAST({column}['{key}'] AS VARCHAR)",
        column = column,
        key = escape_sql_string(key)
    )
}

/// `try_cast` of a VARIANT object field to a DuckDB type.
pub fn variant_try_cast(column: &str, key: &str, duck_type: &str) -> String {
    format!(
        "try_cast({column}['{key}'] AS {duck_type})",
        column = column,
        key = escape_sql_string(key),
        duck_type = duck_type
    )
}

/// Project a VARIANT column as JSON for API serialization.
pub fn variant_as_json(column: &str) -> String {
    format!("CAST({column} AS JSON) AS {column}")
}

/// DuckDB returns `CAST(... AS JSON)` as text; parse to object/array when possible.
///
/// Leaves non-JSON strings and non-string values unchanged so callers can treat
/// VARIANT projections as nested JSON without double-encoding in HTTP responses.
pub fn parse_projected_json_value(value: Value) -> Value {
    match value {
        Value::String(text) => match serde_json::from_str::<Value>(&text) {
            Ok(parsed @ (Value::Object(_) | Value::Array(_))) => parsed,
            _ => Value::String(text),
        },
        other => other,
    }
}

/// DuckLake SELECT list that casts staged JSON columns to VARIANT.
///
/// Example: `SELECT * REPLACE (attributes::JSON::VARIANT AS attributes) FROM ...`
pub fn parquet_select_with_variant_casts(table_name: &str) -> String {
    let cols = hot_variant_columns(table_name);
    if cols.is_empty() {
        return "SELECT *".to_string();
    }
    let replacements = cols
        .iter()
        .map(|col| format!("{col}::JSON::VARIANT AS {col}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT * REPLACE ({replacements})")
}

/// Encode a string map as a JSON object, applying stable typed shredding for hot keys.
pub fn encode_attributes_json(map: &HashMap<String, String>) -> String {
    let mut obj = JsonMap::new();
    for (key, value) in map {
        obj.insert(key.clone(), typed_json_value(key, value));
    }
    Value::Object(obj).to_string()
}

fn typed_json_value(key: &str, value: &str) -> Value {
    if VARIANT_INT64_KEYS.contains(&key) {
        if let Ok(n) = value.parse::<i64>() {
            return Value::Number(Number::from(n));
        }
    }
    if VARIANT_FLOAT64_KEYS.contains(&key) {
        if let Ok(n) = value.parse::<f64>() {
            if let Some(num) = Number::from_f64(n) {
                return Value::Number(num);
            }
        }
    }
    // Only rehydrate values explicitly tagged by any_value encoding (arrays/kvlists).
    // Plain OTLP StringValues that look like JSON stay strings.
    if let Some(payload) = crate::models::strip_nested_json_prefix(value) {
        match serde_json::from_str::<Value>(payload) {
            Ok(parsed @ (Value::Object(_) | Value::Array(_))) => return parsed,
            Ok(other) => return other,
            Err(_) => return Value::String(value.to_string()),
        }
    }
    Value::String(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_hot_keys_with_stable_json_types() {
        let mut map = HashMap::new();
        map.insert("sp.observation.type".into(), "generation".into());
        map.insert("gen_ai.usage.input_tokens".into(), "42".into());
        map.insert("sp.cost.total".into(), "1.5".into());
        map.insert("sp.user.id".into(), "u1".into());

        let json: Value = serde_json::from_str(&encode_attributes_json(&map)).unwrap();
        assert_eq!(json["sp.observation.type"], "generation");
        assert_eq!(json["gen_ai.usage.input_tokens"], 42);
        assert_eq!(json["sp.cost.total"], 1.5);
        assert_eq!(json["sp.user.id"], "u1");
    }

    #[test]
    fn variant_sql_helpers_cast_nested_fields() {
        assert_eq!(
            variant_varchar("attributes", "sp.user.id"),
            "CAST(attributes['sp.user.id'] AS VARCHAR)"
        );
        assert_eq!(
            variant_try_cast("attributes", "gen_ai.usage.input_tokens", "BIGINT"),
            "try_cast(attributes['gen_ai.usage.input_tokens'] AS BIGINT)"
        );
        assert_eq!(
            parquet_select_with_variant_casts("traces"),
            "SELECT * REPLACE (attributes::JSON::VARIANT AS attributes)"
        );
        assert_eq!(
            parquet_select_with_variant_casts("logs"),
            "SELECT * REPLACE (attributes::JSON::VARIANT AS attributes, resource_attributes::JSON::VARIANT AS resource_attributes)"
        );
        assert_eq!(parquet_select_with_variant_casts("scores"), "SELECT *");
    }

    #[test]
    fn parse_projected_json_value_objects_and_leaves_plain_text() {
        let obj = parse_projected_json_value(Value::String(
            r#"{"logger_name":"agent.transform"}"#.to_string(),
        ));
        assert_eq!(obj["logger_name"], "agent.transform");

        let plain = parse_projected_json_value(Value::String("not-json".to_string()));
        assert_eq!(plain, Value::String("not-json".to_string()));
    }

    #[test]
    fn encode_attributes_json_rehydrates_only_tagged_nested() {
        let mut map = HashMap::new();
        map.insert("tags".into(), r#"sp.json:["a",1]"#.into());
        map.insert("meta".into(), r#"sp.json:{"k":false}"#.into());
        map.insert("plain".into(), "hello".into());
        map.insert("looks_like_json".into(), r#"{"a":1}"#.into());
        let json: Value = serde_json::from_str(&encode_attributes_json(&map)).unwrap();
        assert_eq!(json["tags"], serde_json::json!(["a", 1]));
        assert_eq!(json["meta"], serde_json::json!({"k": false}));
        assert_eq!(json["plain"], "hello");
        assert_eq!(json["looks_like_json"], r#"{"a":1}"#);
    }
}
