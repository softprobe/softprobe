//! OTel attribute → Tempo search tags.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

/// Merge resource then span attributes; span wins on collision.
pub fn project_tempo_tags(
    resource: &HashMap<String, String>,
    span_attrs: &HashMap<String, String>,
    additional_attrs: &HashMap<String, String>,
    max_tags: usize,
) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    for (k, v) in resource {
        if !k.is_empty() {
            tags.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in span_attrs {
        if !k.is_empty() {
            tags.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in additional_attrs {
        if !k.is_empty() {
            tags.insert(k.clone(), v.clone());
        }
    }
    if tags.len() <= max_tags {
        return tags;
    }
    tags.into_iter().take(max_tags).collect()
}

/// Flatten link attributes from either the stored object or OTLP array shape.
pub fn project_tempo_link_attributes(links: &[Value]) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    for link in links {
        let Some(values) = link.get("attributes") else {
            continue;
        };
        match values {
            Value::Object(values) => {
                for (key, value) in values {
                    if let Some(value) = scalar_string(value) {
                        attributes.insert(link_key(key), value);
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    let Some(key) = value.get("key").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(value) = value.get("value").and_then(scalar_string) else {
                        continue;
                    };
                    attributes.insert(link_key(key), value);
                }
            }
            _ => {}
        }
    }
    attributes
}

fn link_key(key: &str) -> String {
    if key.starts_with("link.") {
        key.to_string()
    } else {
        format!("link.{key}")
    }
}

fn scalar_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Object(value) => ["stringValue", "intValue", "doubleValue", "boolValue"]
            .iter()
            .find_map(|key| value.get(*key).and_then(scalar_string)),
        Value::Array(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_attrs_win() {
        let mut resource = HashMap::new();
        resource.insert("http.method".into(), "GET".into());
        let mut span = HashMap::new();
        span.insert("http.method".into(), "POST".into());
        let additional = HashMap::new();
        let tags = project_tempo_tags(&resource, &span, &additional, 40);
        assert_eq!(tags.get("http.method").map(String::as_str), Some("POST"));
    }
}
