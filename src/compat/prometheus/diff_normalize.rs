//! Normalize Prometheus API JSON for differential comparison.
//! Only label map order and float tolerance are relaxed — not semantics.

use serde_json::{Map, Value};

const FLOAT_EPS: f64 = 1e-6;

/// Normalize a Prometheus success response body for comparison.
pub fn normalize_prom_response(mut body: Value) -> Value {
    if let Some(data) = body.get_mut("data") {
        *data = normalize_data(data.clone());
    }
    body
}

fn normalize_data(data: Value) -> Value {
    match data {
        Value::Object(mut obj) => {
            let result_type = obj
                .get("resultType")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Some(result) = obj.remove("result") {
                let normed = match result_type.as_str() {
                    // Scalar/string results are a single [ts, value] pair, not a series list.
                    "scalar" | "string" => normalize_value_pair(result),
                    _ => normalize_result(result),
                };
                obj.insert("result".into(), normed);
            }
            Value::Object(obj)
        }
        other => other,
    }
}

fn normalize_result(result: Value) -> Value {
    match result {
        Value::Array(items) => {
            let mut normed: Vec<Value> =
                items.into_iter().map(normalize_series_or_sample).collect();
            normed.sort_by(|a, b| {
                let ka = canonical_metric_key(a);
                let kb = canonical_metric_key(b);
                ka.cmp(&kb)
            });
            Value::Array(normed)
        }
        other => other,
    }
}

fn normalize_series_or_sample(item: Value) -> Value {
    match item {
        Value::Object(mut obj) => {
            if let Some(metric) = obj.remove("metric") {
                obj.insert("metric".into(), sort_object(metric));
            }
            if let Some(value) = obj.remove("value") {
                obj.insert("value".into(), normalize_value_pair(value));
            }
            if let Some(values) = obj.remove("values") {
                obj.insert("values".into(), normalize_value_pairs(values));
            }
            Value::Object(obj)
        }
        other => other,
    }
}

fn normalize_value_pairs(values: Value) -> Value {
    match values {
        Value::Array(items) => Value::Array(items.into_iter().map(normalize_value_pair).collect()),
        other => other,
    }
}

fn normalize_value_pair(pair: Value) -> Value {
    match pair {
        Value::Array(mut items) if items.len() == 2 => {
            let ts = items.remove(0);
            let val = items.remove(0);
            json_array(vec![normalize_ts(ts), normalize_float(val)])
        }
        other => other,
    }
}

fn normalize_ts(ts: Value) -> Value {
    match ts {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Value::Number(serde_json::Number::from_f64(f.round()).unwrap_or(n))
            } else {
                Value::Number(n)
            }
        }
        Value::String(s) => {
            if let Ok(f) = s.parse::<f64>() {
                Value::Number(
                    serde_json::Number::from_f64(f.round())
                        .unwrap_or_else(|| serde_json::Number::from_f64(0.0).unwrap()),
                )
            } else {
                Value::String(s)
            }
        }
        other => other,
    }
}

fn normalize_float(val: Value) -> Value {
    let as_f = match &val {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    };
    match as_f {
        Some(f) if f.is_finite() => {
            let rounded = (f / FLOAT_EPS).round() * FLOAT_EPS;
            Value::String(format!("{rounded}"))
        }
        Some(f) => Value::String(f.to_string()),
        None => val,
    }
}

fn sort_object(v: Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort();
            let mut out = Map::new();
            for k in keys {
                if let Some(val) = map.get(&k) {
                    out.insert(k, val.clone());
                }
            }
            Value::Object(out)
        }
        other => other,
    }
}

fn canonical_metric_key(item: &Value) -> String {
    item.get("metric")
        .and_then(|m| serde_json::to_string(m).ok())
        .unwrap_or_default()
}

fn json_array(items: Vec<Value>) -> Value {
    Value::Array(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sorts_vector_results_by_metric() {
        let body = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"__name__": "m", "job": "b"}, "value": [1, "2"]},
                    {"metric": {"__name__": "m", "job": "a"}, "value": [1, "2.0000001"]}
                ]
            }
        });
        let n = normalize_prom_response(body);
        let jobs: Vec<_> = n["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["metric"]["job"].as_str().unwrap())
            .collect();
        assert_eq!(jobs, vec!["a", "b"]);
        assert_eq!(n["data"]["result"][0]["value"][1], "2");
    }

    #[test]
    fn normalizes_scalar_result_pair() {
        let body = json!({
            "status": "success",
            "data": {
                "resultType": "scalar",
                "result": [1700003000.0, "12340000.0"]
            }
        });
        let n = normalize_prom_response(body);
        assert_eq!(n["data"]["result"][0], 1700003000.0);
        assert_eq!(n["data"]["result"][1], "12340000");
    }
}
