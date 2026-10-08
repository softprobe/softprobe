//! Prometheus success response envelopes.

use crate::compat::backends::metrics::{MetricMetadata, MetricSeries};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::promql::{EvalResult, InstantSample};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub fn success_response_limited(data: Value, max_bytes: usize) -> Result<Response, CompatError> {
    let body = json!({
        "status": "success",
        "data": data,
    });
    let bytes = serde_json::to_vec(&body).map_err(|e| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("failed to encode prometheus response: {e}"),
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

pub fn labels_data(names: &[String]) -> Value {
    Value::Array(names.iter().cloned().map(Value::String).collect())
}

pub fn label_values_data(values: &[String]) -> Value {
    labels_data(values)
}

pub fn series_data(series: &[BTreeMap<String, String>]) -> Value {
    Value::Array(
        series
            .iter()
            .map(|labels| Value::Object(labels_to_map(labels)))
            .collect(),
    )
}

pub fn metadata_data(items: &[MetricMetadata]) -> Value {
    let mut out = Map::new();
    for item in items {
        let entry = json!({
            "type": item.metric_type,
            "help": item.help,
            "unit": item.unit,
        });
        out.entry(item.metric_name.clone())
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .expect("array")
            .push(entry);
    }
    Value::Object(out)
}

pub fn encode_eval_result(result: EvalResult) -> Value {
    match result {
        EvalResult::Vector(v) => json!({
            "resultType": "vector",
            "result": v.samples.iter().map(encode_instant).collect::<Vec<_>>(),
        }),
        EvalResult::Matrix(m) => json!({
            "resultType": "matrix",
            "result": m.series.iter().map(encode_series).collect::<Vec<_>>(),
        }),
        EvalResult::Scalar {
            timestamp_ms,
            value,
        } => json!({
            "resultType": "scalar",
            "result": [ts_secs(timestamp_ms), format_float(value)],
        }),
    }
}

fn encode_instant(s: &InstantSample) -> Value {
    json!({
        "metric": labels_to_map(&s.labels),
        "value": [ts_secs(s.timestamp_ms), format_float(s.value)],
    })
}

fn encode_series(s: &MetricSeries) -> Value {
    json!({
        "metric": labels_to_map(&s.labels),
        "values": s.samples.iter().map(|sm| {
            json!([ts_secs(sm.timestamp_ms), format_float(sm.value)])
        }).collect::<Vec<_>>(),
    })
}

fn labels_to_map(labels: &BTreeMap<String, String>) -> Map<String, Value> {
    labels
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect()
}

fn ts_secs(ms: i64) -> Value {
    // Prometheus uses float unix seconds.
    let secs = ms as f64 / 1000.0;
    json!(secs)
}

fn format_float(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".into()
        } else {
            "-Inf".into()
        }
    } else {
        // Stable string form Prometheus clients accept.
        let s = format!("{v}");
        if s.contains('.') || s.contains('e') || s.contains('E') {
            s
        } else {
            format!("{v}.0")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_envelope_is_string_array() {
        let data = labels_data(&["__name__".into(), "job".into()]);
        assert_eq!(data, json!(["__name__", "job"]));
    }

    #[test]
    fn success_response_limited_enforces_max_bytes() {
        let data = json!({"x": "y"});
        assert!(success_response_limited(data.clone(), 10_000).is_ok());
        let err = success_response_limited(data, 8).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::LimitExceeded);
        assert!(err.message.contains("max_response_bytes"));
    }

    #[test]
    fn metadata_groups_by_name() {
        let data = metadata_data(&[MetricMetadata {
            metric_name: "up".into(),
            metric_type: "gauge".into(),
            help: "up help".into(),
            unit: "".into(),
        }]);
        assert_eq!(data["up"][0]["type"], "gauge");
        assert_eq!(data["up"][0]["help"], "up help");
    }
}
