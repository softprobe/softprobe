use crate::compat::backends::logs::LogHit;
use crate::compat::errors::{CompatError, CompatErrorCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

/// Instant-metric vector response for literal LogQL metric expressions
/// (e.g. Grafana's `vector(1) + vector(1)` datasource health probe).
/// Wire shape matches pinned Loki 3.1.1: timestamp is a JSON *number*, the
/// sample value a JSON *string* (`[4000000000,"2"]`).
pub fn instant_metric_vector_response(
    value: f64,
    timestamp_ns: i64,
    max_bytes: usize,
) -> Result<Response, CompatError> {
    if !value.is_finite() {
        return Err(CompatError::new(
            CompatErrorCode::UnsupportedFeature,
            "metric expression evaluated to a non-finite value",
        ));
    }
    success_response(
        json!({
            "resultType": "vector",
            "result": [{
                "metric": {},
                "value": [timestamp_ns, format!("{value}")]
            }]
        }),
        max_bytes,
    )
}

pub fn streams_response(hits: &[LogHit], max_bytes: usize) -> Result<Response, CompatError> {
    let mut grouped: BTreeMap<BTreeMap<String, String>, Vec<&LogHit>> = BTreeMap::new();
    for hit in hits {
        grouped.entry(hit.labels.clone()).or_default().push(hit);
    }
    let result = grouped
        .into_iter()
        .map(|(labels, entries)| {
            json!({
                "stream": labels_to_object(&labels),
                "values": entries.into_iter().map(encode_entry).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    success_response(
        json!({"resultType": "streams", "result": result}),
        max_bytes,
    )
}

pub fn labels_response(values: &[String], max_bytes: usize) -> Result<Response, CompatError> {
    success_response(json!(values), max_bytes)
}

pub fn series_response(
    series: &[BTreeMap<String, String>],
    max_bytes: usize,
) -> Result<Response, CompatError> {
    let result = series.iter().map(labels_to_object).collect::<Vec<_>>();
    success_response(json!(result), max_bytes)
}

fn success_response(data: Value, max_bytes: usize) -> Result<Response, CompatError> {
    let body = json!({"status": "success", "data": data});
    let bytes = serde_json::to_vec(&body).map_err(|err| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("failed to encode Loki response: {err}"),
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

fn encode_entry(hit: &LogHit) -> Value {
    let timestamp = hit.timestamp_ns.to_string();
    if hit.structured_metadata.is_empty() {
        json!([timestamp, hit.line])
    } else {
        json!([
            timestamp,
            hit.line,
            labels_to_object(&hit.structured_metadata)
        ])
    }
}

fn labels_to_object(labels: &BTreeMap<String, String>) -> Value {
    Value::Object(
        labels
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect::<Map<String, Value>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_nanoseconds_duplicates_and_metadata_without_reordering() {
        let hits = vec![
            LogHit {
                timestamp_ns: 10,
                line: "first".into(),
                labels: [("service_name".into(), "api".into())]
                    .into_iter()
                    .collect(),
                structured_metadata: [("request_id".into(), "r1".into())].into_iter().collect(),
            },
            LogHit {
                timestamp_ns: 10,
                line: "second".into(),
                labels: [("service_name".into(), "api".into())]
                    .into_iter()
                    .collect(),
                structured_metadata: BTreeMap::new(),
            },
        ];
        let response = streams_response(&hits, 10_000).unwrap();
        let body = response.into_body();
        let _ = body;
    }
}
