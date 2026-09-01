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
        grouped
            .entry(response_stream_labels(hit))
            .or_default()
            .push(hit);
    }
    let result = grouped
        .into_iter()
        .map(|(labels, entries)| {
            json!({
                "stream": labels_to_object(&labels),
                // Pinned Loki 3.1.1 + Grafana 11.2: values are always [ts, line].
                // Structured metadata is surfaced on the stream object (categorize-labels
                // read path), never as a third tuple element — that shape breaks Grafana's
                // jsoniter parser with ReadArray errors.
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

/// `/loki/api/v1/index/stats` returns a flat object (no `status`/`data` wrapper).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct IndexStats {
    pub streams: u64,
    pub chunks: u64,
    pub entries: u64,
    pub bytes: u64,
}

pub fn compute_index_stats(hits: &[LogHit]) -> IndexStats {
    let mut stream_keys = BTreeMap::<BTreeMap<String, String>, ()>::new();
    let mut entries = 0u64;
    let mut bytes = 0u64;
    for hit in hits {
        stream_keys.insert(response_stream_labels(hit), ());
        entries += 1;
        bytes += hit.line.len() as u64;
    }
    let streams = stream_keys.len() as u64;
    let chunks = streams.max(entries.div_ceil(100));
    IndexStats {
        streams,
        chunks,
        entries,
        bytes,
    }
}

pub fn matrix_response(series: &[MatrixSeries], max_bytes: usize) -> Result<Response, CompatError> {
    let result = series
        .iter()
        .map(|item| {
            json!({
                "metric": labels_to_object(&item.labels),
                "values": item.samples.iter().map(|sample| {
                    json!([ts_secs(sample.timestamp_ns), format_count(sample.value)])
                }).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    success_response(json!({"resultType": "matrix", "result": result}), max_bytes)
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSeries {
    pub labels: BTreeMap<String, String>,
    pub samples: Vec<MatrixSample>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSample {
    pub timestamp_ns: i64,
    pub value: f64,
}

pub(crate) fn response_stream_labels_for_hit(hit: &LogHit) -> BTreeMap<String, String> {
    response_stream_labels(hit)
}

/// WebSocket tail frames use the same stream grouping as query responses but
/// without the Loki HTTP success envelope.
pub fn tail_message(hits: &[LogHit]) -> Value {
    let mut grouped: BTreeMap<BTreeMap<String, String>, Vec<&LogHit>> = BTreeMap::new();
    for hit in hits {
        grouped
            .entry(response_stream_labels(hit))
            .or_default()
            .push(hit);
    }
    json!({
        "streams": grouped.into_iter().map(|(labels, entries)| {
            json!({
                "stream": labels_to_object(&labels),
                "values": entries.into_iter().map(encode_entry).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
        "dropped_entries": [],
    })
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
    json!([hit.timestamp_ns.to_string(), hit.line])
}

/// Merge index stream labels with structured metadata for the response stream
/// object, matching Loki's categorize-labels read projection.
fn response_stream_labels(hit: &LogHit) -> BTreeMap<String, String> {
    let mut stream = hit.labels.clone();
    for (key, value) in &hit.structured_metadata {
        stream.entry(key.clone()).or_insert_with(|| value.clone());
    }
    stream
}

fn labels_to_object(labels: &BTreeMap<String, String>) -> Value {
    Value::Object(
        labels
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect::<Map<String, Value>>(),
    )
}

fn ts_secs(timestamp_ns: i64) -> f64 {
    timestamp_ns as f64 / 1_000_000_000.0
}

fn format_count(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn encodes_two_tuple_values_and_splits_streams_by_structured_metadata() {
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
                structured_metadata: [("request_id".into(), "r2".into())].into_iter().collect(),
            },
        ];
        let response = streams_response(&hits, 10_000).unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).expect("json body");
        let streams = body["data"]["result"].as_array().expect("streams");
        assert_eq!(streams.len(), 2);
        for stream in streams {
            let values = stream["values"][0].as_array().expect("value tuple");
            assert_eq!(values.len(), 2, "values must be [ts, line] only");
        }
        assert_eq!(streams[0]["stream"]["request_id"], "r1");
        assert_eq!(streams[1]["stream"]["request_id"], "r2");
    }

    #[test]
    fn index_stats_counts_streams_entries_and_bytes() {
        let hits = vec![
            LogHit {
                timestamp_ns: 1,
                line: "abc".into(),
                labels: [("service_name".into(), "ad".into())].into_iter().collect(),
                structured_metadata: BTreeMap::new(),
            },
            LogHit {
                timestamp_ns: 2,
                line: "de".into(),
                labels: [("service_name".into(), "ad".into())].into_iter().collect(),
                structured_metadata: BTreeMap::new(),
            },
        ];
        let stats = compute_index_stats(&hits);
        assert_eq!(stats.streams, 1);
        assert_eq!(stats.entries, 2);
        assert_eq!(stats.bytes, 5);
        assert!(stats.chunks >= 1);
    }

    #[test]
    fn tail_message_uses_loki_tail_frame_shape() {
        let hits = vec![LogHit {
            timestamp_ns: 42,
            line: "hello".into(),
            labels: [("service_name".into(), "ad".into())].into_iter().collect(),
            structured_metadata: BTreeMap::new(),
        }];
        let frame = tail_message(&hits);
        assert!(frame.get("streams").is_some());
        assert_eq!(frame["dropped_entries"], json!([]));
        assert_eq!(frame["streams"][0]["values"][0][0], "42");
    }
}
