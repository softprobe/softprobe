//! Build capture JSON from SQL rows (ported from Go `hostedbackend`).

use anyhow::Result;
use base64::Engine;
use chrono::Utc;
use serde_json::{json, Map, Value};

pub fn capture_query_sql(tenant_id: &str, capture_id: &str) -> String {
    let te = tenant_id.replace('\'', "''");
    let ce = capture_id.replace('\'', "''");
    format!(
        "SELECT timestamp, trace_id, span_id, parent_span_id, app_id, tenant_id, message_type, span_kind, http_request_method, http_request_path, http_request_headers, http_request_body, http_response_status_code, http_response_headers, http_response_body, attributes FROM committed_spans WHERE attributes['sp.capture.id'] = '{ce}' AND attributes['sp.tenant.id'] = '{te}' ORDER BY timestamp ASC"
    )
}

pub fn build_capture_json(
    capture_id: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<Vec<u8>> {
    let mut traces: Vec<Value> = Vec::new();
    for row in rows {
        let mut entry = Map::new();
        for (idx, col) in columns.iter().enumerate() {
            if idx < row.len() && !row[idx].is_null() {
                entry.insert(col.clone(), row[idx].clone());
            }
        }
        let trace_id = as_string(entry.get("trace_id"));
        let span_id = as_string(entry.get("span_id"));
        let parent_span_id = as_string(entry.get("parent_span_id"));
        let mut method = as_string(entry.get("http_request_method"));
        let mut path = as_string(entry.get("http_request_path"));
        let mut full_url = path.clone();
        let mut body = as_string(entry.get("http_response_body"));
        let msg_type = as_string(entry.get("message_type"));
        let mut direction = "outbound".to_string();

        let mut status_code: i64 = 0;
        if let Some(v) = entry.get("http_response_status_code") {
            if let Some(n) = v.as_i64() {
                status_code = n;
            } else if let Some(n) = v.as_f64() {
                status_code = n as i64;
            }
        }

        if let Some(Value::Object(attrs)) = entry.get("attributes") {
            method = first_non_empty(
                &method,
                &[
                    map_lookup(attrs, "http.request.method"),
                    map_lookup(attrs, "http.request.header.:method"),
                ],
            );
            path = first_non_empty(
                &path,
                &[
                    map_lookup(attrs, "url.path"),
                    map_lookup(attrs, "http.request.path"),
                    map_lookup(attrs, "http.request.header.:path"),
                ],
            );
            full_url = first_non_empty(&full_url, &[map_lookup(attrs, "url.full"), path.clone()]);
            body = first_non_empty(&body, &[map_lookup(attrs, "http.response.body")]);
            direction = first_non_empty(&direction, &[map_lookup(attrs, "sp.traffic.direction")]);
            if status_code == 0 {
                let s = map_lookup(attrs, "http.response.status_code");
                if let Ok(n) = s.parse::<i64>() {
                    status_code = n;
                }
            }
        }

        if path.is_empty() && msg_type.starts_with('/') {
            path = msg_type.clone();
        }

        let mut attrs: Vec<Value> = vec![
            kv_string("sp.span.type", "extract"),
            kv_string("sp.traffic.direction", &direction),
            kv_string("http.request.method", &method),
            kv_string("url.path", &path),
            kv_string("url.full", &full_url),
            kv_string("http.response.body", &body),
            kv_int("http.response.status_code", status_code),
        ];
        if !msg_type.is_empty() {
            attrs.push(kv_string("message.type", &msg_type));
        }

        let trace = json!({
            "resourceSpans": [{
                "scopeSpans": [{
                    "spans": [{
                        "traceId": hex_to_b64(&trace_id),
                        "spanId": hex_to_b64(&span_id),
                        "parentSpanId": hex_to_b64(&parent_span_id),
                        "name": "sp.extract",
                        "attributes": attrs
                    }]
                }]
            }]
        });
        traces.push(trace);
    }

    let doc = json!({
        "version": "1.0.0",
        "caseId": capture_id,
        "captureId": capture_id,
        "mode": "capture",
        "createdAt": Utc::now().to_rfc3339(),
        "traces": traces
    });
    Ok(serde_json::to_vec_pretty(&doc)?)
}

fn as_string(v: Option<&Value>) -> String {
    v.and_then(|x| x.as_str().map(|s| s.to_string()))
        .unwrap_or_default()
}

fn map_lookup(attrs: &Map<String, Value>, key: &str) -> String {
    for (k, v) in attrs {
        if k.contains(key) {
            if let Some(s) = v.as_str() {
                return s.to_string();
            }
        }
    }
    String::new()
}

fn first_non_empty(primary: &str, alts: &[String]) -> String {
    if !primary.trim().is_empty() {
        return primary.to_string();
    }
    for a in alts {
        if !a.trim().is_empty() {
            return a.clone();
        }
    }
    String::new()
}

fn kv_string(key: &str, val: &str) -> Value {
    json!({
        "key": key,
        "value": { "stringValue": val }
    })
}

fn kv_int(key: &str, val: i64) -> Value {
    json!({
        "key": key,
        "value": { "intValue": val }
    })
}

fn hex_to_b64(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    match hex::decode(s) {
        Ok(b) => base64::engine::general_purpose::STANDARD.encode(b),
        Err(_) => s.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::capture_query_sql;

    #[test]
    fn capture_query_reads_committed_spans_only() {
        let sql = capture_query_sql("tenant-a", "cap-1");
        assert!(sql.contains("FROM committed_spans"));
        assert!(!sql.contains("FROM union_spans"));
    }
}
