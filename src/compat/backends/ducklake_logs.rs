//! Typed DuckLake log query backend for the Loki adapter.

use crate::compat::backends::logs::{
    LogDirection, LogHit, LogLineFilter, LogParser, LogsDiscoveryRequest, LogsQueryBackend,
    LogsQueryRequest,
};
use crate::compat::backends::metrics::{labels_match, labels_match_any};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::projection::loki::{project_loki, DEFAULT_STREAM_LABEL_ALLOWLIST};
use crate::compat::tenant::TenantContext;
use crate::query::duckdb::QueryResult;
use crate::query::QueryEngine;
use crate::storage::schema::variant::variant_json_to_string_map;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

pub struct DuckLakeLogsBackend {
    query: Arc<QueryEngine>,
}

impl DuckLakeLogsBackend {
    pub fn new(query: Arc<QueryEngine>) -> Self {
        Self { query }
    }

    async fn execute(&self, ctx: &TenantContext, sql: &str) -> Result<QueryResult, CompatError> {
        if ctx.remaining().is_zero() {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                "query deadline exceeded",
            ));
        }
        match tokio::time::timeout(ctx.remaining(), self.query.execute_query(sql)).await {
            Err(_) => Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                "query deadline exceeded",
            )),
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => {
                let message = err.to_string();
                if message.contains("Table with name logs does not exist")
                    || message.contains("Table with name tm_all_log does not exist")
                    || message.contains("Table with name tm_cq_log does not exist")
                {
                    Ok(QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        row_count: 0,
                    })
                } else {
                    Err(CompatError::new(
                        CompatErrorCode::BadRequest,
                        format!("logs query failed: {message}"),
                    ))
                }
            }
        }
    }

    fn sql_window(start_ns: Option<i64>, end_ns: Option<i64>) -> String {
        // Loki query bounds and LogHit timestamps are both Unix nanoseconds.
        let mut clauses = Vec::new();
        if let Some(start) = start_ns {
            clauses.push(format!("epoch_ns(timestamp) >= {start}"));
        }
        if let Some(end) = end_ns {
            clauses.push(format!("epoch_ns(timestamp) < {end}"));
        }
        if clauses.is_empty() {
            String::new()
        } else {
            format!(" AND {}", clauses.join(" AND "))
        }
    }

    async fn scan(
        &self,
        ctx: &TenantContext,
        start_ns: Option<i64>,
        end_ns: Option<i64>,
    ) -> Result<Vec<RawLogRow>, CompatError> {
        ctx.limits.validate_time_range_ms(
            start_ns.map(|value| value / 1_000_000),
            end_ns.map(|value| value / 1_000_000),
        )?;
        let cap = ctx.limits.max_series.saturating_mul(100).max(10_000);
        let sql = format!(
            "SELECT CAST(epoch_ns(timestamp) AS BIGINT) AS timestamp_ns, body, \
             CAST(attributes AS JSON) AS attributes, \
             CAST(resource_attributes AS JSON) AS resource_attributes \
             FROM union_logs WHERE 1=1{} ORDER BY timestamp ASC LIMIT {}",
            Self::sql_window(start_ns, end_ns),
            cap.saturating_add(1)
        );
        let result = self.execute(ctx, &sql).await?;
        enforce_scan_cap(&result, cap)?;
        Ok(parse_rows(&result))
    }

    fn apply_row(
        row: RawLogRow,
        request: &LogsQueryRequest,
    ) -> Result<Option<LogHit>, CompatError> {
        let projection = project_loki(
            &row.resource,
            &row.attributes,
            DEFAULT_STREAM_LABEL_ALLOWLIST,
        );
        if !labels_match(&projection.stream_labels, &request.matchers)? {
            return Ok(None);
        }
        if !line_matches(&row.body, &request.line_filters)? {
            return Ok(None);
        }
        let explicit_parser = request.parser.is_some();
        // Parser errors are per-entry: skip malformed JSON/logfmt and continue
        // deterministically with the remaining entries in the query.
        let parsed = match request.parser {
            Some(parser) => match parse_fields(&row.body, parser) {
                Ok(fields) => Some(fields),
                Err(_) => return Ok(None),
            },
            None => parse_unparsed_fields(&row.body),
        };
        if let Some(fields) = parsed.as_ref() {
            let fields = normalized_fields(fields);
            if !labels_match(&fields, &request.parsed_filters)? {
                return Ok(None);
            }
            if let Some(field) = request.unwrap.as_deref() {
                if fields
                    .get(field)
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_none()
                {
                    return Ok(None);
                }
            }
        } else if request.unwrap.is_some() {
            return Err(CompatError::new(
                CompatErrorCode::UnsupportedFeature,
                "unwrap requires json or logfmt parser",
            ));
        }

        let parsed_query = parsed.is_some();
        let structured_metadata = projection.structured_metadata;
        let mut labels = projection.stream_labels;
        if parsed_query {
            for (key, value) in normalized_fields(&structured_metadata) {
                // Stream labels retain precedence over original metadata, which
                // retains precedence over extracted fields.
                labels.entry(key).or_insert(value);
            }
        }
        if let Some(fields) = parsed.as_ref() {
            let fields = normalized_fields(fields);
            if explicit_parser {
                for (key, value) in &fields {
                    // Original metadata retains precedence when an extracted field collides.
                    labels.entry(key.clone()).or_insert_with(|| value.clone());
                }
            }
            if let Some(level) = fields.get("level") {
                // Match Loki's parsed-log level detection without overriding an
                // explicitly projected stream, metadata, or parsed label.
                labels
                    .entry("detected_level".into())
                    .or_insert_with(|| level.clone());
            }
        }
        Ok(Some(LogHit {
            timestamp_ns: row.timestamp_ns,
            line: row.body,
            labels,
            structured_metadata: if parsed_query {
                BTreeMap::new()
            } else {
                structured_metadata
            },
        }))
    }

    async fn discovery_rows(
        &self,
        ctx: &TenantContext,
        request: &LogsDiscoveryRequest,
    ) -> Result<Vec<LogHit>, CompatError> {
        let rows = self.scan(ctx, request.start_ns, request.end_ns).await?;
        let hits = rows
            .into_iter()
            .filter_map(|row| {
                let projection = project_loki(
                    &row.resource,
                    &row.attributes,
                    DEFAULT_STREAM_LABEL_ALLOWLIST,
                );
                match labels_match_any(&projection.stream_labels, &request.matchers) {
                    Ok(true) => Some(Ok(LogHit {
                        timestamp_ns: row.timestamp_ns,
                        line: row.body,
                        labels: projection.stream_labels,
                        structured_metadata: projection.structured_metadata,
                    })),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        enforce_stream_cap(&hits, ctx.limits.max_series)?;
        Ok(hits)
    }
}

#[async_trait]
impl LogsQueryBackend for DuckLakeLogsBackend {
    async fn query_range(
        &self,
        ctx: &TenantContext,
        request: LogsQueryRequest,
    ) -> Result<Vec<LogHit>, CompatError> {
        let mut hits = self
            .scan(ctx, request.start_ns, request.end_ns)
            .await?
            .into_iter()
            .map(|row| Self::apply_row(row, &request))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        enforce_stream_cap(&hits, ctx.limits.max_series)?;
        hits.sort_by(|a, b| a.timestamp_ns.cmp(&b.timestamp_ns));
        if request.direction == LogDirection::Backward {
            hits.reverse();
        }
        hits.truncate(request.limit);
        Ok(hits)
    }

    async fn label_names(
        &self,
        ctx: &TenantContext,
        request: LogsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError> {
        let mut names = BTreeSet::new();
        for hit in self.discovery_rows(ctx, &request).await? {
            names.extend(hit.labels.into_keys());
        }
        Ok(names.into_iter().collect())
    }

    async fn label_values(
        &self,
        ctx: &TenantContext,
        name: &str,
        request: LogsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError> {
        let mut values = BTreeSet::new();
        for hit in self.discovery_rows(ctx, &request).await? {
            if let Some(value) = hit.labels.get(name) {
                values.insert(value.clone());
            }
        }
        Ok(values.into_iter().collect())
    }

    async fn series(
        &self,
        ctx: &TenantContext,
        request: LogsDiscoveryRequest,
    ) -> Result<Vec<BTreeMap<String, String>>, CompatError> {
        let mut series = BTreeSet::new();
        for hit in self.discovery_rows(ctx, &request).await? {
            series.insert(hit.labels);
        }
        Ok(series.into_iter().collect())
    }
}

#[derive(Debug)]
struct RawLogRow {
    timestamp_ns: i64,
    body: String,
    attributes: HashMap<String, String>,
    resource: HashMap<String, String>,
}

fn parse_rows(result: &QueryResult) -> Vec<RawLogRow> {
    let idx = |name: &str| result.columns.iter().position(|column| column == name);
    let (Some(timestamp), Some(body), attributes, resource) = (
        idx("timestamp_ns"),
        idx("body"),
        idx("attributes"),
        idx("resource_attributes"),
    ) else {
        return Vec::new();
    };
    result
        .rows
        .iter()
        .filter_map(|row| {
            Some(RawLogRow {
                timestamp_ns: cell_i64(row.get(timestamp)?)?,
                body: cell_str(row.get(body)?).unwrap_or_default(),
                attributes: attributes
                    .and_then(|index| row.get(index))
                    .map(json_map)
                    .unwrap_or_default(),
                resource: resource
                    .and_then(|index| row.get(index))
                    .map(json_map)
                    .unwrap_or_default(),
            })
        })
        .collect()
}

fn line_matches(line: &str, filters: &[LogLineFilter]) -> Result<bool, CompatError> {
    for filter in filters {
        let matches = match filter {
            LogLineFilter::Contains(value) => line.contains(value),
            LogLineFilter::NotContains(value) => !line.contains(value),
            LogLineFilter::Regex(value) => regex::Regex::new(value)
                .map_err(|err| bad(format!("invalid line regex: {err}")))?
                .is_match(line),
            LogLineFilter::NotRegex(value) => !regex::Regex::new(value)
                .map_err(|err| bad(format!("invalid line regex: {err}")))?
                .is_match(line),
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(true)
}

fn parse_fields(body: &str, parser: LogParser) -> Result<BTreeMap<String, String>, CompatError> {
    match parser {
        LogParser::Json => {
            let value: Value =
                serde_json::from_str(body).map_err(|_| bad("log line is not valid JSON"))?;
            let Value::Object(map) = value else {
                return Err(bad("JSON log line must be an object"));
            };
            Ok(map
                .into_iter()
                .filter_map(|(key, value)| json_scalar(value).map(|value| (key, value)))
                .collect())
        }
        LogParser::Logfmt => parse_logfmt_fields(body),
    }
}

fn parse_logfmt_fields(body: &str) -> Result<BTreeMap<String, String>, CompatError> {
    let chars: Vec<char> = body.chars().collect();
    let mut fields = BTreeMap::new();
    let mut index = 0;

    while index < chars.len() {
        while index < chars.len() && chars[index].is_whitespace() {
            index += 1;
        }
        if index == chars.len() {
            break;
        }

        let key_start = index;
        while index < chars.len() && !chars[index].is_whitespace() && chars[index] != '=' {
            index += 1;
        }
        if index == chars.len() || chars[index] != '=' {
            while index < chars.len() && !chars[index].is_whitespace() {
                index += 1;
            }
            continue;
        }
        let key: String = chars[key_start..index].iter().collect();
        index += 1;

        let value = if index < chars.len() && chars[index] == '"' {
            index += 1;
            let mut value = String::new();
            let mut closed = false;
            while index < chars.len() {
                let ch = chars[index];
                index += 1;
                match ch {
                    '"' => {
                        closed = true;
                        break;
                    }
                    '\\' if index < chars.len() => {
                        let escaped = chars[index];
                        index += 1;
                        value.push(match escaped {
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            other => other,
                        });
                    }
                    '\\' => return Err(bad("unterminated logfmt escape")),
                    other => value.push(other),
                }
            }
            if !closed {
                return Err(bad("unterminated logfmt quoted value"));
            }
            if index < chars.len() && !chars[index].is_whitespace() {
                return Err(bad("invalid logfmt quoted value"));
            }
            value
        } else {
            let value_start = index;
            while index < chars.len() && !chars[index].is_whitespace() {
                index += 1;
            }
            chars[value_start..index].iter().collect()
        };

        if !key.is_empty() {
            fields.insert(key, value);
        }
    }
    Ok(fields)
}

fn parse_unparsed_fields(body: &str) -> Option<BTreeMap<String, String>> {
    parse_logfmt_fields(body)
        .ok()
        .filter(|fields| !fields.is_empty())
        .or_else(|| parse_fields(body, LogParser::Json).ok())
        .filter(|fields| !fields.is_empty())
}

fn normalized_fields(fields: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut normalized = BTreeMap::new();
    for (key, value) in fields {
        // BTreeMap iteration makes sanitization collisions deterministic: the
        // first raw key in lexical order wins.
        normalized
            .entry(crate::compat::projection::prometheus::sanitize_label_name(
                key,
            ))
            .or_insert_with(|| value.clone());
    }
    normalized
}

fn json_scalar(value: Value) -> Option<String> {
    match value {
        Value::Null | Value::Array(_) | Value::Object(_) => None,
        Value::String(value) => Some(value),
        other => Some(other.to_string()),
    }
}

fn json_map(value: &Value) -> HashMap<String, String> {
    variant_json_to_string_map(value)
}

fn enforce_stream_cap(hits: &[LogHit], max_series: usize) -> Result<(), CompatError> {
    let mut streams = BTreeSet::new();
    for hit in hits {
        streams.insert(hit.labels.clone());
        if streams.len() > max_series {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                format!(
                    "stream count {} exceeds max_series {} after Loki label projection",
                    streams.len(),
                    max_series
                ),
            ));
        }
    }
    Ok(())
}

fn cell_str(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        other => Some(other.to_string()),
    }
}

fn cell_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(value) => value.as_i64().or_else(|| value.as_f64().map(|v| v as i64)),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn bad(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

fn enforce_scan_cap(result: &QueryResult, cap: usize) -> Result<(), CompatError> {
    if result.row_count > cap || result.rows.len() > cap {
        return Err(CompatError::new(
            CompatErrorCode::LimitExceeded,
            format!(
                "log scan exceeded scan_cap {cap} before LogQL filtering; narrow the time window"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::backends::metrics::{LabelMatcher, MatcherOp};

    #[test]
    fn parses_json_fields_and_rejects_non_objects() {
        let fields = parse_fields(r#"{"status":500,"message":"failed"}"#, LogParser::Json).unwrap();
        assert_eq!(fields.get("status"), Some(&"500".into()));
        assert!(parse_fields("[]", LogParser::Json).is_err());
    }

    #[test]
    fn parsed_json_fields_and_metadata_are_returned_as_labels() {
        let request = LogsQueryRequest {
            start_ns: None,
            end_ns: None,
            matchers: vec![LabelMatcher {
                name: "service_name".into(),
                op: MatcherOp::Eq,
                value: "checkout".into(),
            }],
            line_filters: Vec::new(),
            parser: Some(LogParser::Json),
            parsed_filters: vec![LabelMatcher {
                name: "status".into(),
                op: MatcherOp::Eq,
                value: "500".into(),
            }],
            unwrap: None,
            limit: 10,
            direction: LogDirection::Forward,
        };
        let row = RawLogRow {
            timestamp_ns: 1,
            body: r#"{"status":500,"http.method":"GET","http_method":"collision","request_id":"parsed","service_name":"parsed"}"#.into(),
            attributes: [
                ("request_id".into(), "r1".into()),
                ("user_id".into(), "u1".into()),
            ]
            .into_iter()
            .collect(),
            resource: [("service.name".into(), "checkout".into())]
                .into_iter()
                .collect(),
        };

        let hit = DuckLakeLogsBackend::apply_row(row, &request)
            .unwrap()
            .unwrap();

        assert_eq!(
            hit.labels,
            [
                ("http_method".into(), "GET".into()),
                ("request_id".into(), "r1".into()),
                ("service_name".into(), "checkout".into()),
                ("status".into(), "500".into()),
                ("user_id".into(), "u1".into()),
            ]
            .into_iter()
            .collect()
        );
        assert!(hit.structured_metadata.is_empty());
    }

    #[test]
    fn parsed_level_is_exposed_as_detected_level_label() {
        let request = LogsQueryRequest {
            start_ns: None,
            end_ns: None,
            matchers: Vec::new(),
            line_filters: Vec::new(),
            parser: Some(LogParser::Json),
            parsed_filters: Vec::new(),
            unwrap: None,
            limit: 10,
            direction: LogDirection::Forward,
        };
        let row = RawLogRow {
            timestamp_ns: 1,
            body: r#"{"level":"info","message":"checkout started"}"#.into(),
            attributes: HashMap::new(),
            resource: HashMap::new(),
        };

        let hit = DuckLakeLogsBackend::apply_row(row, &request)
            .unwrap()
            .unwrap();

        assert_eq!(hit.labels.get("level"), Some(&"info".into()));
        assert_eq!(hit.labels.get("detected_level"), Some(&"info".into()));
    }

    #[test]
    fn unparsed_logfmt_instant_query_promotes_fields_and_metadata() {
        let request = LogsQueryRequest {
            start_ns: None,
            end_ns: None,
            matchers: Vec::new(),
            line_filters: Vec::new(),
            parser: None,
            parsed_filters: Vec::new(),
            unwrap: None,
            limit: 10,
            direction: LogDirection::Forward,
        };
        let row = RawLogRow {
            timestamp_ns: 1,
            body: "level=warn msg=checkout retry duration_ms=25".into(),
            attributes: [
                ("request_id".into(), "r1".into()),
                ("user_id".into(), "u1".into()),
            ]
            .into_iter()
            .collect(),
            resource: HashMap::new(),
        };

        let hit = DuckLakeLogsBackend::apply_row(row, &request)
            .unwrap()
            .unwrap();

        assert_eq!(
            hit.labels,
            [
                ("detected_level".into(), "warn".into()),
                ("request_id".into(), "r1".into()),
                ("user_id".into(), "u1".into()),
            ]
            .into_iter()
            .collect()
        );
        assert!(!hit.labels.contains_key("level"));
        assert!(!hit.labels.contains_key("msg"));
        assert!(!hit.labels.contains_key("duration_ms"));
        assert!(hit.structured_metadata.is_empty());
    }

    #[test]
    fn unparsed_json_instant_query_promotes_level_and_metadata() {
        let request = LogsQueryRequest {
            start_ns: None,
            end_ns: None,
            matchers: Vec::new(),
            line_filters: Vec::new(),
            parser: None,
            parsed_filters: Vec::new(),
            unwrap: None,
            limit: 10,
            direction: LogDirection::Forward,
        };
        let row = RawLogRow {
            timestamp_ns: 1,
            body: r#"{"level":"info","request_id":"parsed","user_id":"parsed","duration_ms":25,"msg":"checkout"}"#.into(),
            attributes: [
                ("request_id".into(), "r1".into()),
                ("user_id".into(), "u1".into()),
            ]
            .into_iter()
            .collect(),
            resource: HashMap::new(),
        };

        let hit = DuckLakeLogsBackend::apply_row(row, &request)
            .unwrap()
            .unwrap();

        assert_eq!(hit.labels.get("detected_level"), Some(&"info".into()));
        assert_eq!(hit.labels.get("request_id"), Some(&"r1".into()));
        assert_eq!(hit.labels.get("user_id"), Some(&"u1".into()));
        assert!(!hit.labels.contains_key("duration_ms"));
        assert!(!hit.labels.contains_key("msg"));
        assert!(hit.structured_metadata.is_empty());
    }

    #[test]
    fn line_filter_semantics_are_explicit() {
        assert!(line_matches("request failed", &[LogLineFilter::Contains("fail".into())]).unwrap());
        assert!(!line_matches("request ok", &[LogLineFilter::Regex("fail".into())]).unwrap());
    }

    #[test]
    fn scan_cap_failure_is_explicit_before_logql_filters_run() {
        let result = QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 11,
        };
        let err = enforce_scan_cap(&result, 10).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::LimitExceeded);
        assert!(err.message.contains("scan_cap 10"));
    }

    #[test]
    fn query_bounds_are_nanoseconds_with_exclusive_end() {
        assert_eq!(
            DuckLakeLogsBackend::sql_window(
                Some(1_700_000_000_000_000_001),
                Some(1_700_000_000_000_000_002),
            ),
            " AND epoch_ns(timestamp) >= 1700000000000000001 AND epoch_ns(timestamp) < 1700000000000000002"
        );
    }

    #[test]
    fn query_rows_preserve_duplicate_nanosecond_timestamps() {
        let timestamp_ns = 1_700_000_000_000_000_001;
        let result = QueryResult {
            columns: vec!["timestamp_ns".into(), "body".into()],
            rows: vec![
                vec![serde_json::json!(timestamp_ns), serde_json::json!("first")],
                vec![serde_json::json!(timestamp_ns), serde_json::json!("second")],
            ],
            row_count: 2,
        };

        let rows = parse_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp_ns, timestamp_ns);
        assert_eq!(rows[1].timestamp_ns, timestamp_ns);
    }

    #[test]
    fn final_projection_series_cap_returns_limit_exceeded() {
        let hits = vec![
            LogHit {
                timestamp_ns: 1,
                line: "one".into(),
                labels: [("service_name".into(), "one".into())]
                    .into_iter()
                    .collect(),
                structured_metadata: BTreeMap::new(),
            },
            LogHit {
                timestamp_ns: 2,
                line: "two".into(),
                labels: [("service_name".into(), "two".into())]
                    .into_iter()
                    .collect(),
                structured_metadata: BTreeMap::new(),
            },
        ];

        let error = enforce_stream_cap(&hits, 1).unwrap_err();
        assert_eq!(error.code, CompatErrorCode::LimitExceeded);
    }

    #[test]
    fn parses_quoted_logfmt_values_and_escapes() {
        let fields = parse_fields(
            r#"message="hello world" path="a\"b\\c" level=info"#,
            LogParser::Logfmt,
        )
        .unwrap();

        assert_eq!(fields.get("message"), Some(&"hello world".into()));
        assert_eq!(fields.get("path"), Some(&"a\"b\\c".into()));
        assert_eq!(fields.get("level"), Some(&"info".into()));
    }

    #[test]
    fn malformed_parser_input_skips_only_that_entry() {
        for (parser, body) in [
            (LogParser::Json, "not json"),
            (LogParser::Logfmt, "message=\"unterminated"),
        ] {
            let request = LogsQueryRequest {
                start_ns: None,
                end_ns: None,
                matchers: Vec::new(),
                line_filters: Vec::new(),
                parser: Some(parser),
                parsed_filters: Vec::new(),
                unwrap: None,
                limit: 10,
                direction: LogDirection::Forward,
            };
            let row = RawLogRow {
                timestamp_ns: 1,
                body: body.into(),
                attributes: HashMap::new(),
                resource: HashMap::new(),
            };

            assert!(DuckLakeLogsBackend::apply_row(row, &request)
                .unwrap()
                .is_none());
        }
    }
}
