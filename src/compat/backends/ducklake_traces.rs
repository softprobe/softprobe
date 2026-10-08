//! Typed DuckLake trace query backend for the Tempo adapter.

use super::traces::{
    persisted_status_code_numeric_value, scan_reached_cap, trace_scan_cap, trace_scan_sql,
    TraceAttribute, TraceData, TraceEvent, TraceLookupBounds, TraceQueryBackend, TraceSearchHit,
    TraceSearchRequest, TraceSpan,
};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::projection::tempo::{project_tempo_link_attributes, project_tempo_tags};
use crate::compat::tempo::traceql::{is_numeric_field, TraceField, TracePredicate, TraceSelector};
use crate::compat::tenant::TenantContext;
use crate::query::duckdb::QueryResult;
use crate::query::QueryEngine;
use crate::storage::schema::variant::variant_json_to_string_map;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

pub struct DuckLakeTraceBackend {
    query: Arc<QueryEngine>,
}

impl DuckLakeTraceBackend {
    pub fn new(query: Arc<QueryEngine>) -> Self {
        Self { query }
    }

    async fn execute(&self, ctx: &TenantContext, sql: &str) -> Result<QueryResult, CompatError> {
        if ctx.remaining().is_zero() {
            return Err(deadline());
        }
        match tokio::time::timeout(ctx.remaining(), self.query.execute_query(sql)).await {
            Err(_) => Err(deadline()),
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => {
                let message = err.to_string();
                if message.contains("Table with name traces does not exist")
                    || message.contains("Table with name tm_all_span does not exist")
                    || message.contains("Table with name tm_cq_span does not exist")
                {
                    Ok(QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        row_count: 0,
                    })
                } else {
                    Err(CompatError::new(
                        CompatErrorCode::BadRequest,
                        format!("trace query failed: {message}"),
                    ))
                }
            }
        }
    }

    async fn scan(
        &self,
        ctx: &TenantContext,
        request: &TraceSearchRequest,
        trace_id: Option<&str>,
    ) -> Result<Vec<TraceSpan>, CompatError> {
        let result = self
            .execute(ctx, &trace_scan_sql(request, trace_id))
            .await?;
        let scan_cap = trace_scan_cap(request.limit);
        if scan_reached_cap(result.rows.len(), scan_cap) {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                format!(
                    "trace scan reaches bounded scan cap {} before post-filtering",
                    scan_cap
                ),
            ));
        }
        parse_rows(&result)
    }

    fn tags_for(&self, span: &TraceSpan, max_tags: usize) -> BTreeMap<String, String> {
        let mut resource = span
            .resource_attributes
            .iter()
            .map(|attribute| (attribute.key.clone(), attribute.value.clone()))
            .collect::<HashMap<_, _>>();
        if let Some(service) = &span.service_name {
            resource
                .entry("service.name".into())
                .or_insert_with(|| service.clone());
        }
        let attributes = span
            .attributes
            .iter()
            .map(|attribute| (attribute.key.clone(), attribute.value.clone()))
            .collect::<HashMap<_, _>>();
        let mut additional = HashMap::new();
        for event in &span.events {
            for attribute in &event.attributes {
                additional.insert(attribute.key.clone(), attribute.value.clone());
            }
        }
        additional.extend(project_tempo_link_attributes(&span.links));
        project_tempo_tags(&resource, &attributes, &additional, max_tags)
    }

    fn matches_tags(&self, span: &TraceSpan, request: &TraceSearchRequest) -> bool {
        let tags = self.tags_for(span, usize::MAX);
        request.tags.iter().all(|(key, expected)| {
            tags.get(key)
                .map(|actual| {
                    actual
                        .to_ascii_lowercase()
                        .contains(&expected.to_ascii_lowercase())
                })
                .unwrap_or(false)
        })
    }

    fn matches_selector(selector: &TraceSelector, span: &TraceSpan) -> bool {
        match selector {
            TraceSelector::Predicate(predicate) => Self::matches_predicate(predicate, span),
            TraceSelector::And(left, right) => {
                Self::matches_selector(left, span) && Self::matches_selector(right, span)
            }
            TraceSelector::Or(left, right) => {
                Self::matches_selector(left, span) || Self::matches_selector(right, span)
            }
        }
    }

    fn matches_predicate(predicate: &TracePredicate, span: &TraceSpan) -> bool {
        use crate::compat::tempo::traceql::{canonical_status_code, is_status_field};

        let (field, operation, expected) = match predicate {
            TracePredicate::Eq(field, value) => (field, "eq", value),
            TracePredicate::NotEq(field, value) => (field, "ne", value),
            TracePredicate::Regex(field, value) => (field, "regex", value),
            TracePredicate::NotRegex(field, value) => (field, "not_regex", value),
            TracePredicate::Greater(field, value) => (field, "gt", value),
            TracePredicate::GreaterOrEqual(field, value) => (field, "gte", value),
            TracePredicate::Less(field, value) => (field, "lt", value),
            TracePredicate::LessOrEqual(field, value) => (field, "lte", value),
        };
        let actual = field_value(field, span);
        if is_status_field(field) {
            let actual_val = actual.as_deref().unwrap_or_default();
            let actual_code = canonical_status_code(actual_val);
            let expected_code = canonical_status_code(expected.as_str());
            match operation {
                "eq" => {
                    if let (Some(a), Some(e)) = (actual_code, expected_code) {
                        a == e
                    } else if let Some(a) = actual_val.strip_prefix("STATUS_CODE_") {
                        a.eq_ignore_ascii_case(expected.as_str())
                    } else {
                        actual_val.eq_ignore_ascii_case(expected.as_str())
                    }
                }
                "ne" => {
                    if let (Some(a), Some(e)) = (actual_code, expected_code) {
                        a != e
                    } else if let Some(a) = actual_val.strip_prefix("STATUS_CODE_") {
                        !a.eq_ignore_ascii_case(expected.as_str())
                    } else {
                        !actual_val.eq_ignore_ascii_case(expected.as_str())
                    }
                }
                "regex" | "not_regex" => {
                    let canonical_name = match actual_code {
                        Some(0) => "unset",
                        Some(1) => "ok",
                        Some(2) => "error",
                        _ => actual_val,
                    };
                    let matched = regex::Regex::new(expected.as_str())
                        .ok()
                        .map(|re| re.is_match(actual_val) || re.is_match(canonical_name))
                        .unwrap_or(false);
                    if operation == "regex" {
                        matched
                    } else {
                        !matched
                    }
                }
                "gt" | "gte" | "lt" | "lte" => {
                    let a = actual_code.or_else(|| actual_val.parse::<i64>().ok());
                    let e = expected_code.or_else(|| expected.as_str().parse::<i64>().ok());
                    match (a, e) {
                        (Some(a), Some(e)) => match operation {
                            "gt" => a > e,
                            "gte" => a >= e,
                            "lt" => a < e,
                            _ => a <= e,
                        },
                        _ => false,
                    }
                }
                _ => false,
            }
        } else {
            match operation {
                "eq" => actual.as_deref() == Some(expected.as_str()),
                "ne" => actual
                    .as_deref()
                    .is_some_and(|value| value != expected.as_str()),
                "regex" | "not_regex" => {
                    let matched = actual
                        .as_deref()
                        .and_then(|value| {
                            regex::Regex::new(expected.as_str())
                                .ok()
                                .map(|re| re.is_match(value))
                        })
                        .unwrap_or(false);
                    if operation == "regex" {
                        matched
                    } else {
                        !matched
                    }
                }
                "gt" | "gte" | "lt" | "lte" => actual
                    .as_deref()
                    .and_then(|value| compare_value(field, value, expected.as_str()))
                    .map(|ordering| match operation {
                        "gt" => ordering.is_gt(),
                        "gte" => ordering.is_ge(),
                        "lt" => ordering.is_lt(),
                        _ => ordering.is_le(),
                    })
                    .unwrap_or(false),
                _ => false,
            }
        }
    }
}

#[async_trait]
impl TraceQueryBackend for DuckLakeTraceBackend {
    async fn get_trace(
        &self,
        ctx: &TenantContext,
        trace_id: &str,
        bounds: TraceLookupBounds,
    ) -> Result<Option<TraceData>, CompatError> {
        let request = TraceSearchRequest {
            tags: BTreeMap::new(),
            selector: None,
            min_duration_ns: None,
            max_duration_ns: None,
            start_ns: bounds.start_ns,
            end_ns: bounds.end_ns,
            limit: ctx.limits.max_series,
        };
        let spans = self.scan(ctx, &request, Some(trace_id)).await?;
        if spans.is_empty() {
            Ok(None)
        } else {
            Ok(Some(TraceData { spans }))
        }
    }

    async fn search(
        &self,
        ctx: &TenantContext,
        request: TraceSearchRequest,
    ) -> Result<Vec<TraceSearchHit>, CompatError> {
        let spans = self.scan(ctx, &request, None).await?;
        let mut by_trace: BTreeMap<String, Vec<TraceSpan>> = BTreeMap::new();
        for span in spans {
            by_trace
                .entry(span.trace_id.clone())
                .or_default()
                .push(span);
        }

        let mut hits = Vec::new();
        for (trace_id, spans) in by_trace {
            let matching = spans.iter().any(|span| {
                self.matches_tags(span, &request)
                    && request
                        .selector
                        .as_ref()
                        .map(|selector| Self::matches_selector(selector, span))
                        .unwrap_or(true)
            });
            if !matching {
                continue;
            }
            let start = spans
                .iter()
                .map(|span| span.start_time_unix_nano)
                .min()
                .unwrap_or(0);
            let end = spans
                .iter()
                .map(|span| span.end_time_unix_nano.unwrap_or(span.start_time_unix_nano))
                .max()
                .unwrap_or(start);
            let duration = end.saturating_sub(start);
            if request.min_duration_ns.is_some_and(|min| duration < min)
                || request.max_duration_ns.is_some_and(|max| duration > max)
            {
                continue;
            }
            let root = spans
                .iter()
                .find(|span| span.parent_span_id.is_none())
                .or_else(|| spans.first());
            hits.push(TraceSearchHit {
                trace_id,
                root_service_name: root.and_then(|span| span.service_name.clone()),
                root_trace_name: root.map(|span| span.name.clone()),
                start_time_unix_nano: start,
                duration_ms: duration / 1_000_000,
            });
        }
        hits.sort_by(|left, right| {
            left.start_time_unix_nano
                .cmp(&right.start_time_unix_nano)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });
        hits.truncate(request.limit);
        Ok(hits)
    }

    async fn search_tags(&self, ctx: &TenantContext) -> Result<Vec<String>, CompatError> {
        let request = TraceSearchRequest {
            tags: BTreeMap::new(),
            selector: None,
            min_duration_ns: None,
            max_duration_ns: None,
            start_ns: None,
            end_ns: None,
            limit: ctx.limits.max_series,
        };
        let spans = self.scan(ctx, &request, None).await?;
        let mut names = BTreeSet::new();
        for span in spans {
            names.extend(self.tags_for(&span, usize::MAX).into_keys());
        }
        Ok(names.into_iter().collect())
    }

    async fn search_tag_values(
        &self,
        ctx: &TenantContext,
        tag: &str,
    ) -> Result<Vec<String>, CompatError> {
        let request = TraceSearchRequest {
            tags: BTreeMap::new(),
            selector: None,
            min_duration_ns: None,
            max_duration_ns: None,
            start_ns: None,
            end_ns: None,
            limit: ctx.limits.max_series,
        };
        let spans = self.scan(ctx, &request, None).await?;
        let mut values = BTreeSet::new();
        for span in spans {
            if let Some(value) = self.tags_for(&span, usize::MAX).get(tag) {
                values.insert(value.clone());
            }
        }
        Ok(values.into_iter().collect())
    }
}

fn parse_rows(result: &QueryResult) -> Result<Vec<TraceSpan>, CompatError> {
    if result.rows.is_empty() {
        return Ok(Vec::new());
    }

    let index = |name: &str| result.columns.iter().position(|column| column == name);
    let (
        Some(trace_id),
        Some(span_id),
        parent_span_id,
        Some(name),
        kind,
        Some(app_id),
        Some(start),
        end,
        attributes,
        resource_attributes,
        instrumentation_scope,
        links,
        status_code,
        status_message,
        events,
    ) = (
        index("trace_id"),
        index("span_id"),
        index("parent_span_id"),
        index("message_type"),
        index("span_kind"),
        index("app_id"),
        index("start_time_unix_nano"),
        index("end_time_unix_nano"),
        index("attributes"),
        index("resource_attributes"),
        index("instrumentation_scope"),
        index("links"),
        index("status_code"),
        index("status_message"),
        index("events"),
    )
    else {
        return Err(malformed_rows(
            "trace query result is missing required columns",
        ));
    };

    result
        .rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| {
            let trace_id = required_string(row, trace_id, row_index, "trace_id")?;
            let span_id = required_string(row, span_id, row_index, "span_id")?;
            let name = required_string(row, name, row_index, "message_type")?;
            let start_time_unix_nano = required_i64(row, start, row_index, "start_time_unix_nano")?;
            let end_time_unix_nano = optional_i64(row, end, row_index, "end_timestamp")?;
            let attributes = optional_cell(row, attributes, row_index, "attributes")?
                .map(|value| parse_attributes(value, row_index, "attributes"))
                .transpose()?
                .unwrap_or_default();
            Ok(TraceSpan {
                trace_id,
                span_id,
                parent_span_id: parent_span_id
                    .and_then(|idx| row.get(idx))
                    .and_then(cell_string),
                name,
                kind: kind.and_then(|idx| row.get(idx)).and_then(cell_string),
                start_time_unix_nano,
                end_time_unix_nano,
                attributes,
                resource_attributes: optional_cell(
                    row,
                    resource_attributes,
                    row_index,
                    "resource_attributes",
                )?
                .map(|value| parse_attributes(value, row_index, "resource_attributes"))
                .transpose()?
                .unwrap_or_default(),
                instrumentation_scope: optional_cell(
                    row,
                    instrumentation_scope,
                    row_index,
                    "instrumentation_scope",
                )?
                .map(|value| parse_json_object(value, row_index, "instrumentation_scope"))
                .transpose()?,
                status_code: status_code
                    .and_then(|idx| row.get(idx))
                    .and_then(cell_string),
                status_message: status_message
                    .and_then(|idx| row.get(idx))
                    .and_then(cell_string),
                links: optional_cell(row, links, row_index, "links")?
                    .map(|value| parse_links(value, row_index))
                    .transpose()?
                    .unwrap_or_default(),
                events: optional_cell(row, events, row_index, "events")?
                    .map(|value| parse_events(value, start_time_unix_nano, row_index))
                    .transpose()?
                    .unwrap_or_default(),
                service_name: row.get(app_id).and_then(cell_string),
            })
        })
        .collect()
}

fn required_string(
    row: &[Value],
    column: usize,
    row_index: usize,
    field: &str,
) -> Result<String, CompatError> {
    match row.get(column) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        _ => Err(malformed_row(row_index, field)),
    }
}

fn required_i64(
    row: &[Value],
    column: usize,
    row_index: usize,
    field: &str,
) -> Result<i64, CompatError> {
    row.get(column)
        .and_then(cell_i64)
        .ok_or_else(|| malformed_row(row_index, field))
}

fn optional_cell<'a>(
    row: &'a [Value],
    column: Option<usize>,
    row_index: usize,
    field: &str,
) -> Result<Option<&'a Value>, CompatError> {
    let Some(column) = column else {
        return Ok(None);
    };
    match row.get(column) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() || value.trim() == "null" => Ok(None),
        Some(value) => Ok(Some(value)),
        None => Err(malformed_row(row_index, field)),
    }
}

fn optional_i64(
    row: &[Value],
    column: Option<usize>,
    row_index: usize,
    field: &str,
) -> Result<Option<i64>, CompatError> {
    optional_cell(row, column, row_index, field)?
        .map(|value| cell_i64(value).ok_or_else(|| malformed_row(row_index, field)))
        .transpose()
}

fn malformed_row(row_index: usize, field: &str) -> CompatError {
    malformed_rows(format!(
        "trace query row {row_index} has invalid required field '{field}'"
    ))
}

fn malformed_rows(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

fn parse_attributes(
    value: &Value,
    row_index: usize,
    field: &str,
) -> Result<Vec<TraceAttribute>, CompatError> {
    let value = parse_json_object(value, row_index, field)?;
    let mut values = variant_json_to_string_map(&value)
        .into_iter()
        .filter(|(key, _)| {
            key != crate::models::span::INSTRUMENTATION_SCOPE_ATTRIBUTE
                && key != crate::models::span::LINKS_ATTRIBUTE
        })
        .map(|(key, value)| TraceAttribute { key, value })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(values)
}

fn parse_events(
    value: &Value,
    fallback_timestamp: i64,
    row_index: usize,
) -> Result<Vec<TraceEvent>, CompatError> {
    let value = strict_json_value(value, row_index, "events")?;
    let Value::Array(events) = value else {
        return Err(malformed_row(row_index, "events"));
    };
    events
        .into_iter()
        .map(|event| {
            let Value::Object(event) = event else {
                return Err(malformed_row(row_index, "events"));
            };
            let name = event
                .get("name")
                .and_then(|value| match value {
                    Value::String(value) if !value.is_empty() => Some(value.clone()),
                    _ => None,
                })
                .ok_or_else(|| malformed_row(row_index, "events"))?;
            let timestamp_unix_nano = event
                .get("timestamp")
                .and_then(|value| parse_timestamp_value(value, fallback_timestamp))
                .ok_or_else(|| malformed_row(row_index, "events"))?;
            let attributes = event
                .get("attributes")
                .filter(|value| !value.is_null())
                .map(|value| parse_attributes(value, row_index, "events"))
                .transpose()?
                .unwrap_or_default();
            Ok(TraceEvent {
                name,
                timestamp_unix_nano,
                attributes,
            })
        })
        .collect()
}

fn parse_json_object(value: &Value, row_index: usize, field: &str) -> Result<Value, CompatError> {
    let value = strict_json_value(value, row_index, field)?;
    if value.is_object() {
        Ok(value)
    } else {
        Err(malformed_row(row_index, field))
    }
}

fn strict_json_value(value: &Value, row_index: usize, field: &str) -> Result<Value, CompatError> {
    match value {
        Value::String(text) => {
            serde_json::from_str(text).map_err(|_| malformed_row(row_index, field))
        }
        other => Ok(other.clone()),
    }
}

fn field_value(field: &TraceField, span: &TraceSpan) -> Option<String> {
    match field {
        TraceField::Span(key) if key == "status_code" || key == "status" => {
            span_status_code_value(span)
        }
        TraceField::Span(key) => span
            .attributes
            .iter()
            .find(|attribute| attribute.key == *key)
            .map(|attribute| attribute.value.clone()),
        TraceField::Resource(key) => span
            .resource_attributes
            .iter()
            .find(|attribute| attribute.key == *key)
            .map(|attribute| attribute.value.clone())
            .or_else(|| {
                (key == "service.name")
                    .then(|| span.service_name.clone())
                    .flatten()
            }),
        TraceField::Instrumentation(key) => {
            instrumentation_scope_value(span.instrumentation_scope.as_ref(), key)
        }
        TraceField::Intrinsic(key) => match key.as_str() {
            "name" | "span:name" | "span.name" => Some(span.name.clone()),
            "kind" | "span:kind" | "span.kind" => span.kind.clone(),
            "status" | "span:status" | "span.status" | "status_code" | "span:status_code"
            | "span.status_code" => span_status_code_value(span),
            "statusMessage" | "span:statusMessage" | "span.statusMessage" => {
                span.status_message.clone()
            }
            "duration" | "span:duration" | "span.duration" | "traceDuration" | "trace:duration"
            | "trace.duration" => Some(
                span.end_time_unix_nano
                    .unwrap_or(span.start_time_unix_nano)
                    .saturating_sub(span.start_time_unix_nano)
                    .to_string(),
            ),
            _ => None,
        },
    }
}

fn span_status_code_value(span: &TraceSpan) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key == "status_code" || attribute.key == "status")
        .map(|attribute| attribute.value.clone())
        .or_else(|| span.status_code.clone())
}

fn instrumentation_scope_value(scope: Option<&Value>, key: &str) -> Option<String> {
    let scope = scope?;
    match key {
        "name" | "version" => scope
            .get(key)
            .and_then(|value| value.as_str())
            .map(str::to_string),
        _ => None,
    }
}

fn compare_value(field: &TraceField, actual: &str, expected: &str) -> Option<std::cmp::Ordering> {
    if is_numeric_field(field) {
        let actual = if is_persisted_status_code_field(field) {
            persisted_status_code_numeric_value(actual)?.to_string()
        } else {
            actual.to_string()
        };
        return actual
            .parse::<i64>()
            .ok()?
            .partial_cmp(&expected.parse::<i64>().ok()?);
    }
    if crate::compat::tempo::traceql::is_duration_field(field) {
        return actual
            .parse::<i64>()
            .ok()?
            .partial_cmp(&crate::compat::tempo::traceql::parse_duration_ns(expected)?);
    }
    Some(actual.cmp(expected))
}

fn is_persisted_status_code_field(field: &TraceField) -> bool {
    crate::compat::tempo::traceql::is_status_field(field)
}

fn parse_timestamp_value(value: &Value, _fallback: i64) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => chrono::DateTime::parse_from_rfc3339(value)
            .ok()
            .and_then(|timestamp| timestamp.timestamp_nanos_opt())
            .or_else(|| {
                ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"]
                    .into_iter()
                    .find_map(|format| {
                        chrono::NaiveDateTime::parse_from_str(value, format)
                            .ok()
                            .and_then(|timestamp| timestamp.and_utc().timestamp_nanos_opt())
                    })
            })
            .or_else(|| value.parse::<i64>().ok()),
        _ => None,
    }
}

fn parse_links(value: &Value, row_index: usize) -> Result<Vec<Value>, CompatError> {
    let value = strict_json_value(value, row_index, "links")?;
    let Value::Array(values) = value else {
        return Err(malformed_row(row_index, "links"));
    };
    if values.iter().all(Value::is_object) {
        Ok(values)
    } else {
        Err(malformed_row(row_index, "links"))
    }
}

fn cell_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        other => Some(other.to_string()),
    }
}

fn cell_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => value.parse::<i64>().ok(),
        _ => None,
    }
}

fn deadline() -> CompatError {
    CompatError::new(CompatErrorCode::LimitExceeded, "query deadline exceeded")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::errors::CompatErrorCode;

    #[test]
    fn selector_matches_span_and_resource_intrinsics() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "GET /users".into(),
            kind: Some("SPAN_KIND_SERVER".into()),
            start_time_unix_nano: 10,
            end_time_unix_nano: Some(20),
            attributes: vec![TraceAttribute {
                key: "http.method".into(),
                value: "GET".into(),
            }],
            status_code: Some("STATUS_CODE_OK".into()),
            status_message: None,
            events: Vec::new(),
            service_name: Some("api".into()),
            resource_attributes: vec![TraceAttribute {
                key: "deployment.environment".into(),
                value: "prod".into(),
            }],
            instrumentation_scope: Some(serde_json::json!({"name": "otel-rust"})),
            links: vec![serde_json::json!({"traceId": "linked", "spanId": "span"})],
        };
        let selector = crate::compat::tempo::traceql::parse_traceql(
            r#"{ resource.service.name = "api" && span.http.method =~ "G.*" }"#,
        )
        .unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&selector, &span));

        let selector = crate::compat::tempo::traceql::parse_traceql(
            r#"{ instrumentation.name = "otel-rust" }"#,
        )
        .unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&selector, &span));

        let mut numeric_span = span.clone();
        numeric_span.status_code = Some("500".into());
        let selector =
            crate::compat::tempo::traceql::parse_traceql(r#"{ status_code >= 500 }"#).unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(
            &selector,
            &numeric_span
        ));
    }

    #[test]
    fn numeric_status_code_intrinsic_aliases_match() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "root".into(),
            kind: None,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: Vec::new(),
            status_code: Some("500".into()),
            status_message: None,
            events: Vec::new(),
            service_name: None,
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        for query in [
            r#"{ status_code >= 500 }"#,
            r#"{ span:status_code >= 500 }"#,
            r#"{ span.status_code >= 500 }"#,
        ] {
            let selector = crate::compat::tempo::traceql::parse_traceql(query).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &span),
                "query should match: {query}"
            );
        }
    }

    #[test]
    fn status_and_duration_filters_match_in_memory() {
        let error_span = TraceSpan {
            trace_id: "t1".into(),
            span_id: "s1".into(),
            parent_span_id: None,
            name: "checkout".into(),
            kind: Some("SPAN_KIND_SERVER".into()),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: Some(2_500_000_000), // 1.5s duration
            attributes: Vec::new(),
            status_code: Some("STATUS_CODE_ERROR".into()),
            status_message: Some("timeout".into()),
            events: Vec::new(),
            service_name: Some("api".into()),
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        let ok_span = TraceSpan {
            trace_id: "t2".into(),
            span_id: "s2".into(),
            parent_span_id: None,
            name: "query".into(),
            kind: Some("SPAN_KIND_CLIENT".into()),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: Some(1_100_000_000), // 100ms duration
            attributes: Vec::new(),
            status_code: Some("STATUS_CODE_OK".into()),
            status_message: None,
            events: Vec::new(),
            service_name: Some("api".into()),
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        // Status filters
        for query in [
            r#"{ status = error }"#,
            r#"{ status = "error" }"#,
            r#"{ status = "ERROR" }"#,
            r#"{ status != ok }"#,
            r#"{ span:status = error }"#,
            r#"{ span.status = error }"#,
            r#"{ status_code = 2 }"#,
            r#"{ span.status_code >= 2 }"#,
        ] {
            let selector = crate::compat::tempo::traceql::parse_traceql(query).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &error_span),
                "error span should match: {query}"
            );
            assert!(
                !DuckLakeTraceBackend::matches_selector(&selector, &ok_span),
                "ok span should NOT match error query: {query}"
            );
        }

        // OK status filters
        for query in [
            r#"{ status = ok }"#,
            r#"{ status = "ok" }"#,
            r#"{ status = "OK" }"#,
            r#"{ status != error }"#,
            r#"{ span.status = ok }"#,
            r#"{ status_code = 1 }"#,
        ] {
            let selector = crate::compat::tempo::traceql::parse_traceql(query).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &ok_span),
                "ok span should match: {query}"
            );
            assert!(
                !DuckLakeTraceBackend::matches_selector(&selector, &error_span),
                "error span should NOT match ok query: {query}"
            );
        }

        // Duration filters
        let slow_sel =
            crate::compat::tempo::traceql::parse_traceql(r#"{ duration >= 1.5s }"#).unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(
            &slow_sel,
            &error_span
        ));
        assert!(!DuckLakeTraceBackend::matches_selector(&slow_sel, &ok_span));

        let fast_sel =
            crate::compat::tempo::traceql::parse_traceql(r#"{ duration < 500ms }"#).unwrap();
        assert!(!DuckLakeTraceBackend::matches_selector(
            &fast_sel,
            &error_span
        ));
        assert!(DuckLakeTraceBackend::matches_selector(&fast_sel, &ok_span));
    }

    #[test]
    fn numeric_status_code_intrinsics_share_span_alias_field_value() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "root".into(),
            kind: None,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: vec![TraceAttribute {
                key: "status_code".into(),
                value: "500".into(),
            }],
            status_code: Some("STATUS_CODE_OK".into()),
            status_message: None,
            events: Vec::new(),
            service_name: None,
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        for query in [
            r#"{ status_code >= 500 }"#,
            r#"{ span:status_code >= 500 }"#,
            r#"{ span.status_code >= 500 }"#,
        ] {
            let selector = crate::compat::tempo::traceql::parse_traceql(query).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &span),
                "query should use the same post-filter field value: {query}"
            );
        }
    }

    #[test]
    fn persisted_otlp_error_status_is_numeric_for_span_status_code() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "root".into(),
            kind: None,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: Vec::new(),
            status_code: Some("ERROR".into()),
            status_message: None,
            events: Vec::new(),
            service_name: None,
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        let numeric =
            crate::compat::tempo::traceql::parse_traceql(r#"{ span.status_code >= 2 }"#).unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&numeric, &span));

        let exact_status =
            crate::compat::tempo::traceql::parse_traceql(r#"{ status = "ERROR" }"#).unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&exact_status, &span));
    }

    #[test]
    fn resource_predicates_use_all_resource_attributes() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "root".into(),
            kind: None,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: Vec::new(),
            status_code: None,
            status_message: None,
            events: Vec::new(),
            service_name: Some("api".into()),
            resource_attributes: vec![TraceAttribute {
                key: "deployment.environment".into(),
                value: "prod".into(),
            }],
            instrumentation_scope: None,
            links: Vec::new(),
        };
        let selector = crate::compat::tempo::traceql::parse_traceql(
            r#"{ resource.deployment.environment = "prod" }"#,
        )
        .unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&selector, &span));
    }

    #[test]
    fn parses_duckdb_json_timestamp_without_timezone() {
        assert_eq!(
            parse_timestamp_value(&Value::String("2023-11-14 22:13:21.123456789".into()), 0,),
            Some(1_700_000_001_123_456_789)
        );
    }

    #[test]
    fn malformed_required_trace_rows_return_an_explicit_error() {
        let result = QueryResult {
            columns: vec![
                "trace_id".into(),
                "span_id".into(),
                "parent_span_id".into(),
                "message_type".into(),
                "span_kind".into(),
                "app_id".into(),
                "start_time_unix_nano".into(),
                "end_time_unix_nano".into(),
                "attributes".into(),
                "resource_attributes".into(),
                "instrumentation_scope".into(),
                "links".into(),
                "status_code".into(),
                "status_message".into(),
                "events".into(),
            ],
            rows: vec![vec![
                Value::String("trace".into()),
                Value::String("span".into()),
                Value::Null,
                Value::String("root".into()),
                Value::Null,
                Value::String("api".into()),
                Value::String("not-a-timestamp".into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]],
            row_count: 1,
        };

        let err = match parse_rows(&result) {
            Ok(_) => panic!("malformed rows must not be dropped"),
            Err(err) => err,
        };
        assert_eq!(err.code, CompatErrorCode::BadRequest);
        assert!(err.message.contains("start_time_unix_nano"));
    }

    #[tokio::test]
    async fn empty_query_result_is_valid_empty_trace_data_for_encoders() {
        let result = QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
        };

        let spans = parse_rows(&result).expect("empty result");
        assert!(spans.is_empty());

        let data = TraceData { spans };
        assert_eq!(
            crate::compat::tempo::encode::trace_v1_response(&data, 1024)
                .expect("v1 response")
                .status(),
            axum::http::StatusCode::OK
        );
        assert_eq!(
            crate::compat::tempo::encode::trace_v2_response(&data, 1024)
                .expect("v2 response")
                .status(),
            axum::http::StatusCode::OK
        );
    }

    #[test]
    fn older_trace_schema_without_optional_columns_uses_defaults() {
        let result = QueryResult {
            columns: vec![
                "trace_id".into(),
                "span_id".into(),
                "message_type".into(),
                "app_id".into(),
                "start_time_unix_nano".into(),
            ],
            rows: vec![vec![
                Value::String("trace".into()),
                Value::String("span".into()),
                Value::String("root".into()),
                Value::String("api".into()),
                Value::Number(10.into()),
            ]],
            row_count: 1,
        };

        let span = parse_rows(&result)
            .expect("legacy schema")
            .pop()
            .expect("legacy span");
        assert_eq!(span.end_time_unix_nano, None);
        assert!(span.attributes.is_empty());
        assert!(span.resource_attributes.is_empty());
        assert_eq!(span.instrumentation_scope, None);
        assert!(span.links.is_empty());
        assert!(span.events.is_empty());
        assert_eq!(span.service_name.as_deref(), Some("api"));
    }

    fn valid_trace_result() -> QueryResult {
        QueryResult {
            columns: vec![
                "trace_id".into(),
                "span_id".into(),
                "parent_span_id".into(),
                "message_type".into(),
                "span_kind".into(),
                "app_id".into(),
                "start_time_unix_nano".into(),
                "end_time_unix_nano".into(),
                "attributes".into(),
                "resource_attributes".into(),
                "instrumentation_scope".into(),
                "links".into(),
                "status_code".into(),
                "status_message".into(),
                "events".into(),
            ],
            rows: vec![vec![
                Value::String("trace".into()),
                Value::String("span".into()),
                Value::Null,
                Value::String("root".into()),
                Value::Null,
                Value::String("api".into()),
                Value::Number(10.into()),
                Value::Number(20.into()),
                Value::String(r#"{"http.status_code":"2"}"#.into()),
                Value::String(r#"{"service.name":"api"}"#.into()),
                Value::String(r#"{"name":"scope"}"#.into()),
                Value::String("[]".into()),
                Value::String("OK".into()),
                Value::Null,
                Value::String(r#"[{"name":"event","timestamp":20,"attributes":{}}]"#.into()),
            ]],
            row_count: 1,
        }
    }

    #[test]
    fn null_and_empty_optional_trace_fields_are_absent() {
        let optional_fields = [
            ("end_time_unix_nano", "end_timestamp"),
            ("attributes", "attributes"),
            ("resource_attributes", "resource_attributes"),
            ("instrumentation_scope", "instrumentation_scope"),
            ("links", "links"),
            ("events", "events"),
        ];

        for (column_name, field) in optional_fields {
            for value in [
                Value::Null,
                Value::String(String::new()),
                Value::String("null".into()),
            ] {
                let mut result = valid_trace_result();
                let column = result
                    .columns
                    .iter()
                    .position(|column| column == column_name)
                    .expect("test field column");
                result.rows[0][column] = value;

                let span = parse_rows(&result)
                    .unwrap_or_else(|err| panic!("{field} should be absent: {err:?}"))
                    .pop()
                    .expect("test span");
                match field {
                    "end_timestamp" => assert_eq!(span.end_time_unix_nano, None),
                    "attributes" => assert!(span.attributes.is_empty()),
                    "resource_attributes" => assert!(span.resource_attributes.is_empty()),
                    "instrumentation_scope" => assert_eq!(span.instrumentation_scope, None),
                    "links" => assert!(span.links.is_empty()),
                    "events" => assert!(span.events.is_empty()),
                    _ => unreachable!("test field must be handled"),
                }
            }
        }
    }

    #[test]
    fn malformed_optional_trace_fields_return_bad_request() {
        let malformed = [
            (
                "end_time_unix_nano",
                "end_timestamp",
                Value::String("not-a-timestamp".into()),
            ),
            ("attributes", "attributes", Value::String("{".into())),
            (
                "resource_attributes",
                "resource_attributes",
                Value::String("[]".into()),
            ),
            (
                "instrumentation_scope",
                "instrumentation_scope",
                Value::String("not-json".into()),
            ),
            ("links", "links", Value::String("{}".into())),
            ("events", "events", Value::String("{}".into())),
        ];

        for (column_name, field, value) in malformed {
            let mut result = valid_trace_result();
            let column = result
                .columns
                .iter()
                .position(|column| column == column_name)
                .expect("test field column");
            result.rows[0][column] = value;

            let err = parse_rows(&result).expect_err(field);
            assert_eq!(err.code, CompatErrorCode::BadRequest, "{field}: {err:?}");
            assert!(err.message.contains(field), "{field}: {err:?}");
        }
    }
}
