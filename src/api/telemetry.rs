use crate::api::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TelemetrySearchScope {
    Sessions,
    Traces,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TelemetrySortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryTimeRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryFilter {
    pub field: String,
    pub op: String,
    #[serde(default)]
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TelemetryFilterExpr {
    And { and: Vec<TelemetryFilterExpr> },
    Or { or: Vec<TelemetryFilterExpr> },
    Predicate(TelemetryFilter),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySort {
    pub field: String,
    pub direction: TelemetrySortDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetrySearchRequest {
    pub version: u32,
    pub scope: TelemetrySearchScope,
    #[serde(default)]
    pub time_range: Option<TelemetryTimeRange>,
    #[serde(default)]
    pub filter: Option<TelemetryFilterExpr>,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub sort: Vec<TelemetrySort>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryDetailsTarget {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryDetailsRequest {
    pub version: u32,
    pub target: TelemetryDetailsTarget,
    #[serde(default)]
    pub time_range: Option<TelemetryTimeRange>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct CompiledDetailsSql {
    pub spans: String,
    pub logs: String,
    pub metrics: String,
}

#[derive(Clone, Copy)]
struct FieldSpec {
    key: &'static str,
    sql: &'static str,
    value_type: &'static str,
    entity: &'static str,
    filterable: bool,
    sortable: bool,
    projectable: bool,
    ops: &'static [&'static str],
}

const STRING_OPS: &[&str] = &["eq", "neq", "in", "not_in", "prefix", "contains", "exists"];
const ID_OPS: &[&str] = &["eq", "neq", "in", "not_in", "exists"];
const NUMBER_OPS: &[&str] = &[
    "eq", "neq", "in", "not_in", "lt", "lte", "gt", "gte", "exists",
];
const TIME_OPS: &[&str] = &["eq", "neq", "lt", "lte", "gt", "gte", "exists"];

const SEARCH_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        key: "session_id",
        sql: "session_id",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: ID_OPS,
    },
    FieldSpec {
        key: "trace_id",
        sql: "trace_id",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: ID_OPS,
    },
    FieldSpec {
        key: "span_id",
        sql: "span_id",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: ID_OPS,
    },
    FieldSpec {
        key: "service.name",
        sql: "app_id",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: STRING_OPS,
    },
    FieldSpec {
        key: "timestamp",
        sql: "timestamp",
        value_type: "timestamp",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: TIME_OPS,
    },
    FieldSpec {
        key: "http_request_method",
        sql: "http_request_method",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: STRING_OPS,
    },
    FieldSpec {
        key: "http_request_path",
        sql: "http_request_path",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: STRING_OPS,
    },
    FieldSpec {
        key: "http_response_status_code",
        sql: "http_response_status_code",
        value_type: "int",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: NUMBER_OPS,
    },
    FieldSpec {
        key: "status_code",
        sql: "status_code",
        value_type: "string",
        entity: "trace",
        filterable: true,
        sortable: true,
        projectable: true,
        ops: STRING_OPS,
    },
];

/// Compile the typed telemetry search request into an allowlisted DuckDB query.
pub fn compile_search_sql(request: &TelemetrySearchRequest) -> Result<String, String> {
    if request.version != 1 {
        return Err("unsupported telemetry search version".to_string());
    }
    if request.cursor.is_some() {
        return Err("cursor pagination is not implemented for this endpoint".to_string());
    }
    if request.time_range.is_none() && !has_exact_identifier_filter(request.filter.as_ref()) {
        return Err(
            "timeRange is required unless filtering by exact session_id or trace_id".to_string(),
        );
    }

    let mut conditions = Vec::new();
    if let Some(time_range) = &request.time_range {
        conditions.push(format!(
            "timestamp >= {} AND timestamp <= {}",
            timestamp_literal(&time_range.from),
            timestamp_literal(&time_range.to)
        ));
    }
    if let Some(filter) = &request.filter {
        conditions.push(compile_filter_expr(filter)?);
    }
    let where_sql = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let limit = request.limit.unwrap_or(100).clamp(1, 1000);
    let order_sql = compile_order(&request.sort)?;

    let sql = match request.scope {
        TelemetrySearchScope::Sessions => format!(
            "SELECT session_id AS id, 'session' AS kind, session_id, MIN(timestamp) AS start_time, MAX(COALESCE(end_timestamp, timestamp)) AS end_time, COUNT(DISTINCT trace_id) AS trace_count, COUNT(*) AS span_count, SUM(CASE WHEN status_code = 'ERROR' OR http_response_status_code >= 500 THEN 1 ELSE 0 END) AS error_count, date_diff('millisecond', MIN(timestamp), MAX(COALESCE(end_timestamp, timestamp))) AS duration_ms, string_agg(DISTINCT app_id, ',') AS services, any_value(http_request_path) AS entry_path, any_value(status_message) AS last_error FROM union_spans {where_sql} GROUP BY session_id {order_sql} LIMIT {limit}"
        ),
        TelemetrySearchScope::Traces => format!(
            "SELECT trace_id AS id, 'trace' AS kind, session_id, trace_id, MIN(timestamp) AS start_time, MAX(COALESCE(end_timestamp, timestamp)) AS end_time, COUNT(*) AS span_count, SUM(CASE WHEN status_code = 'ERROR' OR http_response_status_code >= 500 THEN 1 ELSE 0 END) AS error_count, date_diff('millisecond', MIN(timestamp), MAX(COALESCE(end_timestamp, timestamp))) AS duration_ms, string_agg(DISTINCT app_id, ',') AS services, any_value(message_type) AS name, any_value(http_request_path) AS entry_path, any_value(status_message) AS last_error FROM union_spans {where_sql} GROUP BY trace_id, session_id {order_sql} LIMIT {limit}"
        ),
    };

    Ok(sql)
}

/// Compile detail queries for all correlated telemetry signals.
pub fn compile_details_sql(
    target: &TelemetryDetailsTarget,
    time_range: Option<&TelemetryTimeRange>,
    limit: usize,
) -> Result<CompiledDetailsSql, String> {
    let limit = limit.clamp(1, 5000);
    let escaped_id = sql_string_literal(&target.id);
    let (span_filter, log_filter, metric_filter) = match target.kind.as_str() {
        "session" => (
            format!("session_id = {escaped_id}"),
            format!("session_id = {escaped_id}"),
            format!(
                "(attributes['sp.session.id'] = {escaped_id} OR attributes['session.id'] = {escaped_id} OR attributes['session_id'] = {escaped_id} OR resource_attributes['sp.session.id'] = {escaped_id} OR resource_attributes['session.id'] = {escaped_id} OR resource_attributes['session_id'] = {escaped_id})"
            ),
        ),
        "trace" => (
            format!("trace_id = {escaped_id}"),
            format!("trace_id = {escaped_id}"),
            format!(
                "(attributes['trace_id'] = {escaped_id} OR attributes['trace.id'] = {escaped_id} OR resource_attributes['trace_id'] = {escaped_id} OR resource_attributes['trace.id'] = {escaped_id})"
            ),
        ),
        _ => return Err("target.kind must be session or trace".to_string()),
    };

    let time_filter = time_range.map(|range| {
        format!(
            "timestamp >= {} AND timestamp <= {}",
            timestamp_literal(&range.from),
            timestamp_literal(&range.to)
        )
    });

    Ok(CompiledDetailsSql {
        spans: detail_sql(
            "union_spans",
            "session_id, trace_id, span_id, parent_span_id, app_id, message_type, span_kind, timestamp, end_timestamp, status_code, status_message, http_request_method, http_request_path, http_response_status_code, attributes",
            &span_filter,
            time_filter.as_deref(),
            limit,
        ),
        logs: detail_sql(
            "union_logs",
            "session_id, timestamp, severity_number, severity_text, body, trace_id, span_id, attributes, resource_attributes",
            &log_filter,
            time_filter.as_deref(),
            limit,
        ),
        metrics: detail_sql(
            "union_metrics",
            "metric_name, description, unit, metric_type, timestamp, value, attributes, resource_attributes",
            &metric_filter,
            time_filter.as_deref(),
            limit,
        ),
    })
}

pub async fn search(
    State(state): State<AppState>,
    Json(request): Json<TelemetrySearchRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let sql = compile_search_sql(&request).map_err(bad_request)?;
    let result = state
        .query_engine
        .execute_query(&sql)
        .await
        .map_err(storage_error)?;
    let rows = rows_to_search_response(&request.scope, &result.columns, &result.rows);

    Ok(Json(json!({
        "version": 1,
        "scope": match request.scope {
            TelemetrySearchScope::Sessions => "sessions",
            TelemetrySearchScope::Traces => "traces",
        },
        "columns": selected_columns(&request.columns),
        "rows": rows,
        "nextCursor": Value::Null,
        "query": { "compiled": false }
    })))
}

pub async fn session_details(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    details_for_target(
        state,
        TelemetryDetailsTarget {
            kind: "session".to_string(),
            id: session_id,
        },
        time_range_from_query(&params),
        1000,
    )
    .await
}

pub async fn trace_details(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    details_for_target(
        state,
        TelemetryDetailsTarget {
            kind: "trace".to_string(),
            id: trace_id,
        },
        time_range_from_query(&params),
        1000,
    )
    .await
}

pub async fn details_post(
    State(state): State<AppState>,
    Json(request): Json<TelemetryDetailsRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if request.version != 1 {
        return Err(bad_request(
            "unsupported telemetry details version".to_string(),
        ));
    }
    details_for_target(
        state,
        request.target,
        request.time_range,
        request.limit.unwrap_or(1000),
    )
    .await
}

pub async fn fields() -> Json<Value> {
    Json(json!({
        "version": 1,
        "fields": SEARCH_FIELDS.iter().map(|field| json!({
            "key": field.key,
            "type": field.value_type,
            "entity": field.entity,
            "filterable": field.filterable,
            "sortable": field.sortable,
            "projectable": field.projectable,
            "operators": field.ops,
        })).collect::<Vec<_>>()
    }))
}

pub async fn field_values(
    State(state): State<AppState>,
    Path(field): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let spec = field_spec(&field).ok_or_else(|| bad_request("unknown field".to_string()))?;
    if !spec.filterable {
        return Err(bad_request("field is not filterable".to_string()));
    }
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(500)
        .clamp(1, 10_000);
    let sql = format!(
        "SELECT DISTINCT {field_sql} AS value FROM union_spans WHERE {field_sql} IS NOT NULL ORDER BY value ASC LIMIT {limit}",
        field_sql = spec.sql
    );
    let result = state
        .query_engine
        .execute_query(&sql)
        .await
        .map_err(storage_error)?;
    let values = result
        .rows
        .iter()
        .filter_map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    Ok(Json(
        json!({ "version": 1, "field": field, "values": values }),
    ))
}

async fn details_for_target(
    state: AppState,
    target: TelemetryDetailsTarget,
    time_range: Option<TelemetryTimeRange>,
    limit: usize,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let compiled = compile_details_sql(&target, time_range.as_ref(), limit).map_err(bad_request)?;
    let spans = execute_objects(&state, &compiled.spans).await?;
    let logs = execute_objects(&state, &compiled.logs).await?;
    let metrics = execute_objects(&state, &compiled.metrics).await?;
    let summary = json!({
        "spanCount": spans.len(),
        "logCount": logs.len(),
        "metricCount": metrics.len(),
        "traceCount": distinct_count(&spans, "trace_id"),
        "errorCount": spans.iter().filter(|row| is_error_span(row)).count(),
        "services": distinct_strings(&spans, "app_id"),
    });

    Ok(Json(json!({
        "version": 1,
        "kind": target.kind,
        "id": target.id,
        "timeRange": time_range,
        "summary": summary,
        "spans": spans,
        "logs": logs,
        "metrics": metrics,
    })))
}

async fn execute_objects(
    state: &AppState,
    sql: &str,
) -> Result<Vec<Value>, (StatusCode, Json<Value>)> {
    let result = state
        .query_engine
        .execute_query(sql)
        .await
        .map_err(storage_error)?;
    Ok(rows_to_objects(&result.columns, &result.rows))
}

fn compile_order(sort: &[TelemetrySort]) -> Result<String, String> {
    if sort.is_empty() {
        return Ok("ORDER BY end_time DESC".to_string());
    }
    let mut parts = Vec::new();
    for sort_item in sort {
        let sql = match sort_item.field.as_str() {
            "timestamp" => "end_time",
            "duration_ms" => "duration_ms",
            "error_count" => "error_count",
            "span_count" => "span_count",
            "trace_count" => "trace_count",
            other => field_spec(other)
                .filter(|field| field.sortable)
                .map(|field| field.sql)
                .ok_or_else(|| format!("unsupported sort field: {other}"))?,
        };
        let dir = match sort_item.direction {
            TelemetrySortDirection::Asc => "ASC",
            TelemetrySortDirection::Desc => "DESC",
        };
        parts.push(format!("{sql} {dir}"));
    }
    Ok(format!("ORDER BY {}", parts.join(", ")))
}

fn compile_filter_expr(expr: &TelemetryFilterExpr) -> Result<String, String> {
    match expr {
        TelemetryFilterExpr::And { and } => compile_compound("AND", and),
        TelemetryFilterExpr::Or { or } => compile_compound("OR", or),
        TelemetryFilterExpr::Predicate(filter) => compile_filter(filter),
    }
}

fn compile_compound(joiner: &str, exprs: &[TelemetryFilterExpr]) -> Result<String, String> {
    if exprs.is_empty() {
        return Err("compound filter cannot be empty".to_string());
    }
    let parts = exprs
        .iter()
        .map(compile_filter_expr)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("({})", parts.join(&format!(" {joiner} "))))
}

fn compile_filter(filter: &TelemetryFilter) -> Result<String, String> {
    let spec = field_spec(&filter.field)
        .filter(|field| field.filterable)
        .ok_or_else(|| format!("unknown filter field: {}", filter.field))?;
    if !spec.ops.contains(&filter.op.as_str()) {
        return Err(format!(
            "operator {} is not allowed for field {}",
            filter.op, filter.field
        ));
    }
    let sql = spec.sql;
    match filter.op.as_str() {
        "exists" => Ok(format!("{sql} IS NOT NULL")),
        "eq" => Ok(format!(
            "{sql} = {}",
            scalar_literal(required_value(filter)?)
        )),
        "neq" => Ok(format!(
            "{sql} <> {}",
            scalar_literal(required_value(filter)?)
        )),
        "lt" => Ok(format!(
            "{sql} < {}",
            scalar_literal(required_value(filter)?)
        )),
        "lte" => Ok(format!(
            "{sql} <= {}",
            scalar_literal(required_value(filter)?)
        )),
        "gt" => Ok(format!(
            "{sql} > {}",
            scalar_literal(required_value(filter)?)
        )),
        "gte" => Ok(format!(
            "{sql} >= {}",
            scalar_literal(required_value(filter)?)
        )),
        "prefix" => Ok(format!(
            "{sql} LIKE {}",
            sql_string_literal(&format!("{}%", string_value(required_value(filter)?)?))
        )),
        "contains" => Ok(format!(
            "{sql} LIKE {}",
            sql_string_literal(&format!("%{}%", string_value(required_value(filter)?)?))
        )),
        "in" | "not_in" => {
            let values = required_value(filter)?
                .as_array()
                .ok_or_else(|| "in/not_in requires an array value".to_string())?;
            if values.is_empty() {
                return Err("in/not_in requires at least one value".to_string());
            }
            let literals = values
                .iter()
                .map(scalar_literal)
                .collect::<Vec<_>>()
                .join(", ");
            let op = if filter.op == "in" { "IN" } else { "NOT IN" };
            Ok(format!("{sql} {op} ({literals})"))
        }
        _ => Err("unsupported operator".to_string()),
    }
}

fn field_spec(key: &str) -> Option<FieldSpec> {
    SEARCH_FIELDS.iter().copied().find(|field| field.key == key)
}

fn required_value(filter: &TelemetryFilter) -> Result<&Value, String> {
    filter
        .value
        .as_ref()
        .ok_or_else(|| format!("operator {} requires value", filter.op))
}

fn string_value(value: &Value) -> Result<String, String> {
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| "operator requires a string value".to_string())
}

fn scalar_literal(value: &Value) -> String {
    match value {
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::String(s) => sql_string_literal(s),
        _ => sql_string_literal(&value.to_string()),
    }
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn timestamp_literal(value: &str) -> String {
    format!("{}::TIMESTAMPTZ", sql_string_literal(value))
}

fn has_exact_identifier_filter(expr: Option<&TelemetryFilterExpr>) -> bool {
    match expr {
        Some(TelemetryFilterExpr::Predicate(filter)) => {
            matches!(filter.field.as_str(), "session_id" | "trace_id") && filter.op == "eq"
        }
        Some(TelemetryFilterExpr::And { and }) => and
            .iter()
            .any(|child| has_exact_identifier_filter(Some(child))),
        Some(TelemetryFilterExpr::Or { or }) => or
            .iter()
            .any(|child| has_exact_identifier_filter(Some(child))),
        None => false,
    }
}

fn detail_sql(
    table: &str,
    columns: &str,
    id_filter: &str,
    time_filter: Option<&str>,
    limit: usize,
) -> String {
    let where_sql = match time_filter {
        Some(time) => format!("{id_filter} AND {time}"),
        None => id_filter.to_string(),
    };
    format!("SELECT {columns} FROM {table} WHERE {where_sql} ORDER BY timestamp ASC LIMIT {limit}")
}

fn time_range_from_query(params: &HashMap<String, String>) -> Option<TelemetryTimeRange> {
    Some(TelemetryTimeRange {
        from: params.get("from")?.clone(),
        to: params.get("to")?.clone(),
    })
}

fn rows_to_objects(columns: &[String], rows: &[Vec<Value>]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            let mut object = Map::new();
            for (idx, column) in columns.iter().enumerate() {
                object.insert(column.clone(), row.get(idx).cloned().unwrap_or(Value::Null));
            }
            Value::Object(object)
        })
        .collect()
}

fn rows_to_search_response(
    scope: &TelemetrySearchScope,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Vec<Value> {
    rows_to_objects(columns, rows)
        .into_iter()
        .map(|row| {
            let id = row.get("id").cloned().unwrap_or(Value::Null);
            let services = row
                .get("services")
                .and_then(Value::as_str)
                .map(|s| {
                    s.split(',')
                        .filter(|part| !part.is_empty())
                        .map(|part| Value::String(part.to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let summary = match scope {
                TelemetrySearchScope::Sessions => json!({
                    "sessionId": row.get("session_id").cloned().unwrap_or(Value::Null),
                    "traceCount": numeric_cell(row.get("trace_count")),
                    "spanCount": numeric_cell(row.get("span_count")),
                    "logCount": 0,
                    "metricCount": 0,
                    "errorCount": numeric_cell(row.get("error_count")),
                    "durationMs": numeric_cell(row.get("duration_ms")),
                    "services": services,
                    "entryPath": row.get("entry_path").cloned().unwrap_or(Value::Null),
                    "lastError": row.get("last_error").cloned().unwrap_or(Value::Null),
                }),
                TelemetrySearchScope::Traces => json!({
                    "sessionId": row.get("session_id").cloned().unwrap_or(Value::Null),
                    "traceId": row.get("trace_id").cloned().unwrap_or(Value::Null),
                    "spanCount": numeric_cell(row.get("span_count")),
                    "errorCount": numeric_cell(row.get("error_count")),
                    "durationMs": numeric_cell(row.get("duration_ms")),
                    "services": services,
                    "name": row.get("name").cloned().unwrap_or(Value::Null),
                    "entryPath": row.get("entry_path").cloned().unwrap_or(Value::Null),
                    "lastError": row.get("last_error").cloned().unwrap_or(Value::Null),
                }),
            };
            json!({
                "id": id,
                "kind": match scope {
                    TelemetrySearchScope::Sessions => "session",
                    TelemetrySearchScope::Traces => "trace",
                },
                "timeRange": {
                    "from": row.get("start_time").cloned().unwrap_or(Value::Null),
                    "to": row.get("end_time").cloned().unwrap_or(Value::Null),
                },
                "summary": summary,
                "cells": row,
            })
        })
        .collect()
}

fn numeric_cell(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Number(_)) => value.cloned().unwrap(),
        Some(Value::String(s)) => s
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| s.parse::<f64>().map(Value::from))
            .unwrap_or(Value::Null),
        _ => json!(0),
    }
}

fn selected_columns(columns: &[String]) -> Vec<Value> {
    let keys = if columns.is_empty() {
        vec![
            "session_id".to_string(),
            "trace_count".to_string(),
            "span_count".to_string(),
        ]
    } else {
        columns.to_vec()
    };
    keys.into_iter()
        .map(|key| {
            json!({
                "key": key,
                "type": field_spec(&key).map(|field| field.value_type).unwrap_or("dynamic"),
                "label": key,
            })
        })
        .collect()
}

fn distinct_count(rows: &[Value], key: &str) -> usize {
    distinct_strings(rows, key).len()
}

fn distinct_strings(rows: &[Value], key: &str) -> Vec<String> {
    let mut values = rows
        .iter()
        .filter_map(|row| row.get(key).and_then(Value::as_str))
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn is_error_span(row: &Value) -> bool {
    row.get("status_code").and_then(Value::as_str) == Some("ERROR")
        || row
            .get("http_response_status_code")
            .and_then(Value::as_i64)
            .is_some_and(|status| status >= 500)
}

fn bad_request(message: String) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": { "code": "bad_request", "message": message } })),
    )
}

fn storage_error(err: anyhow::Error) -> (StatusCode, Json<Value>) {
    warn!("telemetry query failed: {}", err);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": { "code": "storage_error", "message": err.to_string() } })),
    )
}
