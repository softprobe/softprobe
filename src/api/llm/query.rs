use crate::api::sql_support::{
    cursor_predicate, encode_cursor, push_optional_time_bounds, sql_string_literal,
    timestamp_literal,
};
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::models::{Score, ScoreDataType, ScoreSource};
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeSet, HashMap};
use tracing::warn;

const DEFAULT_SEARCH_LIMIT: usize = 50;
const DEFAULT_TRACE_LIMIT: usize = 100;
const DEFAULT_SESSION_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;

type ApiError = (StatusCode, Json<Value>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationSearchRequest {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    #[serde(default)]
    pub observation_types: Vec<String>,
    pub model_name: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub trace_id: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationSearchResponse {
    pub items: Vec<ObservationSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationSummary {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub session_id: Option<String>,
    pub name: String,
    pub observation_type: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status_code: Option<String>,
    pub model_name: Option<String>,
    pub model_provider: Option<String>,
    pub user_id: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationDetail {
    #[serde(flatten)]
    pub summary: ObservationSummary,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    #[serde(default)]
    pub events: Vec<Value>,
    #[serde(default)]
    pub scores: Vec<Score>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSummary {
    pub trace_id: String,
    pub session_id: Option<String>,
    pub name: Option<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub observation_count: i64,
    pub error_count: i64,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceDetail {
    pub trace: TraceSummary,
    pub observations: Vec<ObservationDetail>,
    pub scores: Vec<Score>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDetail {
    pub session_id: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub trace_count: i64,
    pub observation_count: i64,
    #[serde(default)]
    pub user_ids: Vec<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub traces: Vec<TraceSummary>,
    pub scores: Vec<Score>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DetailQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionQuery {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

pub async fn search_observations(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Json(request): Json<ObservationSearchRequest>,
) -> Result<Json<ObservationSearchResponse>, ApiError> {
    let sql = compile_observation_search_sql(&request).map_err(bad_request)?;
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let result = state
        .execute_tenant_scoped_sql(tenant_ref, &sql)
        .await
        .map_err(storage_error)?;

    let limit = clamp_limit(request.limit, DEFAULT_SEARCH_LIMIT);
    let mut summaries = result
        .rows
        .iter()
        .filter_map(|row| map_observation_summary(&result.columns, row))
        .collect::<Vec<_>>();
    let next_cursor = next_cursor_from_summaries(&mut summaries, limit);
    Ok(Json(ObservationSearchResponse {
        items: summaries,
        next_cursor,
    }))
}

pub async fn get_observation(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(span_id): Path<String>,
    Query(params): Query<DetailQuery>,
) -> Result<Json<ObservationDetail>, ApiError> {
    if span_id.trim().is_empty() {
        return Err(bad_request("span_id is required".to_string()));
    }
    let sql =
        compile_observation_detail_sql(&span_id, params.from, params.to).map_err(bad_request)?;
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let result = state
        .execute_tenant_scoped_sql(tenant_ref, &sql)
        .await
        .map_err(storage_error)?;
    let row = result.rows.first().ok_or_else(not_found)?;
    let mut detail = map_observation_detail(&result.columns, row).ok_or_else(not_found)?;
    detail.scores =
        query_scores(&state, tenant_ref, &compile_scores_for_span_sql(&span_id)).await?;
    Ok(Json(detail))
}

pub async fn get_trace(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(trace_id): Path<String>,
    Query(params): Query<DetailQuery>,
) -> Result<Json<TraceDetail>, ApiError> {
    if trace_id.trim().is_empty() {
        return Err(bad_request("trace_id is required".to_string()));
    }
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let summary_sql =
        compile_trace_summary_sql(&trace_id, params.from, params.to).map_err(bad_request)?;
    let summary_result = state
        .execute_tenant_scoped_sql(tenant_ref, &summary_sql)
        .await
        .map_err(storage_error)?;
    let summary_row = summary_result.rows.first().ok_or_else(not_found)?;
    let trace = map_trace_summary(&summary_result.columns, summary_row).ok_or_else(not_found)?;

    let limit = clamp_limit(params.limit, DEFAULT_TRACE_LIMIT);
    let obs_sql = compile_trace_observations_sql(
        &trace_id,
        params.from,
        params.to,
        limit,
        params.cursor.as_deref(),
    )
    .map_err(bad_request)?;
    let obs_result = state
        .execute_tenant_scoped_sql(tenant_ref, &obs_sql)
        .await
        .map_err(storage_error)?;
    let mut observations = obs_result
        .rows
        .iter()
        .filter_map(|row| map_observation_detail(&obs_result.columns, row))
        .collect::<Vec<_>>();
    let next_cursor = next_cursor_from_details(&mut observations, limit);

    let span_ids = observations
        .iter()
        .map(|obs| obs.summary.span_id.clone())
        .collect::<Vec<_>>();
    let scores = query_scores(
        &state,
        tenant_ref,
        &compile_scores_for_trace_sql(&trace_id, &span_ids),
    )
    .await?;

    // Attach span-level scores onto observation details; keep full set on the response.
    let mut by_span: HashMap<String, Vec<Score>> = HashMap::new();
    for score in &scores {
        if let Some(span_id) = &score.span_id {
            by_span
                .entry(span_id.clone())
                .or_default()
                .push(score.clone());
        }
    }
    for observation in &mut observations {
        observation.scores = by_span
            .remove(&observation.summary.span_id)
            .unwrap_or_default();
    }

    Ok(Json(TraceDetail {
        trace,
        observations,
        scores,
        next_cursor,
    }))
}

pub async fn get_session(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(session_id): Path<String>,
    Query(params): Query<SessionQuery>,
) -> Result<Json<SessionDetail>, ApiError> {
    if session_id.trim().is_empty() {
        return Err(bad_request("session_id is required".to_string()));
    }
    if params.from > params.to {
        return Err(bad_request("`from` must be <= `to`".to_string()));
    }
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let agg_sql =
        compile_session_aggregate_sql(&session_id, params.from, params.to).map_err(bad_request)?;
    let agg_result = state
        .execute_tenant_scoped_sql(tenant_ref, &agg_sql)
        .await
        .map_err(storage_error)?;
    let agg_row = agg_result.rows.first().ok_or_else(not_found)?;
    let aggregate = map_session_aggregate(&agg_result.columns, agg_row).ok_or_else(not_found)?;
    if aggregate.observation_count == 0 {
        return Err(not_found());
    }

    let limit = clamp_limit(params.limit, DEFAULT_SESSION_LIMIT);
    let traces_sql = compile_session_traces_sql(
        &session_id,
        params.from,
        params.to,
        limit,
        params.cursor.as_deref(),
    )
    .map_err(bad_request)?;
    let traces_result = state
        .execute_tenant_scoped_sql(tenant_ref, &traces_sql)
        .await
        .map_err(storage_error)?;
    let mut traces = traces_result
        .rows
        .iter()
        .filter_map(|row| map_trace_summary(&traces_result.columns, row))
        .collect::<Vec<_>>();
    let next_cursor = next_cursor_from_traces(&mut traces, limit);

    let trace_ids = traces
        .iter()
        .map(|trace| trace.trace_id.clone())
        .collect::<Vec<_>>();
    let scores = query_scores(
        &state,
        tenant_ref,
        &compile_scores_for_session_sql(&session_id, &trace_ids),
    )
    .await?;

    Ok(Json(SessionDetail {
        session_id,
        from: params.from,
        to: params.to,
        trace_count: aggregate.trace_count,
        observation_count: aggregate.observation_count,
        user_ids: aggregate.user_ids,
        input_tokens: aggregate.input_tokens,
        output_tokens: aggregate.output_tokens,
        total_tokens: aggregate.total_tokens,
        total_cost: aggregate.total_cost,
        traces,
        scores,
        next_cursor,
    }))
}

pub fn compile_observation_search_sql(
    request: &ObservationSearchRequest,
) -> Result<String, String> {
    if request.from > request.to {
        return Err("`from` must be <= `to`".to_string());
    }
    let limit = clamp_limit(request.limit, DEFAULT_SEARCH_LIMIT);
    let mut conditions = vec![format!(
        "timestamp >= {} AND timestamp <= {}",
        timestamp_literal(&request.from),
        timestamp_literal(&request.to)
    )];

    if !request.observation_types.is_empty() {
        let values = request
            .observation_types
            .iter()
            .map(|value| sql_string_literal(value))
            .collect::<Vec<_>>()
            .join(", ");
        conditions.push(format!(
            "COALESCE(attributes['sp.observation.type'], 'span') IN ({values})"
        ));
    }
    if let Some(model_name) = &request.model_name {
        conditions.push(format!(
            "attributes['gen_ai.request.model'] = {}",
            sql_string_literal(model_name)
        ));
    }
    if let Some(user_id) = &request.user_id {
        conditions.push(format!(
            "(attributes['sp.user.id'] = {id} OR attributes['enduser.id'] = {id})",
            id = sql_string_literal(user_id)
        ));
    }
    if let Some(session_id) = &request.session_id {
        conditions.push(format!("session_id = {}", sql_string_literal(session_id)));
    }
    if let Some(trace_id) = &request.trace_id {
        conditions.push(format!("trace_id = {}", sql_string_literal(trace_id)));
    }
    if let Some(cursor) = &request.cursor {
        conditions.push(cursor_predicate(cursor, "timestamp", "span_id")?);
    }

    let where_sql = conditions.join(" AND ");
    Ok(format!(
        "SELECT {projection} FROM union_spans WHERE {where_sql} ORDER BY timestamp DESC, span_id DESC LIMIT {fetch}",
        projection = observation_projection(false),
        fetch = limit + 1
    ))
}

pub fn compile_observation_detail_sql(
    span_id: &str,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Result<String, String> {
    let mut conditions = vec![format!("span_id = {}", sql_string_literal(span_id))];
    push_optional_time_bounds(&mut conditions, from, to)?;
    Ok(format!(
        "SELECT {projection} FROM union_spans WHERE {where_sql} LIMIT 1",
        projection = observation_projection(true),
        where_sql = conditions.join(" AND ")
    ))
}

pub fn compile_trace_summary_sql(
    trace_id: &str,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Result<String, String> {
    let mut conditions = vec![format!("trace_id = {}", sql_string_literal(trace_id))];
    push_optional_time_bounds(&mut conditions, from, to)?;
    Ok(format!(
        "SELECT {projection} FROM union_spans WHERE {where_sql} GROUP BY trace_id",
        projection = trace_summary_projection(),
        where_sql = conditions.join(" AND ")
    ))
}

pub fn compile_trace_observations_sql(
    trace_id: &str,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    limit: usize,
    cursor: Option<&str>,
) -> Result<String, String> {
    let mut conditions = vec![format!("trace_id = {}", sql_string_literal(trace_id))];
    push_optional_time_bounds(&mut conditions, from, to)?;
    if let Some(cursor) = cursor {
        conditions.push(cursor_predicate(cursor, "timestamp", "span_id")?);
    }
    Ok(format!(
        "SELECT {projection} FROM union_spans WHERE {where_sql} ORDER BY timestamp DESC, span_id DESC LIMIT {fetch}",
        projection = observation_projection(true),
        where_sql = conditions.join(" AND "),
        fetch = limit + 1
    ))
}

pub fn compile_session_aggregate_sql(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String, String> {
    if from > to {
        return Err("`from` must be <= `to`".to_string());
    }
    Ok(format!(
        "SELECT \
            COUNT(DISTINCT trace_id) AS trace_count, \
            COUNT(*) AS observation_count, \
            SUM({input_tokens}) AS input_tokens, \
            SUM({output_tokens}) AS output_tokens, \
            SUM({total_tokens}) AS total_tokens, \
            SUM({total_cost}) AS total_cost, \
            list(DISTINCT {user_id}) AS user_ids \
         FROM union_spans \
         WHERE session_id = {session} \
           AND timestamp >= {from_ts} \
           AND timestamp <= {to_ts}",
        input_tokens = expr_input_tokens(),
        output_tokens = expr_output_tokens(),
        total_tokens = expr_total_tokens(),
        total_cost = expr_total_cost(),
        user_id = expr_user_id(),
        session = sql_string_literal(session_id),
        from_ts = timestamp_literal(&from),
        to_ts = timestamp_literal(&to),
    ))
}

pub fn compile_session_traces_sql(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
    cursor: Option<&str>,
) -> Result<String, String> {
    if from > to {
        return Err("`from` must be <= `to`".to_string());
    }
    let where_sql = format!(
        "session_id = {} AND timestamp >= {} AND timestamp <= {}",
        sql_string_literal(session_id),
        timestamp_literal(&from),
        timestamp_literal(&to)
    );
    // Cursor applies to aggregated start_time/trace_id, so filter after GROUP BY.
    let inner = format!(
        "SELECT {projection} FROM union_spans WHERE {where_sql} GROUP BY trace_id",
        projection = trace_summary_projection(),
    );
    let outer_cursor = if let Some(cursor) = cursor {
        format!(
            " WHERE {}",
            cursor_predicate(cursor, "start_time", "trace_id")?
        )
    } else {
        String::new()
    };
    Ok(format!(
        "SELECT * FROM ({inner}) AS t{outer_cursor} ORDER BY start_time DESC, trace_id DESC LIMIT {fetch}",
        fetch = limit + 1
    ))
}

pub fn compile_scores_for_span_sql(span_id: &str) -> String {
    format!(
        "SELECT {cols} FROM scores WHERE span_id = {span} ORDER BY timestamp DESC, score_id DESC",
        cols = score_columns(),
        span = sql_string_literal(span_id)
    )
}

pub fn compile_scores_for_trace_sql(trace_id: &str, span_ids: &[String]) -> String {
    let mut conditions = vec![format!("trace_id = {}", sql_string_literal(trace_id))];
    if !span_ids.is_empty() {
        let values = span_ids
            .iter()
            .map(|id| sql_string_literal(id))
            .collect::<Vec<_>>()
            .join(", ");
        conditions.push(format!("span_id IN ({values})"));
    }
    format!(
        "SELECT {cols} FROM scores WHERE ({predicate}) ORDER BY timestamp DESC, score_id DESC",
        cols = score_columns(),
        predicate = conditions.join(" OR ")
    )
}

pub fn compile_scores_for_session_sql(session_id: &str, trace_ids: &[String]) -> String {
    let mut conditions = vec![format!("session_id = {}", sql_string_literal(session_id))];
    if !trace_ids.is_empty() {
        let values = trace_ids
            .iter()
            .map(|id| sql_string_literal(id))
            .collect::<Vec<_>>()
            .join(", ");
        conditions.push(format!("trace_id IN ({values})"));
    }
    format!(
        "SELECT {cols} FROM scores WHERE ({predicate}) ORDER BY timestamp DESC, score_id DESC",
        cols = score_columns(),
        predicate = conditions.join(" OR ")
    )
}

fn observation_projection(include_payload: bool) -> String {
    let mut cols = vec![
        "trace_id".to_string(),
        "span_id".to_string(),
        "parent_span_id".to_string(),
        "NULLIF(session_id, '') AS session_id".to_string(),
        "message_type AS name".to_string(),
        format!("COALESCE(attributes['sp.observation.type'], 'span') AS observation_type"),
        "timestamp AS start_time".to_string(),
        "end_timestamp AS end_time".to_string(),
        "status_code".to_string(),
        "attributes['gen_ai.request.model'] AS model_name".to_string(),
        "attributes['gen_ai.provider.name'] AS model_provider".to_string(),
        format!("{} AS user_id", expr_user_id()),
        format!("{} AS input_tokens", expr_input_tokens()),
        format!("{} AS output_tokens", expr_output_tokens()),
        format!("{} AS total_tokens", expr_total_tokens()),
        format!("{} AS total_cost", expr_total_cost()),
    ];
    if include_payload {
        cols.push("attributes".to_string());
        cols.push("events".to_string());
    }
    cols.join(", ")
}

fn trace_summary_projection() -> String {
    format!(
        "trace_id, \
         any_value(NULLIF(session_id, '')) AS session_id, \
         any_value(message_type) AS name, \
         MIN(timestamp) AS start_time, \
         MAX(COALESCE(end_timestamp, timestamp)) AS end_time, \
         COUNT(*)::BIGINT AS observation_count, \
         SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END)::BIGINT AS error_count, \
         SUM({input_tokens})::BIGINT AS input_tokens, \
         SUM({output_tokens})::BIGINT AS output_tokens, \
         SUM({total_tokens})::BIGINT AS total_tokens, \
         SUM({total_cost}) AS total_cost, \
         any_value({user_id}) AS user_id",
        input_tokens = expr_input_tokens(),
        output_tokens = expr_output_tokens(),
        total_tokens = expr_total_tokens(),
        total_cost = expr_total_cost(),
        user_id = expr_user_id(),
    )
}

fn expr_user_id() -> &'static str {
    "COALESCE(attributes['sp.user.id'], attributes['enduser.id'])"
}

fn expr_input_tokens() -> &'static str {
    "try_cast(attributes['gen_ai.usage.input_tokens'] AS BIGINT)"
}

fn expr_output_tokens() -> &'static str {
    "try_cast(attributes['gen_ai.usage.output_tokens'] AS BIGINT)"
}

fn expr_total_tokens() -> &'static str {
    "try_cast(attributes['gen_ai.usage.total_tokens'] AS BIGINT)"
}

fn expr_total_cost() -> &'static str {
    "try_cast(attributes['sp.cost.total'] AS DOUBLE)"
}

fn score_columns() -> &'static str {
    "score_id, timestamp, trace_id, span_id, session_id, name, data_type, numeric_value, string_value, boolean_value, source, comment, config_id, author_id, metadata, record_date"
}

fn clamp_limit(limit: Option<usize>, default: usize) -> usize {
    limit.unwrap_or(default).clamp(1, MAX_LIMIT)
}

async fn query_scores(
    state: &AppState,
    tenant: Option<&TenantInfo>,
    sql: &str,
) -> Result<Vec<Score>, ApiError> {
    let result = state
        .execute_tenant_scoped_sql(tenant, sql)
        .await
        .map_err(storage_error)?;
    Ok(result
        .rows
        .iter()
        .filter_map(|row| map_score(&result.columns, row))
        .collect())
}

fn map_observation_summary(columns: &[String], row: &[Value]) -> Option<ObservationSummary> {
    Some(ObservationSummary {
        trace_id: required_string(columns, row, "trace_id")?,
        span_id: required_string(columns, row, "span_id")?,
        parent_span_id: optional_string(columns, row, "parent_span_id"),
        session_id: optional_string(columns, row, "session_id"),
        name: required_string(columns, row, "name").unwrap_or_default(),
        observation_type: required_string(columns, row, "observation_type")
            .unwrap_or_else(|| "span".to_string()),
        start_time: required_timestamp(columns, row, "start_time")?,
        end_time: optional_timestamp(columns, row, "end_time"),
        status_code: optional_string(columns, row, "status_code"),
        model_name: optional_string(columns, row, "model_name"),
        model_provider: optional_string(columns, row, "model_provider"),
        user_id: optional_string(columns, row, "user_id"),
        input_tokens: optional_i64(columns, row, "input_tokens"),
        output_tokens: optional_i64(columns, row, "output_tokens"),
        total_tokens: optional_i64(columns, row, "total_tokens"),
        total_cost: optional_f64(columns, row, "total_cost"),
    })
}

fn map_observation_detail(columns: &[String], row: &[Value]) -> Option<ObservationDetail> {
    let summary = map_observation_summary(columns, row)?;
    Some(ObservationDetail {
        summary,
        attributes: map_string_map(column_value(columns, row, "attributes")),
        events: map_events(column_value(columns, row, "events")),
        scores: Vec::new(),
    })
}

fn map_trace_summary(columns: &[String], row: &[Value]) -> Option<TraceSummary> {
    Some(TraceSummary {
        trace_id: required_string(columns, row, "trace_id")?,
        session_id: optional_string(columns, row, "session_id"),
        name: optional_string(columns, row, "name"),
        start_time: required_timestamp(columns, row, "start_time")?,
        end_time: required_timestamp(columns, row, "end_time")?,
        observation_count: optional_i64(columns, row, "observation_count").unwrap_or(0),
        error_count: optional_i64(columns, row, "error_count").unwrap_or(0),
        input_tokens: optional_i64(columns, row, "input_tokens"),
        output_tokens: optional_i64(columns, row, "output_tokens"),
        total_tokens: optional_i64(columns, row, "total_tokens"),
        total_cost: optional_f64(columns, row, "total_cost"),
        user_id: optional_string(columns, row, "user_id"),
    })
}

struct SessionAggregate {
    trace_count: i64,
    observation_count: i64,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    total_tokens: Option<i64>,
    total_cost: Option<f64>,
    user_ids: Vec<String>,
}

fn map_session_aggregate(columns: &[String], row: &[Value]) -> Option<SessionAggregate> {
    let user_ids = match column_value(columns, row, "user_ids") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        _ => Vec::new(),
    };
    Some(SessionAggregate {
        trace_count: optional_i64(columns, row, "trace_count").unwrap_or(0),
        observation_count: optional_i64(columns, row, "observation_count").unwrap_or(0),
        input_tokens: optional_i64(columns, row, "input_tokens"),
        output_tokens: optional_i64(columns, row, "output_tokens"),
        total_tokens: optional_i64(columns, row, "total_tokens"),
        total_cost: optional_f64(columns, row, "total_cost"),
        user_ids,
    })
}

fn map_score(columns: &[String], row: &[Value]) -> Option<Score> {
    let data_type = match optional_string(columns, row, "data_type")
        .unwrap_or_default()
        .as_str()
    {
        "numeric" => ScoreDataType::Numeric,
        "categorical" => ScoreDataType::Categorical,
        "boolean" => ScoreDataType::Boolean,
        "text" => ScoreDataType::Text,
        _ => return None,
    };
    let source = match optional_string(columns, row, "source")
        .unwrap_or_default()
        .as_str()
    {
        "api" => ScoreSource::Api,
        "user" => ScoreSource::User,
        "evaluator" => ScoreSource::Evaluator,
        "annotation" => ScoreSource::Annotation,
        _ => return None,
    };
    let timestamp = required_timestamp(columns, row, "timestamp")?;
    let record_date = optional_string(columns, row, "record_date")
        .and_then(|value| NaiveDate::parse_from_str(&value, "%Y-%m-%d").ok())
        .unwrap_or_else(|| timestamp.date_naive());
    Some(Score {
        score_id: required_string(columns, row, "score_id")?,
        timestamp,
        trace_id: optional_string(columns, row, "trace_id"),
        span_id: optional_string(columns, row, "span_id"),
        session_id: optional_string(columns, row, "session_id"),
        name: required_string(columns, row, "name")?,
        data_type,
        numeric_value: optional_f64(columns, row, "numeric_value"),
        string_value: optional_string(columns, row, "string_value"),
        boolean_value: column_value(columns, row, "boolean_value").and_then(|v| v.as_bool()),
        source,
        comment: optional_string(columns, row, "comment"),
        config_id: optional_string(columns, row, "config_id"),
        author_id: optional_string(columns, row, "author_id"),
        metadata: map_string_map(column_value(columns, row, "metadata")),
        record_date,
    })
}

fn next_cursor_from_summaries(items: &mut Vec<ObservationSummary>, limit: usize) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.truncate(limit);
    items
        .last()
        .map(|item| encode_cursor(item.start_time, &item.span_id))
}

fn next_cursor_from_details(items: &mut Vec<ObservationDetail>, limit: usize) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.truncate(limit);
    items
        .last()
        .map(|item| encode_cursor(item.summary.start_time, &item.summary.span_id))
}

fn next_cursor_from_traces(items: &mut Vec<TraceSummary>, limit: usize) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.truncate(limit);
    items
        .last()
        .map(|item| encode_cursor(item.start_time, &item.trace_id))
}

fn column_value<'a>(columns: &[String], row: &'a [Value], name: &str) -> Option<&'a Value> {
    let index = columns.iter().position(|column| column == name)?;
    row.get(index)
}

fn required_string(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    optional_string(columns, row, name).filter(|value| !value.is_empty())
}

fn optional_string(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    match column_value(columns, row, name)? {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        other => Some(other.to_string()),
    }
}

fn optional_i64(columns: &[String], row: &[Value], name: &str) -> Option<i64> {
    match column_value(columns, row, name)? {
        Value::Null => None,
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|v| v as i64)),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn optional_f64(columns: &[String], row: &[Value], name: &str) -> Option<f64> {
    match column_value(columns, row, name)? {
        Value::Null => None,
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn required_timestamp(columns: &[String], row: &[Value], name: &str) -> Option<DateTime<Utc>> {
    optional_timestamp(columns, row, name)
}

fn optional_timestamp(columns: &[String], row: &[Value], name: &str) -> Option<DateTime<Utc>> {
    let value = column_value(columns, row, name)?;
    parse_timestamp_value(value)
}

fn parse_timestamp_value(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::Null => None,
        Value::String(text) => parse_timestamp_text(text),
        Value::Number(number) => number
            .as_i64()
            .and_then(DateTime::from_timestamp_micros)
            .or_else(|| number.as_i64().and_then(DateTime::from_timestamp_millis)),
        _ => None,
    }
}

fn parse_timestamp_text(text: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Some(dt.with_timezone(&Utc));
    }
    // DuckDB JSON bridge emits "Microsecond:<epoch>" for TIMESTAMPTZ values.
    if let Some((unit, raw)) = text.split_once(':') {
        if let Ok(epoch) = raw.parse::<i64>() {
            return match unit {
                "Microsecond" | "\"Microsecond\"" => DateTime::from_timestamp_micros(epoch),
                "Millisecond" | "\"Millisecond\"" => DateTime::from_timestamp_millis(epoch),
                "Second" | "\"Second\"" => DateTime::from_timestamp(epoch, 0),
                "Nanosecond" | "\"Nanosecond\"" => {
                    DateTime::from_timestamp(epoch / 1_000_000_000, (epoch % 1_000_000_000) as u32)
                }
                _ => None,
            };
        }
    }
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
    }
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%z") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
    }
    None
}

fn map_string_map(value: Option<&Value>) -> HashMap<String, String> {
    let Some(Value::Object(map)) = value else {
        return HashMap::new();
    };
    map.iter()
        .filter_map(|(key, value)| {
            let normalized = normalize_map_key(key);
            match value {
                Value::Null => None,
                Value::String(text) => Some((normalized, text.clone())),
                other => Some((normalized, other.to_string())),
            }
        })
        .collect()
}

fn map_events(value: Option<&Value>) -> Vec<Value> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                Value::Object(map) => {
                    let mut normalized = Map::new();
                    for (key, value) in map {
                        let key = normalize_map_key(key);
                        if key == "attributes" {
                            normalized.insert(
                                key,
                                Value::Object(
                                    map_string_map(Some(value))
                                        .into_iter()
                                        .map(|(k, v)| (k, Value::String(v)))
                                        .collect(),
                                ),
                            );
                        } else if key == "timestamp" {
                            normalized.insert(
                                key,
                                parse_timestamp_value(value)
                                    .map(|dt| {
                                        Value::String(
                                            dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                                        )
                                    })
                                    .unwrap_or_else(|| value.clone()),
                            );
                        } else {
                            normalized.insert(key, value.clone());
                        }
                    }
                    Value::Object(normalized)
                }
                other => other.clone(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn normalize_map_key(key: &str) -> String {
    // DuckDB map keys are bridged via Debug formatting, e.g. Text("sp.user.id") or "sp.user.id".
    let trimmed = key
        .strip_prefix("Text(")
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or(key);
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\\\"", "\"")
    } else {
        trimmed.to_string()
    }
}

fn bad_request(message: String) -> ApiError {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message })))
}

fn not_found() -> ApiError {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" })))
}

fn storage_error(error: anyhow::Error) -> ApiError {
    warn!("llm query failed: {}", error);
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "error": "query unavailable" })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::sql_support::decode_cursor;

    #[test]
    fn search_sql_requires_time_bounds_and_escapes_literals() {
        let request = ObservationSearchRequest {
            from: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            observation_types: vec!["generation".to_string()],
            model_name: Some("gpt-4o'; DROP TABLE traces; --".to_string()),
            user_id: Some("user-1".to_string()),
            session_id: Some("sess-1".to_string()),
            trace_id: None,
            limit: Some(999),
            cursor: None,
        };
        let sql = compile_observation_search_sql(&request).expect("sql");
        assert!(sql.contains("timestamp >="));
        assert!(sql.contains("LIMIT 201"));
        assert!(sql.contains("gpt-4o''; DROP TABLE traces; --"));
        assert!(
            sql.contains("COALESCE(attributes['sp.observation.type'], 'span') IN ('generation')")
        );
        assert!(sql.contains("ORDER BY timestamp DESC, span_id DESC"));
    }

    #[test]
    fn search_sql_rejects_inverted_range() {
        let request = ObservationSearchRequest {
            from: DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            observation_types: vec![],
            model_name: None,
            user_id: None,
            session_id: None,
            trace_id: None,
            limit: None,
            cursor: None,
        };
        assert!(compile_observation_search_sql(&request).is_err());
    }

    #[test]
    fn cursor_round_trip_and_predicate() {
        let ts = DateTime::parse_from_rfc3339("2026-07-18T23:22:00.123Z")
            .unwrap()
            .with_timezone(&Utc);
        let encoded = encode_cursor(ts, "span-1");
        let decoded = decode_cursor(&encoded).expect("decode");
        assert_eq!(decoded.id, "span-1");
        assert_eq!(decoded.t, ts);
        assert!(decode_cursor("%%%not-base64%%%").is_err());
        let predicate = cursor_predicate(&encoded, "timestamp", "span_id").unwrap();
        assert!(predicate.contains("timestamp <"));
        assert!(predicate.contains("span_id <"));
    }

    #[test]
    fn score_sql_handles_missing_member_ids() {
        let sql = compile_scores_for_trace_sql("trace-1", &[]);
        assert!(sql.contains("trace_id = 'trace-1'"));
        assert!(!sql.contains("span_id IN"));
    }

    #[test]
    fn maps_observation_summary_from_row() {
        let columns = vec![
            "trace_id".into(),
            "span_id".into(),
            "parent_span_id".into(),
            "session_id".into(),
            "name".into(),
            "observation_type".into(),
            "start_time".into(),
            "end_time".into(),
            "status_code".into(),
            "model_name".into(),
            "model_provider".into(),
            "user_id".into(),
            "input_tokens".into(),
            "output_tokens".into(),
            "total_tokens".into(),
            "total_cost".into(),
        ];
        let row = vec![
            json!("trace-1"),
            json!("span-1"),
            Value::Null,
            json!("sess-1"),
            json!("chat"),
            json!("generation"),
            json!("Microsecond:1721349720000000"),
            Value::Null,
            json!("OK"),
            json!("gpt-4o"),
            json!("openai"),
            json!("user-1"),
            json!(10),
            json!(20),
            json!(30),
            json!(0.01),
        ];
        let summary = map_observation_summary(&columns, &row).expect("summary");
        assert_eq!(summary.observation_type, "generation");
        assert_eq!(summary.input_tokens, Some(10));
        assert_eq!(summary.model_name.as_deref(), Some("gpt-4o"));
    }

    #[test]
    fn normalizes_duckdb_debug_map_keys() {
        assert_eq!(
            normalize_map_key("Text(\"sp.observation.type\")"),
            "sp.observation.type"
        );
        assert_eq!(normalize_map_key("\"sp.user.id\""), "sp.user.id");
        assert_eq!(normalize_map_key("plain"), "plain");
    }
}
