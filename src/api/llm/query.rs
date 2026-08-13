use crate::api::sql_support::{
    cursor_predicate, encode_cursor, push_optional_time_bounds, sql_string_literal,
    timestamp_literal,
};
use crate::api::AppState;
use crate::authn::TenantInfo;
use crate::models::{Score, ScoreDataType, ScoreSource};
use crate::storage::schema::variant::{
    variant_as_json, variant_json_to_string_map, variant_try_cast, variant_varchar,
};
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

const RECORDING_EVENT_NAME: &str = "sp.recording.batch";
const RECORDING_EVENTS_ATTR: &str = "sp.recording.events";
const DEFAULT_RECORDING_LIMIT: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingBatch {
    pub span_id: String,
    pub trace_id: String,
    pub start_time: DateTime<Utc>,
    pub batch_index: Option<i64>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    pub events: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecording {
    pub session_id: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    /// True when the batch LIMIT was hit; more recording spans may exist.
    #[serde(default)]
    pub truncated: bool,
    pub batches: Vec<RecordingBatch>,
    pub events: Vec<Value>,
}

/// Fetch web session recording batches for a session (`sp.observation.type=recording`).
pub async fn get_session_recording(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(session_id): Path<String>,
    Query(params): Query<SessionQuery>,
) -> Result<Json<SessionRecording>, ApiError> {
    if session_id.trim().is_empty() {
        return Err(bad_request("session_id is required".to_string()));
    }
    if params.from > params.to {
        return Err(bad_request("`from` must be <= `to`".to_string()));
    }
    let limit = clamp_limit(params.limit, DEFAULT_RECORDING_LIMIT);
    let sql = compile_session_recording_sql(&session_id, params.from, params.to, limit)
        .map_err(bad_request)?;
    let tenant_ref = tenant.as_ref().map(|extension| &extension.0);
    let result = state
        .execute_tenant_scoped_sql(tenant_ref, &sql)
        .await
        .map_err(storage_error)?;

    let mut batches = result
        .rows
        .iter()
        .filter_map(|row| map_recording_batch(&result.columns, row))
        .collect::<Vec<_>>();
    // Prefer producer batch_index; fall back to start_time / span_id.
    batches.sort_by(|a, b| {
        a.batch_index
            .unwrap_or(0)
            .cmp(&b.batch_index.unwrap_or(0))
            .then_with(|| a.start_time.cmp(&b.start_time))
            .then_with(|| a.span_id.cmp(&b.span_id))
    });

    let truncated = batches.len() >= limit;
    let mut events = Vec::new();
    for batch in &batches {
        events.extend(batch.events.iter().cloned());
    }
    events.sort_by(|a, b| {
        event_timestamp(a)
            .cmp(&event_timestamp(b))
            .then_with(|| event_index(a).cmp(&event_index(b)))
    });

    Ok(Json(SessionRecording {
        session_id,
        from: params.from,
        to: params.to,
        truncated,
        batches,
        events,
    }))
}

pub fn compile_session_recording_sql(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
) -> Result<String, String> {
    if from > to {
        return Err("`from` must be <= `to`".to_string());
    }
    let obs_type = format!(
        "COALESCE({}, 'span')",
        variant_varchar("attributes", "sp.observation.type")
    );
    Ok(format!(
        "SELECT {projection} FROM union_spans \
         WHERE session_id = {session} \
           AND timestamp >= {from_ts} \
           AND timestamp <= {to_ts} \
           AND {obs_type} = 'recording' \
         ORDER BY timestamp ASC, span_id ASC \
         LIMIT {limit}",
        projection = observation_projection(true),
        session = sql_string_literal(session_id),
        from_ts = timestamp_literal(&from),
        to_ts = timestamp_literal(&to),
        obs_type = obs_type,
        limit = limit,
    ))
}

fn map_recording_batch(columns: &[String], row: &[Value]) -> Option<RecordingBatch> {
    let detail = map_observation_detail(columns, row)?;
    let events = extract_recording_events(&detail.events);
    let batch_index = detail
        .attributes
        .get("sp.recording.batch_index")
        .and_then(|v| v.parse::<i64>().ok());
    Some(RecordingBatch {
        span_id: detail.summary.span_id,
        trace_id: detail.summary.trace_id,
        start_time: detail.summary.start_time,
        batch_index,
        attributes: detail.attributes,
        events,
    })
}

/// Pull rrweb event arrays out of `sp.recording.batch` span events.
pub fn extract_recording_events(span_events: &[Value]) -> Vec<Value> {
    let mut out = Vec::new();
    for event in span_events {
        let Some(obj) = event.as_object() else {
            continue;
        };
        let name = obj.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name != RECORDING_EVENT_NAME {
            continue;
        }
        let Some(attrs) = obj.get("attributes") else {
            continue;
        };
        let raw = match attrs {
            Value::Object(map) => map.get(RECORDING_EVENTS_ATTR).cloned(),
            Value::String(text) => {
                serde_json::from_str::<Value>(text)
                    .ok()
                    .and_then(|parsed| match parsed {
                        Value::Object(map) => map.get(RECORDING_EVENTS_ATTR).cloned(),
                        _ => None,
                    })
            }
            _ => None,
        };
        let Some(raw) = raw else {
            continue;
        };
        match raw {
            Value::Array(items) => out.extend(items),
            Value::String(text) => {
                if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(&text) {
                    out.extend(items);
                }
            }
            _ => {}
        }
    }
    out
}

fn event_timestamp(event: &Value) -> i64 {
    event
        .get("timestamp")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_f64().map(|f| f as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

fn event_index(event: &Value) -> i64 {
    event
        .get("eventIndex")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_f64().map(|f| f as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

/// How a session list should be ordered.
///
/// Ordering happens in DuckDB over the whole time window. Doing it client-side
/// only ever sorts whatever page happened to be loaded, which is the wrong
/// answer to "show me the worst sessions today".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SessionOrderBy {
    #[default]
    StartTime,
    ErrorCount,
    Duration,
    TotalTokens,
    TotalCost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    #[default]
    Desc,
}

impl SortDirection {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionSearchRequest {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    /// Keep only sessions containing at least one ERROR span.
    #[serde(default)]
    pub has_errors: Option<bool>,
    pub user_id: Option<String>,
    pub model_name: Option<String>,
    #[serde(default)]
    pub order_by: SessionOrderBy,
    #[serde(default)]
    pub order: SortDirection,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub trace_count: i64,
    pub observation_count: i64,
    pub error_count: i64,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub agent_name: Option<String>,
    pub user_ids: Vec<String>,
    pub models: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSearchResponse {
    pub items: Vec<SessionSummary>,
    pub next_cursor: Option<String>,
    /// Cursor paging is only defined for `start_time` ordering; any other
    /// ordering returns a single ranked page. Stated explicitly so a client
    /// cannot mistake "no cursor" for "no more data".
    pub cursor_supported: bool,
}

/// Session list, aggregated in the database.
///
/// Without this endpoint a client has to pull raw observations and group them
/// in memory, which makes every aggregate a per-page partial sum, breaks
/// paging (one session gets split across pages), and reduces "sessions with
/// errors" to "sessions with errors among the rows already fetched".
pub async fn search_sessions(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Json(request): Json<SessionSearchRequest>,
) -> Result<Json<SessionSearchResponse>, ApiError> {
    let limit = clamp_limit(request.limit, DEFAULT_SESSION_LIMIT);
    // Must mirror what compile_session_search_sql actually accepts, `order`
    // included. Advertising cursor support for order=asc handed the client a
    // next_cursor that its own follow-up request would reject with 400 -- the
    // same "page 1 works, page 2 always fails" shape this endpoint was meant
    // to fix, just with a different status code.
    let cursor_supported =
        request.order_by == SessionOrderBy::StartTime && request.order == SortDirection::Desc;
    let sql = compile_session_search_sql(&request, limit).map_err(bad_request)?;
    let result = state
        .execute_tenant_scoped_sql(tenant.as_ref().map(|extension| &extension.0), &sql)
        .await
        .map_err(storage_error)?;

    let mut items = result
        .rows
        .iter()
        .filter_map(|row| map_session_summary(&result.columns, row))
        .collect::<Vec<_>>();

    let next_cursor = if cursor_supported {
        next_cursor_from_sessions(&mut items, limit)
    } else {
        items.truncate(limit);
        None
    };

    Ok(Json(SessionSearchResponse {
        items,
        next_cursor,
        cursor_supported,
    }))
}

pub fn compile_session_search_sql(
    request: &SessionSearchRequest,
    limit: usize,
) -> Result<String, String> {
    if request.from > request.to {
        return Err("`from` must be <= `to`".to_string());
    }

    let mut predicates = vec![
        format!("timestamp >= {}", timestamp_literal(&request.from)),
        format!("timestamp <= {}", timestamp_literal(&request.to)),
        // Spans without a session id cannot belong to a session row.
        "session_id IS NOT NULL AND session_id <> ''".to_string(),
        // Web recording shares session_id with LLM spans but is not an LLM
        // observation — keep it off list aggregates / trace counts.
        exclude_recording_observation_sql(),
    ];
    if let Some(user_id) = request.user_id.as_deref().filter(|v| !v.trim().is_empty()) {
        predicates.push(format!(
            "{} = {}",
            expr_user_id(),
            sql_string_literal(user_id)
        ));
    }
    if let Some(model) = request
        .model_name
        .as_deref()
        .filter(|v| !v.trim().is_empty())
    {
        predicates.push(format!(
            "{} = {}",
            variant_varchar("attributes", "gen_ai.request.model"),
            sql_string_literal(model)
        ));
    }

    // Cursor paging is defined against (start_time, session_id) descending.
    //
    // The predicate must sit on the OUTER query: start_time is an aggregate
    // alias (MIN(timestamp)), so pushing it into the inner WHERE both fails to
    // bind ("WHERE clause cannot contain aggregates") and would be wrong even
    // if it bound -- trimming raw spans by timestamp makes every SUM/COUNT for
    // that session cover only the post-cursor slice, so the aggregates would
    // shift as the caller pages.
    //
    // `order=asc` is rejected too: cursor_predicate emits `<`, which under an
    // ascending sort walks backwards and loops.
    let cursor_sql = match request.cursor.as_deref().filter(|v| !v.is_empty()) {
        Some(cursor) => {
            if request.order_by != SessionOrderBy::StartTime {
                return Err("`cursor` is only supported with order_by=start_time".to_string());
            }
            if request.order != SortDirection::Desc {
                return Err("`cursor` is only supported with order=desc".to_string());
            }
            format!(
                " WHERE {}",
                cursor_predicate(cursor, "start_time", "session_id")?
            )
        }
        None => String::new(),
    };

    let mut having = Vec::new();
    if request.has_errors == Some(true) {
        having.push("error_count > 0".to_string());
    } else if request.has_errors == Some(false) {
        having.push("error_count = 0".to_string());
    }

    let direction = request.order.as_sql();
    let order_sql = match request.order_by {
        SessionOrderBy::StartTime => format!("start_time {direction}, session_id {direction}"),
        SessionOrderBy::ErrorCount => {
            format!("error_count {direction}, start_time DESC, session_id DESC")
        }
        SessionOrderBy::Duration => {
            format!("duration_ms {direction}, start_time DESC, session_id DESC")
        }
        SessionOrderBy::TotalTokens => {
            format!("total_tokens {direction} NULLS LAST, start_time DESC, session_id DESC")
        }
        SessionOrderBy::TotalCost => {
            format!("total_cost {direction} NULLS LAST, start_time DESC, session_id DESC")
        }
    };

    let observation_type = format!(
        "COALESCE({}, 'span')",
        variant_varchar("attributes", "sp.observation.type")
    );

    Ok(format!(
        "SELECT * FROM ( \
           SELECT \
             session_id, \
             MIN(timestamp) AS start_time, \
             MAX(COALESCE(end_timestamp, timestamp)) AS end_time, \
             date_diff('millisecond', MIN(timestamp), MAX(COALESCE(end_timestamp, timestamp)))::BIGINT AS duration_ms, \
             COUNT(DISTINCT trace_id)::BIGINT AS trace_count, \
             COUNT(*)::BIGINT AS observation_count, \
             SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END)::BIGINT AS error_count, \
             SUM({input_tokens})::BIGINT AS input_tokens, \
             SUM({output_tokens})::BIGINT AS output_tokens, \
             SUM({total_tokens})::BIGINT AS total_tokens, \
             SUM({total_cost}) AS total_cost, \
             arg_min(message_type, timestamp) FILTER (WHERE {obs_type} = 'agent') AS agent_name, \
             list(DISTINCT {user_id}) AS user_ids, \
             list(DISTINCT {model_name}) AS models \
           FROM union_spans \
           WHERE {where_sql} \
           GROUP BY session_id \
           {having_sql} \
         ){cursor_sql} \
         ORDER BY {order_sql} \
         LIMIT {fetch}",
        cursor_sql = cursor_sql,
        input_tokens = expr_input_tokens(),
        output_tokens = expr_output_tokens(),
        total_tokens = expr_total_tokens(),
        total_cost = expr_total_cost(),
        obs_type = observation_type,
        user_id = expr_user_id(),
        model_name = variant_varchar("attributes", "gen_ai.request.model"),
        where_sql = predicates.join(" AND "),
        having_sql = if having.is_empty() {
            String::new()
        } else {
            format!("HAVING {}", having.join(" AND "))
        },
        order_sql = order_sql,
        // one extra row tells us whether another page exists
        fetch = limit + 1,
    ))
}

fn string_list(columns: &[String], row: &[Value], key: &str) -> Vec<String> {
    match column_value(columns, row, key) {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

fn map_session_summary(columns: &[String], row: &[Value]) -> Option<SessionSummary> {
    Some(SessionSummary {
        session_id: required_string(columns, row, "session_id")?,
        start_time: optional_timestamp(columns, row, "start_time")?,
        end_time: optional_timestamp(columns, row, "end_time"),
        trace_count: optional_i64(columns, row, "trace_count").unwrap_or(0),
        observation_count: optional_i64(columns, row, "observation_count").unwrap_or(0),
        error_count: optional_i64(columns, row, "error_count").unwrap_or(0),
        input_tokens: optional_i64(columns, row, "input_tokens"),
        output_tokens: optional_i64(columns, row, "output_tokens"),
        total_tokens: optional_i64(columns, row, "total_tokens"),
        total_cost: optional_f64(columns, row, "total_cost"),
        agent_name: optional_string(columns, row, "agent_name"),
        user_ids: string_list(columns, row, "user_ids"),
        models: string_list(columns, row, "models"),
    })
}

fn next_cursor_from_sessions(items: &mut Vec<SessionSummary>, limit: usize) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.truncate(limit);
    items
        .last()
        .map(|item| encode_cursor(item.start_time, &item.session_id))
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
            "COALESCE({}, 'span') IN ({values})",
            variant_varchar("attributes", "sp.observation.type")
        ));
    }
    if let Some(model_name) = &request.model_name {
        conditions.push(format!(
            "{} = {}",
            variant_varchar("attributes", "gen_ai.request.model"),
            sql_string_literal(model_name)
        ));
    }
    if let Some(user_id) = &request.user_id {
        conditions.push(format!(
            "({sp} = {id} OR {enduser} = {id})",
            sp = variant_varchar("attributes", "sp.user.id"),
            enduser = variant_varchar("attributes", "enduser.id"),
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
           AND timestamp <= {to_ts} \
           AND {not_recording}",
        input_tokens = expr_input_tokens(),
        output_tokens = expr_output_tokens(),
        total_tokens = expr_total_tokens(),
        total_cost = expr_total_cost(),
        user_id = expr_user_id(),
        session = sql_string_literal(session_id),
        from_ts = timestamp_literal(&from),
        to_ts = timestamp_literal(&to),
        not_recording = exclude_recording_observation_sql(),
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
        "session_id = {} AND timestamp >= {} AND timestamp <= {} AND {}",
        sql_string_literal(session_id),
        timestamp_literal(&from),
        timestamp_literal(&to),
        exclude_recording_observation_sql(),
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

/// Recording spans share `session_id` with LLM work but must not inflate
/// session list / detail LLM aggregates or crowd out conversation traces.
fn exclude_recording_observation_sql() -> String {
    format!(
        "COALESCE({}, '') <> 'recording'",
        variant_varchar("attributes", "sp.observation.type")
    )
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
        format!(
            "COALESCE({}, 'span') AS observation_type",
            variant_varchar("attributes", "sp.observation.type")
        ),
        "timestamp AS start_time".to_string(),
        "end_timestamp AS end_time".to_string(),
        "status_code".to_string(),
        format!(
            "{} AS model_name",
            variant_varchar("attributes", "gen_ai.request.model")
        ),
        format!(
            "{} AS model_provider",
            variant_varchar("attributes", "gen_ai.provider.name")
        ),
        format!("{} AS user_id", expr_user_id()),
        format!("{} AS input_tokens", expr_input_tokens()),
        format!("{} AS output_tokens", expr_output_tokens()),
        format!("{} AS total_tokens", expr_total_tokens()),
        format!("{} AS total_cost", expr_total_cost()),
    ];
    if include_payload {
        cols.push(variant_as_json("attributes"));
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

fn expr_user_id() -> String {
    format!(
        "COALESCE({}, {})",
        variant_varchar("attributes", "sp.user.id"),
        variant_varchar("attributes", "enduser.id")
    )
}

fn expr_input_tokens() -> String {
    variant_try_cast("attributes", "gen_ai.usage.input_tokens", "BIGINT")
}

fn expr_output_tokens() -> String {
    variant_try_cast("attributes", "gen_ai.usage.output_tokens", "BIGINT")
}

fn expr_total_tokens() -> String {
    variant_try_cast("attributes", "gen_ai.usage.total_tokens", "BIGINT")
}

fn expr_total_cost() -> String {
    variant_try_cast("attributes", "sp.cost.total", "DOUBLE")
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
    value.map(variant_json_to_string_map).unwrap_or_default()
}

fn map_events(value: Option<&Value>) -> Vec<Value> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                Value::Object(map) => {
                    let mut normalized = Map::new();
                    for (key, value) in map {
                        let key = key.to_owned();
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

fn bad_request(message: String) -> ApiError {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message })))
}

fn not_found() -> ApiError {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" })))
}

/// Map a storage-layer failure to a response the caller can act on.
///
/// This used to flatten every error into a bare 503 with `{"error": "query
/// unavailable"}`, keeping the real cause server-side only. That made an
/// operational fault (a dead query worker) indistinguishable from a malformed
/// SQL bug from the client's side, and cost a long debugging session to trace
/// back by elimination. Classify instead, and pass the detail through.
/// Classify a storage-layer failure without handing internals to the caller.
///
/// Two separate concerns, both learned the hard way:
///
/// 1. **Never echo the raw error.** DuckDB surfaces the full ATTACH target on
///    connection failure, and for a Postgres catalog that string is the DSN --
///    including the plaintext password. GCS HMAC secrets reach it the same way
///    via `CREATE SECRET` statement echo, and binder errors leak catalog names,
///    tenant metadata schemas and column lists. A tenant bearer token is enough
///    to trigger all of it. The caller gets a category; operators get the detail
///    from the log line, correlated by `error_id`.
///
/// 2. **Classify on the error, not on a substring of it.** DuckDB echoes the
///    offending statement, and that statement embeds caller-supplied literals
///    (`user_id`, `model_name`), so a client could previously flip a permanent
///    500 into a retryable 503 just by searching for a model named
///    "IO Error". Matching now happens only against the prefix DuckDB puts at
///    the very start of its message.
fn storage_error(error: anyhow::Error) -> ApiError {
    let raw = error.to_string();

    // Correlates the client-visible response with the full server-side detail.
    // Deterministic hash, not a UUID: no new dependency, and identical failures
    // collapse to the same id in the logs.
    let error_id = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        raw.hash(&mut h);
        format!("{:016x}", h.finish())
    };
    warn!("llm query failed [{}]: {}", error_id, raw);

    let kind = classify_storage_error(&raw);
    let status = if kind.retryable {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    (
        status,
        Json(json!({
            "error": kind.code,
            "retryable": kind.retryable,
            "error_id": error_id,
        })),
    )
}

struct StorageErrorKind {
    code: &'static str,
    retryable: bool,
}

/// Match only on the leading marker DuckDB emits, so echoed SQL (which contains
/// caller-controlled literals) cannot influence the classification.
fn classify_storage_error(raw: &str) -> StorageErrorKind {
    let head = raw.trim_start();

    // Our own engine-construction and worker-pool failures: operational, worth
    // retrying. The connect/attach prefixes matter most -- they wrap the
    // underlying "IO Error: could not connect to Postgres", so anchoring on the
    // leading marker (correct, since DuckDB's echoed SQL must not reach it)
    // would otherwise report the single most likely production fault, a
    // Postgres blip, as a permanent defect telling the caller not to retry.
    for marker in [
        "DuckDB worker",
        "DuckDB query engine failed to start",
        "DuckDB open failed",
        "DuckDB ATTACH failed",
        "DuckLake attach failed",
    ] {
        if head.starts_with(marker) {
            return StorageErrorKind {
                code: "query_unavailable",
                retryable: true,
            };
        }
    }

    // A FATAL (invalidated database) error that escapes worker self-heal
    // means the rebuild failed mid-request; the next attempt gets a fresh
    // connection, so it is retryable. INTERNAL errors are NOT listed here:
    // the query that trips the assertion fails deterministically.
    if head.starts_with("FATAL Error") {
        return StorageErrorKind {
            code: "query_unavailable",
            retryable: true,
        };
    }

    // Object-store and network faults, including S3/MinIO throttling and 5xx.
    // "HTTP Error" was missing before, so every 429 and 502 from the object
    // store was reported as a permanent 500 -- exactly the case worth retrying.
    for marker in [
        "Connection Error",
        "IO Error",
        "HTTP Error",
        "Network Error",
    ] {
        if head.starts_with(marker) {
            return StorageErrorKind {
                code: "query_unavailable",
                retryable: true,
            };
        }
    }

    // Binder/Catalog/Parser/Conversion errors are defects: identical on retry.
    StorageErrorKind {
        code: "query_failed",
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_never_echoes_the_underlying_message() {
        // DuckDB puts the full ATTACH target in connection errors, and for a
        // Postgres catalog that string carries the plaintext password.
        let leaky = anyhow::anyhow!(
            "IO Error: Failed to attach DuckLake at path \"postgres:host=h dbname=d \
             user=u password=hunter2\": connection refused"
        );
        let (_, body) = storage_error(leaky);
        let rendered = body.0.to_string();
        assert!(!rendered.contains("hunter2"), "password leaked: {rendered}");
        assert!(!rendered.contains("postgres:"), "DSN leaked: {rendered}");
        assert!(
            rendered.contains("error_id"),
            "no correlation id: {rendered}"
        );
    }

    #[test]
    fn storage_error_ignores_client_controlled_text_in_echoed_sql() {
        // DuckDB echoes the offending statement, and that statement embeds
        // caller-supplied literals -- a model named "IO Error" must not be able
        // to turn a permanent failure into a retryable one.
        let (status, body) = storage_error(anyhow::anyhow!(
            "Binder Error: no such column\nLINE 1: ... model_name = 'IO Error injected'"
        ));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.0["retryable"], serde_json::json!(false));
    }

    #[test]
    fn storage_error_marks_invalidated_database_as_retryable() {
        // A FATAL that escapes worker self-heal means the rebuild failed
        // mid-request; the next attempt gets a fresh connection. The INTERNAL
        // error that *triggered* the invalidation stays non-retryable: that
        // query fails deterministically (2026-08-03 outage, ducklake inlined
        // data reader).
        let (status, body) = storage_error(anyhow::anyhow!(
            "FATAL Error: Failed: database has been invalidated because of a previous \
             fatal error. The database must be restarted prior to being used again."
        ));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0["retryable"], serde_json::json!(true));

        let (status, body) = storage_error(anyhow::anyhow!(
            "INTERNAL Error: Attempted to access index 0 within vector of size 0"
        ));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.0["retryable"], serde_json::json!(false));
    }

    #[test]
    fn storage_error_treats_object_store_faults_as_retryable() {
        // S3/MinIO throttling and 5xx arrive with an "HTTP Error" prefix; these
        // were previously reported as permanent 500s.
        let (status, body) = storage_error(anyhow::anyhow!(
            "HTTP Error: HTTP GET error reading 's3://w/x.parquet' (HTTP 503)"
        ));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0["retryable"], serde_json::json!(true));
    }

    #[test]
    fn cursor_literal_keeps_microsecond_precision() {
        // start_time is a microsecond-precision TIMESTAMPTZ and the cursor
        // round-trips a real column value. Rendering it at millisecond
        // precision truncated the literal below the true value, so the
        // keyset predicate `start_time < cursor` silently dropped every row
        // sharing that millisecond: the last page came back empty with a null
        // next_cursor and no error. Verified on DuckDB 1.5.5 -- paging 8
        // sessions at limit=2 lost the final two.
        let mut request = session_search_request();
        request.cursor = Some(encode_cursor(
            DateTime::parse_from_rfc3339("2026-07-20T00:00:00.123456Z")
                .unwrap()
                .with_timezone(&Utc),
            "s1",
        ));
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(
            sql.contains("00.123456"),
            "cursor literal truncated below the true value, pages will drop rows: {sql}"
        );
    }

    #[test]
    fn session_search_rejects_cursor_with_ascending_order() {
        // cursor_predicate emits `<`; under ASC that pages backwards forever.
        let mut request = session_search_request();
        request.cursor = Some(encode_cursor(
            DateTime::parse_from_rfc3339("2026-07-20T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            "s1",
        ));
        request.order = SortDirection::Asc;
        assert!(compile_session_search_sql(&request, 50).is_err());
    }

    #[test]
    fn session_search_puts_cursor_predicate_outside_the_aggregate() {
        // start_time is MIN(timestamp): in the inner WHERE it neither binds
        // ("WHERE clause cannot contain aggregates") nor would be correct.
        let mut request = session_search_request();
        request.cursor = Some(encode_cursor(
            DateTime::parse_from_rfc3339("2026-07-20T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            "s1",
        ));
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        let group_by = sql.find("GROUP BY session_id").expect("group by");
        let cursor_at = sql.rfind("start_time <").expect("cursor predicate");
        assert!(
            cursor_at > group_by,
            "cursor predicate must sit after the aggregation, got: {sql}"
        );
    }

    #[test]
    fn storage_error_separates_transient_faults_from_defects() {
        // A dead query worker is operational and retryable
        let (status, body) = storage_error(anyhow::anyhow!("DuckDB worker channel closed"));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0["retryable"], serde_json::json!(true));

        // A SQL defect will fail identically on retry -- 503 would tell the
        // caller to keep hammering a request that can never succeed
        let (status, body) = storage_error(anyhow::anyhow!("Binder Error: no such column"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.0["retryable"], serde_json::json!(false));
    }

    #[test]
    fn storage_error_correlates_without_disclosing() {
        // Replaces an earlier test that asserted the cause was passed through
        // to the caller -- that behaviour is exactly what leaked the Postgres
        // password. Operators correlate via error_id in the log line instead.
        let (_, body) = storage_error(anyhow::anyhow!("open_connection: too many clients"));
        let rendered = body.0.to_string();
        assert!(
            !rendered.contains("too many clients"),
            "cause disclosed: {rendered}"
        );
        let id = body.0["error_id"].as_str().expect("error_id");
        assert_eq!(id.len(), 16, "error_id should be a stable 16-hex digest");
    }
    use crate::api::sql_support::decode_cursor;

    fn session_search_request() -> SessionSearchRequest {
        SessionSearchRequest {
            from: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_rfc3339("2026-07-25T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            has_errors: None,
            user_id: None,
            model_name: None,
            order_by: SessionOrderBy::StartTime,
            order: SortDirection::Desc,
            limit: Some(50),
            cursor: None,
        }
    }

    #[test]
    fn session_search_aggregates_in_sql_and_bounds_time() {
        let sql = compile_session_search_sql(&session_search_request(), 50).expect("sql");
        assert!(sql.contains("GROUP BY session_id"));
        assert!(sql.contains("timestamp >="));
        assert!(sql.contains("timestamp <="));
        // spans with no session id must not become a session row
        assert!(sql.contains("session_id IS NOT NULL AND session_id <> ''"));
        // recording spans share session_id but must not inflate LLM session rows
        assert!(sql.contains("<> 'recording'"));
        // one extra row is what tells us another page exists
        assert!(sql.contains("LIMIT 51"));
    }

    #[test]
    fn session_search_filters_errors_in_having_not_in_memory() {
        let mut request = session_search_request();
        request.has_errors = Some(true);
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("HAVING error_count > 0"));

        request.has_errors = Some(false);
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("HAVING error_count = 0"));

        request.has_errors = None;
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(!sql.contains("HAVING"));
    }

    #[test]
    fn session_search_orders_over_the_whole_window() {
        let mut request = session_search_request();
        request.order_by = SessionOrderBy::ErrorCount;
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("ORDER BY error_count DESC"));

        request.order_by = SessionOrderBy::Duration;
        request.order = SortDirection::Asc;
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("ORDER BY duration_ms ASC"));

        // NULL cost must not outrank a real one
        request.order_by = SessionOrderBy::TotalCost;
        request.order = SortDirection::Desc;
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        assert!(sql.contains("total_cost DESC NULLS LAST"));
    }

    #[test]
    fn session_search_rejects_cursor_with_incompatible_ordering() {
        // The cursor encodes (start_time, session_id); reusing it under another
        // ordering would silently skip or repeat rows.
        let mut request = session_search_request();
        request.order_by = SessionOrderBy::ErrorCount;
        request.cursor = Some(encode_cursor(request.from, "sess-1"));
        let err = compile_session_search_sql(&request, 50).expect_err("must reject");
        assert!(err.contains("order_by=start_time"));

        request.order_by = SessionOrderBy::StartTime;
        assert!(compile_session_search_sql(&request, 50).is_ok());
    }

    #[test]
    fn session_search_escapes_filter_literals() {
        let mut request = session_search_request();
        request.user_id = Some("u'; DROP TABLE traces; --".to_string());
        request.model_name = Some("gpt-5.2'".to_string());
        let sql = compile_session_search_sql(&request, 50).expect("sql");
        // The whole payload must land inside one literal with its quote doubled,
        // so the `;` never terminates a statement.
        assert!(sql.contains("'u''; DROP TABLE traces; --'"));
        assert!(sql.contains("'gpt-5.2'''"));
        // and the injected quote must never appear unescaped
        assert!(!sql.contains("'u'; "));
    }

    #[test]
    fn session_search_rejects_inverted_range() {
        let mut request = session_search_request();
        std::mem::swap(&mut request.from, &mut request.to);
        assert!(compile_session_search_sql(&request, 50).is_err());
    }

    #[test]
    fn session_cursor_only_emitted_when_a_page_was_actually_cut() {
        let make = |id: &str| SessionSummary {
            session_id: id.to_string(),
            start_time: DateTime::parse_from_rfc3339("2026-07-20T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            end_time: None,
            trace_count: 1,
            observation_count: 1,
            error_count: 0,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            total_cost: None,
            agent_name: None,
            user_ids: vec![],
            models: vec![],
        };

        let mut exact = vec![make("a"), make("b")];
        assert!(next_cursor_from_sessions(&mut exact, 2).is_none());
        assert_eq!(exact.len(), 2);

        let mut overflowing = vec![make("a"), make("b"), make("c")];
        let cursor = next_cursor_from_sessions(&mut overflowing, 2).expect("cursor");
        assert_eq!(overflowing.len(), 2);
        assert_eq!(decode_cursor(&cursor).expect("decode").id, "b");
    }

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
        assert!(sql.contains(&format!(
            "COALESCE({}, 'span') IN ('generation')",
            variant_varchar("attributes", "sp.observation.type")
        )));
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
    fn recording_sql_filters_observation_type_and_orders_ascending() {
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let sql = compile_session_recording_sql("sess-1", from, to, 50).expect("sql");
        assert!(sql.contains("session_id = "));
        assert!(sql.contains("sess-1"));
        assert!(sql.contains("recording"));
        assert!(sql.contains("ORDER BY timestamp ASC, span_id ASC"));
        assert!(sql.contains("LIMIT 50"));
        assert!(sql.contains("events"));
    }

    #[test]
    fn recording_sql_rejects_inverted_range() {
        let from = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(compile_session_recording_sql("sess-1", from, to, 10).is_err());
    }

    #[test]
    fn session_llm_sql_excludes_recording_observation_type() {
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let agg = compile_session_aggregate_sql("sess-1", from, to).expect("agg");
        let traces = compile_session_traces_sql("sess-1", from, to, 50, None).expect("traces");
        assert!(agg.contains("<> 'recording'"));
        assert!(traces.contains("<> 'recording'"));
    }

    #[test]
    fn extract_recording_events_parses_json_string_payload() {
        let events_json =
            r#"[{"type":4,"timestamp":100},{"type":2,"timestamp":200,"isCompressed":true}]"#;
        let span_events = vec![serde_json::json!({
            "name": "sp.recording.batch",
            "timestamp": "2026-07-18T00:00:01.000Z",
            "attributes": {
                "sp.recording.events": events_json
            }
        })];
        let events = extract_recording_events(&span_events);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], 4);
        assert_eq!(events[1]["isCompressed"], true);
    }

    #[test]
    fn extract_recording_events_ignores_other_event_names() {
        let span_events = vec![serde_json::json!({
            "name": "gen_ai.content.prompt",
            "attributes": { "content": "hi" }
        })];
        assert!(extract_recording_events(&span_events).is_empty());
    }
}
