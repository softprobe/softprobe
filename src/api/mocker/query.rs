//! Mocker aggregation + entry-point + fetch-by-trace/span SQL recipes.
//!
//! Mirrors the "bounded range + group count" pattern in `api/llm/query.rs` (same
//! `sql_string_literal` / `timestamp_literal` / time-bounds helpers, extracted to
//! `api::sql_support` in Phase 1 instead of duplicated here) rather than inventing a parallel
//! query stack. Column names (`record_operation`, `record_category`, `record_environment`,
//! `record_version`, `record_deleted`, `expiration_time`) come from `sp-llm/manifests/mocker-v1.yaml`
//! (Phase 0); see `backend/docs/thelake-telemetry-mocker-migration-plan.md` §"Attribute / column
//! map".
//!
//! These are pure SQL-compiling functions (like `llm::query::compile_*`), executed via
//! `AppState::execute_tenant_scoped_sql`. No HTTP handlers are wired in Phase 1 — the backend read
//! facade (`RepositoryReader`, Phase 5/6) is the intended caller.

use crate::api::sql_support::{push_optional_time_bounds, sql_string_literal, timestamp_literal};
use chrono::{DateTime, Utc};

const MAX_ENTRY_POINT_LIMIT: usize = 500;
const DEFAULT_ENTRY_POINT_LIMIT: usize = 100;

/// Bounded-range filter shared by `countByOperationName` / `countByRange` / `queryEntryPointByRange`
/// semantics — mirrors the subset of backend
/// `ai.softprobe.model.replay.PagedRequestType` fields relevant to thelake SQL (appId, category,
/// operation, env, recordVersion, begin/endTime); pagination and sort options stay on the future
/// backend read facade.
#[derive(Debug, Clone, Default)]
pub struct MockerRangeFilter {
    pub app_id: Option<String>,
    pub record_category: Option<String>,
    pub record_operation: Option<String>,
    pub record_environment: Option<i64>,
    pub record_version: Option<String>,
}

/// Compile `countByRange` semantics: total row count for the filter + bounded range, excluding
/// soft-deleted (`record_deleted`) and expired (`expiration_time`) rows. Uses latest-wins per
/// `mocker_id` (fallback `span_id`) so Phase 3 dual-write tombstones / expiry re-exports hide
/// superseded append-only rows.
pub fn compile_count_by_range_sql(
    filter: &MockerRangeFilter,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<String, String> {
    let where_sql = mocker_where_clause(filter, from, to, now)?;
    Ok(format!(
        "SELECT COUNT(*) AS count FROM {latest} WHERE {where_sql}",
        latest = latest_mocker_subquery(),
    ))
}

/// Compile `countByOperationName` semantics: per-`(app_id, record_operation, record_category)`
/// counts for the filter + bounded range (`GROUP BY app_id, record_operation, record_category` per
/// the SSOT doc's "Aggregations and replay" section), excluding soft-deleted/expired rows.
pub fn compile_count_by_operation_sql(
    filter: &MockerRangeFilter,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<String, String> {
    let where_sql = mocker_where_clause(filter, from, to, now)?;
    Ok(format!(
        "SELECT app_id, record_operation, record_category, COUNT(*) AS count \
         FROM {latest} WHERE {where_sql} \
         GROUP BY app_id, record_operation, record_category",
        latest = latest_mocker_subquery(),
    ))
}

/// Compile `queryEntryPointByRange` semantics: root spans (`parent_span_id IS NULL`) for the filter
/// + bounded range, most recent first, excluding soft-deleted/expired rows.
pub fn compile_entry_point_by_range_sql(
    filter: &MockerRangeFilter,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    now: DateTime<Utc>,
    limit: Option<usize>,
) -> Result<String, String> {
    let mut where_sql = mocker_where_clause(filter, from, to, now)?;
    where_sql.push_str(" AND parent_span_id IS NULL");
    let limit = limit
        .unwrap_or(DEFAULT_ENTRY_POINT_LIMIT)
        .clamp(1, MAX_ENTRY_POINT_LIMIT);
    Ok(format!(
        "SELECT trace_id, span_id, app_id, record_operation, record_category, \
                record_environment, record_version, timestamp, \
                http_request_method, http_request_path, http_response_status_code \
         FROM {latest} WHERE {where_sql} \
         ORDER BY timestamp DESC, span_id DESC LIMIT {limit}",
        latest = latest_mocker_subquery(),
    ))
}

/// Fetch-by-trace SQL recipe for replay preload: every non-deleted span on one `trace_id`, oldest
/// first (span tree replay order), regardless of the (optional) expiration filter used by the
/// aggregation queries above — preload needs the full trace even if individual spans are near
/// expiry.
pub fn compile_fetch_by_trace_sql(trace_id: &str) -> String {
    format!(
        "SELECT * FROM {latest} WHERE trace_id = {trace} AND {not_deleted} \
         ORDER BY timestamp ASC",
        latest = latest_mocker_subquery(),
        trace = sql_string_literal(trace_id),
        not_deleted = not_deleted_predicate(),
    )
}

/// Fetch-by-span SQL recipe (point lookup, e.g. `queryById`-style preload of one mocker entry).
pub fn compile_fetch_by_span_sql(span_id: &str) -> String {
    format!(
        "SELECT * FROM {latest} WHERE span_id = {span} AND {not_deleted} LIMIT 1",
        latest = latest_mocker_subquery(),
        span = sql_string_literal(span_id),
        not_deleted = not_deleted_predicate(),
    )
}

/// Latest dual-written version per logical mocker (append-only thelake). Partition key prefers
/// promoted `mocker_id`, else `span_id`.
fn latest_mocker_subquery() -> String {
    "(SELECT * EXCLUDE (_mocker_rn) FROM ( \
         SELECT *, ROW_NUMBER() OVER ( \
           PARTITION BY COALESCE(NULLIF(CAST(mocker_id AS VARCHAR), ''), span_id) \
           ORDER BY COALESCE(update_time, timestamp) DESC NULLS LAST \
         ) AS _mocker_rn \
         FROM union_spans \
       ) WHERE _mocker_rn = 1)"
        .to_string()
}

fn not_deleted_predicate() -> String {
    "(record_deleted IS NULL OR record_deleted = false)".to_string()
}

fn not_expired_predicate(now: DateTime<Utc>) -> String {
    format!(
        "(expiration_time IS NULL OR expiration_time > {})",
        timestamp_literal(&now)
    )
}

fn mocker_where_clause(
    filter: &MockerRangeFilter,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<String, String> {
    let mut conditions = Vec::new();
    push_optional_time_bounds(&mut conditions, Some(from), Some(to))?;
    conditions.push(not_deleted_predicate());
    conditions.push(not_expired_predicate(now));
    if let Some(app_id) = &filter.app_id {
        conditions.push(format!("app_id = {}", sql_string_literal(app_id)));
    }
    if let Some(category) = &filter.record_category {
        conditions.push(format!(
            "record_category = {}",
            sql_string_literal(category)
        ));
    }
    if let Some(operation) = &filter.record_operation {
        conditions.push(format!(
            "record_operation = {}",
            sql_string_literal(operation)
        ));
    }
    if let Some(env) = filter.record_environment {
        conditions.push(format!("record_environment = {env}"));
    }
    if let Some(version) = &filter.record_version {
        conditions.push(format!("record_version = {}", sql_string_literal(version)));
    }
    Ok(conditions.join(" AND "))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range() -> (DateTime<Utc>, DateTime<Utc>, DateTime<Utc>) {
        let from = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let now = DateTime::parse_from_rfc3339("2026-07-19T00:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        (from, to, now)
    }

    #[test]
    fn count_by_range_excludes_deleted_and_expired_and_escapes_literals() {
        let (from, to, now) = range();
        let filter = MockerRangeFilter {
            app_id: Some("checkout-api".to_string()),
            record_category: Some("http".to_string()),
            record_operation: Some("GET /orders'; DROP TABLE traces; --".to_string()),
            record_environment: Some(2),
            record_version: Some("v3".to_string()),
        };
        let sql = compile_count_by_range_sql(&filter, from, to, now).expect("sql");
        assert!(sql.contains("SELECT COUNT(*) AS count FROM"));
        assert!(sql.contains("ROW_NUMBER() OVER"));
        assert!(sql.contains("timestamp >="));
        assert!(sql.contains("(record_deleted IS NULL OR record_deleted = false)"));
        assert!(sql.contains("(expiration_time IS NULL OR expiration_time >"));
        assert!(sql.contains("app_id = 'checkout-api'"));
        assert!(sql.contains("record_category = 'http'"));
        assert!(sql.contains("record_environment = 2"));
        assert!(sql.contains("record_version = 'v3'"));
        assert!(sql.contains("GET /orders''; DROP TABLE traces; --"));
    }

    #[test]
    fn count_by_range_rejects_inverted_range() {
        let (from, to, now) = range();
        let filter = MockerRangeFilter::default();
        assert!(compile_count_by_range_sql(&filter, to, from, now).is_err());
    }

    #[test]
    fn count_by_operation_groups_by_app_operation_category() {
        let (from, to, now) = range();
        let filter = MockerRangeFilter::default();
        let sql = compile_count_by_operation_sql(&filter, from, to, now).expect("sql");
        assert!(sql.contains(
            "SELECT app_id, record_operation, record_category, COUNT(*) AS count"
        ));
        assert!(sql.contains("ROW_NUMBER() OVER"));
        assert!(sql.contains("GROUP BY app_id, record_operation, record_category"));
    }

    #[test]
    fn entry_point_filters_root_spans_and_clamps_limit() {
        let (from, to, now) = range();
        let filter = MockerRangeFilter {
            record_category: Some("http".to_string()),
            ..Default::default()
        };
        let sql =
            compile_entry_point_by_range_sql(&filter, from, to, now, Some(10_000)).expect("sql");
        assert!(sql.contains("parent_span_id IS NULL"));
        assert!(sql.contains(&format!("LIMIT {MAX_ENTRY_POINT_LIMIT}")));
        assert!(sql.contains("ORDER BY timestamp DESC, span_id DESC"));

        let default_sql =
            compile_entry_point_by_range_sql(&filter, from, to, now, None).expect("sql");
        assert!(default_sql.contains(&format!("LIMIT {DEFAULT_ENTRY_POINT_LIMIT}")));
    }

    #[test]
    fn fetch_by_trace_and_span_exclude_deleted_and_escape_literals() {
        let trace_sql = compile_fetch_by_trace_sql("trace-1'; DROP TABLE traces; --");
        assert!(trace_sql.contains("trace_id = 'trace-1''; DROP TABLE traces; --'"));
        assert!(trace_sql.contains("(record_deleted IS NULL OR record_deleted = false)"));
        assert!(trace_sql.contains("ORDER BY timestamp ASC"));

        let span_sql = compile_fetch_by_span_sql("span-1");
        assert!(span_sql.contains("span_id = 'span-1'"));
        assert!(span_sql.contains("LIMIT 1"));
    }
}
