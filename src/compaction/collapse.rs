//! `metric_collapse_job_1h` builder + PromQL collapse planner (§6.6 / §7.2 / §9.1).
//!
//! Collapse key is `(metric_name, job, record_date, window_ts)`. Long-window
//! `sum by (job) (rate|irate|increase(…))`
//! reads this table (AC-Q5 / AC-W3).

use crate::compat::backends::grain::RAW_RANGE_MS;
use crate::storage::schema::metrics_layout::qualified_metrics_layout_table;
use promql_parser::parser::token::T_SUM;
use promql_parser::parser::{AggregateExpr, Expr, LabelModifier};

/// Minimum query window to prefer collapse over wide series fetch (§9.1 step 5).
pub const COLLAPSE_MIN_RANGE_MS: i64 = RAW_RANGE_MS; // 2h

/// Incremental INSERT for `metric_collapse_job_1h` from 1h samples + job postings.
pub fn collapse_job_1h_sql(catalog_alias: &str) -> String {
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_collapse_job_1h");
    let samples_1h = qualified_metrics_layout_table(catalog_alias, "metric_samples_1h");
    let series = qualified_metrics_layout_table(catalog_alias, "metric_series");
    let postings = qualified_metrics_layout_table(catalog_alias, "metric_postings");
    format!(
        "INSERT INTO {dest} (metric_name, job, window_ts, record_date, count, sum, min, max, last)\n\
         SELECT\n\
           s.metric_name,\n\
           p.label_value AS job,\n\
           h.window_ts,\n\
           h.record_date,\n\
           sum(h.count)::UBIGINT AS count,\n\
           sum(h.sum) AS sum,\n\
           min(h.min) AS min,\n\
           max(h.max) AS max,\n\
           sum(h.last) AS last\n\
         FROM {samples_1h} h\n\
         JOIN {series} s\n\
           ON h.series_id = s.series_id AND h.record_date = s.record_date\n\
         JOIN {postings} p\n\
           ON p.series_id = h.series_id AND p.record_date = h.record_date\n\
          AND p.label_name = 'job'\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {dest} existing\n\
           WHERE existing.metric_name = s.metric_name\n\
             AND existing.job = p.label_value\n\
             AND existing.record_date = h.record_date\n\
             AND existing.window_ts = h.window_ts\n\
         )\n\
         GROUP BY s.metric_name, p.label_value, h.window_ts, h.record_date;"
    )
}

/// Fallback collapse from raw when 1h is empty (still key-scoped incremental).
pub fn collapse_job_1h_from_raw_sql(catalog_alias: &str) -> String {
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_collapse_job_1h");
    let samples = qualified_metrics_layout_table(catalog_alias, "metric_samples");
    let series = qualified_metrics_layout_table(catalog_alias, "metric_series");
    let postings = qualified_metrics_layout_table(catalog_alias, "metric_postings");
    format!(
        "INSERT INTO {dest} (metric_name, job, window_ts, record_date, count, sum, min, max, last)\n\
         SELECT\n\
           s.metric_name,\n\
           p.label_value AS job,\n\
           time_bucket(INTERVAL '1 hour', sm.timestamp) AS window_ts,\n\
           CAST(time_bucket(INTERVAL '1 hour', sm.timestamp) AS DATE) AS record_date,\n\
           count(*)::UBIGINT AS count,\n\
           sum(sm.value) AS sum,\n\
           min(sm.value) AS min,\n\
           max(sm.value) AS max,\n\
           arg_max(sm.value, sm.timestamp) AS last\n\
         FROM {samples} sm\n\
         JOIN {series} s\n\
           ON sm.series_id = s.series_id AND sm.record_date = s.record_date\n\
         JOIN {postings} p\n\
           ON p.series_id = sm.series_id AND p.record_date = sm.record_date\n\
          AND p.label_name = 'job'\n\
         WHERE sm.timestamp < now() - INTERVAL '24 hours'\n\
           AND NOT EXISTS (\n\
             SELECT 1 FROM {dest} existing\n\
             WHERE existing.metric_name = s.metric_name\n\
               AND existing.job = p.label_value\n\
               AND existing.record_date = CAST(time_bucket(INTERVAL '1 hour', sm.timestamp) AS DATE)\n\
               AND existing.window_ts = time_bucket(INTERVAL '1 hour', sm.timestamp)\n\
           )\n\
         GROUP BY s.metric_name, p.label_value, time_bucket(INTERVAL '1 hour', sm.timestamp),\n\
           CAST(time_bucket(INTERVAL '1 hour', sm.timestamp) AS DATE);"
    )
}

fn unwrap_parens(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(p) => unwrap_parens(&p.expr),
        other => other,
    }
}

fn is_rate_family(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "rate" | "irate" | "increase"
    )
}

/// True when AST is `sum by (job) (rate|irate|increase (selector))` (possibly parenthesized).
pub fn is_sum_by_job_rate_shape(expr: &Expr) -> bool {
    match unwrap_parens(expr) {
        Expr::Aggregate(a) => aggregate_is_sum_by_job_rate(a),
        _ => false,
    }
}

fn aggregate_is_sum_by_job_rate(a: &AggregateExpr) -> bool {
    if a.op.id() != T_SUM {
        return false;
    }
    // Exactly `by (job)` — single grouping label (not `without`).
    let Some(LabelModifier::Include(ls)) = a.modifier.as_ref() else {
        return false;
    };
    if ls.labels.len() != 1 || ls.labels[0] != "job" {
        return false;
    }
    match unwrap_parens(a.expr.as_ref()) {
        Expr::Call(c) => {
            if !is_rate_family(c.func.name) || c.args.args.len() != 1 {
                return false;
            }
            matches!(
                unwrap_parens(c.args.args[0].as_ref()),
                Expr::MatrixSelector(_)
            )
        }
        _ => false,
    }
}

/// §9.1 step 5: use collapse table when shape matches and window ≥ 2h.
pub fn should_use_collapse(expr: &Expr, range_ms: Option<i64>) -> bool {
    let range = range_ms.unwrap_or(0);
    range >= COLLAPSE_MIN_RANGE_MS && is_sum_by_job_rate_shape(expr)
}

/// Extract metric `__name__` equality from a sum-by-job-rate AST (best effort).
pub fn collapse_metric_name(expr: &Expr) -> Option<String> {
    let Expr::Aggregate(a) = unwrap_parens(expr) else {
        return None;
    };
    let Expr::Call(c) = unwrap_parens(a.expr.as_ref()) else {
        return None;
    };
    let Expr::MatrixSelector(ms) = unwrap_parens(c.args.args[0].as_ref()) else {
        return None;
    };
    for m in ms.vs.matchers.matchers.iter() {
        if m.name == "__name__" || m.name == promql_parser::label::METRIC_NAME {
            return Some(m.value.clone());
        }
    }
    ms.vs.name.clone()
}

/// True when SQL is the collapse Prom path (AC-Q5 / AC-W3 EXPLAIN shape).
pub fn sql_is_collapse_prom_path(sql: &str) -> bool {
    sql.contains("metric_collapse_job_1h")
        && sql.contains("metric_name")
        && !sql.contains("to_timestamp(")
        && !sql.contains("FROM union_metrics")
        && !sql.contains("metric_samples sm")
}

/// Row fetch budget for collapse scans (hourly grain × series).
///
/// Raw/5m sample paths use `max_series * 10` as a scan_cap. Collapse stores one
/// row per `(job, hour)`, so a honest 90d × J=50 window is ~108k rows — above
/// that cap — and must not fail AC-W3 with an empty/error result. Cap by
/// `max_series` on **parsed series count**, not hourly row count.
pub fn collapse_fetch_limit(
    max_series: usize,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> usize {
    const HOUR_MS: u64 = 3_600_000;
    let range_ms = match (start_ms, end_ms) {
        (Some(s), Some(e)) => (e - s).unsigned_abs(),
        _ => HOUR_MS,
    };
    // +2 hours of slack for step/lookback alignment on the closed-hour grid.
    let hours = (range_ms / HOUR_MS).saturating_add(2).max(1) as usize;
    max_series
        .saturating_add(1)
        .saturating_mul(hours)
        .max(10_000)
}

/// Scan SQL for collapse path (AC-Q5 / AC-W3 EXPLAIN target).
pub fn collapse_scan_sql(
    catalog_alias: &str,
    metric_name: &str,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    fetch_limit: usize,
) -> String {
    use crate::compat::backends::postings_resolve::timestamptz_literal_ms;
    let table = qualified_metrics_layout_table(catalog_alias, "metric_collapse_job_1h");
    let mut time = String::new();
    if let Some(s) = start_ms {
        time.push_str(&format!(
            " AND c.window_ts >= {}",
            timestamptz_literal_ms(s)
        ));
    }
    if let Some(e) = end_ms {
        time.push_str(&format!(
            " AND c.window_ts <= {}",
            timestamptz_literal_ms(e)
        ));
    }
    let name = metric_name.replace('\'', "''");
    format!(
        "SELECT c.metric_name, c.job, \
         CAST((epoch(c.window_ts) * 1000) AS BIGINT) AS timestamp_ms, \
         c.last AS value, c.count, c.sum, c.min, c.max \
         FROM {table} c \
         WHERE c.metric_name = '{name}'{time} \
         ORDER BY c.job, c.window_ts \
         LIMIT {fetch_limit}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::promql::parse_promql;

    #[test]
    fn collapse_sql_groups_by_job_and_is_incremental() {
        let sql = collapse_job_1h_sql("softprobe");
        assert!(sql.contains("INSERT INTO softprobe.metric_collapse_job_1h"));
        assert!(sql.contains("metric_samples_1h"));
        assert!(sql.contains("label_name = 'job'"));
        assert!(sql.contains("NOT EXISTS"));
        assert!(!sql.to_lowercase().contains("delete"));
    }

    /// T-Q5 / AC-Q5 planner unit: AST match when window ≥ 2h.
    #[test]
    fn planner_picks_collapse_for_sum_by_job_rate() {
        let expr = parse_promql(r#"sum by (job) (rate(layout_http[5m]))"#).unwrap();
        assert!(is_sum_by_job_rate_shape(&expr));
        assert!(should_use_collapse(&expr, Some(30 * 24 * 3_600_000)));
        assert!(should_use_collapse(&expr, Some(COLLAPSE_MIN_RANGE_MS)));
        assert!(!should_use_collapse(&expr, Some(COLLAPSE_MIN_RANGE_MS - 1)));

        let sql = collapse_scan_sql(
            "softprobe",
            "layout_http",
            Some(1_700_000_000_000),
            Some(1_700_000_000_000 + 30 * 24 * 3_600_000),
            10_000,
        );
        assert!(
            sql.contains("metric_collapse_job_1h"),
            "AC-Q5/W3 EXPLAIN must reference collapse table: {sql}"
        );
        assert!(sql.contains("layout_http"));
        assert!(!sql.contains("metric_samples "));
        assert!(!sql.contains("to_timestamp("));
        assert!(
            sql_is_collapse_prom_path(&sql),
            "AC-Q5/W3 collapse path shape: {sql}"
        );
    }

    #[test]
    fn planner_rejects_non_job_or_non_rate_shapes() {
        let other = parse_promql(r#"sum by (instance) (rate(layout_http[5m]))"#).unwrap();
        assert!(!is_sum_by_job_rate_shape(&other));
        let avg = parse_promql(r#"avg by (job) (rate(layout_http[5m]))"#).unwrap();
        assert!(!is_sum_by_job_rate_shape(&avg));
        let bare = parse_promql(r#"sum by (job) (layout_http)"#).unwrap();
        assert!(!is_sum_by_job_rate_shape(&bare));
    }

    #[test]
    fn collapse_metric_name_from_selector() {
        let expr = parse_promql(r#"sum by (job) (rate(layout_http[5m]))"#).unwrap();
        assert_eq!(collapse_metric_name(&expr).as_deref(), Some("layout_http"));
    }

    /// T-W3: 90d × J=50 hourly rows must fit the collapse fetch budget (not raw scan_cap).
    #[test]
    fn collapse_fetch_limit_covers_90d_job_series() {
        let end = 1_700_000_000_000i64;
        let start = end - 90 * 86_400_000;
        let max_series = 50;
        let lim = collapse_fetch_limit(max_series, Some(start), Some(end));
        let rows_90d = 90usize * 24 * max_series;
        assert!(
            lim > rows_90d,
            "AC-W3: collapse LIMIT {lim} must exceed 90d×24×J={rows_90d} (raw scan_cap would be ~100k)"
        );
        // Old max_series*10 scan_cap must not be used as the collapse row budget.
        assert!(lim > max_series.saturating_mul(10).max(10_000));
    }
}
