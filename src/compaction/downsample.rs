//! Incremental 5m / 1h downsample ladder (§7.2 steps 3–4).
//!
//! - `metric_samples_5m` from raw older than 5m (closed 5m buckets)
//! - `metric_samples_1h` from 5m (fallback raw) older than 1h
//! - Watermark = `max(window_ts)` already in the destination (AC-M2)
//! - Raw rows are never deleted (AC-S2)

use crate::storage::schema::metrics_layout::qualified_metrics_layout_table;

/// Raw samples must be older than this before entering 5m (closed buckets).
/// Kept short so Grafana long windows hit filled ladders within minutes of
/// demo ingest (empty-UNION was blowing the 100ms SLO).
pub const DOWNSAMPLE_5M_LAG: &str = "INTERVAL '5 minutes'";
/// 5m / raw must be older than this before entering 1h.
pub const DOWNSAMPLE_1H_LAG: &str = "INTERVAL '1 hour'";
/// Max closed days processed per maintenance pass (AC-Q9 / G2).
pub const HIST_DOWNSAMPLE_MAX_DAYS_PER_PASS: usize = 4;

/// Watermark expression: latest `window_ts` already materialised in `dest`.
pub fn watermark_expr(dest_table: &str) -> String {
    format!("(SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM {dest_table})")
}

/// INSERT … SELECT building `metric_samples_5m` from raw (incremental).
pub fn downsample_5m_sql(catalog_alias: &str) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_samples_5m");
    let wm = watermark_expr(&dest);
    format!(
        "INSERT INTO {dest} (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)\n\
         SELECT\n\
           series_id,\n\
           time_bucket(INTERVAL '5 minutes', timestamp) AS window_ts,\n\
           CAST(time_bucket(INTERVAL '5 minutes', timestamp) AS DATE) AS record_date,\n\
           count(*)::UBIGINT AS count,\n\
           sum(value) AS sum,\n\
           min(value) AS min,\n\
           max(value) AS max,\n\
           arg_max(value, timestamp) AS last,\n\
           max(timestamp) AS last_ts\n\
         FROM {src}\n\
         WHERE timestamp < now() - {DOWNSAMPLE_5M_LAG}\n\
           AND time_bucket(INTERVAL '5 minutes', timestamp) <= now() - {DOWNSAMPLE_5M_LAG}\n\
           AND time_bucket(INTERVAL '5 minutes', timestamp) > {wm}\n\
         GROUP BY series_id, time_bucket(INTERVAL '5 minutes', timestamp);"
    )
}

/// INSERT … SELECT building `metric_samples_1h` from 5m (incremental).
pub fn downsample_1h_from_5m_sql(catalog_alias: &str) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_samples_5m");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_samples_1h");
    let wm = watermark_expr(&dest);
    format!(
        "INSERT INTO {dest} (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)\n\
         SELECT\n\
           series_id,\n\
           time_bucket(INTERVAL '1 hour', window_ts) AS window_ts,\n\
           CAST(time_bucket(INTERVAL '1 hour', window_ts) AS DATE) AS record_date,\n\
           sum(count)::UBIGINT AS count,\n\
           sum(sum) AS sum,\n\
           min(min) AS min,\n\
           max(max) AS max,\n\
           arg_max(last, last_ts) AS last,\n\
           max(last_ts) AS last_ts\n\
         FROM {src}\n\
         WHERE window_ts < now() - {DOWNSAMPLE_1H_LAG}\n\
           AND time_bucket(INTERVAL '1 hour', window_ts) > {wm}\n\
         GROUP BY series_id, time_bucket(INTERVAL '1 hour', window_ts);"
    )
}

/// Fallback: build 1h directly from raw when 5m is empty / lagging.
pub fn downsample_1h_from_raw_sql(catalog_alias: &str) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_samples_1h");
    let wm = watermark_expr(&dest);
    format!(
        "INSERT INTO {dest} (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)\n\
         SELECT\n\
           series_id,\n\
           time_bucket(INTERVAL '1 hour', timestamp) AS window_ts,\n\
           CAST(time_bucket(INTERVAL '1 hour', timestamp) AS DATE) AS record_date,\n\
           count(*)::UBIGINT AS count,\n\
           sum(value) AS sum,\n\
           min(value) AS min,\n\
           max(value) AS max,\n\
           arg_max(value, timestamp) AS last,\n\
           max(timestamp) AS last_ts\n\
         FROM {src}\n\
         WHERE timestamp < now() - {DOWNSAMPLE_1H_LAG}\n\
           AND time_bucket(INTERVAL '1 hour', timestamp) > {wm}\n\
         GROUP BY series_id, time_bucket(INTERVAL '1 hour', timestamp);"
    )
}

/// Count SQL for AC-S2 / AC-M2 assertions.
pub fn count_sql(catalog_alias: &str, table: &str) -> String {
    let q = qualified_metrics_layout_table(catalog_alias, table);
    format!("SELECT count(*)::BIGINT FROM {q}")
}

/// INSERT … SELECT building `metric_hist_samples_5m` from raw hist (incremental).
///
/// Merges `bucket_counts` element-wise (Thanos compact analog). Rows without
/// bucket arrays still land count/sum aggregates.
pub fn hist_downsample_5m_sql(catalog_alias: &str) -> String {
    hist_downsample_5m_for_day_sql(catalog_alias, None)
}

/// Days with raw hist rows still above the 5m watermark (bounded per pass).
pub fn hist_downsample_5m_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_5m");
    let wm = watermark_expr(&dest);
    format!(
        "SELECT DISTINCT CAST(record_date AS VARCHAR) AS record_date FROM {src}\n\
         WHERE timestamp < now() - {DOWNSAMPLE_5M_LAG}\n\
           AND time_bucket(INTERVAL '5 minutes', timestamp) <= now() - {DOWNSAMPLE_5M_LAG}\n\
           AND time_bucket(INTERVAL '5 minutes', timestamp) > {wm}\n\
         ORDER BY record_date\n\
         LIMIT {limit};"
    )
}

/// One calendar-day slice of hist 5m downsample (partition-scoped for memory).
pub fn hist_downsample_5m_for_day_sql(
    catalog_alias: &str,
    record_date: Option<chrono::NaiveDate>,
) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_5m");
    let wm = watermark_expr(&dest);
    let day_filter = record_date
        .map(|d| format!("AND record_date = DATE '{}'", d.format("%Y-%m-%d")))
        .unwrap_or_default();
    format!(
        "INSERT INTO {dest} (series_id, window_ts, record_date, count, sum, bucket_counts, explicit_bounds, last_ts)\n\
         WITH src AS (\n\
           SELECT * FROM {src}\n\
           WHERE timestamp < now() - {DOWNSAMPLE_5M_LAG}\n\
             AND time_bucket(INTERVAL '5 minutes', timestamp) <= now() - {DOWNSAMPLE_5M_LAG}\n\
             AND time_bucket(INTERVAL '5 minutes', timestamp) > {wm}\n\
             {day_filter}\n\
         ),\n\
         scalars AS (\n\
           SELECT series_id,\n\
             time_bucket(INTERVAL '5 minutes', timestamp) AS window_ts,\n\
             sum(count)::UBIGINT AS count,\n\
             sum(sum) AS sum,\n\
             arg_max(explicit_bounds, timestamp) AS explicit_bounds,\n\
             max(timestamp) AS last_ts\n\
           FROM src\n\
           GROUP BY 1, 2\n\
         ),\n\
         bucket_parts AS (\n\
           SELECT s.series_id,\n\
             time_bucket(INTERVAL '5 minutes', s.timestamp) AS window_ts,\n\
             u.bucket_idx,\n\
             sum(u.bucket_val::UBIGINT) AS bucket_sum\n\
           FROM src s\n\
           CROSS JOIN LATERAL unnest(s.bucket_counts) WITH ORDINALITY AS u(bucket_val, bucket_idx)\n\
           WHERE s.bucket_counts IS NOT NULL AND len(s.bucket_counts) > 0\n\
           GROUP BY 1, 2, 3\n\
         ),\n\
         bucket_lists AS (\n\
           SELECT series_id, window_ts,\n\
             list(bucket_sum ORDER BY bucket_idx) AS bucket_counts\n\
           FROM bucket_parts\n\
           GROUP BY 1, 2\n\
         )\n\
         SELECT sc.series_id, sc.window_ts, CAST(sc.window_ts AS DATE), sc.count, sc.sum,\n\
           bl.bucket_counts, sc.explicit_bounds, sc.last_ts\n\
         FROM scalars sc\n\
         LEFT JOIN bucket_lists bl USING (series_id, window_ts);"
    )
}

/// INSERT … SELECT building `metric_hist_samples_1h` from 5m hist (incremental).
pub fn hist_downsample_1h_from_5m_sql(catalog_alias: &str) -> String {
    hist_downsample_1h_from_5m_for_day_sql(catalog_alias, None)
}

pub fn hist_downsample_1h_from_5m_pending_days_sql(catalog_alias: &str, limit: usize) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_5m");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_1h");
    let wm = watermark_expr(&dest);
    format!(
        "SELECT DISTINCT CAST(record_date AS VARCHAR) AS record_date FROM {src}\n\
         WHERE window_ts < now() - {DOWNSAMPLE_1H_LAG}\n\
           AND time_bucket(INTERVAL '1 hour', window_ts) > {wm}\n\
         ORDER BY record_date\n\
         LIMIT {limit};"
    )
}

pub fn hist_downsample_1h_from_5m_for_day_sql(
    catalog_alias: &str,
    record_date: Option<chrono::NaiveDate>,
) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_5m");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_1h");
    let wm = watermark_expr(&dest);
    let day_filter = record_date
        .map(|d| format!("AND record_date = DATE '{}'", d.format("%Y-%m-%d")))
        .unwrap_or_default();
    format!(
        "INSERT INTO {dest} (series_id, window_ts, record_date, count, sum, bucket_counts, explicit_bounds, last_ts)\n\
         WITH src AS (\n\
           SELECT * FROM {src}\n\
           WHERE window_ts < now() - {DOWNSAMPLE_1H_LAG}\n\
             AND time_bucket(INTERVAL '1 hour', window_ts) > {wm}\n\
             {day_filter}\n\
         ),\n\
         scalars AS (\n\
           SELECT series_id,\n\
             time_bucket(INTERVAL '1 hour', window_ts) AS window_ts,\n\
             sum(count)::UBIGINT AS count,\n\
             sum(sum) AS sum,\n\
             arg_max(explicit_bounds, last_ts) AS explicit_bounds,\n\
             max(last_ts) AS last_ts\n\
           FROM src\n\
           GROUP BY 1, 2\n\
         ),\n\
         bucket_parts AS (\n\
           SELECT s.series_id,\n\
             time_bucket(INTERVAL '1 hour', s.window_ts) AS window_ts,\n\
             u.bucket_idx,\n\
             sum(u.bucket_val::UBIGINT) AS bucket_sum\n\
           FROM src s\n\
           CROSS JOIN LATERAL unnest(s.bucket_counts) WITH ORDINALITY AS u(bucket_val, bucket_idx)\n\
           WHERE s.bucket_counts IS NOT NULL AND len(s.bucket_counts) > 0\n\
           GROUP BY 1, 2, 3\n\
         ),\n\
         bucket_lists AS (\n\
           SELECT series_id, window_ts,\n\
             list(bucket_sum ORDER BY bucket_idx) AS bucket_counts\n\
           FROM bucket_parts\n\
           GROUP BY 1, 2\n\
         )\n\
         SELECT sc.series_id, sc.window_ts, CAST(sc.window_ts AS DATE), sc.count, sc.sum,\n\
           bl.bucket_counts, sc.explicit_bounds, sc.last_ts\n\
         FROM scalars sc\n\
         LEFT JOIN bucket_lists bl USING (series_id, window_ts);"
    )
}

/// Fallback: build hist 1h directly from raw when 5m hist is empty / lagging.
pub fn hist_downsample_1h_from_raw_sql(catalog_alias: &str) -> String {
    hist_downsample_1h_from_raw_for_day_sql(catalog_alias, None)
}

pub fn hist_downsample_1h_from_raw_for_day_sql(
    catalog_alias: &str,
    record_date: Option<chrono::NaiveDate>,
) -> String {
    let src = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples");
    let dest = qualified_metrics_layout_table(catalog_alias, "metric_hist_samples_1h");
    let wm = watermark_expr(&dest);
    let day_filter = record_date
        .map(|d| format!("AND record_date = DATE '{}'", d.format("%Y-%m-%d")))
        .unwrap_or_default();
    format!(
        "INSERT INTO {dest} (series_id, window_ts, record_date, count, sum, bucket_counts, explicit_bounds, last_ts)\n\
         WITH src AS (\n\
           SELECT * FROM {src}\n\
           WHERE timestamp < now() - {DOWNSAMPLE_1H_LAG}\n\
             AND time_bucket(INTERVAL '1 hour', timestamp) > {wm}\n\
             {day_filter}\n\
         ),\n\
         scalars AS (\n\
           SELECT series_id,\n\
             time_bucket(INTERVAL '1 hour', timestamp) AS window_ts,\n\
             sum(count)::UBIGINT AS count,\n\
             sum(sum) AS sum,\n\
             arg_max(explicit_bounds, timestamp) AS explicit_bounds,\n\
             max(timestamp) AS last_ts\n\
           FROM src\n\
           GROUP BY 1, 2\n\
         ),\n\
         bucket_parts AS (\n\
           SELECT s.series_id,\n\
             time_bucket(INTERVAL '1 hour', s.timestamp) AS window_ts,\n\
             u.bucket_idx,\n\
             sum(u.bucket_val::UBIGINT) AS bucket_sum\n\
           FROM src s\n\
           CROSS JOIN LATERAL unnest(s.bucket_counts) WITH ORDINALITY AS u(bucket_val, bucket_idx)\n\
           WHERE s.bucket_counts IS NOT NULL AND len(s.bucket_counts) > 0\n\
           GROUP BY 1, 2, 3\n\
         ),\n\
         bucket_lists AS (\n\
           SELECT series_id, window_ts,\n\
             list(bucket_sum ORDER BY bucket_idx) AS bucket_counts\n\
           FROM bucket_parts\n\
           GROUP BY 1, 2\n\
         )\n\
         SELECT sc.series_id, sc.window_ts, CAST(sc.window_ts AS DATE), sc.count, sc.sum,\n\
           bl.bucket_counts, sc.explicit_bounds, sc.last_ts\n\
         FROM scalars sc\n\
         LEFT JOIN bucket_lists bl USING (series_id, window_ts);"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downsample_5m_sql_is_incremental_and_additive() {
        let sql = downsample_5m_sql("softprobe");
        assert!(sql.starts_with("INSERT INTO softprobe.metric_samples_5m"));
        assert!(sql.contains("FROM softprobe.metric_samples"));
        assert!(sql.contains("INTERVAL '5 minutes'"));
        assert!(sql.contains("max(window_ts)"));
        assert!(!sql.to_lowercase().contains("delete"));
        assert!(!sql.to_lowercase().contains("truncate"));
    }

    #[test]
    fn downsample_1h_sql_uses_1h_lag_and_watermark() {
        let from_5m = downsample_1h_from_5m_sql("softprobe");
        assert!(from_5m.contains("metric_samples_5m"));
        assert!(from_5m.contains("INTERVAL '1 hour'"));
        assert!(from_5m.contains("INSERT INTO softprobe.metric_samples_1h"));
        assert!(from_5m.contains("max(window_ts)"));

        let from_raw = downsample_1h_from_raw_sql("softprobe");
        assert!(from_raw.contains("FROM softprobe.metric_samples\n"));
        assert!(from_raw.contains("INTERVAL '1 hour'"));
    }

    /// AC-M2 shape: watermark predicate prevents full rebuild.
    #[test]
    fn watermark_expr_anchors_incremental_pass() {
        let wm = watermark_expr("softprobe.metric_samples_5m");
        assert!(wm.contains("max(window_ts)"));
        assert!(wm.contains("softprobe.metric_samples_5m"));
        let sql = downsample_5m_sql("softprobe");
        assert!(sql.contains(&wm) || sql.contains("max(window_ts)"));
    }

    #[test]
    fn hist_downsample_5m_sql_is_incremental() {
        let sql = hist_downsample_5m_sql("softprobe");
        assert!(sql.contains("INSERT INTO softprobe.metric_hist_samples_5m"));
        assert!(sql.contains("FROM softprobe.metric_hist_samples"));
        assert!(sql.contains("unnest(s.bucket_counts)"));
        assert!(sql.contains("max(window_ts)"));
        assert!(!sql.to_lowercase().contains("delete"));
    }

    #[test]
    fn hist_downsample_5m_for_day_scopes_record_date() {
        let day = chrono::NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();
        let sql = hist_downsample_5m_for_day_sql("softprobe", Some(day));
        assert!(sql.contains("record_date = DATE '2026-08-14'"));
        assert!(sql.contains("INSERT INTO softprobe.metric_hist_samples_5m"));
    }

    #[test]
    fn hist_downsample_pending_days_sql_is_bounded() {
        let sql = hist_downsample_5m_pending_days_sql("softprobe", 4);
        assert!(sql.contains("LIMIT 4"));
        assert!(sql.contains("metric_hist_samples"));
    }

    #[test]
    fn hist_downsample_1h_sql_uses_1h_lag() {
        let from_5m = hist_downsample_1h_from_5m_sql("softprobe");
        assert!(from_5m.contains("metric_hist_samples_5m"));
        assert!(from_5m.contains("INTERVAL '1 hour'"));
        let from_raw = hist_downsample_1h_from_raw_sql("softprobe");
        assert!(from_raw.contains("metric_hist_samples\n"));
    }
}
