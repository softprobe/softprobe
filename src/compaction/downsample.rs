//! Incremental 5m / 1h downsample ladder (§7.2 steps 3–4).
//!
//! - `metric_samples_5m` from raw older than 2h (closed hours only)
//! - `metric_samples_1h` from 5m (fallback raw) older than 24h
//! - Watermark = `max(window_ts)` already in the destination (AC-M2)
//! - Raw rows are never deleted (AC-S2)

use crate::storage::schema::metrics_layout::qualified_metrics_layout_table;

/// Raw samples must be older than this before entering 5m (closed hours).
pub const DOWNSAMPLE_5M_LAG: &str = "INTERVAL '2 hours'";
/// 5m / raw must be older than this before entering 1h.
pub const DOWNSAMPLE_1H_LAG: &str = "INTERVAL '24 hours'";

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
           AND time_bucket(INTERVAL '5 minutes', timestamp) < date_trunc('hour', now())\n\
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downsample_5m_sql_is_incremental_and_additive() {
        let sql = downsample_5m_sql("softprobe");
        assert!(sql.starts_with("INSERT INTO softprobe.metric_samples_5m"));
        assert!(sql.contains("FROM softprobe.metric_samples"));
        assert!(sql.contains("INTERVAL '2 hours'"));
        assert!(sql.contains("date_trunc('hour', now())"));
        assert!(sql.contains("max(window_ts)"));
        assert!(!sql.to_lowercase().contains("delete"));
        assert!(!sql.to_lowercase().contains("truncate"));
    }

    #[test]
    fn downsample_1h_sql_uses_24h_lag_and_watermark() {
        let from_5m = downsample_1h_from_5m_sql("softprobe");
        assert!(from_5m.contains("metric_samples_5m"));
        assert!(from_5m.contains("INTERVAL '24 hours'"));
        assert!(from_5m.contains("INSERT INTO softprobe.metric_samples_1h"));
        assert!(from_5m.contains("max(window_ts)"));

        let from_raw = downsample_1h_from_raw_sql("softprobe");
        assert!(from_raw.contains("FROM softprobe.metric_samples\n"));
        assert!(from_raw.contains("INTERVAL '24 hours'"));
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
}
