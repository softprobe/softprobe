//! Downsample correctness checklist (§7.2 ladder + §9.1 query path).
//!
//! | # | Feature | Covered by |
//! |---|---------|------------|
//! | 1 | 5m count/sum/min/max/last from raw | `downsample_5m_aggregates_match_raw_oracle` |
//! | 2 | 5m per-series isolation | `downsample_5m_isolates_series` |
//! | 3 | 1h rollup from 5m (gauge) | `downsample_1h_from_5m_rollup_matches_oracle` |
//! | 4 | 1h from raw fallback | `downsample_1h_from_raw_when_5m_empty` |
//! | 4b | Incremental 1h waits for full 5m hour | `incremental_1h_from_5m_waits_for_complete_hour` |
//! | 5 | Watermark incremental (AC-M2) | `ladder_tests::downsample_keeps_raw_and_second_pass_is_noop` |
//! | 6 | Raw preserved (AC-S2) | `ladder_tests::downsample_keeps_raw_and_second_pass_is_noop` |
//! | 7 | Hist 5m bucket_counts merge | `hist_downsample_5m_merges_bucket_counts` |
//! | 8 | Hist 1h rollup from 5m | `hist_downsample_1h_from_5m_rollup` |
//! | 9 | Query: historical 1h grain values | `query_1h_grain_returns_downsampled_last` |
//! | 10 | Query: historical 5m grain values | `query_5m_grain_returns_downsampled_last` |
//! | 11 | Query: label filter + downsample | `label_filter_with_downsample_returns_correct_series` |
//! | 12 | Grain planner boundaries | `grain::tests::*` + `postings_resolve` SQL shape tests |

use crate::compaction::downsample::{
    downsample_1h_from_5m_sql, downsample_1h_from_raw_sql, downsample_5m_sql,
    hist_downsample_1h_from_5m_for_day_sql, hist_downsample_5m_for_day_sql,
};
use crate::compat::backends::metrics::{LabelMatcher, MatcherOp};
use crate::compat::backends::postings_resolve::{
    equality_postings, resolve_series_ids_sql, samples_scan_sql_for_window, RecordDateRange,
};
use crate::storage::schema::metrics_layout::ensure_metrics_layout_family_tables;
use chrono::{NaiveDate, TimeZone, Utc};
use duckdb::Connection;
use tempfile::TempDir;

/// Fixed historical anchor (matches harness EVAL_END) — always older than downsample lag.
const EVAL_DAY: &str = "2023-11-14";
const EVAL_HOUR: &str = "2023-11-14 10:00:00+00";

fn attach_ducklake(temp: &TempDir) -> (Connection, String) {
    let meta = temp.path().join("metadata.sqlite");
    let data = temp.path().join("data");
    std::fs::create_dir_all(&data).expect("data dir");
    let conn = Connection::open_in_memory().expect("duckdb");
    conn.execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
        .expect("extensions");
    let catalog = "softprobe";
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:sqlite:{}' AS {catalog} \
         (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, \
          DATA_INLINING_ROW_LIMIT 0);",
        meta.to_string_lossy().replace('\'', "''"),
        data.to_string_lossy().replace('\'', "''"),
    ))
    .expect("attach");
    (conn, catalog.to_string())
}

fn commit_sql(body: &str) -> String {
    let body = body.trim().trim_end_matches(';');
    format!("BEGIN TRANSACTION;\n{body};\nCOMMIT;")
}

fn seed_series(conn: &Connection, catalog: &str, series_id: u64, metric_name: &str, job: &str) {
    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_series VALUES \
           ({series_id}, '{metric_name}', 'gauge', '', '', \
            json_object('job', '{job}')::JSON::VARIANT, DATE '{EVAL_DAY}');\n\
         INSERT INTO {catalog}.metric_postings VALUES \
           ('__name__', '{metric_name}', {series_id}, DATE '{EVAL_DAY}'),\
           ('job', '{job}', {series_id}, DATE '{EVAL_DAY}');"
    ))
    .expect("seed series");
}

/// Checklist #1: 5m aggregates match a hand-computed oracle for one closed bucket.
#[test]
fn downsample_5m_aggregates_match_raw_oracle() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");
    seed_series(&conn, &catalog, 1, "layout_gauge", "api");

    // Five points in the 10:00–10:04 bucket (closed well before now()).
    let values = [1.0, 2.0, 3.0, 4.0, 5.0];
    let mut inserts = String::new();
    for (i, v) in values.iter().enumerate() {
        inserts.push_str(&format!(
            "(1, TIMESTAMPTZ '2023-11-14 10:0{i}:00+00', {v}, DATE '{EVAL_DAY}'),"
        ));
    }
    inserts.pop();
    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_samples VALUES {inserts};"
    ))
    .expect("seed raw");

    conn.execute_batch(&commit_sql(&downsample_5m_sql(&catalog)))
        .expect("5m downsample");

    let row: (i64, f64, f64, f64, f64) = conn
        .query_row(
            &format!(
                "SELECT count, sum, min, max, last FROM {catalog}.metric_samples_5m \
                 WHERE series_id = 1 AND window_ts = TIMESTAMPTZ '2023-11-14 10:00:00+00'"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .expect("5m row");

    assert_eq!(row.0, 5, "count");
    assert!((row.1 - 15.0).abs() < 1e-9, "sum");
    assert!((row.2 - 1.0).abs() < 1e-9, "min");
    assert!((row.3 - 5.0).abs() < 1e-9, "max");
    assert!((row.4 - 5.0).abs() < 1e-9, "last");
}

/// Checklist #2: downsample never mixes series_id buckets.
#[test]
fn downsample_5m_isolates_series() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");
    seed_series(&conn, &catalog, 1, "layout_gauge", "job_a");
    seed_series(&conn, &catalog, 2, "layout_gauge", "job_b");

    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_samples VALUES \
           (1, TIMESTAMPTZ '{EVAL_HOUR}', 10.0, DATE '{EVAL_DAY}'),\
           (2, TIMESTAMPTZ '{EVAL_HOUR}', 20.0, DATE '{EVAL_DAY}');"
    ))
    .expect("seed");

    conn.execute_batch(&commit_sql(&downsample_5m_sql(&catalog)))
        .expect("5m");

    let a: f64 = conn
        .query_row(
            &format!(
                "SELECT last FROM {catalog}.metric_samples_5m \
                 WHERE series_id = 1 AND window_ts = TIMESTAMPTZ '{EVAL_HOUR}'"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    let b: f64 = conn
        .query_row(
            &format!(
                "SELECT last FROM {catalog}.metric_samples_5m \
                 WHERE series_id = 2 AND window_ts = TIMESTAMPTZ '{EVAL_HOUR}'"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!((a - 10.0).abs() < 1e-9);
    assert!((b - 20.0).abs() < 1e-9);
}

/// Checklist #3: 1h rollup from 5m matches raw oracle for count/sum/min/max/last.
#[test]
fn downsample_1h_from_5m_rollup_matches_oracle() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");
    seed_series(&conn, &catalog, 1, "layout_gauge", "api");

    // Twelve 5m buckets in hour 10:00 — one point per bucket with increasing values.
    let mut inserts = String::new();
    for bucket in 0..12 {
        let minute = bucket * 5;
        let value = (bucket + 1) as f64;
        inserts.push_str(&format!(
            "(1, TIMESTAMPTZ '2023-11-14 10:{minute:02}:00+00', {value}, DATE '{EVAL_DAY}'),"
        ));
    }
    inserts.pop();
    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_samples VALUES {inserts};"
    ))
    .expect("seed");

    conn.execute_batch(&commit_sql(&downsample_5m_sql(&catalog)))
        .expect("5m");
    conn.execute_batch(&commit_sql(&downsample_1h_from_5m_sql(&catalog)))
        .expect("1h from 5m");

    let row: (i64, f64, f64, f64, f64) = conn
        .query_row(
            &format!(
                "SELECT count, sum, min, max, last FROM {catalog}.metric_samples_1h \
                 WHERE series_id = 1 AND window_ts = TIMESTAMPTZ '2023-11-14 10:00:00+00'"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .expect("1h row");

    assert_eq!(row.0, 12, "count");
    assert!((row.1 - 78.0).abs() < 1e-9, "sum 1..=12");
    assert!((row.2 - 1.0).abs() < 1e-9, "min");
    assert!((row.3 - 12.0).abs() < 1e-9, "max");
    assert!((row.4 - 12.0).abs() < 1e-9, "last from latest 5m bucket");
}

/// Checklist #4: 1h-from-raw fallback works when 5m table is empty.
#[test]
fn downsample_1h_from_raw_when_5m_empty() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");
    seed_series(&conn, &catalog, 1, "layout_gauge", "api");

    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_samples VALUES \
           (1, TIMESTAMPTZ '2023-11-14 10:00:00+00', 7.0, DATE '{EVAL_DAY}'),\
           (1, TIMESTAMPTZ '2023-11-14 10:30:00+00', 9.0, DATE '{EVAL_DAY}');"
    ))
    .expect("seed");

    let five_n: i64 = conn
        .query_row(
            &format!("SELECT count(*) FROM {catalog}.metric_samples_5m"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(five_n, 0);

    conn.execute_batch(&commit_sql(&downsample_1h_from_raw_sql(&catalog)))
        .expect("1h from raw");

    let row: (i64, f64) = conn
        .query_row(
            &format!(
                "SELECT count, last FROM {catalog}.metric_samples_1h \
                 WHERE series_id = 1 AND window_ts = TIMESTAMPTZ '2023-11-14 10:00:00+00'"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("1h row");
    assert_eq!(row.0, 2);
    assert!((row.1 - 9.0).abs() < 1e-9, "last from later point in hour");
}

/// Checklist #4b: partial 5m hour must not freeze 1h watermark; full hour lands once complete.
#[test]
fn incremental_1h_from_5m_waits_for_complete_hour() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");
    seed_series(&conn, &catalog, 1, "layout_gauge", "api");

    // Pass 1: only the first 5m bucket exists in the ladder.
    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_samples_5m VALUES \
           (1, TIMESTAMPTZ '2023-11-14 10:00:00+00', DATE '{EVAL_DAY}', 1::UBIGINT, 1.0, \
            1.0, 1.0, 1.0, TIMESTAMPTZ '2023-11-14 10:00:00+00');"
    ))
    .expect("partial 5m");
    conn.execute_batch(&commit_sql(&downsample_1h_from_5m_sql(&catalog)))
        .expect("1h pass1");
    let n1: i64 = conn
        .query_row(
            &format!("SELECT count(*) FROM {catalog}.metric_samples_1h"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n1, 0, "partial hour must not materialize 1h");

    // Pass 2: remaining 5m buckets through :55 (oracle count=12).
    let mut inserts = String::new();
    for bucket in 1..12 {
        let minute = bucket * 5;
        let value = (bucket + 1) as f64;
        inserts.push_str(&format!(
            "(1, TIMESTAMPTZ '2023-11-14 10:{minute:02}:00+00', DATE '{EVAL_DAY}', \
             1::UBIGINT, {value}, {value}, {value}, {value}, \
             TIMESTAMPTZ '2023-11-14 10:{minute:02}:00+00'),"
        ));
    }
    inserts.pop();
    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_samples_5m VALUES {inserts};"
    ))
    .expect("rest of 5m");
    conn.execute_batch(&commit_sql(&downsample_1h_from_5m_sql(&catalog)))
        .expect("1h pass2");

    let row: (i64, f64) = conn
        .query_row(
            &format!(
                "SELECT count, last FROM {catalog}.metric_samples_1h \
                 WHERE series_id = 1 AND window_ts = TIMESTAMPTZ '2023-11-14 10:00:00+00'"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("1h row");
    assert_eq!(row.0, 12);
    assert!((row.1 - 12.0).abs() < 1e-9);
}

/// Checklist #7: hist 5m merges bucket_counts element-wise.
#[test]
fn hist_downsample_5m_merges_bucket_counts() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");

    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_series VALUES \
           (10, 'layout_latency', 'histogram', 's', '', '{{}}'::JSON::VARIANT, DATE '{EVAL_DAY}');\n\
         INSERT INTO {catalog}.metric_hist_samples VALUES \
           (10, TIMESTAMPTZ '2023-11-14 10:01:00+00', 2::UBIGINT, 0.2, \
            [1::UBIGINT, 2::UBIGINT], [0.0, 1.0]::DOUBLE[], DATE '{EVAL_DAY}'),\
           (10, TIMESTAMPTZ '2023-11-14 10:03:00+00', 3::UBIGINT, 0.3, \
            [4::UBIGINT, 5::UBIGINT], [0.0, 1.0]::DOUBLE[], DATE '{EVAL_DAY}');"
    ))
    .expect("seed hist");

    let day = NaiveDate::from_ymd_opt(2023, 11, 14).unwrap();
    conn.execute_batch(&commit_sql(&hist_downsample_5m_for_day_sql(
        &catalog,
        Some(day),
    )))
    .expect("hist 5m");

    let (b0, b1): (i64, i64) = conn
        .query_row(
            &format!(
                "SELECT bucket_counts[1], bucket_counts[2] FROM {catalog}.metric_hist_samples_5m \
                 WHERE series_id = 10 AND window_ts = TIMESTAMPTZ '2023-11-14 10:00:00+00'"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("hist 5m buckets");
    assert_eq!(b0, 5);
    assert_eq!(b1, 7, "element-wise sum of [1,2] + [4,5]");
    let total_count: i64 = conn
        .query_row(
            &format!(
                "SELECT count FROM {catalog}.metric_hist_samples_5m \
                 WHERE series_id = 10 AND window_ts = TIMESTAMPTZ '2023-11-14 10:00:00+00'"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(total_count, 5);
}

/// Checklist #8: hist 1h rollup from 5m preserves merged bucket_counts.
#[test]
fn hist_downsample_1h_from_5m_rollup() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");

    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_series VALUES \
           (10, 'layout_latency', 'histogram', 's', '', '{{}}'::JSON::VARIANT, DATE '{EVAL_DAY}');\n\
         INSERT INTO {catalog}.metric_hist_samples_5m VALUES \
           (10, TIMESTAMPTZ '2023-11-14 10:00:00+00', DATE '{EVAL_DAY}', 5::UBIGINT, 0.5, \
            [1::UBIGINT, 2::UBIGINT], [0.0, 1.0]::DOUBLE[], TIMESTAMPTZ '2023-11-14 10:04:00+00'),\
           (10, TIMESTAMPTZ '2023-11-14 10:05:00+00', DATE '{EVAL_DAY}', 7::UBIGINT, 0.7, \
            [3::UBIGINT, 4::UBIGINT], [0.0, 1.0]::DOUBLE[], TIMESTAMPTZ '2023-11-14 10:09:00+00'),\
           (10, TIMESTAMPTZ '2023-11-14 10:55:00+00', DATE '{EVAL_DAY}', 0::UBIGINT, 0.0, \
            [0::UBIGINT, 0::UBIGINT], [0.0, 1.0]::DOUBLE[], TIMESTAMPTZ '2023-11-14 10:59:00+00');"
    ))
    .expect("seed hist 5m");

    let day = NaiveDate::from_ymd_opt(2023, 11, 14).unwrap();
    conn.execute_batch(&commit_sql(&hist_downsample_1h_from_5m_for_day_sql(
        &catalog,
        Some(day),
    )))
    .expect("hist 1h");

    let (b0, b1): (i64, i64) = conn
        .query_row(
            &format!(
                "SELECT bucket_counts[1], bucket_counts[2] FROM {catalog}.metric_hist_samples_1h \
                 WHERE series_id = 10 AND window_ts = TIMESTAMPTZ '2023-11-14 10:00:00+00'"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("hist 1h buckets");
    assert_eq!(b0, 4);
    assert_eq!(b1, 6);
    let total_count: i64 = conn
        .query_row(
            &format!(
                "SELECT count FROM {catalog}.metric_hist_samples_1h \
                 WHERE series_id = 10 AND window_ts = TIMESTAMPTZ '2023-11-14 10:00:00+00'"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(total_count, 12);
}

fn eval_end_ms() -> i64 {
    1_700_000_000_000
}

/// Checklist #9: Prom sample scan on historical 30d window returns 1h `last` values.
#[test]
fn query_1h_grain_returns_downsampled_last() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");
    seed_series(&conn, &catalog, 1, "layout_tall", "tall");

    // One hourly point for a closed hour.
    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_samples VALUES \
           (1, TIMESTAMPTZ '2023-11-14 10:00:00+00', 42.0, DATE '{EVAL_DAY}');"
    ))
    .expect("seed");
    conn.execute_batch(&commit_sql(&downsample_1h_from_raw_sql(&catalog)))
        .expect("1h");

    let end = eval_end_ms();
    let start = end - 30 * 86_400_000;
    let sql = samples_scan_sql_for_window(
        &catalog,
        &[1],
        Some(start),
        Some(end),
        Some(3_600_000),
        "NULL::VARCHAR AS lbl",
        false,
        false,
        true,
        1000,
    );
    assert!(
        sql.contains("metric_samples_1h"),
        "historical 30d must use 1h grain: {sql}"
    );

    let mut stmt = conn.prepare(&sql).expect("prepare");
    let rows: Vec<f64> = stmt
        .query_map([], |r| r.get::<_, f64>(2))
        .expect("query")
        .map(|r| r.expect("row"))
        .collect();
    assert!(
        rows.iter().any(|v| (*v - 42.0).abs() < 1e-9),
        "expected downsampled last=42.0 in query results, got {rows:?}"
    );
}

/// Checklist #10: Prom sample scan on historical 2d window returns 5m `last` values.
#[test]
fn query_5m_grain_returns_downsampled_last() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");
    seed_series(&conn, &catalog, 1, "layout_gauge", "api");

    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_samples VALUES \
           (1, TIMESTAMPTZ '2023-11-14 10:02:00+00', 11.0, DATE '{EVAL_DAY}'),\
           (1, TIMESTAMPTZ '2023-11-14 10:04:00+00', 22.0, DATE '{EVAL_DAY}');"
    ))
    .expect("seed");
    conn.execute_batch(&commit_sql(&downsample_5m_sql(&catalog)))
        .expect("5m");

    let end = eval_end_ms();
    let start = end - 2 * 86_400_000;
    let sql = samples_scan_sql_for_window(
        &catalog,
        &[1],
        Some(start),
        Some(end),
        Some(300_000),
        "NULL::VARCHAR AS lbl",
        false,
        false,
        true,
        1000,
    );
    assert!(
        sql.contains("metric_samples_5m"),
        "historical 2d must use 5m grain: {sql}"
    );

    let mut stmt = conn.prepare(&sql).expect("prepare");
    let rows: Vec<f64> = stmt
        .query_map([], |r| r.get::<_, f64>(2))
        .expect("query")
        .map(|r| r.expect("row"))
        .collect();
    assert!(
        rows.iter().any(|v| (*v - 22.0).abs() < 1e-9),
        "expected 5m last=22.0, got {rows:?}"
    );
}

/// Checklist #11: postings label filter + downsampled 1h query returns the right series.
#[test]
fn label_filter_with_downsample_returns_correct_series() {
    let temp = TempDir::new().unwrap();
    let (conn, catalog) = attach_ducklake(&temp);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");
    seed_series(&conn, &catalog, 1, "layout_http", "job0");
    seed_series(&conn, &catalog, 2, "layout_http", "job1");

    conn.execute_batch(&format!(
        "INSERT INTO {catalog}.metric_samples VALUES \
           (1, TIMESTAMPTZ '2023-11-14 10:00:00+00', 100.0, DATE '{EVAL_DAY}'),\
           (2, TIMESTAMPTZ '2023-11-14 10:00:00+00', 200.0, DATE '{EVAL_DAY}');"
    ))
    .expect("seed");
    conn.execute_batch(&commit_sql(&downsample_1h_from_raw_sql(&catalog)))
        .expect("1h");

    let day = Utc.with_ymd_and_hms(2023, 11, 14, 12, 0, 0).unwrap();
    let days = RecordDateRange {
        start: Some(day.date_naive()),
        end: Some(day.date_naive()),
    };
    let eq = equality_postings(&[
        LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_http".into(),
        },
        LabelMatcher {
            name: "job".into(),
            op: MatcherOp::Eq,
            value: "job1".into(),
        },
    ]);
    let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, 10_000);
    let mut stmt = conn.prepare(&resolve_sql).expect("resolve");
    let ids: Vec<u64> = stmt
        .query_map([], |r| r.get(0))
        .expect("query")
        .map(|r| r.expect("row"))
        .collect();
    assert_eq!(ids, vec![2], "job=job1 must resolve series_id 2");

    let end = eval_end_ms();
    let start = end - 30 * 86_400_000;
    let samples_sql = samples_scan_sql_for_window(
        &catalog,
        &ids,
        Some(start),
        Some(end),
        Some(3_600_000),
        "NULL::VARCHAR AS lbl",
        false,
        false,
        true,
        1000,
    );
    let mut sstmt = conn.prepare(&samples_sql).expect("samples");
    let values: Vec<f64> = sstmt
        .query_map([], |r| r.get::<_, f64>(2))
        .expect("query")
        .map(|r| r.expect("row"))
        .collect();
    assert_eq!(values.len(), 1);
    assert!((values[0] - 200.0).abs() < 1e-9);
}
