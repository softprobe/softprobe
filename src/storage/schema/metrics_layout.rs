//! DuckLake DDL for the metrics time-series table family (§6 of metrics-timeseries-layout).
//!
//! After `CREATE TABLE`, DuckLake requires explicit
//! `SET PARTITIONED BY (record_date)` and `SET SORTED BY (...)` — `CREATE TABLE AS … LIMIT 0`
//! alone leaves `ducklake_partition_info` / `ducklake_sort_info` empty.

use anyhow::{anyhow, Result};
use duckdb::Connection;

/// Calendar-day partition key shared by every metrics-layout table.
pub const METRICS_LAYOUT_PARTITION_COLUMN: &str = "record_date";

/// One physical metrics-layout table: column DDL + sort key order from the layout SoT.
#[derive(Debug, Clone, Copy)]
pub struct MetricsLayoutTable {
    pub name: &'static str,
    /// Column definitions inside `CREATE TABLE ( … )` (no surrounding parens).
    pub columns_sql: &'static str,
    /// `SET SORTED BY` column list (no surrounding parens).
    pub sorted_by: &'static str,
}

/// Core ingest tables (AC-D2). Downsample/collapse share the same partition/sort helpers.
pub const METRICS_LAYOUT_CORE_TABLES: &[MetricsLayoutTable] = &[
    MetricsLayoutTable {
        name: "metric_series",
        columns_sql: "\
series_id UBIGINT, \
metric_name VARCHAR, \
metric_type VARCHAR, \
unit VARCHAR, \
description VARCHAR, \
labels VARIANT, \
record_date DATE",
        sorted_by: "metric_name, series_id",
    },
    MetricsLayoutTable {
        name: "metric_postings",
        columns_sql: "\
label_name VARCHAR, \
label_value VARCHAR, \
series_id UBIGINT, \
record_date DATE",
        sorted_by: "label_name, label_value, series_id",
    },
    MetricsLayoutTable {
        name: "metric_samples",
        columns_sql: "\
series_id UBIGINT, \
timestamp TIMESTAMPTZ, \
value DOUBLE, \
record_date DATE",
        sorted_by: "series_id, timestamp",
    },
    MetricsLayoutTable {
        name: "metric_hist_samples",
        columns_sql: "\
series_id UBIGINT, \
timestamp TIMESTAMPTZ, \
count UBIGINT, \
sum DOUBLE, \
bucket_counts UBIGINT[], \
explicit_bounds DOUBLE[], \
record_date DATE",
        sorted_by: "series_id, timestamp",
    },
];

/// Maintenance ladder tables (same PARTITIONED BY; distinct SORTED BY). Not required for AC-D2.
pub const METRICS_LAYOUT_DOWNSAMPLE_TABLES: &[MetricsLayoutTable] = &[
    MetricsLayoutTable {
        name: "metric_samples_5m",
        columns_sql: DOWNSAMPLE_COLUMNS_SQL,
        sorted_by: "series_id, window_ts",
    },
    MetricsLayoutTable {
        name: "metric_samples_1h",
        columns_sql: DOWNSAMPLE_COLUMNS_SQL,
        sorted_by: "series_id, window_ts",
    },
];

const DOWNSAMPLE_COLUMNS_SQL: &str = "\
series_id UBIGINT, \
window_ts TIMESTAMPTZ, \
record_date DATE, \
count UBIGINT, \
sum DOUBLE, \
min DOUBLE, \
max DOUBLE, \
last DOUBLE, \
last_ts TIMESTAMPTZ";

pub const METRICS_LAYOUT_COLLAPSE_TABLES: &[MetricsLayoutTable] = &[MetricsLayoutTable {
    name: "metric_collapse_job_1h",
    columns_sql: "\
metric_name VARCHAR, \
job VARCHAR, \
window_ts TIMESTAMPTZ, \
record_date DATE, \
count UBIGINT, \
sum DOUBLE, \
min DOUBLE, \
max DOUBLE, \
last DOUBLE",
    sorted_by: "metric_name, job, window_ts",
}];

/// Fully qualified table name: `{catalog}.{table}`.
pub fn qualified_metrics_layout_table(catalog_alias: &str, table_name: &str) -> String {
    format!("{catalog_alias}.{table_name}")
}

/// `CREATE TABLE IF NOT EXISTS` for one layout table (no partition/sort yet).
pub fn create_metrics_layout_table_sql(catalog_alias: &str, table: &MetricsLayoutTable) -> String {
    let qualified = qualified_metrics_layout_table(catalog_alias, table.name);
    format!(
        "CREATE TABLE IF NOT EXISTS {qualified} (\n  {}\n);",
        table.columns_sql.replace(", ", ",\n  ")
    )
}

/// Idempotent `SET PARTITIONED BY` + `SET SORTED BY` for one layout table (§7.2).
pub fn apply_metrics_layout_partition_sort_sql(
    catalog_alias: &str,
    table: &MetricsLayoutTable,
) -> String {
    let qualified = qualified_metrics_layout_table(catalog_alias, table.name);
    format!(
        "ALTER TABLE {qualified} SET PARTITIONED BY ({METRICS_LAYOUT_PARTITION_COLUMN});\n\
         ALTER TABLE {qualified} SET SORTED BY ({});",
        table.sorted_by
    )
}

/// Create (if needed) then apply partition + sort for one table.
pub fn ensure_metrics_layout_table_sql(catalog_alias: &str, table: &MetricsLayoutTable) -> String {
    format!(
        "{}\n{}",
        create_metrics_layout_table_sql(catalog_alias, table),
        apply_metrics_layout_partition_sort_sql(catalog_alias, table)
    )
}

/// SQL batch that creates all core metrics-layout tables with partition + sort.
pub fn ensure_metrics_layout_core_tables_sql(catalog_alias: &str) -> String {
    METRICS_LAYOUT_CORE_TABLES
        .iter()
        .map(|t| ensure_metrics_layout_table_sql(catalog_alias, t))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Execute create + partition/sort for every core table on an attached DuckLake connection.
///
/// Skips tables that already have live partition + sort catalog rows so repeated ingest
/// does not publish DuckLake DDL snapshots (G5: one snapshot per `/v1/metrics` data commit).
pub fn ensure_metrics_layout_core_tables(conn: &Connection, catalog_alias: &str) -> Result<()> {
    for table in METRICS_LAYOUT_CORE_TABLES {
        ensure_metrics_layout_table(conn, catalog_alias, table)?;
    }
    Ok(())
}

/// True when a live DuckLake table already has partition + sort metadata (AC-D2 satisfied).
fn metrics_layout_table_ready(
    conn: &Connection,
    catalog_alias: &str,
    table_name: &str,
) -> Result<bool> {
    // catalog_alias may be `softprobe` or `softprobe.tenant_schema` — metadata is always
    // `__ducklake_metadata_<attach_alias>` where attach_alias is the first path segment.
    let attach = catalog_alias.split('.').next().unwrap_or(catalog_alias);
    let meta = format!("__ducklake_metadata_{attach}");
    let sql = format!(
        "SELECT \
            (SELECT count(*) FROM {meta}.ducklake_partition_info info \
             JOIN {meta}.ducklake_table t ON info.table_id = t.table_id \
             WHERE t.table_name = ? AND t.end_snapshot IS NULL) AS parts, \
            (SELECT count(*) FROM {meta}.ducklake_sort_info info \
             JOIN {meta}.ducklake_table t ON info.table_id = t.table_id \
             WHERE t.table_name = ? AND t.end_snapshot IS NULL) AS sorts"
    );
    let (parts, sorts): (i64, i64) = conn
        .query_row(&sql, [table_name, table_name], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap_or((0, 0));
    Ok(parts > 0 && sorts > 0)
}

/// Create + partition/sort one layout table, or no-op when already ready (no DDL snapshot).
pub fn ensure_metrics_layout_table(
    conn: &Connection,
    catalog_alias: &str,
    table: &MetricsLayoutTable,
) -> Result<()> {
    if metrics_layout_table_ready(conn, catalog_alias, table.name)? {
        return Ok(());
    }
    let sql = ensure_metrics_layout_table_sql(catalog_alias, table);
    conn.execute_batch(&sql).map_err(|e| {
        anyhow!(
            "failed to ensure metrics layout table {}.{}: {e}",
            catalog_alias,
            table.name
        )
    })?;
    Ok(())
}

/// Maintenance compaction/expire targets for the metrics family (AC-M1).
///
/// Order matches §7.2: raw/index first, then downsample/collapse. Does not include
/// legacy fat `metrics` or traces/logs/scores.
pub const MAINTENANCE_METRICS_FAMILY_TABLES: &[&str] = &[
    "metric_samples",
    "metric_postings",
    "metric_series",
    "metric_hist_samples",
    "metric_samples_5m",
    "metric_samples_1h",
    "metric_collapse_job_1h",
];

/// All layout tables that maintenance may touch (core + downsample + collapse).
pub fn metrics_layout_family_tables() -> Vec<&'static MetricsLayoutTable> {
    METRICS_LAYOUT_CORE_TABLES
        .iter()
        .chain(METRICS_LAYOUT_DOWNSAMPLE_TABLES.iter())
        .chain(METRICS_LAYOUT_COLLAPSE_TABLES.iter())
        .collect()
}

/// Create (if needed) + partition/sort for the full metrics family (ingest + maintenance).
///
/// Idempotent without catalog churn: once partition/sort exist, further calls are no-ops
/// (AC-N3 — flush-through must stay ≈ one DuckLake snapshot per OTLP metrics commit).
pub fn ensure_metrics_layout_family_tables(conn: &Connection, catalog_alias: &str) -> Result<()> {
    for table in metrics_layout_family_tables() {
        ensure_metrics_layout_table(conn, catalog_alias, table)?;
    }
    Ok(())
}

/// Apply partition + sort only (table must already exist). Useful for maintenance §7.2 step 1.
pub fn apply_metrics_layout_partition_sort(
    conn: &Connection,
    catalog_alias: &str,
    table: &MetricsLayoutTable,
) -> Result<()> {
    let sql = apply_metrics_layout_partition_sort_sql(catalog_alias, table);
    conn.execute_batch(&sql).map_err(|e| {
        anyhow!(
            "failed to apply partition/sort on {}.{}: {e}",
            catalog_alias,
            table.name
        )
    })?;
    Ok(())
}

/// Compatibility SELECT body for public `union_metrics` / `committed_metrics` (AC-D4 / §6.7).
///
/// Joins skinny samples (+ hist) to `metric_series`. Column list matches fat `metrics` so
/// existing SQL / telemetry / Prom scanners keep working without dual-writing fat rows.
/// `labels` is exposed as both `attributes` and `resource_attributes` ( Prom identity +
/// original OTel keys are stored on the series VARIANT at ingest).
pub fn union_metrics_from_layout_sql(catalog_prefix: &str) -> String {
    let series = qualified_metrics_layout_table(catalog_prefix, "metric_series");
    let samples = qualified_metrics_layout_table(catalog_prefix, "metric_samples");
    let hist = qualified_metrics_layout_table(catalog_prefix, "metric_hist_samples");
    format!(
        "SELECT \
s.metric_name, \
s.description, \
s.unit, \
s.metric_type, \
sm.timestamp, \
sm.value, \
s.labels AS attributes, \
s.labels AS resource_attributes, \
NULL::UBIGINT AS count, \
NULL::DOUBLE AS sum, \
NULL::UBIGINT[] AS bucket_counts, \
NULL::DOUBLE[] AS explicit_bounds, \
NULL AS quantiles, \
NULL::VARCHAR AS aggregation_temporality, \
NULL::VARCHAR AS exemplars_json, \
sm.record_date \
FROM {samples} sm \
JOIN {series} s \
  ON sm.series_id = s.series_id AND sm.record_date = s.record_date \
UNION ALL \
SELECT \
s.metric_name, \
s.description, \
s.unit, \
s.metric_type, \
h.timestamp, \
COALESCE(h.sum, 0.0) AS value, \
s.labels AS attributes, \
s.labels AS resource_attributes, \
h.count, \
h.sum, \
h.bucket_counts, \
h.explicit_bounds, \
NULL AS quantiles, \
NULL::VARCHAR AS aggregation_temporality, \
NULL::VARCHAR AS exemplars_json, \
h.record_date \
FROM {hist} h \
JOIN {series} s \
  ON h.series_id = s.series_id AND h.record_date = s.record_date"
    )
}

/// Parenthesized relation for identifier rewrite: `(SELECT …) AS <alias>`.
pub fn union_metrics_layout_relation_sql(catalog_prefix: &str, relation_alias: &str) -> String {
    format!(
        "({inner}) AS {relation_alias}",
        inner = union_metrics_from_layout_sql(catalog_prefix)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn attach_ducklake(temp: &TempDir) -> (duckdb::Connection, String) {
        let meta = temp.path().join("metadata.sqlite");
        let data = temp.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");
        let conn = duckdb::Connection::open_in_memory().expect("duckdb");
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

    fn catalog_count(conn: &Connection, catalog: &str, meta_table: &str, table_name: &str) -> i64 {
        // Join live ducklake_table so empty CREATE-without-ALTER still yields 0 per table.
        let sql = format!(
            "SELECT count(*) \
             FROM __ducklake_metadata_{catalog}.{meta_table} info \
             JOIN __ducklake_metadata_{catalog}.ducklake_table t \
               ON info.table_id = t.table_id \
             WHERE t.table_name = ? \
               AND t.end_snapshot IS NULL"
        );
        conn.query_row(&sql, [table_name], |row| row.get(0))
            .unwrap_or_else(|e| panic!("count {meta_table} for {table_name}: {e}"))
    }

    #[test]
    fn core_table_sort_keys_match_layout_sot() {
        let by_name: std::collections::HashMap<_, _> = METRICS_LAYOUT_CORE_TABLES
            .iter()
            .map(|t| (t.name, t.sorted_by))
            .collect();
        assert_eq!(by_name["metric_series"], "metric_name, series_id");
        assert_eq!(
            by_name["metric_postings"],
            "label_name, label_value, series_id"
        );
        assert_eq!(by_name["metric_samples"], "series_id, timestamp");
        assert_eq!(by_name["metric_hist_samples"], "series_id, timestamp");
    }

    #[test]
    fn partition_sort_sql_uses_record_date_and_sorted_by() {
        let t = &METRICS_LAYOUT_CORE_TABLES[2]; // metric_samples
        let sql = apply_metrics_layout_partition_sort_sql("softprobe", t);
        assert!(
            sql.contains("SET PARTITIONED BY (record_date)"),
            "sql={sql}"
        );
        assert!(
            sql.contains("SET SORTED BY (series_id, timestamp)"),
            "sql={sql}"
        );
    }

    /// T-D2 / AC-D2: after creating the four core tables, partition + sort catalogs are non-empty.
    #[test]
    fn core_metrics_layout_tables_have_partition_and_sort_info() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);

        ensure_metrics_layout_core_tables(&conn, &catalog).expect("ensure core tables");

        for table in METRICS_LAYOUT_CORE_TABLES {
            let partitions =
                catalog_count(&conn, &catalog, "ducklake_partition_info", table.name);
            let sorts = catalog_count(&conn, &catalog, "ducklake_sort_info", table.name);
            assert!(
                partitions > 0,
                "AC-D2/T-D2: expected non-empty ducklake_partition_info for {}",
                table.name
            );
            assert!(
                sorts > 0,
                "AC-D2/T-D2: expected non-empty ducklake_sort_info for {}",
                table.name
            );
        }

        // Idempotent re-apply (§7.2) must not fail.
        ensure_metrics_layout_core_tables(&conn, &catalog).expect("re-ensure core tables");
        for table in METRICS_LAYOUT_CORE_TABLES {
            assert!(catalog_count(&conn, &catalog, "ducklake_partition_info", table.name) > 0);
            assert!(catalog_count(&conn, &catalog, "ducklake_sort_info", table.name) > 0);
        }
    }

    /// AC-N3 root cause: re-ensure must not publish DuckLake snapshots once layout is ready.
    #[test]
    fn reensure_family_tables_does_not_grow_snapshots() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);

        ensure_metrics_layout_family_tables(&conn, &catalog).expect("ensure family");
        let snap_sql = format!(
            "SELECT count(*) FROM __ducklake_metadata_{catalog}.ducklake_snapshot"
        );
        let before: i64 = conn
            .query_row(&snap_sql, [], |row| row.get(0))
            .expect("snap count before");
        assert!(before > 0, "expected DDL snapshots from first ensure");

        for _ in 0..30 {
            ensure_metrics_layout_family_tables(&conn, &catalog).expect("re-ensure");
        }
        let after: i64 = conn
            .query_row(&snap_sql, [], |row| row.get(0))
            .expect("snap count after");
        assert_eq!(
            after, before,
            "re-ensure must be a pure no-op for snapshot count (got {after} vs {before})"
        );
    }

    #[test]
    fn union_metrics_from_layout_sql_joins_samples_and_series() {
        let sql = union_metrics_from_layout_sql("softprobe");
        assert!(sql.contains("softprobe.metric_samples"));
        assert!(sql.contains("softprobe.metric_series"));
        assert!(sql.contains("softprobe.metric_hist_samples"));
        assert!(sql.contains("s.labels AS attributes"));
        assert!(sql.contains("UNION ALL"));
        assert!(!sql.contains("softprobe.metrics ") && !sql.contains("FROM softprobe.metrics"));
        let rel = union_metrics_layout_relation_sql("softprobe", "tm_all_metric");
        assert!(rel.starts_with('('));
        assert!(rel.ends_with(") AS tm_all_metric"));
    }
}
