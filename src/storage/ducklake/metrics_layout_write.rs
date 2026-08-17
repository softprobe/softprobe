//! One-transaction metrics layout ingest (§8 of metrics-timeseries-layout).
//!
//! Decode/canonicalize → `series_id` → INSERT series + postings + samples|hist
//! in a single BEGIN…COMMIT. Does not write 5m/1h/collapse or legacy fat `metrics`.

use crate::compat::projection::prometheus::project_prometheus_labels;
use crate::models::Metric;
use crate::storage::ducklake::util::escape_sql_literal;
use crate::storage::schema::metrics_layout::{
    ensure_metrics_layout_family_tables, qualified_metrics_layout_table,
};
use crate::storage::schema::variant::encode_attributes_json;
use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, Utc};
use duckdb::Connection;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Matches capability `limits.max_labels_per_series` default.
pub const DEFAULT_MAX_LABELS_PER_SERIES: usize = 40;

/// Stable FNV-1a 64-bit hash for `series_id = hash(metric_name, sorted label pairs)`.
pub fn series_id_hash(metric_name: &str, labels: &BTreeMap<String, String>) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    hash = fnv1a64_update(hash, metric_name.as_bytes());
    hash = fnv1a64_update(hash, &[0xff]);
    for (k, v) in labels {
        hash = fnv1a64_update(hash, k.as_bytes());
        hash = fnv1a64_update(hash, &[0xfe]);
        hash = fnv1a64_update(hash, v.as_bytes());
        hash = fnv1a64_update(hash, &[0xfd]);
    }
    hash
}

fn fnv1a64_update(mut hash: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn is_hist_metric_type(metric_type: &str) -> bool {
    matches!(
        metric_type.to_ascii_lowercase().as_str(),
        "histogram" | "summary"
    )
}

#[derive(Debug, Clone)]
struct SeriesRow {
    series_id: u64,
    metric_name: String,
    metric_type: String,
    unit: String,
    description: String,
    labels_json: String,
    record_date: NaiveDate,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct PostingKey {
    label_name: String,
    label_value: String,
    series_id: u64,
    record_date: NaiveDate,
}

#[derive(Debug, Clone)]
struct SampleRow {
    series_id: u64,
    timestamp: DateTime<Utc>,
    value: f64,
    record_date: NaiveDate,
}

#[derive(Debug, Clone)]
struct HistSampleRow {
    series_id: u64,
    timestamp: DateTime<Utc>,
    count: Option<u64>,
    sum: Option<f64>,
    bucket_counts: Option<Vec<u64>>,
    explicit_bounds: Option<Vec<f64>>,
    record_date: NaiveDate,
}

struct PreparedIngest {
    series: Vec<SeriesRow>,
    postings: Vec<PostingKey>,
    samples: Vec<SampleRow>,
    hist_samples: Vec<HistSampleRow>,
}

/// Series VARIANT for SQL/Prom bridges: original OTel keys (dotted) plus sanitized
/// Prom identity labels (`job`, `instance`, `__name__`, …). Postings / series_id still
/// use only the sanitized map.
fn labels_to_json(
    labels: &BTreeMap<String, String>,
    resource: &HashMap<String, String>,
    datapoint: &HashMap<String, String>,
) -> String {
    let mut as_map: HashMap<String, String> = HashMap::new();
    for (k, v) in resource {
        if !k.is_empty() {
            as_map.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in datapoint {
        if !k.is_empty() {
            as_map.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in labels {
        as_map.insert(k.clone(), v.clone());
    }
    // Rehydrate nested `sp.json:` values the same way fat VARIANT encoding does.
    encode_attributes_json(&as_map)
}

fn prepare_ingest(metrics: &[Metric], max_labels: usize) -> PreparedIngest {
    let mut series_seen: HashSet<(NaiveDate, u64)> = HashSet::new();
    let mut posting_seen: HashSet<PostingKey> = HashSet::new();
    let mut series = Vec::new();
    let mut postings = Vec::new();
    let mut samples = Vec::new();
    let mut hist_samples = Vec::new();

    for m in metrics {
        let labels = project_prometheus_labels(
            &m.metric_name,
            &m.resource_attributes,
            &m.attributes,
            max_labels,
        );
        let series_id = series_id_hash(&m.metric_name, &labels);
        let record_date = m.timestamp.date_naive();

        if series_seen.insert((record_date, series_id)) {
            series.push(SeriesRow {
                series_id,
                metric_name: m.metric_name.clone(),
                metric_type: m.metric_type.clone(),
                unit: m.unit.clone(),
                description: m.description.clone(),
                labels_json: labels_to_json(
                    &labels,
                    &m.resource_attributes,
                    &m.attributes,
                ),
                record_date,
            });
        }

        for (name, value) in &labels {
            let key = PostingKey {
                label_name: name.clone(),
                label_value: value.clone(),
                series_id,
                record_date,
            };
            if posting_seen.insert(key.clone()) {
                postings.push(key);
            }
        }

        if is_hist_metric_type(&m.metric_type) {
            hist_samples.push(HistSampleRow {
                series_id,
                timestamp: m.timestamp,
                count: m.count,
                sum: m.sum,
                bucket_counts: m.bucket_counts.clone(),
                explicit_bounds: m.explicit_bounds.clone(),
                record_date,
            });
        } else {
            samples.push(SampleRow {
                series_id,
                timestamp: m.timestamp,
                value: m.value,
                record_date,
            });
        }
    }

    PreparedIngest {
        series,
        postings,
        samples,
        hist_samples,
    }
}

fn sql_str(s: &str) -> String {
    format!("'{}'", escape_sql_literal(s))
}

fn sql_date(d: NaiveDate) -> String {
    format!("DATE '{}'", d)
}

fn sql_ts(ts: DateTime<Utc>) -> String {
    format!(
        "TIMESTAMPTZ '{}'",
        ts.format("%Y-%m-%d %H:%M:%S%.6f+00")
    )
}

fn sql_f64(v: f64) -> String {
    if v.is_nan() {
        "CAST('NaN' AS DOUBLE)".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            "CAST('Infinity' AS DOUBLE)".to_string()
        } else {
            "CAST('-Infinity' AS DOUBLE)".to_string()
        }
    } else {
        // Debug keeps enough precision for gauges without scientific surprises.
        format!("{v:?}")
    }
}

fn sql_u64_array(vals: Option<&[u64]>) -> String {
    match vals {
        None => "NULL".to_string(),
        Some(v) => {
            let inner = v
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{inner}]::UBIGINT[]")
        }
    }
}

fn sql_f64_array(vals: Option<&[f64]>) -> String {
    match vals {
        None => "NULL".to_string(),
        Some(v) => {
            let inner = v.iter().map(|x| sql_f64(*x)).collect::<Vec<_>>().join(", ");
            format!("[{inner}]::DOUBLE[]")
        }
    }
}

fn insert_series_sql(catalog: &str, rows: &[SeriesRow]) -> String {
    let table = qualified_metrics_layout_table(catalog, "metric_series");
    let values = rows
        .iter()
        .map(|r| {
            format!(
                "({id}::UBIGINT, {name}, {mtype}, {unit}, {desc}, {labels}::JSON::VARIANT, {rd})",
                id = r.series_id,
                name = sql_str(&r.metric_name),
                mtype = sql_str(&r.metric_type),
                unit = sql_str(&r.unit),
                desc = sql_str(&r.description),
                labels = sql_str(&r.labels_json),
                rd = sql_date(r.record_date),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "INSERT INTO {table} (series_id, metric_name, metric_type, unit, description, labels, record_date)\n\
         SELECT * FROM (VALUES\n{values}\n) AS v(series_id, metric_name, metric_type, unit, description, labels, record_date)\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {table} e\n\
           WHERE e.record_date = v.record_date AND e.series_id = v.series_id\n\
         );"
    )
}

fn insert_postings_sql(catalog: &str, rows: &[PostingKey]) -> String {
    let table = qualified_metrics_layout_table(catalog, "metric_postings");
    let values = rows
        .iter()
        .map(|r| {
            format!(
                "({ln}, {lv}, {id}::UBIGINT, {rd})",
                ln = sql_str(&r.label_name),
                lv = sql_str(&r.label_value),
                id = r.series_id,
                rd = sql_date(r.record_date),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "INSERT INTO {table} (label_name, label_value, series_id, record_date)\n\
         SELECT * FROM (VALUES\n{values}\n) AS v(label_name, label_value, series_id, record_date)\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {table} e\n\
           WHERE e.record_date = v.record_date\n\
             AND e.label_name = v.label_name\n\
             AND e.label_value = v.label_value\n\
             AND e.series_id = v.series_id\n\
         );"
    )
}

fn insert_samples_sql(catalog: &str, rows: &[SampleRow]) -> String {
    let table = qualified_metrics_layout_table(catalog, "metric_samples");
    let values = rows
        .iter()
        .map(|r| {
            format!(
                "({id}::UBIGINT, {ts}, {val}, {rd})",
                id = r.series_id,
                ts = sql_ts(r.timestamp),
                val = sql_f64(r.value),
                rd = sql_date(r.record_date),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "INSERT INTO {table} (series_id, timestamp, value, record_date) VALUES\n{values};"
    )
}

fn insert_hist_sql(catalog: &str, rows: &[HistSampleRow]) -> String {
    let table = qualified_metrics_layout_table(catalog, "metric_hist_samples");
    let values = rows
        .iter()
        .map(|r| {
            let count = r
                .count
                .map(|c| format!("{c}::UBIGINT"))
                .unwrap_or_else(|| "NULL".to_string());
            let sum = r
                .sum
                .map(sql_f64)
                .unwrap_or_else(|| "NULL".to_string());
            format!(
                "({id}::UBIGINT, {ts}, {count}, {sum}, {buckets}, {bounds}, {rd})",
                id = r.series_id,
                ts = sql_ts(r.timestamp),
                buckets = sql_u64_array(r.bucket_counts.as_deref()),
                bounds = sql_f64_array(r.explicit_bounds.as_deref()),
                rd = sql_date(r.record_date),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "INSERT INTO {table} (series_id, timestamp, count, sum, bucket_counts, explicit_bounds, record_date)\n\
         VALUES\n{values};"
    )
}

const INSERT_CHUNK: usize = 256;

fn exec_chunked<T>(
    conn: &Connection,
    rows: &[T],
    mut build: impl FnMut(&[T]) -> String,
) -> Result<()> {
    for chunk in rows.chunks(INSERT_CHUNK) {
        if chunk.is_empty() {
            continue;
        }
        let sql = build(chunk);
        conn.execute_batch(&sql)
            .map_err(|e| anyhow!("metrics layout insert failed: {e}\nSQL head: {}", &sql[..sql.len().min(400)]))?;
    }
    Ok(())
}

/// Ensure core (+ family DDL for maintenance targets) then ingest in one transaction.
pub fn write_metrics_layout_txn(
    conn: &Connection,
    catalog_alias: &str,
    metrics: &[Metric],
    max_labels: usize,
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }
    // Family ensure is outside the sample txn so CREATE/ALTER snapshots stay separate
    // from the data commit (one data BEGIN…COMMIT per successful /v1/metrics).
    ensure_metrics_layout_family_tables(conn, catalog_alias)?;

    let prepared = prepare_ingest(metrics, max_labels);
    conn.execute_batch("BEGIN TRANSACTION;")?;
    let write = (|| -> Result<()> {
        exec_chunked(conn, &prepared.series, |c| insert_series_sql(catalog_alias, c))?;
        exec_chunked(conn, &prepared.postings, |c| {
            insert_postings_sql(catalog_alias, c)
        })?;
        exec_chunked(conn, &prepared.samples, |c| {
            insert_samples_sql(catalog_alias, c)
        })?;
        exec_chunked(conn, &prepared.hist_samples, |c| {
            insert_hist_sql(catalog_alias, c)
        })?;
        Ok(())
    })();
    match write {
        Ok(()) => {
            conn.execute_batch("COMMIT;")
                .map_err(|e| anyhow!("metrics layout COMMIT failed: {e}"))?;
            Ok(())
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK;");
            Err(e)
        }
    }
}

/// Catalog prefix for layout DDL/DML (`alias` or `alias.schema`).
pub fn layout_catalog_prefix(catalog_alias: &str, metadata_schema: &str) -> String {
    if metadata_schema == "main" {
        catalog_alias.to_string()
    } else {
        format!("{catalog_alias}.{metadata_schema}")
    }
}

#[cfg(test)]
mod layout_catalog_prefix_tests {
    use super::layout_catalog_prefix;

    #[test]
    fn layout_catalog_prefix_keeps_main_unqualified() {
        assert_eq!(layout_catalog_prefix("softprobe", "main"), "softprobe");
    }

    #[test]
    fn layout_catalog_prefix_includes_tenant_schema() {
        assert_eq!(
            layout_catalog_prefix("softprobe", "metrics_layout_local_dev_tenant"),
            "softprobe.metrics_layout_local_dev_tenant"
        );
    }
}

/// Sum on-disk parquet bytes under `data_path` for one DuckLake table folder.
///
/// Matches path segments `/<table_name>/` or `/<table_name>.` so `metrics` does
/// not accidentally include `metric_samples` / `metric_series` / …
#[cfg(test)]
pub fn sum_parquet_bytes_for_table(data_path: &std::path::Path, table_name: &str) -> u64 {
    let needle_slash = format!("/{table_name}/");
    let needle_dot = format!("/{table_name}.");
    fn walk(dir: &std::path::Path, needle_slash: &str, needle_dot: &str, acc: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, needle_slash, needle_dot, acc);
                continue;
            }
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if !(name.ends_with(".parquet") || name.ends_with(".parq")) {
                continue;
            }
            let path_str = path.to_string_lossy().replace('\\', "/");
            if path_str.contains(needle_slash) || path_str.contains(needle_dot) {
                if let Ok(meta) = entry.metadata() {
                    *acc += meta.len();
                }
            }
        }
    }
    let mut total = 0u64;
    walk(data_path, &needle_slash, &needle_dot, &mut total);
    total
}

/// Write the same points through the legacy fat `metrics` column list (throwaway AC-S1 path).
#[cfg(test)]
pub fn write_legacy_fat_metrics_throwaway(
    conn: &Connection,
    catalog_alias: &str,
    metrics: &[Metric],
) -> Result<()> {
    use crate::storage::schema::tables::OtlpMetricsTable;
    use crate::storage::schema::{arrow, variant};

    if metrics.is_empty() {
        return Ok(());
    }
    let table = qualified_metrics_layout_table(catalog_alias, "metrics");
    let schema = OtlpMetricsTable::schema();
    let batch = arrow::metrics_to_record_batch(metrics, &schema)?;
    let base = std::env::temp_dir().join("splake-ducklake-fat-throwaway");
    std::fs::create_dir_all(&base)?;
    let temp_path = base.join(format!(
        "fat-{}.parquet",
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    ));
    {
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            file,
            batch.schema(),
            Some(parquet::file::properties::WriterProperties::builder().build()),
        )?;
        writer.write(&batch)?;
        writer.close()?;
    }
    let escaped = escape_sql_literal(temp_path.to_string_lossy().as_ref());
    let select = variant::parquet_select_with_variant_casts("metrics");
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {table} AS {select} FROM read_parquet('{escaped}') LIMIT 0;"
    );
    let insert = format!(
        "INSERT INTO {table} BY NAME {select} FROM read_parquet('{escaped}');"
    );
    conn.execute_batch("BEGIN TRANSACTION;")?;
    let result = (|| -> Result<()> {
        conn.execute_batch(&ddl)?;
        conn.execute_batch(&insert)?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            let _ = std::fs::remove_file(&temp_path);
            Ok(())
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK;");
            let _ = std::fs::remove_file(&temp_path);
            Err(e)
        }
    }
}

/// Count VARIANT-typed columns on a live DuckLake table (AC-S1).
#[cfg(test)]
pub fn count_variant_columns(conn: &Connection, catalog: &str, table_name: &str) -> Result<i64> {
    let describe_sql = format!(
        "DESCRIBE {}",
        qualified_metrics_layout_table(catalog, table_name)
    );
    let mut stmt = conn.prepare(&describe_sql)?;
    let rows = stmt.query_map([], |row| {
        let col_type: String = row.get(1)?;
        Ok(col_type)
    })?;
    let mut n = 0i64;
    for r in rows {
        let t = r?;
        if t.to_ascii_uppercase().contains("VARIANT") {
            n += 1;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::schema::metrics_layout::{
        ensure_metrics_layout_core_tables, MAINTENANCE_METRICS_FAMILY_TABLES,
        METRICS_LAYOUT_CORE_TABLES,
    };
    use chrono::TimeZone;
    use tempfile::TempDir;

    fn attach_ducklake(temp: &TempDir) -> (Connection, String, std::path::PathBuf) {
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
        (conn, catalog.to_string(), data)
    }

    fn gauge(name: &str, instance: &str, ts: DateTime<Utc>, value: f64) -> Metric {
        let mut attrs = HashMap::new();
        attrs.insert("service.instance.id".into(), instance.into());
        let mut resource = HashMap::new();
        resource.insert("service.name".into(), "layout-test".into());
        Metric {
            metric_name: name.into(),
            description: "d".into(),
            unit: "1".into(),
            metric_type: "gauge".into(),
            timestamp: ts,
            value,
            attributes: attrs,
            resource_attributes: resource,
            ..Default::default()
        }
    }

    fn hist(name: &str, ts: DateTime<Utc>) -> Metric {
        let mut resource = HashMap::new();
        resource.insert("service.name".into(), "layout-test".into());
        Metric {
            metric_name: name.into(),
            description: "latency".into(),
            unit: "ms".into(),
            metric_type: "histogram".into(),
            timestamp: ts,
            value: 100.0,
            resource_attributes: resource,
            count: Some(10),
            sum: Some(100.0),
            bucket_counts: Some(vec![2, 5, 3]),
            explicit_bounds: Some(vec![10.0, 50.0]),
            ..Default::default()
        }
    }

    fn table_exists(conn: &Connection, catalog: &str, name: &str) -> bool {
        let sql = format!(
            "SELECT count(*) FROM __ducklake_metadata_{catalog}.ducklake_table \
             WHERE table_name = ? AND end_snapshot IS NULL"
        );
        let n: i64 = conn.query_row(&sql, [name], |r| r.get(0)).unwrap_or(0);
        n > 0
    }

    /// T-D1 / AC-D1: after ingest, core layout tables exist and receive rows.
    #[test]
    fn ingest_creates_layout_tables_and_rows() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog, _) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[gauge("k6_vus", "i1", ts, 3.0)],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("ingest");

        for t in METRICS_LAYOUT_CORE_TABLES {
            assert!(
                table_exists(&conn, &catalog, t.name),
                "AC-D1: missing table {}",
                t.name
            );
        }
        let series_n: i64 = conn
            .query_row(
                "SELECT count(*) FROM softprobe.metric_series",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let samples_n: i64 = conn
            .query_row(
                "SELECT count(*) FROM softprobe.metric_samples",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let postings_n: i64 = conn
            .query_row(
                "SELECT count(*) FROM softprobe.metric_postings",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(series_n, 1);
        assert_eq!(samples_n, 1);
        assert!(postings_n >= 1);
    }

    /// T-C2 / AC-C2: wide ingest → metric_series count = N (test-scale).
    /// Documented N=500 (labels on series, not exploded onto samples).
    #[test]
    fn wide_ingest_series_count_equals_n() {
        const N: i64 = 500; // test-scale cardinality for AC-C2
        let temp = TempDir::new().expect("temp");
        let (conn, catalog, _) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let metrics: Vec<Metric> = (0..N)
            .map(|i| gauge("wide_metric", &format!("pod-{i}"), ts, i as f64))
            .collect();
        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("wide ingest");

        let today = ts.date_naive();
        let series_n: i64 = conn
            .query_row(
                "SELECT count(*) FROM softprobe.metric_series WHERE record_date = ?",
                [today.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(series_n, N, "AC-C2: expected {N} series rows for today");

        let samples_n: i64 = conn
            .query_row(
                "SELECT count(*) FROM softprobe.metric_samples",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(samples_n, N);

        // Labels must live on series (VARIANT), not on samples columns.
        let sample_variant = count_variant_columns(&conn, &catalog, "metric_samples").unwrap();
        assert_eq!(sample_variant, 0);
        let series_labels: i64 = conn
            .query_row(
                "SELECT count(*) FROM softprobe.metric_series WHERE labels IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(series_labels, N);
    }

    /// T-H1 ingest half / AC-H1 (ingest): hist rows only in metric_hist_samples.
    #[test]
    fn histogram_lands_only_in_hist_samples() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog, _) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[hist("layout_latency", ts)],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("hist ingest");

        let hist_n: i64 = conn
            .query_row(
                "SELECT count(*) FROM softprobe.metric_hist_samples h \
                 JOIN softprobe.metric_series s \
                   ON h.series_id = s.series_id AND h.record_date = s.record_date \
                 WHERE s.metric_name = 'layout_latency'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hist_n, 1);

        let sample_n: i64 = conn
            .query_row(
                "SELECT count(*) FROM softprobe.metric_samples sm \
                 JOIN softprobe.metric_series s \
                   ON sm.series_id = s.series_id AND sm.record_date = s.record_date \
                 WHERE s.metric_name = 'layout_latency'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            sample_n, 0,
            "AC-H1 ingest: hist metric must not land in metric_samples"
        );

        // EVAL_END-shaped timestamp (harness F-hist): old record_date must still land.
        let eval_end = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[hist("layout_latency_eval", eval_end)],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("hist ingest eval_end");
        let hist_old: i64 = conn
            .query_row(
                "SELECT count(*) FROM softprobe.metric_hist_samples h \
                 JOIN softprobe.metric_series s \
                   ON h.series_id = s.series_id AND h.record_date = s.record_date \
                 WHERE s.metric_name = 'layout_latency_eval'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hist_old, 1, "AC-H1: EVAL_END-day hist must persist");
    }

    /// T-S1 / AC-S1: no VARIANT on samples + skinny/fat byte ratio < 0.20.
    #[test]
    fn skinny_samples_smaller_than_fat_and_no_variant() {
        const N: usize = 600; // scaled fixture; ratio must be measured, not hardcoded
        let temp = TempDir::new().expect("temp");
        let (conn, catalog, data) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let metrics: Vec<Metric> = (0..N)
            .map(|i| {
                // Fat path stores two VARIANT maps + description/unit/name per row —
                // keep labels wide enough that fat rows stay expensive vs skinny samples.
                let mut m = gauge("s1_metric", &format!("inst-{i}"), ts, i as f64);
                for k in 0..16 {
                    m.attributes.insert(
                        format!("label_{k}"),
                        format!("value-{i}-{k}-{}", "pad".repeat(8)),
                    );
                }
                m.resource_attributes
                    .insert("service.version".into(), format!("v1.2.3-build-{i}"));
                m.description = format!(
                    "description for series {i} with padding text {}",
                    "x".repeat(64)
                );
                m
            })
            .collect();

        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("skinny ingest");
        write_legacy_fat_metrics_throwaway(&conn, &catalog, &metrics).expect("fat throwaway");

        let variant_n = count_variant_columns(&conn, &catalog, "metric_samples").unwrap();
        assert_eq!(variant_n, 0, "AC-S1: metric_samples must have no VARIANT");

        let skinny_bytes = sum_parquet_bytes_for_table(&data, "metric_samples");
        let fat_bytes = sum_parquet_bytes_for_table(&data, "metrics");

        assert!(
            skinny_bytes > 0 && fat_bytes > 0,
            "AC-S1: need real parquet sizes skinny={skinny_bytes} fat={fat_bytes} under {}",
            data.display()
        );
        let ratio = skinny_bytes as f64 / fat_bytes as f64;
        assert!(
            ratio < 0.20,
            "AC-S1: skinny/fat ratio {ratio:.4} (skinny={skinny_bytes}, fat={fat_bytes}) must be < 0.20"
        );
    }

    /// T-M1 / AC-M1: maintenance list is exactly the metrics family.
    #[test]
    fn maintenance_tables_include_metric_family() {
        assert_eq!(
            MAINTENANCE_METRICS_FAMILY_TABLES,
            &[
                "metric_samples",
                "metric_postings",
                "metric_series",
                "metric_hist_samples",
                "metric_samples_5m",
                "metric_samples_1h",
                "metric_collapse_job_1h",
            ]
        );
        // Compaction source of truth must match this constant (wired in executor).
        assert_eq!(
            crate::compaction::executor::maintenance_metrics_family_tables(),
            MAINTENANCE_METRICS_FAMILY_TABLES
        );
    }

    /// T-D4 / AC-D4: layout JOIN exposes the same gauge facts as public union_metrics shape.
    #[test]
    fn union_metrics_layout_join_returns_gauge_facts() {
        use crate::storage::schema::union_metrics_from_layout_sql;

        let temp = TempDir::new().expect("temp");
        let (conn, catalog, _) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 30, 0).unwrap();
        let mut m = gauge("app.gauge", "web-1", ts, 42.5);
        m.attributes
            .insert("sp.session.id".into(), "sess-d4".into());
        m.resource_attributes
            .insert("service.name".into(), "checkout".into());
        write_metrics_layout_txn(&conn, &catalog, &[m], DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("ingest");

        let view_sql = union_metrics_from_layout_sql(&catalog);
        let row: (String, f64, String) = conn
            .query_row(
                &format!(
                    "SELECT metric_name, value, \
                     CAST(attributes['sp.session.id'] AS VARCHAR) \
                     FROM ({view_sql}) AS um \
                     WHERE metric_name = 'app.gauge'"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .expect("AC-D4 read via layout join");
        assert_eq!(row.0, "app.gauge");
        assert_eq!(row.1, 42.5);
        assert_eq!(row.2, "sess-d4");

        // committed_metrics alias uses the same JOIN body.
        let n: i64 = conn
            .query_row(
                &format!("SELECT count(*) FROM ({view_sql}) AS cq WHERE value = 42.5"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn layout_ingest_preserves_nan_gauge_values() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog, _) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[gauge("stale", "j1", ts, f64::NAN)],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("nan ingest");
        let is_nan: bool = conn
            .query_row(
                "SELECT isnan(value) FROM softprobe.metric_samples LIMIT 1",
                [],
                |r| r.get(0),
            )
            .expect("read nan");
        assert!(is_nan, "NO_RECORDED_VALUE / NaN must round-trip in metric_samples");
    }

    #[test]
    fn series_id_is_stable_for_same_labels() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "m".into());
        labels.insert("job".into(), "api".into());
        let a = series_id_hash("m", &labels);
        let b = series_id_hash("m", &labels);
        assert_eq!(a, b);
        labels.insert("instance".into(), "x".into());
        assert_ne!(a, series_id_hash("m", &labels));
    }

    #[test]
    fn ensure_core_still_applies_partition_sort() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog, _) = attach_ducklake(&temp);
        ensure_metrics_layout_core_tables(&conn, &catalog).unwrap();
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[gauge(
                "x",
                "i",
                Utc.with_ymd_and_hms(2026, 8, 15, 1, 0, 0).unwrap(),
                1.0,
            )],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .unwrap();
        // Re-ensure must stay idempotent with data present.
        ensure_metrics_layout_core_tables(&conn, &catalog).unwrap();
    }
}
