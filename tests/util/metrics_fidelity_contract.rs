//! Shared metrics layout ingest contract (SQLite + PostgreSQL adapters).
//!
//! After §11.3, gauges land in `metric_samples` (joined via `metric_series`).
//! Legacy fat `metrics` widen-on-write is no longer the ingest path.

use async_trait::async_trait;
use chrono::Utc;
use softprobe_runtime::models::Metric;

#[async_trait]
pub trait MetricsFidelityBackend: Send + Sync {
    /// Fully qualified layout catalog prefix (`softprobe` or `softprobe.<schema>`).
    fn catalog_prefix(&self) -> String {
        "softprobe".to_string()
    }

    fn attach(&self) -> duckdb::Connection;
    async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> anyhow::Result<()>;
}

/// AC-D1-ish smoke: gauge ingest creates layout rows readable by SQL.
pub async fn contract_gauge_ingest_lands_in_metric_samples(backend: &impl MetricsFidelityBackend) {
    let gauge = Metric {
        metric_name: "cpu.usage".into(),
        description: "".into(),
        unit: "%".into(),
        metric_type: "gauge".into(),
        timestamp: Utc::now(),
        value: 55.0,
        ..Default::default()
    };
    backend
        .write_metric_batches(vec![vec![gauge]])
        .await
        .expect("gauge layout ingest must succeed");

    let prefix = backend.catalog_prefix();
    let conn = backend.attach();
    let value: f64 = conn
        .query_row(
            &format!(
                "SELECT sm.value \
                 FROM {prefix}.metric_samples sm \
                 JOIN {prefix}.metric_series s \
                   ON sm.series_id = s.series_id AND sm.record_date = s.record_date \
                 WHERE s.metric_name = 'cpu.usage'"
            ),
            [],
            |row| row.get(0),
        )
        .expect("read gauge from metric_samples");
    assert_eq!(value, 55.0);

    for table in [
        "metric_series",
        "metric_postings",
        "metric_samples",
        "metric_hist_samples",
    ] {
        let n: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM duckdb_tables() \
                     WHERE schema_name LIKE '%' AND table_name = '{table}'"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        // Prefer ducklake metadata when available; fall back to SELECT count.
        let _ = n;
        let exists: i64 = conn
            .query_row(
                &format!("SELECT count(*) FROM {prefix}.{table}"),
                [],
                |row| row.get(0),
            )
            .unwrap_or(-1);
        assert!(
            exists >= 0,
            "expected layout table {prefix}.{table} to be queryable"
        );
    }
}

/// AC-D4: public union_metrics / committed_metrics shape (layout JOIN) returns gauge facts.
pub async fn contract_union_metrics_reads_layout_gauges(backend: &impl MetricsFidelityBackend) {
    use softprobe_runtime::storage::schema::union_metrics_from_layout_sql;

    let gauge = Metric {
        metric_name: "sql.bridge.gauge".into(),
        description: "d4".into(),
        unit: "1".into(),
        metric_type: "gauge".into(),
        timestamp: Utc::now(),
        value: 7.0,
        ..Default::default()
    };
    backend
        .write_metric_batches(vec![vec![gauge]])
        .await
        .expect("AC-D4 gauge ingest");

    let prefix = backend.catalog_prefix();
    let view = union_metrics_from_layout_sql(&prefix);
    let conn = backend.attach();
    let (name, value): (String, f64) = conn
        .query_row(
            &format!(
                "SELECT metric_name, value FROM ({view}) AS union_metrics \
                 WHERE metric_name = 'sql.bridge.gauge'"
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("AC-D4 union_metrics layout join");
    assert_eq!(name, "sql.bridge.gauge");
    assert_eq!(value, 7.0);

    let n: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM ({view}) AS committed_metrics \
                 WHERE metric_name = 'sql.bridge.gauge' AND value = 7.0"
            ),
            [],
            |row| row.get(0),
        )
        .expect("AC-D4 committed_metrics layout join");
    assert_eq!(n, 1);
}

/// Backward-compatible name used by existing test modules.
pub async fn contract_legacy_metrics_table_widens_on_gauge_ingest(
    backend: &impl MetricsFidelityBackend,
) {
    contract_gauge_ingest_lands_in_metric_samples(backend).await;
    contract_union_metrics_reads_layout_gauges(backend).await;
}
