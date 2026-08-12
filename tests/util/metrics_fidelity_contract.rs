//! Shared metrics fidelity migration contract (SQLite + PostgreSQL adapters).
//!
//! Verifies Phase 0 `ensure_metrics_fidelity_columns` widen-on-write works on both
//! DuckLake metadata backends used in production (Postgres) and local (SQLite).

use async_trait::async_trait;
use chrono::Utc;
use softprobe_runtime::models::Metric;

#[async_trait]
pub trait MetricsFidelityBackend: Send + Sync {
    /// Fully qualified metrics table the writer will hit (`softprobe.metrics` or
    /// `softprobe.<metadata_schema>.metrics`).
    fn metrics_table(&self) -> String {
        "softprobe.metrics".to_string()
    }

    fn attach(&self) -> duckdb::Connection;
    async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> anyhow::Result<()>;
    fn create_legacy_metrics_table(&self);
}

pub async fn contract_legacy_metrics_table_widens_on_gauge_ingest(
    backend: &impl MetricsFidelityBackend,
) {
    backend.create_legacy_metrics_table();

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
        .expect("gauge ingest after widen must succeed");

    let table = backend.metrics_table();
    let conn = backend.attach();
    let value: f64 = conn
        .query_row(
            &format!("SELECT value FROM {table} WHERE metric_name = 'cpu.usage'"),
            [],
            |row| row.get(0),
        )
        .expect("read gauge");
    assert_eq!(value, 55.0);

    let mut stmt = conn.prepare(&format!("DESCRIBE {table}")).unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    for required in [
        "count",
        "sum",
        "bucket_counts",
        "explicit_bounds",
        "quantiles",
        "aggregation_temporality",
        "exemplars_json",
    ] {
        assert!(
            names.iter().any(|n| n == required),
            "expected fidelity column '{required}' after widen, got {names:?}"
        );
    }
}
