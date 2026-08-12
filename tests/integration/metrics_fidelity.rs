//! DuckLake round-trip for classic histogram / summary fidelity columns (Phase 0).

use chrono::Utc;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::models::{Metric, SummaryQuantile};
use std::collections::HashMap;
use tempfile::TempDir;

use crate::util::config::file_backed_test_config;

fn attach(metadata_path: &str, data_path: &str) -> duckdb::Connection {
    let connection = duckdb::Connection::open_in_memory().expect("duckdb");
    connection
        .execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
        .expect("extensions");
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS softprobe \
             (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, \
              DATA_INLINING_ROW_LIMIT 0);",
            metadata_path.replace('\'', "''"),
            data_path.replace('\'', "''"),
        ))
        .expect("attach");
    connection
}

#[tokio::test]
async fn classic_histogram_and_summary_round_trip_ducklake() {
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");

    let now = Utc::now();
    let mut attrs = HashMap::new();
    attrs.insert("http.route".into(), "/api/orders".into());
    let mut resource = HashMap::new();
    resource.insert("service.name".into(), "checkout".into());

    let histogram = Metric {
        metric_name: "http.server.duration".into(),
        description: "latency".into(),
        unit: "ms".into(),
        metric_type: "histogram".into(),
        timestamp: now,
        value: 100.0,
        attributes: attrs.clone(),
        resource_attributes: resource.clone(),
        count: Some(10),
        sum: Some(100.0),
        bucket_counts: Some(vec![2, 5, 3]),
        explicit_bounds: Some(vec![10.0, 50.0]),
        quantiles: None,
        aggregation_temporality: Some("CUMULATIVE".into()),
        exemplars_json: Some(r#"[{"value":1.5}]"#.into()),
    };

    let summary = Metric {
        metric_name: "rpc.latency".into(),
        description: "".into(),
        unit: "ms".into(),
        metric_type: "summary".into(),
        timestamp: now,
        value: 500.0,
        attributes: attrs,
        resource_attributes: resource,
        count: Some(100),
        sum: Some(500.0),
        bucket_counts: None,
        explicit_bounds: None,
        quantiles: Some(vec![
            SummaryQuantile {
                quantile: 0.5,
                value: 4.0,
            },
            SummaryQuantile {
                quantile: 0.99,
                value: 20.0,
            },
        ]),
        aggregation_temporality: None,
        exemplars_json: None,
    };

    pipeline
        .write_metric_batches(vec![vec![histogram, summary]])
        .await
        .expect("write metrics");

    let conn = attach(&metadata_path, &data_path);
    let (count, sum, buckets, bounds, temporality): (
        Option<i64>,
        Option<f64>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT count, sum, CAST(bucket_counts AS VARCHAR), CAST(explicit_bounds AS VARCHAR), \
             aggregation_temporality \
             FROM softprobe.metrics WHERE metric_name = 'http.server.duration'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("query histogram");
    assert_eq!(count, Some(10));
    assert_eq!(sum, Some(100.0));
    assert!(
        buckets.as_deref().unwrap_or("").contains('2'),
        "bucket_counts={buckets:?}"
    );
    assert!(
        bounds.as_deref().unwrap_or("").contains("10"),
        "explicit_bounds={bounds:?}"
    );
    assert_eq!(temporality.as_deref(), Some("CUMULATIVE"));

    let (qcount, qsum, quantiles): (Option<i64>, Option<f64>, Option<String>) = conn
        .query_row(
            "SELECT count, sum, CAST(quantiles AS VARCHAR) \
             FROM softprobe.metrics WHERE metric_name = 'rpc.latency'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("query summary");
    assert_eq!(qcount, Some(100));
    assert_eq!(qsum, Some(500.0));
    assert!(
        quantiles.as_deref().unwrap_or("").contains("0.5"),
        "quantiles={quantiles:?}"
    );
}

#[tokio::test]
async fn legacy_metrics_table_widens_on_gauge_ingest() {
    // Simulate a pre-Phase-0 metrics table (no fidelity columns), then ingest a gauge.
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();

    // Create legacy-shaped metrics table via DuckLake attach.
    {
        let conn = attach(&metadata_path, &data_path);
        conn.execute_batch(
            "CREATE TABLE softprobe.metrics (
                metric_name VARCHAR,
                description VARCHAR,
                unit VARCHAR,
                metric_type VARCHAR,
                timestamp TIMESTAMPTZ,
                value DOUBLE,
                attributes VARIANT,
                resource_attributes VARIANT,
                record_date DATE
            );",
        )
        .expect("legacy create");
    }

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let gauge = Metric {
        metric_name: "cpu.usage".into(),
        description: "".into(),
        unit: "%".into(),
        metric_type: "gauge".into(),
        timestamp: Utc::now(),
        value: 55.0,
        ..Default::default()
    };
    pipeline
        .write_metric_batches(vec![vec![gauge]])
        .await
        .expect("gauge ingest after widen must succeed");

    let conn = attach(&metadata_path, &data_path);
    let value: f64 = conn
        .query_row(
            "SELECT value FROM softprobe.metrics WHERE metric_name = 'cpu.usage'",
            [],
            |row| row.get(0),
        )
        .expect("read gauge");
    assert_eq!(value, 55.0);

    // Fidelity columns exist (nullable) after ensure_metrics_fidelity_columns.
    let has_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.columns \
             WHERE table_name = 'metrics' AND column_name = 'count'",
            [],
            |row| row.get(0),
        )
        .unwrap_or_else(|_| {
            // DuckLake may not expose information_schema the same way; fall back to DESCRIBE.
            let mut stmt = conn.prepare("DESCRIBE softprobe.metrics").unwrap();
            let names: Vec<String> = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            assert!(
                names.iter().any(|n| n == "count"),
                "expected count column after widen, got {names:?}"
            );
            1
        });
    assert!(has_count >= 1);
}
