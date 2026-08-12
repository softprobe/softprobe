//! DuckLake round-trip for classic histogram / summary fidelity columns (Phase 0).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use chrono::Utc;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, summary_data_point::ValueAtQuantile, Histogram, HistogramDataPoint, Metric,
    ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::models::{Metric as MetricRow, SummaryQuantile};
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant;

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

async fn build_tenant_router(config: Config) -> Router {
    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_local_sqlite_tenant))
}

fn histogram_and_summary_otlp() -> ExportMetricsServiceRequest {
    let hist = Metric {
        name: "http.server.duration".into(),
        description: "latency".into(),
        unit: "ms".into(),
        data: Some(Data::Histogram(Histogram {
            data_points: vec![HistogramDataPoint {
                attributes: vec![KeyValue {
                    key: "http.route".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("/api/orders".into())),
                    }),
                }],
                start_time_unix_nano: 0,
                time_unix_nano: 1_640_995_200_000_000_000,
                count: 10,
                sum: Some(100.0),
                bucket_counts: vec![2, 5, 3],
                explicit_bounds: vec![10.0, 50.0],
                exemplars: vec![],
                flags: 0,
                min: Some(1.0),
                max: Some(80.0),
            }],
            aggregation_temporality: 2,
        })),
        metadata: vec![],
    };
    let summary = Metric {
        name: "rpc.latency".into(),
        description: "".into(),
        unit: "ms".into(),
        data: Some(Data::Summary(Summary {
            data_points: vec![SummaryDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: 1_640_995_200_000_000_000,
                count: 100,
                sum: 500.0,
                quantile_values: vec![
                    ValueAtQuantile {
                        quantile: 0.5,
                        value: 4.0,
                    },
                    ValueAtQuantile {
                        quantile: 0.99,
                        value: 20.0,
                    },
                ],
                flags: 0,
            }],
        })),
        metadata: vec![],
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("checkout".into())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![hist, summary],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn http_otlp_histogram_ingest_then_sql_and_compat_stub() {
    // End-to-end for Phase 0: HTTP OTLP → DuckLake fidelity columns → compat stub 501.
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let router = build_tenant_router(config).await;

    let body = histogram_and_summary_otlp().encode_to_vec();
    let ingest = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/metrics")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ingest.status(), StatusCode::OK, "OTLP metrics ingest");
    let ingest_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(ingest.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(ingest_json["success"], true);
    assert_eq!(ingest_json["ingested_count"], 2);

    let conn = attach(&metadata_path, &data_path);
    let (count, sum, buckets): (Option<i64>, Option<f64>, Option<String>) = conn
        .query_row(
            "SELECT count, sum, CAST(bucket_counts AS VARCHAR) \
             FROM softprobe.metrics WHERE metric_name = 'http.server.duration'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("SQL read histogram after HTTP ingest");
    assert_eq!(count, Some(10));
    assert_eq!(sum, Some(100.0));
    assert!(
        buckets.as_deref().unwrap_or("").contains('2'),
        "bucket_counts={buckets:?}"
    );

    let (qcount, quantiles): (Option<i64>, Option<String>) = conn
        .query_row(
            "SELECT count, CAST(quantiles AS VARCHAR) \
             FROM softprobe.metrics WHERE metric_name = 'rpc.latency'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("SQL read summary");
    assert_eq!(qcount, Some(100));
    assert!(quantiles.as_deref().unwrap_or("").contains("0.99"));

    let stub = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/query")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stub.status(), StatusCode::NOT_IMPLEMENTED);
    let stub_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(stub.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(stub_json["error"]["code"], "unsupported_feature");
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

    let histogram = MetricRow {
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

    let summary = MetricRow {
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
    use async_trait::async_trait;
    use softprobe_runtime::ingest_engine::IngestPipeline;

    use crate::util::metrics_fidelity_contract::{
        contract_legacy_metrics_table_widens_on_gauge_ingest, MetricsFidelityBackend,
    };

    struct SqliteBackend {
        _temp: TempDir,
        pipeline: IngestPipeline,
        metadata_path: String,
        data_path: String,
    }

    #[async_trait]
    impl MetricsFidelityBackend for SqliteBackend {
        fn attach(&self) -> duckdb::Connection {
            attach(&self.metadata_path, &self.data_path)
        }

        async fn write_metric_batches(
            &self,
            batches: Vec<Vec<softprobe_runtime::models::Metric>>,
        ) -> anyhow::Result<()> {
            self.pipeline.write_metric_batches(batches).await
        }

        fn create_legacy_metrics_table(&self) {
            let conn = self.attach();
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
    }

    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let backend = SqliteBackend {
        _temp: temp,
        pipeline,
        metadata_path,
        data_path,
    };
    contract_legacy_metrics_table_widens_on_gauge_ingest(&backend).await;
}
