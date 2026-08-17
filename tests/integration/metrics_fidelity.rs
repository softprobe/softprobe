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
async fn http_otlp_histogram_ingest_then_sql_and_prom_query() {
    // End-to-end: HTTP OTLP → DuckLake fidelity columns → Prometheus query success.
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
            "SELECT h.count, h.sum, CAST(h.bucket_counts AS VARCHAR) \
             FROM softprobe.metric_hist_samples h \
             JOIN softprobe.metric_series s \
               ON h.series_id = s.series_id AND h.record_date = s.record_date \
             WHERE s.metric_name = 'http.server.duration'",
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

    let (qcount, qsum): (Option<i64>, Option<f64>) = conn
        .query_row(
            "SELECT h.count, h.sum \
             FROM softprobe.metric_hist_samples h \
             JOIN softprobe.metric_series s \
               ON h.series_id = s.series_id AND h.record_date = s.record_date \
             WHERE s.metric_name = 'rpc.latency'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("SQL read summary count/sum");
    assert_eq!(qcount, Some(100));
    assert_eq!(qsum, Some(500.0));
    // Quantile expansion stays out of scope for metric_hist_samples (§6.4).

        // AC-H1 Prom path: classic `_count` / `_bucket` query_range over hist table.
    // Fixture timestamp is 2022-01-01T00:00:00Z (1_640_995_200s).
    // Short (30m) and mid (3h) windows must both return series — mid used to divert
    // onto empty metric_samples_1h (AC-H3 regression).
    let windows = [
        ("1640994300", "1640996100"), // ±30m around sample
        ("1640985300", "1640996100"), // ~3h before → sample end
    ];
    for (start, end) in windows {
        for query in [
            "http_server_duration_count",
            "http_server_duration_bucket",
            "rpc_latency_count",
        ] {
            let uri = format!(
                "/api/v1/query_range?query={query}&start={start}&end={end}&step=15"
            );
            let resp = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(&uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "AC-H1/H3 HTTP {query} window={start}..{end} status"
            );
            let body: serde_json::Value = serde_json::from_slice(
                &axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(
                body["status"], "success",
                "AC-H1/H3 {query} window={start}..{end} body={body}"
            );
            assert_eq!(body["data"]["resultType"], "matrix");
            let result = body["data"]["result"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            assert!(
                !result.is_empty(),
                "AC-H1/H3 {query} window={start}..{end}: expected non-empty Prom series from metric_hist_samples, got {body}"
            );
            let values = result[0]["values"].as_array().cloned().unwrap_or_default();
            assert!(
                !values.is_empty(),
                "AC-H1/H3 {query} window={start}..{end}: expected sample points, got {body}"
            );
        }
    }
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
    let (count, sum, buckets, bounds): (
        Option<i64>,
        Option<f64>,
        Option<String>,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT h.count, h.sum, CAST(h.bucket_counts AS VARCHAR), CAST(h.explicit_bounds AS VARCHAR) \
             FROM softprobe.metric_hist_samples h \
             JOIN softprobe.metric_series s \
               ON h.series_id = s.series_id AND h.record_date = s.record_date \
             WHERE s.metric_name = 'http.server.duration'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
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

    let (qcount, qsum): (Option<i64>, Option<f64>) = conn
        .query_row(
            "SELECT h.count, h.sum \
             FROM softprobe.metric_hist_samples h \
             JOIN softprobe.metric_series s \
               ON h.series_id = s.series_id AND h.record_date = s.record_date \
             WHERE s.metric_name = 'rpc.latency'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("query summary");
    assert_eq!(qcount, Some(100));
    assert_eq!(qsum, Some(500.0));
}

#[tokio::test]
async fn classic_histogram_absent_sum_persists_null_in_ducklake() {
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");

    let histogram = MetricRow {
        metric_name: "http.server.duration".into(),
        description: "latency".into(),
        unit: "ms".into(),
        metric_type: "histogram".into(),
        timestamp: Utc::now(),
        // Scalar `value` stays 0.0 when OTLP sum is absent; fidelity `sum` must be NULL.
        value: 0.0,
        count: Some(3),
        sum: None,
        bucket_counts: Some(vec![1, 2]),
        explicit_bounds: Some(vec![10.0]),
        aggregation_temporality: Some("CUMULATIVE".into()),
        ..Default::default()
    };

    pipeline
        .write_metric_batches(vec![vec![histogram]])
        .await
        .expect("write histogram without sum");

    let conn = attach(&metadata_path, &data_path);
    let sum: Option<f64> = conn
        .query_row(
            "SELECT h.sum FROM softprobe.metric_hist_samples h \
             JOIN softprobe.metric_series s \
               ON h.series_id = s.series_id AND h.record_date = s.record_date \
             WHERE s.metric_name = 'http.server.duration'",
            [],
            |row| row.get(0),
        )
        .expect("query absent-sum histogram");
    assert_eq!(
        sum, None,
        "absent OTLP histogram sum must persist as SQL NULL"
    );
}

#[tokio::test]
async fn nested_otlp_attributes_round_trip_ducklake() {
    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");

    let mut attrs = HashMap::new();
    attrs.insert("tags".into(), r#"sp.json:["a",1]"#.into());
    attrs.insert("meta".into(), r#"sp.json:{"region":"us","ok":true}"#.into());
    attrs.insert("http.route".into(), "/api".into());
    let mut resource = HashMap::new();
    resource.insert("service.name".into(), "checkout".into());

    let gauge = MetricRow {
        metric_name: "app.gauge".into(),
        description: "".into(),
        unit: "1".into(),
        metric_type: "gauge".into(),
        timestamp: Utc::now(),
        value: 1.0,
        attributes: attrs,
        resource_attributes: resource,
        ..Default::default()
    };

    pipeline
        .write_metric_batches(vec![vec![gauge]])
        .await
        .expect("write gauge with nested attrs");

    let conn = attach(&metadata_path, &data_path);
    let attrs_json: String = conn
        .query_row(
            "SELECT CAST(labels AS JSON) FROM softprobe.metric_series WHERE metric_name = 'app.gauge'",
            [],
            |row| row.get(0),
        )
        .expect("read labels JSON");
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).expect("parse labels");
    // Nested sp.json: values rehydrate; OTel keys are Prom-sanitized on the series labels map.
    assert_eq!(parsed["tags"], serde_json::json!(["a", 1]));
    assert_eq!(parsed["meta"]["region"], "us");
    assert_eq!(parsed["meta"]["ok"], true);
    assert_eq!(parsed["http_route"], "/api");
}

#[tokio::test]
async fn http_otlp_nested_attributes_round_trip_ducklake() {
    use opentelemetry_proto::tonic::common::v1::{
        any_value, AnyValue, ArrayValue, KeyValue, KeyValueList,
    };
    use opentelemetry_proto::tonic::metrics::v1::{
        metric::Data, Gauge, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use prost::Message;

    let temp = TempDir::new().expect("temp");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let router = build_tenant_router(config).await;

    let gauge = opentelemetry_proto::tonic::metrics::v1::Metric {
        name: "app.nested".into(),
        description: "".into(),
        unit: "1".into(),
        data: Some(Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![
                    KeyValue {
                        key: "tags".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::ArrayValue(ArrayValue {
                                values: vec![
                                    AnyValue {
                                        value: Some(any_value::Value::StringValue("a".into())),
                                    },
                                    AnyValue {
                                        value: Some(any_value::Value::IntValue(1)),
                                    },
                                ],
                            })),
                        }),
                    },
                    KeyValue {
                        key: "meta".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::KvlistValue(KeyValueList {
                                values: vec![KeyValue {
                                    key: "region".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue("us".into())),
                                    }),
                                }],
                            })),
                        }),
                    },
                ],
                start_time_unix_nano: 0,
                time_unix_nano: 1_640_995_200_000_000_000,
                value: Some(
                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                        3.0,
                    ),
                ),
                exemplars: vec![],
                flags: 0,
            }],
        })),
        metadata: vec![],
    };
    let req = ExportMetricsServiceRequest {
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
                metrics: vec![gauge],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let ingest = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/metrics")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(req.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ingest.status(), StatusCode::OK);
    let ingest_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(ingest.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(ingest_json["ingested_count"], 1);

    let conn = attach(&metadata_path, &data_path);
    let attrs_json: String = conn
        .query_row(
            "SELECT CAST(labels AS JSON) FROM softprobe.metric_series WHERE metric_name = 'app.nested'",
            [],
            |row| row.get(0),
        )
        .expect("read nested labels");
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();
    assert_eq!(parsed["tags"], serde_json::json!(["a", 1]));
    assert_eq!(parsed["meta"]["region"], "us");
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
