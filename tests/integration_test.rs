use softprobe_otlp_backend::api;
use softprobe_otlp_backend::config::Config;
use softprobe_otlp_backend::storage;
use softprobe_otlp_backend::query;
use softprobe_otlp_backend::models::Span as ModelSpan;
use reqwest::StatusCode;
use std::time::Duration;
use tokio::net::TcpListener;
use reqwest::Client;
use serde_json;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::common::v1::{KeyValue, AnyValue, any_value};
use prost::Message;
use std::path::{PathBuf};
use std::fs;
use uuid::Uuid;
use tempfile::{tempdir, TempDir};

async fn start_test_server() -> (String, TempDir) {
    let mut config = Config::default();
    // Use REST catalog for integration tests
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181/catalog".to_string();
    config.iceberg.warehouse = "default".to_string();
    config.iceberg.force_close_after_append = true; // Force immediate commits for tests
    config.ingest_engine.optimizer_interval_seconds = 3600;
    config.s3.endpoint = Some("http://localhost:9002".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();
    
    // Ensure MinIO/S3 env for REST catalog (dev-compose defaults)
    std::env::set_var("S3_ENDPOINT", "http://localhost:9002");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    let cache_dir = tempdir().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());

    let pipeline = storage::IngestPipeline::new(&config).await.unwrap();
    let storage::IngestPipeline { storage, span_buffer, .. } = pipeline;
    let query_engine = query::create_query_engine(&config).await.unwrap();
    
    // Create router
    let app = api::create_router(storage, query_engine, Some(span_buffer), None, None).await.unwrap();
    
    // Bind to a random available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);
    
    // Start the server in a background task using axum 0.8
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });
    
    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    (base_url, cache_dir)
}

fn create_test_otlp_request() -> ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::trace::v1::span::Event;

    let trace_id_bytes = vec![0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    let span_id_bytes = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];

    let span = Span {
        trace_id: trace_id_bytes,
        span_id: span_id_bytes,
        parent_span_id: vec![],
        name: "test_operation".to_string(),
        kind: 2, // SPAN_KIND_SERVER
        start_time_unix_nano: 1640995200000000000, // 2022-01-01 00:00:00 UTC
        end_time_unix_nano: 1640995260000000000,   // 2022-01-01 00:01:00 UTC
        attributes: vec![
            KeyValue {
                key: "sp.session.id".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("test_session_123".to_string())),
                }),
            },
            KeyValue {
                key: "http.method".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("POST".to_string())),
                }),
            },
            KeyValue {
                key: "http.url".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("https://api.example.com/users".to_string())),
                }),
            },
        ],
        events: vec![
            Event {
                time_unix_nano: 1640995210000000000, // 10 seconds after start
                name: "http.request.started".to_string(),
                attributes: vec![
                    KeyValue {
                        key: "request.body_size".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::IntValue(1024)),
                        }),
                    },
                    KeyValue {
                        key: "request.path".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("/api/users".to_string())),
                        }),
                    },
                ],
                ..Default::default()
            },
            Event {
                time_unix_nano: 1640995250000000000, // 50 seconds after start
                name: "http.request.completed".to_string(),
                attributes: vec![
                    KeyValue {
                        key: "response.status_code".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::IntValue(200)),
                        }),
                    },
                    KeyValue {
                        key: "response.body_size".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::IntValue(2048)),
                        }),
                    },
                ],
                ..Default::default()
            },
        ],
        status: Some(Status {
            code: 1, // STATUS_CODE_OK
            message: "".to_string(),
        }),
        ..Default::default()
    };

    let resource_spans = ResourceSpans {
        resource: Some(Resource {
            attributes: vec![
                KeyValue {
                    key: "sp.app.id".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test_application".to_string())),
                    }),
                },
                KeyValue {
                    key: "sp.organization.id".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("org_test_123".to_string())),
                    }),
                },
                KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("user_service".to_string())),
                    }),
                },
                KeyValue {
                    key: "service.version".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("1.0.0".to_string())),
                    }),
                },
            ],
            ..Default::default()
        }),
        scope_spans: vec![ScopeSpans {
            scope: None,
            spans: vec![span],
            schema_url: "".to_string(),
        }],
        schema_url: "".to_string(),
    };

    ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    }
}

#[tokio::test]
async fn test_health_endpoint() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let response = client
        .get(&format!("{}/health", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_ready_endpoint() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let response = client
        .get(&format!("{}/ready", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_traces_json_endpoint_success() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();
    let sid = format!("it-{}", Uuid::new_v4());
    let mut otlp_request = create_test_otlp_request();
    {
        let span = &mut otlp_request.resource_spans[0].scope_spans[0].spans[0];
        if let Some(kv) = span.attributes.iter_mut().find(|kv| kv.key == "sp.session.id") {
            kv.value = Some(AnyValue { value: Some(any_value::Value::StringValue(sid.clone())) });
        }
    }
    
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Parse response
    let response_json: serde_json::Value = response.json().await.unwrap();
    
    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 1);
    assert!(response_json["message"].as_str().unwrap().contains("Successfully ingested"));
}

#[tokio::test]
async fn test_traces_protobuf_endpoint_success() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();
    let otlp_request = create_test_otlp_request();

    let body = otlp_request.encode_to_vec();

    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Parse response as JSON body
    let response_json: serde_json::Value = response.json().await.unwrap();
    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 1);
}


#[tokio::test]
async fn test_traces_json_endpoint_multiple_spans() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();
    
    // Create request with multiple spans
    let mut otlp_request = create_test_otlp_request();
    
    // Add another span to the same resource
    let span2 = Span {
        trace_id: vec![0x22, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
        span_id: vec![0x21, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
        parent_span_id: vec![],
        name: "another_operation".to_string(),
        kind: 1, // SPAN_KIND_INTERNAL
        start_time_unix_nano: 1640995200000000000,
        end_time_unix_nano: 1640995260000000000,
        attributes: vec![
            KeyValue {
                key: "sp.session.id".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("test_session_123".to_string())),
                }),
            },
        ],
        events: vec![],
        status: Some(Status {
            code: 1,
            message: "".to_string(),
        }),
        ..Default::default()
    };
    
    otlp_request.resource_spans[0].scope_spans[0].spans.push(span2);
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let response_json: serde_json::Value = response.json().await.unwrap();
    
    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 2);
}

#[tokio::test]
async fn test_traces_json_endpoint_empty_request() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();
    
    let empty_request = ExportTraceServiceRequest {
        resource_spans: vec![],
    };
    
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&empty_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let response_json: serde_json::Value = response.json().await.unwrap();
    
    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 0);
}

#[tokio::test]
async fn test_traces_json_endpoint_invalid_json() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .body("invalid json")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_traces_with_missing_app_id() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();
    
    // Create request without sp.app.id
    let mut otlp_request = create_test_otlp_request();
    otlp_request.resource_spans[0].resource = Some(Resource {
        attributes: vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("fallback_service".to_string())),
                }),
            },
        ],
        ..Default::default()
    });
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let response_json: serde_json::Value = response.json().await.unwrap();
    
    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 1);
}

#[tokio::test]
async fn test_traces_session_grouping() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();
    
    // Create multiple spans with the same session ID
    let mut otlp_request = create_test_otlp_request();
    
    // Modify the existing span and add more spans with same session
    for i in 2..=5 {
        let span = Span {
            trace_id: vec![i, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
            span_id: vec![i, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
            parent_span_id: vec![],
            name: format!("operation_{}", i),
            kind: 1,
            start_time_unix_nano: 1640995200000000000 + (i as u64 * 1000000000),
            end_time_unix_nano: 1640995260000000000 + (i as u64 * 1000000000),
            attributes: vec![
                KeyValue {
                    key: "sp.session.id".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test_session_123".to_string())),
                    }),
                },
            ],
            events: vec![],
            status: Some(Status {
                code: 1,
                message: "".to_string(),
            }),
            ..Default::default()
        };
        
        otlp_request.resource_spans[0].scope_spans[0].spans.push(span);
    }
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let response_json: serde_json::Value = response.json().await.unwrap();
    
    assert_eq!(response_json["success"], true);
    assert_eq!(response_json["ingested_count"], 5); // All 5 spans should be processed
}

#[tokio::test] 
async fn test_endpoint_not_found() {
    let (base_url, _cache_dir) = start_test_server().await;
    let client = Client::new();

    let response = client
        .get(&format!("{}/nonexistent", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_parquet_output_written_and_closed() {
    // Arrange: Use REST catalog - files will be written to warehouse/iceberg via catalog
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
    let warehouse_dir = repo_root.join("warehouse/iceberg");
    fs::create_dir_all(&warehouse_dir).unwrap();

    // Set warehouse name for Lakekeeper REST catalog
    std::env::set_var("ICEBERG_WAREHOUSE", "default");
    std::env::set_var("S3_ENDPOINT", "http://localhost:9002");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    // Start server with REST catalog configuration
    let mut config = Config::default();
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181/catalog".to_string();
    // Ensure writer uses the same Lakekeeper warehouse name as scanner
    config.iceberg.warehouse = "default".to_string();
    // Provide S3 settings for client-side file IO to MinIO
    config.s3.endpoint = Some("http://localhost:9002".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();
    config.span_buffering.flush_interval_seconds = 1; // flush after 1s
    config.span_buffering.max_buffer_spans = 1; // flush after a single span
    config.iceberg.force_close_after_append = true; // Force immediate commits
    config.ingest_engine.optimizer_interval_seconds = 3600;
    let cache_dir = tempdir().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());

    let pipeline = storage::IngestPipeline::new(&config).await.unwrap();
    let storage::IngestPipeline { storage, span_buffer, .. } = pipeline;
    let ingest_engine = storage.ingest_engine.clone();
    let query_engine = query::create_query_engine(&config).await.unwrap();
    let query_engine_handle = query_engine.clone();
    let app = api::create_router(storage, query_engine, Some(span_buffer), None, None).await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service()).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Act: send one JSON OTLP request (single session)
    let client = Client::new();
    let sid = format!("it-{}", Uuid::new_v4());
    let mut otlp_request = create_test_otlp_request();
    {
        let span = &mut otlp_request.resource_spans[0].scope_spans[0].spans[0];
        if let Some(kv) = span.attributes.iter_mut().find(|kv| kv.key == "sp.session.id") {
            kv.value = Some(AnyValue { value: Some(any_value::Value::StringValue(sid.clone())) });
        }
    }
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Wait to exceed timeout and trigger a second append to invoke maybe_flush
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let mut second = create_test_otlp_request();
    {
        let span = &mut second.resource_spans[0].scope_spans[0].spans[0];
        if let Some(kv) = span.attributes.iter_mut().find(|kv| kv.key == "sp.session.id") {
            kv.value = Some(AnyValue { value: Some(any_value::Value::StringValue(format!("{}-2", sid))) });
        }
    }
    let response2 = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&second)
        .send()
        .await
        .unwrap();
    assert_eq!(response2.status(), StatusCode::OK);

    // Wait longer for optimizer commits to complete (avoid flakiness)
    tokio::time::sleep(Duration::from_secs(2)).await;
    if let Some(engine) = ingest_engine.as_ref() {
        for _ in 0..5 {
            engine.run_optimizer_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    
    // Query through DuckDB SQL to validate data was committed.
    let escaped_sid = sid.replace('\'', "''");
    let sql = format!(
        "SELECT COUNT(*) AS count FROM union_spans WHERE session_id = '{}'",
        escaped_sid
    );
    let mut found = false;
    let mut total_rows = 0i64;
    for _ in 0..25 {
        let result = query_engine_handle.execute_query(&sql).await.unwrap();
        total_rows = result
            .rows
            .get(0)
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        if total_rows > 0 {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    assert!(
        found,
        "expected session_id '{}' to be present in table. Found {} rows total.",
        sid,
        total_rows
    );
}

#[tokio::test]
async fn test_multi_sessions_are_written_and_queryable() {
    // Arrange: Use REST catalog - files will be written to warehouse/iceberg via catalog
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
    let warehouse_dir = repo_root.join("warehouse/iceberg");
    fs::create_dir_all(&warehouse_dir).unwrap();

    // Set warehouse name for Lakekeeper REST catalog
    std::env::set_var("ICEBERG_WAREHOUSE", "default");
    std::env::set_var("S3_ENDPOINT", "http://localhost:9002");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    // Start server with REST catalog configuration
    let mut config = Config::default();
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181/catalog".to_string();
    // Ensure writer uses same Lakekeeper warehouse name and S3 settings as scanner
    config.iceberg.warehouse = "default".to_string();
    config.s3.endpoint = Some("http://localhost:9002".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();
    let cache_dir = tempdir().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
    config.span_buffering.flush_interval_seconds = 1; // flush after 1s
    config.span_buffering.max_buffer_spans = 1; // flush after a single span
    config.iceberg.force_close_after_append = true; // Force immediate commits
    config.ingest_engine.optimizer_interval_seconds = 3600;

    let pipeline = storage::IngestPipeline::new(&config).await.unwrap();
    let storage::IngestPipeline { storage, span_buffer, .. } = pipeline;
    let ingest_engine = storage.ingest_engine.clone();
    let query_engine = query::create_query_engine(&config).await.unwrap();
    let query_engine_handle = query_engine.clone();
    let app = api::create_router(storage, query_engine, Some(span_buffer), None, None).await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);
    tokio::spawn(async move { axum::serve(listener, app.into_make_service()).await.unwrap(); });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // First session
    let client = Client::new();
    let mut req1 = create_test_otlp_request();
    let s1 = format!("S1-{}", Uuid::new_v4());
    {
        // ensure session id is S1
        let span = &mut req1.resource_spans[0].scope_spans[0].spans[0];
        if let Some(kv) = span.attributes.iter_mut().find(|kv| kv.key == "sp.session.id") {
            kv.value = Some(AnyValue{ value: Some(any_value::Value::StringValue(s1.clone()))});
        }
    }
    let r1 = client.post(&format!("{}/v1/traces", base_url)).header("content-type", "application/json").json(&req1).send().await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);

    // Second session (different id)
    let mut req2 = create_test_otlp_request();
    let s2 = format!("S2-{}", Uuid::new_v4());
    {
        let span = &mut req2.resource_spans[0].scope_spans[0].spans[0];
        if let Some(kv) = span.attributes.iter_mut().find(|kv| kv.key == "sp.session.id") {
            kv.value = Some(AnyValue{ value: Some(any_value::Value::StringValue(s2.clone()))});
        }
    }
    let r2 = client.post(&format!("{}/v1/traces", base_url)).header("content-type", "application/json").json(&req2).send().await.unwrap();
    assert_eq!(r2.status(), StatusCode::OK);

    // Wait near timeout, then trigger another append to invoke maybe_flush
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let mut req3 = create_test_otlp_request();
    let s3 = format!("S3-{}", Uuid::new_v4());
    {
        let span = &mut req3.resource_spans[0].scope_spans[0].spans[0];
        if let Some(kv) = span.attributes.iter_mut().find(|kv| kv.key == "sp.session.id") {
            kv.value = Some(AnyValue{ value: Some(any_value::Value::StringValue(s3.clone()))});
        }
    }
    let r3 = client.post(&format!("{}/v1/traces", base_url)).header("content-type", "application/json").json(&req3).send().await.unwrap();
    assert_eq!(r3.status(), StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait for optimizer ticks to commit buffered batches
    tokio::time::sleep(Duration::from_secs(3)).await;
    if let Some(engine) = ingest_engine.as_ref() {
        for _ in 0..5 {
            engine.run_optimizer_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    
    // Query via DuckDB SQL and only count rows for our three generated sessions.
    let s1_escaped = s1.replace('\'', "''");
    let s2_escaped = s2.replace('\'', "''");
    let s3_escaped = s3.replace('\'', "''");
    let sql = format!(
        "SELECT session_id FROM union_spans WHERE session_id IN ('{}', '{}', '{}') GROUP BY session_id",
        s1_escaped, s2_escaped, s3_escaped
    );
    let mut found_sessions = std::collections::HashSet::new();
    let mut success = false;
    for _ in 0..10 {
        let result = query_engine_handle.execute_query(&sql).await.unwrap();
        found_sessions.clear();
        for row in result.rows.iter() {
            if let Some(session) = row.get(0).and_then(|value| value.as_str()) {
                found_sessions.insert(session.to_string());
            }
        }
        if found_sessions.len() == 3 {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        success,
        "Expected 3 sessions via SQL union view, found {:?}",
        found_sessions
    );
}

#[tokio::test]
async fn test_union_read_sql_reads_flushed_span() {
    let mut config = Config::default();
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181/catalog".to_string();
    config.iceberg.warehouse = "default".to_string();
    config.s3.endpoint = Some("http://localhost:9002".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();
    config.ingest_engine.optimizer_interval_seconds = 60;
    config.span_buffering.flush_interval_seconds = 1;
    config.span_buffering.max_buffer_spans = 1;
    let cache_dir = tempdir().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());

    let pipeline = storage::IngestPipeline::new(&config).await.unwrap();
    let storage::IngestPipeline { storage, span_buffer, .. } = pipeline;
    let query_engine = query::create_query_engine(&config).await.unwrap();
    let query_engine_handle = query_engine.clone();
    let app = api::create_router(storage, query_engine, Some(span_buffer), None, None)
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);
    tokio::spawn(async move { axum::serve(listener, app.into_make_service()).await.unwrap(); });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut otlp_request = create_test_otlp_request();
    let session_id = format!("sql-flush-{}", Uuid::new_v4());
    let expected_span_id = vec![0xaa; 8];
    {
        let span = &mut otlp_request.resource_spans[0].scope_spans[0].spans[0];
        span.span_id = expected_span_id.clone();
        if let Some(kv) = span.attributes.iter_mut().find(|kv| kv.key == "sp.session.id") {
            kv.value = Some(AnyValue {
                value: Some(any_value::Value::StringValue(session_id.clone())),
            });
        }
    }

    let client = Client::new();
    let response = client
        .post(&format!("{}/v1/traces", base_url))
        .header("content-type", "application/json")
        .json(&otlp_request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(1200)).await;

    let sql = format!(
        "SELECT COUNT(*) AS count FROM union_spans WHERE session_id = '{}'",
        session_id.replace('\'', "''")
    );
    let result = query_engine_handle.execute_query(&sql).await.unwrap();
    let count = result.rows[0][0].as_i64().unwrap_or(0);
    assert_eq!(count, 1, "Expected flushed span to be visible via SQL union view");
}

#[tokio::test]
async fn test_duckdb_union_read_from_local_wal() {
    let cache_dir = tempdir().unwrap();
    let cache_path = cache_dir.path().to_path_buf();

    std::env::set_var("ICEBERG_WAREHOUSE", "default");
    std::env::set_var("S3_ENDPOINT", "http://localhost:9002");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    let mut config = Config::default();
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181/catalog".to_string();
    config.iceberg.warehouse = "default".to_string();
    config.s3.endpoint = Some("http://localhost:9002".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();
    config.ingest_engine.cache_dir = Some(cache_path.to_string_lossy().to_string());
    config.ingest_engine.enabled = true;

    let pipeline = storage::IngestPipeline::new(&config).await.unwrap();
    let query_engine = query::create_query_engine(&config).await.unwrap();

    let session_id = format!("wal-duckdb-{}", Uuid::new_v4());
    let now = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let span = ModelSpan {
        session_id: session_id.clone(),
        trace_id: "trace-wal-duckdb".to_string(),
        span_id: "span-wal-duckdb".to_string(),
        parent_span_id: None,
        app_id: "app-wal-duckdb".to_string(),
        organization_id: None,
        tenant_id: None,
        message_type: "span".to_string(),
        span_kind: Some("server".to_string()),
        timestamp: now,
        end_timestamp: Some(now),
        attributes: std::collections::HashMap::new(),
        events: Vec::new(),
        status_code: None,
        status_message: None,
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
    };

    pipeline.write_wal_spans(vec![span], 128).await.unwrap();

    let escaped_session = session_id.replace('\'', "''");
    let sql = format!(
        "SELECT COUNT(*) AS count FROM union_spans WHERE session_id = '{}'",
        escaped_session
    );
    let result = query_engine.execute_query(&sql).await.unwrap();

    assert_eq!(result.row_count, 1);
    let count = result.rows[0][0].as_i64().unwrap_or(0);
    assert_eq!(count, 1, "Expected WAL-backed union_read to see 1 row");
}

#[tokio::test]
async fn test_wal_replay_recovers_spans() {
    std::env::set_var("ICEBERG_WAREHOUSE", "default");
    std::env::set_var("S3_ENDPOINT", "http://localhost:9002");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    let mut config = Config::default();
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181/catalog".to_string();
    config.iceberg.warehouse = "default".to_string();
    config.s3.endpoint = Some("http://localhost:9002".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();
    config.ingest_engine.optimizer_interval_seconds = 1;
    config.ingest_engine.wal_prefix = format!("test-wal-{}", Uuid::new_v4().simple());
    config.ingest_engine.enabled = true;
    let cache_dir = tempdir().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());

    let session_id = format!("replay-{}", Uuid::new_v4());
    let now = chrono::Utc::now();
    let span = ModelSpan {
        session_id: session_id.clone(),
        trace_id: "trace-replay".to_string(),
        span_id: "span-replay".to_string(),
        parent_span_id: None,
        app_id: "app-replay".to_string(),
        organization_id: None,
        tenant_id: None,
        message_type: "span".to_string(),
        span_kind: Some("server".to_string()),
        timestamp: now,
        end_timestamp: Some(now),
        attributes: std::collections::HashMap::new(),
        events: Vec::new(),
        status_code: None,
        status_message: None,
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
    };

    config.ingest_engine.replay_wal_on_startup = false;
    let pipeline = storage::IngestPipeline::new(&config).await.unwrap();
    pipeline.write_wal_spans(vec![span], 128).await.unwrap();
    drop(pipeline);

    config.ingest_engine.replay_wal_on_startup = true;
    let storage = storage::create_storage(&config).await.unwrap();
    if let Some(engine) = storage.ingest_engine.as_ref() {
        engine.run_optimizer_once().await.unwrap();
    }
    let query_engine = query::create_query_engine(&config).await.unwrap();

    let wal_count = storage
        .ingest_engine
        .as_ref()
        .and_then(|engine| engine.list_wal_files("spans").ok())
        .map(|files| files.len())
        .unwrap_or(0);
    assert!(wal_count >= 1, "Expected at least one WAL entry, found {}", wal_count);

    let escaped_session = session_id.replace('\'', "''");
    let sql = format!(
        "SELECT COUNT(*) AS count FROM union_spans WHERE session_id = '{}'",
        escaped_session
    );
    let mut found = false;
    for _ in 0..30 {
        let result = query_engine.execute_query(&sql).await.unwrap();
        let count = result
            .rows
            .get(0)
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        if count > 0 {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(found, "Expected WAL replay to commit session_id {}", session_id);
}

#[tokio::test]
async fn test_metadata_maintenance_job_expires_snapshots() {
    use std::collections::HashMap;
    use chrono::{Duration as ChronoDuration, Utc};
    use iceberg::{Catalog, TableCreation, TableIdent};
    use softprobe_otlp_backend::compaction::executor::{ActionStatus, CompactionStatus, MaintenanceExecutor};
    use softprobe_otlp_backend::models::Span as ModelSpan;
    use softprobe_otlp_backend::storage::iceberg::{IcebergCatalog, TableWriter, TraceTable};

    std::env::set_var("ICEBERG_WAREHOUSE", "default");
    std::env::set_var("S3_ENDPOINT", "http://localhost:9002");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    let mut config = Config::default();
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181/catalog".to_string();
    config.iceberg.warehouse = "default".to_string();
    config.s3.endpoint = Some("http://localhost:9002".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();
    config.compaction.enabled = true;
    config.compaction.min_files_to_compact = 1;
    config.compaction.metadata_maintenance_enabled = true;
    config.compaction.metadata_min_snapshots_to_keep = 1;
    config.compaction.metadata_max_snapshot_age_seconds = 0;

    let catalog = IcebergCatalog::new(&config).await.unwrap();
    let namespace = iceberg::NamespaceIdent::from_strs(["default"]).unwrap();
    let _ = catalog
        .catalog()
        .create_namespace(&namespace, HashMap::new())
        .await;

    let table_name = format!("traces_maintenance_{}", Uuid::new_v4().simple());
    let table_ident = TableIdent::from_strs(["default", table_name.as_str()]).unwrap();

    let schema = TraceTable::schema();
    let partition_spec = TraceTable::partition_spec(&schema).unwrap();
    let sort_order = TraceTable::sort_order(&schema).unwrap();
    let properties = TraceTable::table_properties();
    let creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema)
        .partition_spec(partition_spec)
        .sort_order(sort_order)
        .properties(properties)
        .build();
    catalog
        .catalog()
        .create_table(&namespace, creation)
        .await
        .unwrap();

    let writer = TableWriter::new(catalog.catalog().clone(), table_ident.clone());
    let base_time = Utc::now();
    for offset in 0..3 {
        let timestamp = base_time + ChronoDuration::seconds(offset);
        let span = ModelSpan {
            session_id: format!("maint-{}", offset),
            trace_id: format!("trace-{}", offset),
            span_id: format!("span-{}", offset),
            parent_span_id: None,
            app_id: "app-test".to_string(),
            organization_id: None,
            tenant_id: None,
            message_type: "span".to_string(),
            span_kind: Some("server".to_string()),
            timestamp,
            end_timestamp: Some(timestamp + ChronoDuration::seconds(1)),
            attributes: HashMap::new(),
            events: Vec::new(),
            status_code: Some("OK".to_string()),
            status_message: None,
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
        };
        writer
            .write_batches(vec![vec![span]], ModelSpan::to_record_batch)
            .await
            .unwrap();
    }

    let table_before = catalog.catalog().load_table(&table_ident).await.unwrap();
    let snapshot_count_before = table_before.metadata().snapshots().count();
    assert!(
        snapshot_count_before >= 3,
        "Expected at least 3 snapshots, found {}",
        snapshot_count_before
    );

    let executor = MaintenanceExecutor::new(&config).await.unwrap();
    let summary = executor
        .run_once_for_tables(&[table_ident.clone()])
        .await
        .unwrap();
    let result = summary.tables.first().unwrap();
    assert!(
        result.metadata.expired_snapshots >= 2,
        "Expected at least 2 snapshots expired, found {}",
        result.metadata.expired_snapshots
    );
    assert!(
        matches!(
            result.compaction.status,
            CompactionStatus::Completed | CompactionStatus::Unsupported
        ),
        "Unexpected compaction status"
    );
    assert!(
        matches!(
            result.rewrite_manifests.status,
            ActionStatus::Completed | ActionStatus::Unsupported
        ),
        "Unexpected rewrite manifests status"
    );
    assert!(
        matches!(
            result.remove_orphan_files.status,
            ActionStatus::Completed | ActionStatus::Unsupported
        ),
        "Unexpected remove orphan files status"
    );

    let mut remaining = snapshot_count_before;
    for _ in 0..10 {
        let table_after = catalog.catalog().load_table(&table_ident).await.unwrap();
        remaining = table_after.metadata().snapshots().count();
        if remaining <= config.compaction.metadata_min_snapshots_to_keep {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        remaining <= config.compaction.metadata_min_snapshots_to_keep,
        "Expected snapshot count <= {}, found {}",
        config.compaction.metadata_min_snapshots_to_keep,
        remaining
    );

    let _ = catalog.catalog().drop_table(&table_ident).await;
}
