use softprobe_otlp_backend::api;
use softprobe_otlp_backend::config::Config;
use softprobe_otlp_backend::storage;
use softprobe_otlp_backend::query;
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
use arrow::array::{Array, StringArray};
use futures::stream::StreamExt;

async fn start_test_server() -> String {
    let mut config = Config::default();
    // Use REST catalog for integration tests
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181".to_string();
    config.iceberg.force_close_after_append = true; // Force immediate commits for tests
    
    // Ensure MinIO/S3 env for REST catalog (dev-compose defaults)
    std::env::set_var("S3_ENDPOINT", "http://localhost:9002");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    // Initialize storage components
    let storage = storage::create_storage(&config).await.unwrap();
    let query_engine = query::create_query_engine(&config).await.unwrap();
    
    // Initialize span buffer
    let span_buffer = storage::create_span_buffer(&config, storage.iceberg_writer.clone()).await.unwrap();
    
    // Create router
    let app = api::create_router(storage, query_engine, Some(span_buffer), None).await.unwrap();
    
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
    
    base_url
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
    let base_url = start_test_server().await;
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
    let base_url = start_test_server().await;
    let client = Client::new();

    let response = client
        .get(&format!("{}/ready", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_otlp_traces_json_endpoint_success() {
    let base_url = start_test_server().await;
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
async fn test_otlp_traces_protobuf_endpoint_success() {
    let base_url = start_test_server().await;
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
async fn test_otlp_traces_json_endpoint_multiple_spans() {
    let base_url = start_test_server().await;
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
async fn test_otlp_traces_json_endpoint_empty_request() {
    let base_url = start_test_server().await;
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
async fn test_otlp_traces_json_endpoint_invalid_json() {
    let base_url = start_test_server().await;
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
async fn test_otlp_traces_with_missing_app_id() {
    let base_url = start_test_server().await;
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
async fn test_otlp_traces_session_grouping() {
    let base_url = start_test_server().await;
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
    let base_url = start_test_server().await;
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

    // Set environment variables for REST catalog warehouse mapping
    std::env::set_var("ICEBERG_REST_WAREHOUSE_URI_PREFIX", "s3://warehouse/iceberg");
    std::env::set_var("ICEBERG_WAREHOUSE", "s3://warehouse/iceberg");
    std::env::set_var("S3_ENDPOINT", "http://localhost:9002");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    // Start server with REST catalog configuration
    let mut config = Config::default();
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181".to_string();
    // Ensure writer uses the same warehouse URI as scanner
    config.iceberg.warehouse = "s3://warehouse/iceberg".to_string();
    // Provide S3 settings for client-side file IO to MinIO
    config.s3.endpoint = Some("http://localhost:9002".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();
    config.span_buffering.flush_interval_seconds = 1; // flush after 1s
    config.span_buffering.max_buffer_spans = 1; // flush after a single span
    config.iceberg.force_close_after_append = true; // Force immediate commits

    let storage = storage::create_storage(&config).await.unwrap();
    let query_engine = query::create_query_engine(&config).await.unwrap();
    let span_buffer = storage::create_span_buffer(&config, storage.iceberg_writer.clone()).await.unwrap();
    let app = api::create_router(storage, query_engine, Some(span_buffer), None).await.unwrap();

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

    // Wait a bit longer for commits to complete (avoid flakiness)
    tokio::time::sleep(Duration::from_millis(1200)).await;
    
    // Query the table via Iceberg catalog to validate data was committed
    use std::collections::HashMap;
    use iceberg::{Catalog, CatalogBuilder};
    use iceberg_catalog_rest::{
        REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
    };
    use iceberg::TableIdent;
    
    let catalog = RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([
                (REST_CATALOG_PROP_URI.to_string(), "http://localhost:8181".to_string()),
                (
                    REST_CATALOG_PROP_WAREHOUSE.to_string(),
                    std::env::var("ICEBERG_REST_WAREHOUSE_URI_PREFIX")
                        .unwrap_or_else(|_| "s3://warehouse/iceberg".to_string()),
                ),
                ("s3.endpoint".to_string(), std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9002".to_string())),
                ("s3.access-key-id".to_string(), std::env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string())),
                ("s3.secret-access-key".to_string(), std::env::var("S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string())),
                ("s3.region".to_string(), std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string())),
            ]),
        )
        .await
        .unwrap();
    
    let table_ident = TableIdent::from_strs(["default", "otlp_traces"])
        .or_else(|_| TableIdent::from_strs(["otlp_traces"]))
        .unwrap();
    let table = match catalog.load_table(&table_ident).await {
        Ok(t) => t,
        Err(e) => {
            panic!("Table 'otlp_traces' does not exist in REST catalog. Please create it using the DDL in schemas/iceberg/otlp_traces.sql. Error: {}", e);
        }
    };
    
    // Retry loop to handle eventual consistency in object store/REST catalog
    let mut found = false;
    let mut total_rows = 0;
    for _ in 0..10 {
        let scan = table.scan().build().unwrap();
        let mut arrow_stream = scan.to_arrow().await.unwrap();
        found = false;
        total_rows = 0;
        while let Some(batch_result) = arrow_stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
            // Find session_id column
            let schema = batch.schema();
            if let Some((idx, _)) = schema.fields().iter().enumerate().find(|(_, f)| f.name() == "session_id") {
                let col = batch.column(idx);
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..arr.len() {
                    if arr.value(i) == sid { found = true; break; }
                }
            }
            if found { break; }
        }
        if found { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(found, "expected session_id '{}' to be present in table. Found {} rows total.", sid, total_rows);
}

#[tokio::test]
async fn test_multi_sessions_are_written_and_queryable() {
    // Arrange: Use REST catalog - files will be written to warehouse/iceberg via catalog
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
    let warehouse_dir = repo_root.join("warehouse/iceberg");
    fs::create_dir_all(&warehouse_dir).unwrap();

    // Set environment variables for REST catalog warehouse mapping
    std::env::set_var("ICEBERG_REST_WAREHOUSE_URI_PREFIX", "s3://warehouse/iceberg");
    std::env::set_var("ICEBERG_WAREHOUSE", "s3://warehouse/iceberg");
    std::env::set_var("S3_ENDPOINT", "http://localhost:9002");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    // Start server with REST catalog configuration
    let mut config = Config::default();
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181".to_string();
    // Ensure writer uses same warehouse and S3 settings as scanner
    config.iceberg.warehouse = "s3://warehouse/iceberg".to_string();
    config.s3.endpoint = Some("http://localhost:9002".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();
    // Use the default table; we filter results to our sessions to avoid data pollution
    let table_name = "otlp_traces".to_string();
    config.span_buffering.flush_interval_seconds = 1; // flush after 1s
    config.span_buffering.max_buffer_spans = 1; // flush after a single span
    config.iceberg.force_close_after_append = true; // Force immediate commits

    let storage = storage::create_storage(&config).await.unwrap();
    let query_engine = query::create_query_engine(&config).await.unwrap();
    let span_buffer = storage::create_span_buffer(&config, storage.iceberg_writer.clone()).await.unwrap();
    let app = api::create_router(storage, query_engine, Some(span_buffer), None).await.unwrap();

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

    // Wait a bit for commits to complete
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Query the table via Iceberg catalog to validate data was committed
    use std::collections::HashMap;
    use iceberg::{Catalog, CatalogBuilder};
    use iceberg_catalog_rest::{
        REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
    };
    use iceberg::TableIdent;
    
    let catalog = RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([
                (REST_CATALOG_PROP_URI.to_string(), "http://localhost:8181".to_string()),
                (
                    REST_CATALOG_PROP_WAREHOUSE.to_string(),
                    std::env::var("ICEBERG_REST_WAREHOUSE_URI_PREFIX")
                        .unwrap_or_else(|_| "s3://warehouse/iceberg".to_string()),
                ),
                ("s3.endpoint".to_string(), std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9002".to_string())),
                ("s3.access-key-id".to_string(), std::env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string())),
                ("s3.secret-access-key".to_string(), std::env::var("S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string())),
                ("s3.region".to_string(), std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string())),
            ]),
        )
        .await
        .unwrap();
    
    let table_ident = TableIdent::from_strs(["default", table_name.as_str()])
        .or_else(|_| TableIdent::from_strs([table_name.as_str()]))
        .unwrap();
    let table = match catalog.load_table(&table_ident).await {
        Ok(t) => t,
        Err(e) => {
            panic!("Table 'otlp_traces' does not exist in REST catalog. Please create it using the DDL in schemas/iceberg/otlp_traces.sql. Error: {}", e);
        }
    };
    
    // Retry and only count rows for our three generated sessions to avoid pre-existing data
    let target_sessions: std::collections::HashSet<String> = std::collections::HashSet::from_iter([s1.clone(), s2.clone(), s3.clone()]);
    let mut filtered_total_rows = 0usize;
    let mut filtered_found_sessions: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut success = false;
    for _ in 0..10 {
        // Rebuild the scan each iteration to observe new commits
        let scan = table.scan().build().unwrap();
        filtered_total_rows = 0;
        filtered_found_sessions.clear();
        let mut arrow_stream = scan.to_arrow().await.unwrap();
        while let Some(batch_result) = arrow_stream.next().await {
            let batch = batch_result.unwrap();
            // Find session_id column
            let schema = batch.schema();
            if let Some((idx, _)) = schema.fields().iter().enumerate().find(|(_, f)| f.name() == "session_id") {
                let col = batch.column(idx);
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..arr.len() {
                    let sid = arr.value(i);
                    if target_sessions.contains(sid) {
                        filtered_total_rows += 1;
                        filtered_found_sessions.insert(sid.to_string());
                    }
                }
            }
        }
        if filtered_total_rows == 3 && filtered_found_sessions.len() == 3 {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(success, "Expected exactly 3 rows from 3 sessions, found {} rows and sessions {:?}. This may indicate test isolation issues or eventual consistency delays.", filtered_total_rows, filtered_found_sessions);
}
