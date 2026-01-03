use softprobe_otlp_backend::config::Config;
use softprobe_otlp_backend::models::{Span as SpanData, SpanEvent, Log as LogData};
use chrono::Utc;
use std::collections::HashMap;
use std::time::Instant;

// ========================================
// Performance Metrics Collection Helpers
// ========================================

#[derive(Debug, Clone)]
struct PerformanceMetrics {
    operation: String,
    duration_ms: u128,
    rows_processed: usize,
    bytes_processed: Option<usize>,
    files_scanned: Option<usize>,
    partitions_scanned: Option<usize>,
    throughput_rows_per_sec: f64,
    throughput_mb_per_sec: Option<f64>,
}

impl PerformanceMetrics {
    fn new(operation: &str) -> Self {
        Self {
            operation: operation.to_string(),
            duration_ms: 0,
            rows_processed: 0,
            bytes_processed: None,
            files_scanned: None,
            partitions_scanned: None,
            throughput_rows_per_sec: 0.0,
            throughput_mb_per_sec: None,
        }
    }

    fn with_duration(mut self, duration: std::time::Duration) -> Self {
        self.duration_ms = duration.as_millis();
        self.calculate_throughput();
        self
    }

    fn with_rows(mut self, rows: usize) -> Self {
        self.rows_processed = rows;
        self.calculate_throughput();
        self
    }

    fn calculate_throughput(&mut self) {
        if self.duration_ms > 0 {
            let duration_sec = self.duration_ms as f64 / 1000.0;
            self.throughput_rows_per_sec = self.rows_processed as f64 / duration_sec;

            if let Some(bytes) = self.bytes_processed {
                let mb = bytes as f64 / (1024.0 * 1024.0);
                self.throughput_mb_per_sec = Some(mb / duration_sec);
            }
        }
    }

    fn print_report(&self) {
        println!("\n📊 Performance Metrics Report: {}", self.operation);
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("⏱️  Duration:              {} ms", self.duration_ms);
        println!("📝 Rows Processed:        {}", self.rows_processed);

        if let Some(bytes) = self.bytes_processed {
            println!("💾 Bytes Processed:       {} ({:.2} MB)", bytes, bytes as f64 / (1024.0 * 1024.0));
        }

        if let Some(files) = self.files_scanned {
            println!("📁 Files Scanned:         {}", files);
        }

        if let Some(partitions) = self.partitions_scanned {
            println!("🗂️  Partitions Scanned:    {}", partitions);
        }

        println!("⚡ Throughput (rows/sec): {:.2}", self.throughput_rows_per_sec);

        if let Some(mb_per_sec) = self.throughput_mb_per_sec {
            println!("⚡ Throughput (MB/sec):   {:.2}", mb_per_sec);
        }

        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    fn assert_performance_target(&self, max_duration_ms: u128, target_name: &str) {
        if self.duration_ms > max_duration_ms {
            println!("⚠️  WARNING: {} exceeded target of {} ms (actual: {} ms)",
                target_name, max_duration_ms, self.duration_ms);
        } else {
            println!("✅ {} within target of {} ms (actual: {} ms)",
                target_name, max_duration_ms, self.duration_ms);
        }
    }
}

struct Timer {
    start: Instant,
    operation: String,
}

impl Timer {
    fn start(operation: &str) -> Self {
        println!("⏱️  Starting: {}", operation);
        Self {
            start: Instant::now(),
            operation: operation.to_string(),
        }
    }

    fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    fn stop(&self) -> std::time::Duration {
        let duration = self.elapsed();
        println!("⏹️  Completed: {} in {:?}", self.operation, duration);
        duration
    }
}

fn load_test_config() -> Config {
    // Allow selecting test config via environment variable
    // ICEBERG_TEST_TYPE=r2 cargo test  - for Cloudflare R2
    // ICEBERG_TEST_TYPE=local cargo test - for local MinIO (default)
    let test_type = std::env::var("ICEBERG_TEST_TYPE").unwrap_or_else(|_| "local".to_string());

    let config_file = match test_type.as_str() {
        "r2" => "tests/config/test-r2.yaml",
        _ => "tests/config/test.yaml",
    };

    println!("Loading test config from: {}", config_file);
    std::env::set_var("CONFIG_FILE", config_file);
    Config::load().expect("Failed to load test config")
}

#[tokio::test]
async fn test_iceberg_writer_initialization() {
    let config = load_test_config();

    println!("Testing with catalog URI: {}", config.iceberg.catalog_uri);

    // Performance tracking: measure initialization time
    let timer = Timer::start("Iceberg Writer Initialization");

    // Test that the Iceberg writer can be initialized with real REST catalog
    let result = softprobe_otlp_backend::storage::iceberg::IcebergWriter::new(&config).await;

    let init_duration = timer.stop();

    match result {
        Ok(_) => {
            println!("✅ Iceberg writer initialized successfully with REST catalog");

            // Report initialization performance
            let metrics = PerformanceMetrics::new("Writer Initialization")
                .with_duration(init_duration)
                .with_rows(0);
            metrics.print_report();

            // Assert initialization is reasonably fast (should be < 5 seconds for catalog connection)
            metrics.assert_performance_target(5000, "Initialization time");
        }
        Err(e) => {
            println!("❌ Iceberg writer failed to initialize: {}", e);

            // Print full error chain to see the REAL error (anyhow provides chain())
            println!("\n🔍 Full error chain:");
            for (i, cause) in e.chain().enumerate() {
                if i == 0 {
                    println!("  Error: {}", cause);
                } else {
                    println!("  {}. Caused by: {}", i, cause);
                }
            }

            // If this fails, it means either the catalog is not running or there's a real issue
            panic!("Expected Iceberg writer to initialize successfully with local REST catalog");
        }
    }
}

#[tokio::test]
async fn test_config_loading() {
    // Test that test config can be loaded and has valid iceberg section
    let config = load_test_config();
    assert!(!config.iceberg.catalog_uri.is_empty(), "Catalog URI should not be empty");
    assert_eq!(config.span_buffering.max_buffer_spans, 1, "Should have test buffer config");
}


#[tokio::test]
async fn test_iceberg_writer_bulk_session_roundtrip() {
    use iceberg_catalog_rest::RestCatalogBuilder;
    use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
    use iceberg::{Catalog, CatalogBuilder, TableIdent};
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;
    use arrow::array::{Array, StringArray};
    use futures::StreamExt;

    let config = load_test_config();
    let writer = softprobe_otlp_backend::storage::iceberg::IcebergWriter::new(&config).await
        .expect("writer init");

    // Create multiple sessions with spans to test multi-session row groups
    let num_sessions = 5;
    let spans_per_session = 1000;
    let now = Utc::now();

    let mut all_session_batches = Vec::new();
    let mut session_ids = Vec::new();

    for session_idx in 0..num_sessions {
        let session_id = format!("session-{}", uuid::Uuid::new_v4());
        session_ids.push(session_id.clone());

        let mut session_spans = Vec::new();
        for i in 0..spans_per_session {
            let mut attributes = HashMap::new();
            attributes.insert("sp.session.id".to_string(), session_id.clone());
            attributes.insert("span.index".to_string(), i.to_string());

            // Add events to every 10th span
            let events = if i % 10 == 0 {
                vec![SpanEvent {
                    name: format!("event.{}", i),
                    timestamp: now + chrono::Duration::milliseconds((session_idx * 1000 + i) as i64),
                    attributes: HashMap::from([
                        ("event.index".to_string(), i.to_string()),
                    ]),
                }]
            } else {
                Vec::new()
            };

            // Add HTTP data to first span of each session for verification
            let (http_method, http_path, http_headers, http_req_body, http_status, http_resp_headers, http_resp_body) = if i == 0 {
                (
                    Some("POST".to_string()),
                    Some(format!("/api/v1/session/{}", session_idx)),
                    Some(r#"{"Content-Type":"application/json","Authorization":"Bearer test-token"}"#.to_string()),
                    Some(format!(r#"{{"session_id":"{}","action":"create"}}"#, session_id)),
                    Some(200),
                    Some(r#"{"Content-Type":"application/json","X-Request-Id":"req-123"}"#.to_string()),
                    Some(format!(r#"{{"success":true,"session_id":"{}","created_at":"2025-12-31T00:00:00Z"}}"#, session_id)),
                )
            } else {
                (None, None, None, None, None, None, None)
            };

            session_spans.push(SpanData {
                session_id: session_id.clone(), // Explicit session_id field
                trace_id: format!("trace-{}-{}", session_idx, i),
                span_id: format!("span-{}-{}", session_idx, i),
                parent_span_id: None,
                app_id: format!("app-{}", session_idx % 2), // Alternate between 2 apps
                organization_id: Some("org-test".to_string()),
                tenant_id: Some("tenant-test".to_string()),
                message_type: "HTTP_REQUEST".to_string(),
                span_kind: Some("SERVER".to_string()),
                timestamp: now + chrono::Duration::milliseconds((session_idx * 1000 + i) as i64),
                end_timestamp: Some(now + chrono::Duration::milliseconds((session_idx * 1000 + i + 5) as i64)),
                attributes,
                events,
                http_request_method: http_method,
                http_request_path: http_path,
                http_request_headers: http_headers,
                http_request_body: http_req_body,
                http_response_status_code: http_status,
                http_response_headers: http_resp_headers,
                http_response_body: http_resp_body,
                status_code: Some("OK".to_string()),
                status_message: Some("Success".to_string()),
            });
        }
        all_session_batches.push(session_spans);
    }

    let total_spans = num_sessions * spans_per_session;

    // Act: write all sessions in one batch (creates 1 file with multiple row groups)
    println!("🧪 Writing {} sessions ({} total spans) to Iceberg...", num_sessions, total_spans);
    let write_timer = Timer::start(&format!("Multi-Session Write ({} sessions)", num_sessions));
    writer.write_span_batches(all_session_batches).await.expect("multi-session write should succeed");
    let write_duration = write_timer.stop();

    // Report write performance
    let write_metrics = PerformanceMetrics::new(&format!("Multi-Session Write ({} sessions, {} spans)", num_sessions, total_spans))
        .with_duration(write_duration)
        .with_rows(total_spans);
    write_metrics.print_report();
    // Target: should be faster than writing sessions individually
    write_metrics.assert_performance_target(5000, "Multi-session write time");

    println!("✅ Write completed, querying back each session to verify row group isolation...");

    // Assert: query back via REST catalog scan
    let mut props = std::collections::HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), config.iceberg.catalog_uri.clone());
    let warehouse = std::env::var("ICEBERG_WAREHOUSE")
        .unwrap_or_else(|_| config.iceberg.warehouse.clone());
    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse);

    // Add bearer token if present
    if let Some(ref token) = config.iceberg.catalog_token {
        props.insert("token".to_string(), token.clone());
    }

    if let Some(ep) = std::env::var("S3_ENDPOINT").ok().or(config.s3.endpoint.clone()) {
        props.insert("s3.endpoint".to_string(), ep);
    }
    if let Some(ak) = std::env::var("S3_ACCESS_KEY").ok().or(config.s3.access_key_id.clone()) {
        props.insert("s3.access-key-id".to_string(), ak);
    }
    if let Some(sk) = std::env::var("S3_SECRET_KEY").ok().or(config.s3.secret_access_key.clone()) {
        props.insert("s3.secret-access-key".to_string(), sk);
    }
    props.insert("s3.region".to_string(), config.storage.s3_region.clone());

    // For testing/development with TLS interception, use custom client
    let catalog = if std::env::var("ICEBERG_DISABLE_TLS_VALIDATION").is_ok() {
        let http_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("build http client");
        RestCatalogBuilder::default()
            .with_client(http_client)
            .load("rest", props).await
            .expect("load catalog")
    } else {
        RestCatalogBuilder::default().load("rest", props).await
            .expect("load catalog")
    };

    // Use the traces table (refactored architecture has separate tables for spans and logs)
    let table_ident = TableIdent::from_strs(["default", "traces"])
        .expect("table ident");

    // Performance tracking: measure table load time
    let load_timer = Timer::start("Table Load");
    let table = catalog.load_table(&table_ident).await.expect("load table");
    let load_duration = load_timer.stop();

    // Report table metadata loading performance
    let load_metrics = PerformanceMetrics::new("Table Load")
        .with_duration(load_duration);
    load_metrics.print_report();
    load_metrics.assert_performance_target(1000, "Table load time");

    // Query each session individually to verify row group isolation
    let mut total_query_duration = std::time::Duration::ZERO;
    let mut all_selectivities = Vec::new();

    for (session_idx, session_id) in session_ids.iter().enumerate() {
        println!("\n🔍 Querying session {}/{}: {}", session_idx + 1, num_sessions, session_id);

        let predicate = Reference::new("session_id").equal_to(Datum::string(session_id));
        let scan = table.scan()
            .with_filter(predicate)
            .build()
            .expect("scan build");

        let query_timer = Timer::start(&format!("Query session {}", session_idx + 1));
        let mut arrow_stream = scan.to_arrow().await.expect("arrow stream");

        let mut found = 0usize;
        let mut rows_scanned = 0usize;
        let mut http_verified = false;

        while let Some(batch_result) = arrow_stream.next().await {
            let batch = batch_result.expect("batch ok");
            rows_scanned += batch.num_rows();

            // Count matching session_id rows
            if let Some((idx, _)) = batch.schema().fields().iter().enumerate().find(|(_, f)| f.name() == "session_id") {
                let col = batch.column(idx);
                let arr = col.as_any().downcast_ref::<StringArray>().expect("string arr");
                for i in 0..arr.len() {
                    if arr.value(i) == session_id.as_str() { found += 1; }
                }
            }

            // Verify HTTP fields for first span (i==0) of this session
            if !http_verified && batch.num_rows() > 0 {
                // Get schema field indices
                let schema = batch.schema();
                let get_field_idx = |name: &str| schema.fields().iter().position(|f| f.name() == name);

                // Verify HTTP request fields
                if let Some(method_idx) = get_field_idx("http_request_method") {
                    let method_col = batch.column(method_idx).as_any().downcast_ref::<StringArray>().unwrap();
                    if !method_col.is_null(0) {
                        assert_eq!(method_col.value(0), "POST", "HTTP method should be POST for session {}", session_idx);
                        http_verified = true;
                    }
                }

                if let Some(path_idx) = get_field_idx("http_request_path") {
                    let path_col = batch.column(path_idx).as_any().downcast_ref::<StringArray>().unwrap();
                    if !path_col.is_null(0) {
                        assert_eq!(path_col.value(0), format!("/api/v1/session/{}", session_idx),
                            "HTTP path should match for session {}", session_idx);
                    }
                }

                if let Some(headers_idx) = get_field_idx("http_request_headers") {
                    let headers_col = batch.column(headers_idx).as_any().downcast_ref::<StringArray>().unwrap();
                    if !headers_col.is_null(0) {
                        let headers = headers_col.value(0);
                        assert!(headers.contains("Content-Type"), "Request headers should contain Content-Type");
                        assert!(headers.contains("Authorization"), "Request headers should contain Authorization");
                    }
                }

                if let Some(body_idx) = get_field_idx("http_request_body") {
                    let body_col = batch.column(body_idx).as_any().downcast_ref::<StringArray>().unwrap();
                    if !body_col.is_null(0) {
                        let body = body_col.value(0);
                        assert!(body.contains(session_id.as_str()), "Request body should contain session_id");
                        assert!(body.contains("action"), "Request body should contain action field");
                    }
                }

                // Verify HTTP response fields
                if let Some(status_idx) = get_field_idx("http_response_status_code") {
                    let status_col = batch.column(status_idx).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
                    if !status_col.is_null(0) {
                        assert_eq!(status_col.value(0), 200, "HTTP response status should be 200");
                    }
                }

                if let Some(resp_headers_idx) = get_field_idx("http_response_headers") {
                    let resp_headers_col = batch.column(resp_headers_idx).as_any().downcast_ref::<StringArray>().unwrap();
                    if !resp_headers_col.is_null(0) {
                        let headers = resp_headers_col.value(0);
                        assert!(headers.contains("X-Request-Id"), "Response headers should contain X-Request-Id");
                    }
                }

                if let Some(resp_body_idx) = get_field_idx("http_response_body") {
                    let resp_body_col = batch.column(resp_body_idx).as_any().downcast_ref::<StringArray>().unwrap();
                    if !resp_body_col.is_null(0) {
                        let body = resp_body_col.value(0);
                        assert!(body.contains(session_id.as_str()), "Response body should contain session_id");
                        assert!(body.contains("success"), "Response body should contain success field");
                    }
                }
            }
        }

        let query_duration = query_timer.stop();
        total_query_duration += query_duration;

        // Verify HTTP fields were checked
        if http_verified {
            println!("  ✓ HTTP fields verified for first span of session {}", session_idx);
        }

        let selectivity = (found as f64 / rows_scanned.max(1) as f64) * 100.0;
        all_selectivities.push(selectivity);

        println!("  ✓ Found {} rows, scanned {} rows (selectivity: {:.1}%)", found, rows_scanned, selectivity);

        assert_eq!(found, spans_per_session,
                   "Expected exactly {} spans for session {}, found {}",
                   spans_per_session, session_id, found);

        // Verify perfect selectivity (100% = only scanned the target row group)
        assert_eq!(found, rows_scanned,
                   "Expected perfect selectivity (100%), but scanned {} rows and matched {} rows",
                   rows_scanned, found);
    }

    println!("\n📊 Multi-Session Query Performance Summary:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("⏱️  Total query time ({} sessions): {:?}", num_sessions, total_query_duration);
    println!("⏱️  Average per session: {:?}", total_query_duration / num_sessions as u32);
    println!("📊 Selectivity: {:.1}% (all sessions)", all_selectivities.iter().sum::<f64>() / all_selectivities.len() as f64);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("\n✅ All {} sessions verified with perfect selectivity (100%)", num_sessions);
    println!("✅ Row group per session design validated: each session query scans only its own row group");
}

#[tokio::test]
async fn test_iceberg_writer_bulk_log_roundtrip() {
    use iceberg_catalog_rest::RestCatalogBuilder;
    use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
    use iceberg::{Catalog, CatalogBuilder, TableIdent};
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;
    use arrow::array::{Array, StringArray};
    use futures::StreamExt;

    let config = load_test_config();
    let writer = softprobe_otlp_backend::storage::iceberg::IcebergWriter::new(&config).await
        .expect("writer init");

    // Create multiple sessions with logs to test multi-session row groups
    let num_sessions = 5;
    let logs_per_session = 1000;
    let now = Utc::now();

    let mut all_session_batches = Vec::new();
    let mut session_ids = Vec::new();

    for session_idx in 0..num_sessions {
        let session_id = format!("log-session-{}", uuid::Uuid::new_v4());
        session_ids.push(session_id.clone());

        let mut session_logs = Vec::new();
        for i in 0..logs_per_session {
            let mut attributes = HashMap::new();
            attributes.insert("log.index".to_string(), i.to_string());
            attributes.insert("source".to_string(), "test".to_string());

            let mut resource_attributes = HashMap::new();
            resource_attributes.insert("service.name".to_string(), format!("test-service-{}", session_idx));
            resource_attributes.insert("host.name".to_string(), "localhost".to_string());

            // Vary severity across logs
            let severity_number = (i % 5 + 1) * 4; // INFO=4, WARN=8, ERROR=12, etc.
            let severity_text = match severity_number {
                4 => "INFO",
                8 => "WARN",
                12 => "ERROR",
                16 => "FATAL",
                _ => "DEBUG",
            }.to_string();

            // Add trace correlation for every 5th log
            let (trace_id, span_id) = if i % 5 == 0 {
                (
                    Some(format!("trace-{}-{}", session_idx, i)),
                    Some(format!("span-{}-{}", session_idx, i)),
                )
            } else {
                (None, None)
            };

            session_logs.push(LogData {
                session_id: Some(session_id.clone()),
                timestamp: now + chrono::Duration::milliseconds((session_idx * 1000 + i) as i64),
                observed_timestamp: Some(now + chrono::Duration::milliseconds((session_idx * 1000 + i + 1) as i64)),
                severity_number: severity_number as i32,
                severity_text,
                body: format!("Test log message {} from session {}", i, session_idx),
                attributes,
                resource_attributes,
                trace_id,
                span_id,
            });
        }
        all_session_batches.push(session_logs);
    }

    let total_logs = num_sessions * logs_per_session;

    // Act: write all sessions in one batch (creates 1 file with multiple row groups)
    println!("🧪 Writing {} sessions ({} total logs) to Iceberg logs table...", num_sessions, total_logs);
    let write_timer = Timer::start(&format!("Multi-Session Log Write ({} sessions)", num_sessions));
    writer.write_log_batches(all_session_batches).await.expect("multi-session log write should succeed");
    let write_duration = write_timer.stop();

    // Report write performance
    let write_metrics = PerformanceMetrics::new(&format!("Multi-Session Log Write ({} sessions, {} logs)", num_sessions, total_logs))
        .with_duration(write_duration)
        .with_rows(total_logs);
    write_metrics.print_report();
    // Target: should be faster than writing sessions individually
    write_metrics.assert_performance_target(5000, "Multi-session log write time");

    println!("✅ Write completed, querying back each session to verify row group isolation...");

    // Assert: query back via REST catalog scan
    let mut props = std::collections::HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), config.iceberg.catalog_uri.clone());
    let warehouse = std::env::var("ICEBERG_WAREHOUSE")
        .unwrap_or_else(|_| config.iceberg.warehouse.clone());
    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse);

    // Add bearer token if present
    if let Some(ref token) = config.iceberg.catalog_token {
        props.insert("token".to_string(), token.clone());
    }

    if let Some(ep) = std::env::var("S3_ENDPOINT").ok().or(config.s3.endpoint.clone()) {
        props.insert("s3.endpoint".to_string(), ep);
    }
    if let Some(ak) = std::env::var("S3_ACCESS_KEY").ok().or(config.s3.access_key_id.clone()) {
        props.insert("s3.access-key-id".to_string(), ak);
    }
    if let Some(sk) = std::env::var("S3_SECRET_KEY").ok().or(config.s3.secret_access_key.clone()) {
        props.insert("s3.secret-access-key".to_string(), sk);
    }
    props.insert("s3.region".to_string(), config.storage.s3_region.clone());

    // For testing/development with TLS interception, use custom client
    let catalog = if std::env::var("ICEBERG_DISABLE_TLS_VALIDATION").is_ok() {
        let http_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("build http client");
        RestCatalogBuilder::default()
            .with_client(http_client)
            .load("rest", props).await
            .expect("load catalog")
    } else {
        RestCatalogBuilder::default().load("rest", props).await
            .expect("load catalog")
    };

    // Use the logs table (refactored architecture has separate tables for spans and logs)
    let table_ident = TableIdent::from_strs(["default", "logs"])
        .expect("table ident");

    // Performance tracking: measure table load time
    let load_timer = Timer::start("Table Load (logs)");
    let table = catalog.load_table(&table_ident).await.expect("load table");
    let load_duration = load_timer.stop();

    // Report table metadata loading performance
    let load_metrics = PerformanceMetrics::new("Table Load (logs)")
        .with_duration(load_duration);
    load_metrics.print_report();
    load_metrics.assert_performance_target(1000, "Table load time");

    // Query each session individually to verify row group isolation
    let mut total_query_duration = std::time::Duration::ZERO;
    let mut all_selectivities = Vec::new();

    for (session_idx, session_id) in session_ids.iter().enumerate() {
        println!("\n🔍 Querying log session {}/{}: {}", session_idx + 1, num_sessions, session_id);

        let predicate = Reference::new("session_id").equal_to(Datum::string(session_id));
        let scan = table.scan()
            .with_filter(predicate)
            .build()
            .expect("scan build");

        let query_timer = Timer::start(&format!("Query log session {}", session_idx + 1));
        let mut arrow_stream = scan.to_arrow().await.expect("arrow stream");

        let mut found = 0usize;
        let mut rows_scanned = 0usize;

        while let Some(batch_result) = arrow_stream.next().await {
            let batch = batch_result.expect("batch ok");
            rows_scanned += batch.num_rows();

            if let Some((idx, _)) = batch.schema().fields().iter().enumerate().find(|(_, f)| f.name() == "session_id") {
                let col = batch.column(idx);
                let arr = col.as_any().downcast_ref::<StringArray>().expect("string arr");
                for i in 0..arr.len() {
                    if arr.value(i) == session_id.as_str() { found += 1; }
                }
            }
        }

        let query_duration = query_timer.stop();
        total_query_duration += query_duration;

        let selectivity = (found as f64 / rows_scanned.max(1) as f64) * 100.0;
        all_selectivities.push(selectivity);

        println!("  ✓ Found {} rows, scanned {} rows (selectivity: {:.1}%)", found, rows_scanned, selectivity);

        assert_eq!(found, logs_per_session,
                   "Expected exactly {} logs for session {}, found {}",
                   logs_per_session, session_id, found);

        // Verify perfect selectivity (100% = only scanned the target row group)
        assert_eq!(found, rows_scanned,
                   "Expected perfect selectivity (100%), but scanned {} rows and matched {} rows",
                   rows_scanned, found);
    }

    println!("\n📊 Multi-Session Log Query Performance Summary:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("⏱️  Total query time ({} sessions): {:?}", num_sessions, total_query_duration);
    println!("⏱️  Average per session: {:?}", total_query_duration / num_sessions as u32);
    println!("📊 Selectivity: {:.1}% (all sessions)", all_selectivities.iter().sum::<f64>() / all_selectivities.len() as f64);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("\n✅ All {} log sessions verified with perfect selectivity (100%)", num_sessions);
    println!("✅ Row group per session design validated for logs: each session query scans only its own row group");
}

#[tokio::test]
async fn test_iceberg_writer_bulk_metric_roundtrip() {
    use iceberg_catalog_rest::RestCatalogBuilder;
    use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
    use iceberg::{Catalog, CatalogBuilder, TableIdent};
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;
    use arrow::array::{Array, StringArray, Float64Array};
    use futures::StreamExt;
    use softprobe_otlp_backend::models::Metric;

    let config = load_test_config();
    let writer = softprobe_otlp_backend::storage::iceberg::IcebergWriter::new(&config).await
        .expect("writer init");

    // Create multiple metric names with data points to test metric_name-based row groups
    let num_metric_names = 5;
    let data_points_per_metric = 1000;
    let now = Utc::now();

    let mut all_metric_batches = Vec::new();
    let mut metric_names = Vec::new();

    for metric_idx in 0..num_metric_names {
        // Use UUID to ensure unique metric names across test runs
        let metric_name = format!("test.metric.{}.{}", metric_idx, uuid::Uuid::new_v4());
        metric_names.push(metric_name.clone());

        let mut metric_data_points = Vec::new();
        for i in 0..data_points_per_metric {
            let mut attributes = HashMap::new();
            attributes.insert("data_point.index".to_string(), i.to_string());
            attributes.insert("host".to_string(), format!("host-{}", i % 3)); // 3 different hosts
            attributes.insert("region".to_string(), format!("region-{}", i % 2)); // 2 different regions

            let mut resource_attributes = HashMap::new();
            resource_attributes.insert("service.name".to_string(), format!("service-{}", metric_idx));
            resource_attributes.insert("service.version".to_string(), "1.0.0".to_string());

            // Vary metric values
            let value = 100.0 + (i as f64 * 0.5) + (metric_idx as f64 * 10.0);

            metric_data_points.push(Metric {
                metric_name: metric_name.clone(),
                description: format!("Test metric {} description", metric_idx),
                unit: if metric_idx % 2 == 0 { "ms" } else { "bytes" }.to_string(),
                metric_type: if metric_idx % 3 == 0 { "gauge" } else if metric_idx % 3 == 1 { "sum" } else { "histogram" }.to_string(),
                timestamp: now + chrono::Duration::milliseconds((metric_idx * 1000 + i) as i64),
                value,
                attributes,
                resource_attributes,
            });
        }
        all_metric_batches.push(metric_data_points);
    }

    let total_metrics = num_metric_names * data_points_per_metric;

    // Act: write all metric batches in one write (creates 1 file with multiple row groups)
    println!("🧪 Writing {} metric names ({} total data points) to Iceberg metrics table...", num_metric_names, total_metrics);
    let write_timer = Timer::start(&format!("Multi-Metric Write ({} metric names)", num_metric_names));
    writer.write_metric_batches(all_metric_batches).await.expect("multi-metric write should succeed");
    let write_duration = write_timer.stop();

    // Report write performance
    let write_metrics = PerformanceMetrics::new(&format!("Multi-Metric Write ({} metric names, {} data points)", num_metric_names, total_metrics))
        .with_duration(write_duration)
        .with_rows(total_metrics);
    write_metrics.print_report();
    // Target: should be faster than writing metrics individually
    write_metrics.assert_performance_target(5000, "Multi-metric write time");

    println!("✅ Write completed, querying back each metric name to verify row group isolation...");

    // Assert: query back via REST catalog scan
    let mut props = std::collections::HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), config.iceberg.catalog_uri.clone());
    let warehouse = std::env::var("ICEBERG_WAREHOUSE")
        .unwrap_or_else(|_| config.iceberg.warehouse.clone());
    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse);

    // Add bearer token if present
    if let Some(ref token) = config.iceberg.catalog_token {
        props.insert("token".to_string(), token.clone());
    }

    if let Some(ep) = std::env::var("S3_ENDPOINT").ok().or(config.s3.endpoint.clone()) {
        props.insert("s3.endpoint".to_string(), ep);
    }
    if let Some(ak) = std::env::var("S3_ACCESS_KEY").ok().or(config.s3.access_key_id.clone()) {
        props.insert("s3.access-key-id".to_string(), ak);
    }
    if let Some(sk) = std::env::var("S3_SECRET_KEY").ok().or(config.s3.secret_access_key.clone()) {
        props.insert("s3.secret-access-key".to_string(), sk);
    }
    props.insert("s3.region".to_string(), config.storage.s3_region.clone());

    // For testing/development with TLS interception, use custom client
    let catalog = if std::env::var("ICEBERG_DISABLE_TLS_VALIDATION").is_ok() {
        let http_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("build http client");
        RestCatalogBuilder::default()
            .with_client(http_client)
            .load("rest", props).await
            .expect("load catalog")
    } else {
        RestCatalogBuilder::default().load("rest", props).await
            .expect("load catalog")
    };

    // Use the metrics table
    let table_ident = TableIdent::from_strs(["default", "metrics"])
        .expect("table ident");

    // Performance tracking: measure table load time
    let load_timer = Timer::start("Table Load (metrics)");
    let table = catalog.load_table(&table_ident).await.expect("load table");
    let load_duration = load_timer.stop();

    // Report table metadata loading performance
    let load_metrics = PerformanceMetrics::new("Table Load (metrics)")
        .with_duration(load_duration);
    load_metrics.print_report();
    load_metrics.assert_performance_target(1000, "Table load time");

    // Query each metric name individually to verify row group isolation
    let mut total_query_duration = std::time::Duration::ZERO;
    let mut all_selectivities = Vec::new();

    for (metric_idx, metric_name) in metric_names.iter().enumerate() {
        println!("\n🔍 Querying metric {}/{}: {}", metric_idx + 1, num_metric_names, metric_name);

        let predicate = Reference::new("metric_name").equal_to(Datum::string(metric_name));
        let scan = table.scan()
            .with_filter(predicate)
            .build()
            .expect("scan build");

        let query_timer = Timer::start(&format!("Query metric {}", metric_idx + 1));
        let mut arrow_stream = scan.to_arrow().await.expect("arrow stream");

        let mut found = 0usize;
        let mut rows_scanned = 0usize;
        let mut values_sum = 0.0;

        while let Some(batch_result) = arrow_stream.next().await {
            let batch = batch_result.expect("batch ok");
            rows_scanned += batch.num_rows();

            // Verify metric_name matches
            if let Some((name_idx, _)) = batch.schema().fields().iter().enumerate().find(|(_, f)| f.name() == "metric_name") {
                let col = batch.column(name_idx);
                let arr = col.as_any().downcast_ref::<StringArray>().expect("string arr");
                
                // Also get values to verify correctness
                if let Some((val_idx, _)) = batch.schema().fields().iter().enumerate().find(|(_, f)| f.name() == "value") {
                    let val_col = batch.column(val_idx);
                    let val_arr = val_col.as_any().downcast_ref::<Float64Array>().expect("float64 arr");
                    
                    for i in 0..arr.len() {
                        if arr.value(i) == metric_name.as_str() {
                            found += 1;
                            values_sum += val_arr.value(i);
                        }
                    }
                }
            }
        }

        let query_duration = query_timer.stop();
        total_query_duration += query_duration;

        let selectivity = (found as f64 / rows_scanned.max(1) as f64) * 100.0;
        all_selectivities.push(selectivity);

        println!("  ✓ Found {} data points, scanned {} rows (selectivity: {:.1}%)", found, rows_scanned, selectivity);
        println!("  ✓ Sum of values: {:.2}", values_sum);

        assert_eq!(found, data_points_per_metric,
                   "Expected exactly {} data points for metric {}, found {}",
                   data_points_per_metric, metric_name, found);

        // Verify perfect selectivity (100% = only scanned the target row group)
        assert_eq!(found, rows_scanned,
                   "Expected perfect selectivity (100%), but scanned {} rows and matched {} rows",
                   rows_scanned, found);
    }

    println!("\n📊 Multi-Metric Query Performance Summary:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("⏱️  Total query time ({} metric names): {:?}", num_metric_names, total_query_duration);
    println!("⏱️  Average per metric: {:?}", total_query_duration / num_metric_names as u32);
    println!("📊 Selectivity: {:.1}% (all metrics)", all_selectivities.iter().sum::<f64>() / all_selectivities.len() as f64);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("\n✅ All {} metric names verified with perfect selectivity (100%)", num_metric_names);
    println!("✅ Row group per metric_name design validated: each metric query scans only its own row group");
}

#[tokio::test]
async fn test_http_fields_in_span_model() {
    use softprobe_otlp_backend::models::Span as SpanData;
    use chrono::Utc;
    use std::collections::HashMap;

    println!("🧪 Testing HTTP fields in Span model...");

    // Create a span with all HTTP fields populated
    let session_id = "test-session-123";
    let span = SpanData {
        session_id: session_id.to_string(), // Explicit session_id field
        trace_id: "trace-abc".to_string(),
        span_id: "span-xyz".to_string(),
        parent_span_id: None,
        app_id: "test-app".to_string(),
        organization_id: Some("org-test".to_string()),
        tenant_id: Some("tenant-test".to_string()),
        message_type: "HTTP_REQUEST".to_string(),
        span_kind: Some("SERVER".to_string()),
        timestamp: Utc::now(),
        end_timestamp: Some(Utc::now()),
        attributes: HashMap::from([
            ("sp.session.id".to_string(), session_id.to_string()),
        ]),
        events: Vec::new(),
        http_request_method: Some("POST".to_string()),
        http_request_path: Some("/api/v1/test".to_string()),
        http_request_headers: Some(r#"{"Content-Type":"application/json"}"#.to_string()),
        http_request_body: Some(r#"{"test":"data"}"#.to_string()),
        http_response_status_code: Some(200),
        http_response_headers: Some(r#"{"X-Request-Id":"req-123"}"#.to_string()),
        http_response_body: Some(r#"{"success":true}"#.to_string()),
        status_code: Some("OK".to_string()),
        status_message: Some("Success".to_string()),
    };

    // Verify all HTTP fields are set correctly
    assert_eq!(span.http_request_method, Some("POST".to_string()));
    assert_eq!(span.http_request_path, Some("/api/v1/test".to_string()));
    assert!(span.http_request_headers.as_ref().unwrap().contains("Content-Type"));
    assert!(span.http_request_body.as_ref().unwrap().contains("test"));
    assert_eq!(span.http_response_status_code, Some(200));
    assert!(span.http_response_headers.as_ref().unwrap().contains("X-Request-Id"));
    assert!(span.http_response_body.as_ref().unwrap().contains("success"));

    println!("✅ HTTP request method: {:?}", span.http_request_method);
    println!("✅ HTTP request path: {:?}", span.http_request_path);
    println!("✅ HTTP request headers: {:?}", span.http_request_headers);
    println!("✅ HTTP request body: {:?}", span.http_request_body);
    println!("✅ HTTP response status: {:?}", span.http_response_status_code);
    println!("✅ HTTP response headers: {:?}", span.http_response_headers);
    println!("✅ HTTP response body: {:?}", span.http_response_body);

    // Verify the span can be converted to Arrow RecordBatch
    let config = load_test_config();
    let writer = softprobe_otlp_backend::storage::iceberg::IcebergWriter::new(&config).await
        .expect("writer init");

    // Write the span and verify it succeeds
    let result = writer.write_span_batches(vec![vec![span]]).await;
    assert!(result.is_ok(), "Failed to write span with HTTP fields: {:?}", result.err());

    println!("✅ Successfully wrote span with HTTP fields to Iceberg");
    println!("✅ HTTP fields are correctly included in the schema and can be persisted");
}
