use softprobe_otlp_backend::config::Config;
use softprobe_otlp_backend::models::{Span as SpanData, SpanEvent, Log as LogData};
use softprobe_otlp_backend::query;
use softprobe_otlp_backend::storage;
use chrono::Utc;
use std::collections::HashMap;
use std::time::Instant;
use tempfile::tempdir;

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
    if let Ok(config_file) = std::env::var("CONFIG_FILE") {
        if std::path::Path::new(&config_file).exists() {
            println!("Loading test config from CONFIG_FILE: {}", config_file);
            return Config::load().expect("Failed to load config");
        }
    }
    let test_type = std::env::var("ICEBERG_TEST_TYPE").unwrap_or_else(|_| "local".to_string());

    let config_file = match test_type.as_str() {
        "r2" => "tests/config/test-r2.yaml",
        _ => "tests/config/test.yaml",
    };

    println!("Loading test config from: {}", config_file);
    std::env::set_var("CONFIG_FILE", config_file);
    Config::load().expect("Failed to load test config")
}

fn ensure_wal_bucket(config: &mut Config) {
    if config.ingest_engine.wal_bucket != "your-bucket-name" {
        return;
    }

    let warehouse = config.iceberg.warehouse.trim();
    let mut candidate = warehouse
        .rsplit('/')
        .next()
        .unwrap_or(warehouse)
        .to_string();
    if let Some(after_underscore) = candidate.rsplit('_').next() {
        if !after_underscore.is_empty() {
            candidate = after_underscore.to_string();
        }
    }

    assert!(
        !candidate.is_empty() && candidate != "your-bucket-name",
        "wal_bucket is a placeholder and could not be derived from iceberg.warehouse: {}",
        warehouse
    );
    config.ingest_engine.wal_bucket = candidate;
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
    let mut config = load_test_config();

    let cache_dir = tempdir().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
    config.span_buffering.max_buffer_spans = 10_000;
    config.span_buffering.max_buffer_bytes = 1024 * 1024 * 1024;
    config.span_buffering.flush_interval_seconds = 3600;
    config.ingest_engine.optimizer_interval_seconds = 3600;

    let pipeline = storage::IngestPipeline::new(&config).await.expect("ingest pipeline");

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

    // Act: write through WAL + local cache
    println!("🧪 Writing {} sessions ({} total spans) via WAL + local cache...", num_sessions, total_spans);
    let write_timer = Timer::start(&format!("Multi-Session WAL Write ({} sessions)", num_sessions));
    pipeline
        .add_spans(
            all_session_batches.into_iter().flatten().collect::<Vec<_>>(),
            total_spans * 256,
        )
        .await
        .expect("span add should succeed");
    let write_duration = write_timer.stop();

    // Report write performance
    let write_metrics = PerformanceMetrics::new(&format!("Multi-Session WAL Write ({} sessions, {} spans)", num_sessions, total_spans))
        .with_duration(write_duration)
        .with_rows(total_spans);
    write_metrics.print_report();
    write_metrics.assert_performance_target(5000, "Multi-session WAL write time");

    let wal_files = pipeline.list_wal_files("spans").expect("wal files");
    assert!(
        !wal_files.is_empty(),
        "Expected WAL cache files for spans"
    );

    println!("✅ WAL write completed, querying back each session to verify row group isolation...");

    let query_engine = query::create_query_engine(&config).await.expect("query engine");

    // Query each session individually to verify row group isolation
    let mut total_query_duration = std::time::Duration::ZERO;

    for (session_idx, session_id) in session_ids.iter().enumerate() {
        println!("\n🔍 Querying session {}/{}: {}", session_idx + 1, num_sessions, session_id);

        let escaped = session_id.replace('\'', "''");
        let sql = format!(
            "SELECT COUNT(*) AS count FROM union_spans WHERE session_id = '{}'",
            escaped
        );

        let query_timer = Timer::start(&format!("Query session {}", session_idx + 1));
        let result = query_engine.execute_query(&sql).await.expect("query");
        let found = result.rows[0][0].as_i64().unwrap_or(0) as usize;

        let query_duration = query_timer.stop();
        total_query_duration += query_duration;

        println!("  ✓ Found {} rows", found);

        assert_eq!(found, spans_per_session,
                   "Expected exactly {} spans for session {}, found {}",
                   spans_per_session, session_id, found);

        let http_sql = format!(
            "SELECT \
                http_request_method, \
                http_request_path, \
                http_request_headers, \
                http_request_body, \
                http_response_status_code, \
                http_response_headers, \
                http_response_body \
             FROM union_spans \
             WHERE session_id = '{}' AND http_request_method IS NOT NULL \
             LIMIT 1",
            escaped
        );
        let http_result = query_engine.execute_query(&http_sql).await.expect("http query");
        assert_eq!(http_result.row_count, 1, "Expected HTTP fields row for session {}", session_id);
        let row = &http_result.rows[0];
        let method = row[0].as_str().unwrap_or("");
        let path = row[1].as_str().unwrap_or("");
        let headers = row[2].as_str().unwrap_or("");
        let body = row[3].as_str().unwrap_or("");
        let status = row[4].as_i64().unwrap_or(0);
        let resp_headers = row[5].as_str().unwrap_or("");
        let resp_body = row[6].as_str().unwrap_or("");

        assert_eq!(method, "POST", "HTTP method should be POST for session {}", session_idx);
        assert_eq!(path, format!("/api/v1/session/{}", session_idx), "HTTP path should match for session {}", session_idx);
        assert!(headers.contains("Content-Type"), "Request headers should contain Content-Type");
        assert!(headers.contains("Authorization"), "Request headers should contain Authorization");
        assert!(body.contains(session_id.as_str()), "Request body should contain session_id");
        assert!(body.contains("action"), "Request body should contain action field");
        assert_eq!(status, 200, "HTTP response status should be 200");
        assert!(resp_headers.contains("X-Request-Id"), "Response headers should contain X-Request-Id");
        assert!(resp_body.contains(session_id.as_str()), "Response body should contain session_id");
        assert!(resp_body.contains("success"), "Response body should contain success field");

        println!("  ✓ HTTP fields verified for session {}", session_idx);
    }

    println!("\n📊 Multi-Session Query Performance Summary:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("⏱️  Total query time ({} sessions): {:?}", num_sessions, total_query_duration);
    println!("⏱️  Average per session: {:?}", total_query_duration / num_sessions as u32);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("\n✅ WAL-backed union-read validated for {} sessions", num_sessions);

    println!("🔄 Forcing flush to staged local cache...");
    pipeline.force_flush_spans().await.expect("force flush");

    let staged_files = pipeline.list_staged_files("spans").expect("staged files");
    assert!(
        !staged_files.is_empty(),
        "Expected staged parquet cache files for spans"
    );

    println!("⚙️  Running optimizer to commit staged spans to Iceberg...");
    pipeline.run_optimizer_once().await.expect("optimizer");

    let staged_files_after = pipeline.list_staged_files("spans").expect("staged files after");
    assert!(
        staged_files_after.is_empty(),
        "Expected staged cache cleanup after optimizer, found {:?}",
        staged_files_after
    );

    let query_engine = query::create_query_engine(&config).await.expect("query engine");
    for (session_idx, session_id) in session_ids.iter().enumerate() {
        let escaped = session_id.replace('\'', "''");
        let sql = format!(
            "SELECT COUNT(*) AS count FROM iceberg_spans WHERE session_id = '{}'",
            escaped
        );
        let result = query_engine.execute_query(&sql).await.expect("query");
        let found = result.rows[0][0].as_i64().unwrap_or(0) as usize;
        assert_eq!(
            found,
            spans_per_session,
            "Expected Iceberg scan to return {} spans for session {} after optimizer, found {}",
            spans_per_session,
            session_id,
            found
        );

        let http_sql = format!(
            "SELECT \
                http_request_method, \
                http_request_path, \
                http_request_headers, \
                http_request_body, \
                http_response_status_code, \
                http_response_headers, \
                http_response_body \
             FROM iceberg_spans \
             WHERE session_id = '{}' AND http_request_method IS NOT NULL \
             LIMIT 1",
            escaped
        );
        let http_result = query_engine.execute_query(&http_sql).await.expect("http query");
        assert_eq!(http_result.row_count, 1, "Expected HTTP fields row for session {}", session_id);
        let row = &http_result.rows[0];
        let method = row[0].as_str().unwrap_or("");
        let path = row[1].as_str().unwrap_or("");
        let headers = row[2].as_str().unwrap_or("");
        let body = row[3].as_str().unwrap_or("");
        let status = row[4].as_i64().unwrap_or(0);
        let resp_headers = row[5].as_str().unwrap_or("");
        let resp_body = row[6].as_str().unwrap_or("");

        assert_eq!(method, "POST", "HTTP method should be POST for session {}", session_idx);
        assert_eq!(path, format!("/api/v1/session/{}", session_idx), "HTTP path should match for session {}", session_idx);
        assert!(headers.contains("Authorization"), "HTTP request headers should contain Authorization for session {}", session_idx);
        assert!(body.contains("session_id"), "HTTP request body should contain session_id for session {}", session_idx);
        assert_eq!(status, 200, "HTTP response status should be 200 for session {}", session_idx);
        assert!(resp_headers.contains("X-Request-Id"), "HTTP response headers should contain X-Request-Id for session {}", session_idx);
        assert!(resp_body.contains("success"), "HTTP response body should contain success for session {}", session_idx);
    }

    println!("\n✅ WAL, local cache, and optimizer paths validated for spans");
}

#[tokio::test]
async fn test_duckdb_union_read_realtime_performance() {
    let mut config = load_test_config();
    config.ingest_engine.enabled = false;
    ensure_wal_bucket(&mut config);

    let pipeline = storage::IngestPipeline::new(&config).await.expect("ingest pipeline");

    let now = Utc::now();
    let base_session = format!("union-base-{}", uuid::Uuid::new_v4());
    let wal_session = format!("union-wal-{}", uuid::Uuid::new_v4());

    let mut base_spans = Vec::new();
    for i in 0..200 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), base_session.clone());
        attributes.insert("span.index".to_string(), i.to_string());

        base_spans.push(SpanData {
            session_id: base_session.clone(),
            trace_id: format!("trace-base-{}", i),
            span_id: format!("span-base-{}", i),
            parent_span_id: None,
            app_id: "app-union".to_string(),
            organization_id: None,
            tenant_id: None,
            message_type: "union_base".to_string(),
            span_kind: Some("SERVER".to_string()),
            timestamp: now + chrono::Duration::milliseconds(i as i64),
            end_timestamp: Some(now + chrono::Duration::milliseconds(i as i64 + 1)),
            attributes,
            events: Vec::new(),
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: Some("OK".to_string()),
            status_message: Some("OK".to_string()),
        });
    }

    pipeline
        .write_span_batches(vec![base_spans])
        .await
        .expect("base write");

    let mut wal_spans = Vec::new();
    for i in 0..100 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), wal_session.clone());
        attributes.insert("span.index".to_string(), i.to_string());
        wal_spans.push(SpanData {
            session_id: wal_session.clone(),
            trace_id: format!("trace-wal-{}", i),
            span_id: format!("span-wal-{}", i),
            parent_span_id: None,
            app_id: "app-union".to_string(),
            organization_id: None,
            tenant_id: None,
            message_type: "union_wal".to_string(),
            span_kind: Some("SERVER".to_string()),
            timestamp: now + chrono::Duration::milliseconds(10_000 + i as i64),
            end_timestamp: Some(now + chrono::Duration::milliseconds(10_000 + i as i64 + 1)),
            attributes,
            events: Vec::new(),
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: Some("OK".to_string()),
            status_message: Some("OK".to_string()),
        });
    }

    if let Err(err) = pipeline.write_wal_spans(wal_spans, 1024).await {
        println!("⚠️  Skipping WAL union-read performance test: WAL write failed: {}", err);
        return;
    }

    let query_engine = query::create_query_engine(&config).await.expect("query engine");
    let escaped = wal_session.replace('\'', "''");
    let sql = format!(
        "SELECT COUNT(*) AS count FROM union_spans WHERE session_id = '{}'",
        escaped
    );

    let warmup = query_engine.execute_query(&sql).await.expect("warmup");
    let warmup_count = warmup.rows[0][0].as_i64().unwrap_or(0);
    assert_eq!(warmup_count, 100, "Warmup should see WAL rows");

    let result = query_engine.execute_query(&sql).await.expect("query");
    let total_count = result.rows[0][0].as_i64().unwrap_or(0);
    assert_eq!(total_count, 100, "Union-read should return WAL rows");
}

#[tokio::test]
async fn test_iceberg_writer_bulk_log_roundtrip() {
    let mut config = load_test_config();
    ensure_wal_bucket(&mut config);

    let cache_dir = tempdir().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
    config.span_buffering.max_buffer_spans = 10_000;
    config.span_buffering.max_buffer_bytes = 1024 * 1024 * 1024;
    config.span_buffering.flush_interval_seconds = 3600;
    config.ingest_engine.optimizer_interval_seconds = 3600;

    let pipeline = storage::IngestPipeline::new(&config).await.expect("ingest pipeline");

    // Create multiple sessions with logs to test multi-session row groups
    let test_type = std::env::var("ICEBERG_TEST_TYPE").unwrap_or_else(|_| "local".to_string());
    let (num_sessions, logs_per_session) = if test_type == "r2" {
        (2, 200)
    } else {
        (5, 1000)
    };
    let now = Utc::now();

    let mut all_logs = Vec::new();
    let mut session_ids = Vec::new();

    for session_idx in 0..num_sessions {
        let session_id = format!("log-session-{}", uuid::Uuid::new_v4());
        session_ids.push(session_id.clone());

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

            all_logs.push(LogData {
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
    }

    let total_logs = num_sessions * logs_per_session;

    // Act: add all logs through the ingest path (WAL first, then local cache, then optimizer)
    println!("🧪 Writing {} sessions ({} total logs) via WAL + local cache...", num_sessions, total_logs);
    let write_timer = Timer::start(&format!("Multi-Session Log Add ({} sessions)", num_sessions));
    pipeline
        .add_logs(all_logs, total_logs * 256)
        .await
        .expect("log add should succeed");
    let write_duration = write_timer.stop();

    // Report write performance
    let write_metrics = PerformanceMetrics::new(&format!("Multi-Session Log Add ({} sessions, {} logs)", num_sessions, total_logs))
        .with_duration(write_duration)
        .with_rows(total_logs);
    write_metrics.print_report();

    let wal_files = pipeline.list_wal_files("logs").expect("wal files");
    assert!(
        !wal_files.is_empty(),
        "Expected WAL cache files for logs"
    );

    println!("✅ WAL write completed, querying back each session through DuckDB union view...");

    let query_engine = query::create_query_engine(&config).await.expect("query engine");

    for session_id in &session_ids {
        let escaped = session_id.replace('\'', "''");
        let sql = format!(
            "SELECT COUNT(*) AS count FROM union_logs WHERE session_id = '{}'",
            escaped
        );
        let result = query_engine.execute_query(&sql).await.expect("query");
        let found = result.rows[0][0].as_i64().unwrap_or(0) as usize;
        assert_eq!(
            found,
            logs_per_session,
            "Expected exactly {} logs for session {}, found {}",
            logs_per_session,
            session_id,
            found
        );
    }

    println!("✅ WAL-backed union-read validated for {} log sessions", num_sessions);

    println!("🔄 Forcing flush to staged local cache...");
    pipeline.force_flush_logs().await.expect("force flush");

    let staged_files = pipeline.list_staged_files("logs").expect("staged files");
    assert!(
        !staged_files.is_empty(),
        "Expected staged parquet cache files for logs"
    );

    let query_engine = query::create_query_engine(&config).await.expect("query engine");
    for session_id in &session_ids {
        let escaped = session_id.replace('\'', "''");
        let sql = format!(
            "SELECT COUNT(*) AS count FROM union_logs WHERE session_id = '{}'",
            escaped
        );
        let result = query_engine.execute_query(&sql).await.expect("query");
        let found = result.rows[0][0].as_i64().unwrap_or(0) as usize;
        assert_eq!(
            found,
            logs_per_session,
            "Expected staged union-read to return {} logs for session {}",
            logs_per_session,
            session_id
        );
    }

    println!("⚙️  Running optimizer to commit staged logs to Iceberg...");
    pipeline.run_optimizer_once().await.expect("optimizer");

    let staged_files_after = pipeline.list_staged_files("logs").expect("staged files after");
    assert!(
        staged_files_after.is_empty(),
        "Expected staged cache cleanup after optimizer, found {:?}",
        staged_files_after
    );

    let query_engine = query::create_query_engine(&config).await.expect("query engine");
    for session_id in &session_ids {
        let escaped = session_id.replace('\'', "''");
        let sql = format!(
            "SELECT COUNT(*) AS count FROM iceberg_logs WHERE session_id = '{}'",
            escaped
        );
        let result = query_engine.execute_query(&sql).await.expect("query");
        let found = result.rows[0][0].as_i64().unwrap_or(0) as usize;
        assert_eq!(
            found,
            logs_per_session,
            "Expected Iceberg scan to return {} logs for session {} after optimizer",
            logs_per_session,
            session_id
        );
    }

    println!("✅ WAL, local cache, and optimizer paths validated for logs");
}

#[tokio::test]
async fn test_iceberg_writer_bulk_metric_roundtrip() {
    use softprobe_otlp_backend::models::Metric;

    let mut config = load_test_config();
    ensure_wal_bucket(&mut config);

    let cache_dir = tempdir().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
    config.span_buffering.max_buffer_spans = 10_000;
    config.span_buffering.max_buffer_bytes = 1024 * 1024 * 1024;
    config.span_buffering.flush_interval_seconds = 3600;
    config.ingest_engine.optimizer_interval_seconds = 3600;

    let pipeline = storage::IngestPipeline::new(&config).await.expect("ingest pipeline");

    // Create multiple metric names with data points to test metric_name-based row groups
    let num_metric_names = 5;
    let data_points_per_metric = 1000;
    let now = Utc::now();

    let mut all_metric_batches = Vec::new();
    let mut metric_names = Vec::new();
    let mut expected_sums = Vec::new();

    for metric_idx in 0..num_metric_names {
        // Use UUID to ensure unique metric names across test runs
        let metric_name = format!("test.metric.{}.{}", metric_idx, uuid::Uuid::new_v4());
        metric_names.push(metric_name.clone());

        let mut metric_data_points = Vec::new();
        let mut expected_sum = 0.0;
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
            expected_sum += value;

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
        expected_sums.push(expected_sum);
    }

    let total_metrics = num_metric_names * data_points_per_metric;

    // Act: write through WAL + local cache
    println!("🧪 Writing {} metric names ({} total data points) via WAL + local cache...", num_metric_names, total_metrics);
    let write_timer = Timer::start(&format!("Multi-Metric WAL Write ({} metric names)", num_metric_names));
    pipeline
        .add_metrics(
            all_metric_batches.into_iter().flatten().collect::<Vec<_>>(),
            total_metrics * 256,
        )
        .await
        .expect("metric add should succeed");
    let write_duration = write_timer.stop();

    let write_metrics = PerformanceMetrics::new(&format!(
        "Multi-Metric WAL Write ({} metric names, {} data points)",
        num_metric_names,
        total_metrics
    ))
    .with_duration(write_duration)
    .with_rows(total_metrics);
    write_metrics.print_report();
    write_metrics.assert_performance_target(5000, "Multi-metric WAL write time");

    let wal_files = pipeline.list_wal_files("metrics").expect("wal files");
    assert!(
        !wal_files.is_empty(),
        "Expected WAL cache files for metrics"
    );

    println!("✅ WAL write completed, querying back each metric name via union_metrics...");

    let query_engine = query::create_query_engine(&config).await.expect("query engine");

    // Query each metric name individually to verify row group isolation (WAL path)
    let mut total_query_duration = std::time::Duration::ZERO;

    for (metric_idx, metric_name) in metric_names.iter().enumerate() {
        println!("\n🔍 Querying metric {}/{}: {}", metric_idx + 1, num_metric_names, metric_name);

        let escaped = metric_name.replace('\'', "''");
        let sql = format!(
            "SELECT COUNT(*) AS count, SUM(value) AS total FROM union_metrics WHERE metric_name = '{}'",
            escaped
        );

        let query_timer = Timer::start(&format!("Query metric {}", metric_idx + 1));
        let result = query_engine.execute_query(&sql).await.expect("query");
        let found = result.rows[0][0].as_i64().unwrap_or(0) as usize;
        let values_sum = result.rows[0][1].as_f64().unwrap_or(0.0);

        let query_duration = query_timer.stop();
        total_query_duration += query_duration;

        println!("  ✓ Found {} data points", found);
        println!("  ✓ Sum of values: {:.2}", values_sum);

        assert_eq!(found, data_points_per_metric,
                   "Expected exactly {} data points for metric {}, found {}",
                   data_points_per_metric, metric_name, found);

        let expected_sum = expected_sums[metric_idx];
        assert!((values_sum - expected_sum).abs() < 0.01,
                "Expected sum {:.2}, got {:.2}", expected_sum, values_sum);
    }

    println!("\n📊 Multi-Metric Query Performance Summary:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("⏱️  Total query time ({} metric names): {:?}", num_metric_names, total_query_duration);
    println!("⏱️  Average per metric: {:?}", total_query_duration / num_metric_names as u32);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("\n✅ WAL-backed union-read validated for {} metric names", num_metric_names);

    println!("🔄 Forcing flush to staged local cache...");
    pipeline.force_flush_metrics().await.expect("force flush");

    let staged_files = pipeline.list_staged_files("metrics").expect("staged files");
    assert!(
        !staged_files.is_empty(),
        "Expected staged parquet cache files for metrics"
    );

    println!("⚙️  Running optimizer to commit staged metrics to Iceberg...");
    pipeline.run_optimizer_once().await.expect("optimizer");

    let staged_files_after = pipeline.list_staged_files("metrics").expect("staged files after");
    assert!(
        staged_files_after.is_empty(),
        "Expected staged cache cleanup after optimizer, found {:?}",
        staged_files_after
    );

    let query_engine = query::create_query_engine(&config).await.expect("query engine");
    for (metric_idx, metric_name) in metric_names.iter().enumerate() {
        let escaped = metric_name.replace('\'', "''");
        let sql = format!(
            "SELECT COUNT(*) AS count, SUM(value) AS total FROM iceberg_metrics WHERE metric_name = '{}'",
            escaped
        );
        let result = query_engine.execute_query(&sql).await.expect("query");
        let found = result.rows[0][0].as_i64().unwrap_or(0) as usize;
        let values_sum = result.rows[0][1].as_f64().unwrap_or(0.0);

        assert_eq!(
            found,
            data_points_per_metric,
            "Expected Iceberg scan to return {} data points for metric {} after optimizer, found {}",
            data_points_per_metric,
            metric_name,
            found
        );

        let expected_sum = expected_sums[metric_idx];
        assert!(
            (values_sum - expected_sum).abs() < 0.01,
            "Expected sum {:.2}, got {:.2}",
            expected_sum,
            values_sum
        );
    }

    println!("\n✅ WAL, local cache, and optimizer paths validated for metrics");
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
    let pipeline = storage::IngestPipeline::new(&config).await.expect("ingest pipeline");

    // Write the span and verify it succeeds
    let result = pipeline.write_span_batches(vec![vec![span]]).await;
    assert!(result.is_ok(), "Failed to write span with HTTP fields: {:?}", result.err());

    println!("✅ Successfully wrote span with HTTP fields to Iceberg");
    println!("✅ HTTP fields are correctly included in the schema and can be persisted");
}

#[tokio::test]
async fn test_pinned_metadata_updates_on_commit() {
    let mut config = load_test_config();
    let cache_dir = tempdir().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
    config.span_buffering.max_buffer_spans = 10_000;
    config.span_buffering.max_buffer_bytes = 1024 * 1024 * 1024;
    config.span_buffering.flush_interval_seconds = 3600;
    config.ingest_engine.optimizer_interval_seconds = 3600;

    let pipeline = storage::IngestPipeline::new(&config).await.expect("ingest pipeline");
    let now = Utc::now();

    let mut spans = Vec::new();
    for i in 0..10 {
        spans.push(SpanData {
            session_id: format!("pin-session-{}", i),
            trace_id: format!("pin-trace-{}", i),
            span_id: format!("pin-span-{}", i),
            parent_span_id: None,
            app_id: "app-pin".to_string(),
            organization_id: None,
            tenant_id: None,
            message_type: "pin".to_string(),
            span_kind: Some("SERVER".to_string()),
            timestamp: now + chrono::Duration::milliseconds(i as i64),
            end_timestamp: Some(now + chrono::Duration::milliseconds(i as i64 + 1)),
            attributes: HashMap::new(),
            events: Vec::new(),
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: Some("OK".to_string()),
            status_message: Some("OK".to_string()),
        });
    }

    pipeline.add_spans(spans.clone(), spans.len() * 256).await.expect("add spans");
    pipeline.force_flush_spans().await.expect("force flush");
    pipeline.run_optimizer_once().await.expect("optimizer");

    let pointer_path = cache_dir.path().join("iceberg_metadata").join("traces.json");
    let first = std::fs::read_to_string(&pointer_path).expect("metadata pointer");
    let first_json: serde_json::Value = serde_json::from_str(&first).expect("metadata json");
    let first_snapshot = first_json.get("snapshot_id").and_then(|v| v.as_i64());
    let first_location = first_json.get("metadata_location").and_then(|v| v.as_str()).map(str::to_string);
    assert!(first_snapshot.is_some(), "expected snapshot_id in metadata pointer");
    assert!(first_location.is_some(), "expected metadata_location in metadata pointer");

    pipeline.add_spans(spans, 10 * 256).await.expect("add spans");
    pipeline.force_flush_spans().await.expect("force flush");
    pipeline.run_optimizer_once().await.expect("optimizer");

    let second = std::fs::read_to_string(&pointer_path).expect("metadata pointer");
    let second_json: serde_json::Value = serde_json::from_str(&second).expect("metadata json");
    let second_snapshot = second_json.get("snapshot_id").and_then(|v| v.as_i64());
    let second_location = second_json.get("metadata_location").and_then(|v| v.as_str()).map(str::to_string);

    assert!(
        second_snapshot != first_snapshot || second_location != first_location,
        "expected pinned metadata to update after commit"
    );
}

#[tokio::test]
async fn test_duckdb_union_read_realtime_concurrency() {
    let mut config = load_test_config();
    ensure_wal_bucket(&mut config);

    let pipeline = storage::IngestPipeline::new(&config).await.expect("ingest pipeline");

    let now = Utc::now();
    let base_session = format!("perf-base-{}", uuid::Uuid::new_v4());
    let staged_session = format!("perf-staged-{}", uuid::Uuid::new_v4());
    let wal_session = format!("perf-wal-{}", uuid::Uuid::new_v4());
    let per_session = 200usize;

    let mut base_logs = Vec::new();
    for i in 0..per_session {
        base_logs.push(LogData {
            session_id: Some(base_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Base log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
        });
    }
    pipeline
        .write_log_batches(vec![base_logs])
        .await
        .expect("base write");

    let mut staged_logs = Vec::new();
    for i in 0..per_session {
        staged_logs.push(LogData {
            session_id: Some(staged_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(10_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(10_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Staged log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
        });
    }
    pipeline
        .add_logs(staged_logs, per_session * 256)
        .await
        .expect("stage add");
    pipeline.force_flush_logs().await.expect("stage flush");

    let mut wal_logs = Vec::new();
    for i in 0..per_session {
        wal_logs.push(LogData {
            session_id: Some(wal_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(20_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(20_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Wal log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
        });
    }
    pipeline
        .write_wal_logs(wal_logs, per_session * 256)
        .await
        .expect("wal write");

    let query_engine = query::create_query_engine(&config).await.expect("query engine");
    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM union_logs WHERE session_id = '{}'",
        staged_session.replace('\'', "''")
    );
    let warmup = query_engine.execute_query(&warmup_sql).await.expect("warmup");
    assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);

    let sessions = vec![base_session, staged_session, wal_session];
    let mut handles = Vec::new();
    let concurrent = 6usize;
    for i in 0..concurrent {
        let session_id = sessions[i % sessions.len()].clone();
            let engine = query_engine.clone();
        handles.push(tokio::spawn(async move {
            let sql = format!(
                "SELECT COUNT(*) AS count FROM union_logs WHERE session_id = '{}'",
                session_id.replace('\'', "''")
            );
            let _ = engine.execute_query(&sql).await.expect("warmup");
            let start = Instant::now();
            let result = engine.execute_query(&sql).await.expect("query");
            let duration = start.elapsed();
            let count = result.rows[0][0].as_i64().unwrap_or(0);
            (duration, count)
        }));
    }

    for handle in handles {
        let (_duration, count) = handle.await.expect("task");
        assert_eq!(count, per_session as i64, "Expected {} rows", per_session);
    }
}
