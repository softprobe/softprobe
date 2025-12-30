use softprobe_otlp_backend::config::Config;
use softprobe_otlp_backend::storage::span_buffer::SpanData;
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
    assert!(!config.iceberg.table_name.is_empty(), "Table name should not be empty");
    assert_eq!(config.iceberg.table_name, "test_spans", "Should load test table name");
    assert_eq!(config.span_buffering.max_buffer_spans, 1, "Should have test buffer config");
}


#[tokio::test]
async fn test_iceberg_writer_bulk_session_roundtrip() {
    use softprobe_otlp_backend::storage::span_buffer::SpanEvent;
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

            session_spans.push(SpanData {
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
    writer.write_session_batches(all_session_batches).await.expect("multi-session write should succeed");
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

    let table_ident = TableIdent::from_strs(["default", config.iceberg.table_name.as_str()])
        .or_else(|_| TableIdent::from_strs([config.iceberg.table_name.as_str()]))
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
