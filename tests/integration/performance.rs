use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use splake::config::Config;
use splake::models::Log as LogData;
use splake::query;
use splake::query::duckdb::{reset_view_counters, view_counters_snapshot};
use splake::ingest_engine::IngestPipeline;
use splake::storage;
use splake::storage::iceberg::arrow::logs_to_record_batch;
use std::collections::HashMap;
use std::time::Instant;
use tempfile::tempdir;
use anyhow::Result;

use crate::util::pipeline::TestPipeline;

// ========================================
// Performance test goals (tunable via env):
// - Ingest N events (WAL + staged + Iceberg)
// - Query last N days via DuckDB union view
// - Warm p95 latency target under PERF_TARGET_MS
// - Parallel queries under PERF_CONCURRENCY
// ========================================

fn load_perf_config() -> Config {
    if let Ok(config_file) = std::env::var("PERF_CONFIG_FILE") {
        std::env::set_var("CONFIG_FILE", &config_file);
        return Config::load().expect("Failed to load perf config");
    }
    if let Ok(config_file) = std::env::var("CONFIG_FILE") {
        if std::path::Path::new(&config_file).exists() {
            return Config::load().expect("Failed to load config");
        }
    }

    let test_type = std::env::var("ICEBERG_TEST_TYPE").unwrap_or_else(|_| "local".to_string());
    let config_file = match test_type.as_str() {
        "r2" => "tests/config/test-r2.yaml",
        _ => "tests/config/test.yaml",
    };
    std::env::set_var("CONFIG_FILE", config_file);
    Config::load().expect("Failed to load config")
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

fn perf_target() -> std::time::Duration {
    if let Ok(target_ms) = std::env::var("PERF_TARGET_MS") {
        let ms = target_ms.parse::<u64>().unwrap_or(1000);
        return std::time::Duration::from_millis(ms);
    }
    std::time::Duration::from_millis(1000)
}

fn record_date_start(days_back: i64) -> String {
    let today = Utc::now().date_naive();
    let start = today - chrono::Duration::days(days_back);
    start.format("%Y-%m-%d").to_string()
}

fn diagnostics_enabled() -> bool {
    std::env::var("PERF_DIAG").ok().as_deref() == Some("1")
}

fn profile_enabled() -> bool {
    std::env::var("PERF_CACHE_PROFILE").ok().as_deref() == Some("1")
}

fn format_result(result: &splake::query::duckdb::QueryResult) -> String {
    let mut output = String::new();
    output.push_str(&format!("columns: {:?}\n", result.columns));
    for row in &result.rows {
        output.push_str(&format!("row: {:?}\n", row));
    }
    output
}

async fn maybe_log_cache_profile(engine: &splake::query::QueryEngine, label: &str) {
    if !profile_enabled() {
        return;
    }
    println!("cache_httpfs profile ({label})");
    if let Ok(result) = engine
        .execute_query("SELECT cache_httpfs_get_profile();")
        .await
    {
        println!("{}", format_result(&result));
    }
    if let Ok(result) = engine
        .execute_query(
            "SELECT name, value FROM duckdb_settings() WHERE name LIKE 'cache_httpfs_%';",
        )
        .await
    {
        println!("{}", format_result(&result));
    }
    if let Ok(result) = engine
        .execute_query("SELECT * FROM cache_httpfs_get_cache_filesystems();")
        .await
    {
        println!("{}", format_result(&result));
    }
    if let Ok(result) = engine
        .execute_query("SELECT * FROM cache_httpfs_cache_status_query();")
        .await
    {
        println!("{}", format_result(&result));
    }
    if let Ok(result) = engine
        .execute_query("SELECT * FROM cache_httpfs_cache_access_info_query();")
        .await
    {
        println!("{}", format_result(&result));
    }
}

async fn explain_analyze(
    engine: &splake::query::QueryEngine,
    label: &str,
    sql: &str,
) {
    let explain_sql = format!("EXPLAIN ANALYZE {sql}");
    match engine.execute_query(&explain_sql).await {
        Ok(result) => {
            println!("EXPLAIN ANALYZE ({label})");
            println!("{}", format_result(&result));
        }
        Err(err) => {
            println!("EXPLAIN ANALYZE ({label}) failed: {err}");
        }
    }
}

async fn run_diagnostics(
    engine: &splake::query::QueryEngine,
    label: &str,
    sql: &str,
) {
    let start = Instant::now();
    let result = engine.execute_query(sql).await;
    let duration = start.elapsed();
    match result {
        Ok(result) => {
            println!(
                "DIAG {label}: duration={:?}, rows={}",
                duration, result.row_count
            );
        }
        Err(err) => {
            println!("DIAG {label}: failed after {:?}: {}", duration, err);
        }
    }
    explain_analyze(engine, label, sql).await;
}

/// Retry a query with exponential backoff to handle R2 eventual consistency
/// Returns the query result once it succeeds, or the last error after max retries
async fn retry_query_until_count(
    engine: &splake::query::QueryEngine,
    sql: &str,
    expected_count: i64,
    max_retries: u32,
) -> Result<splake::query::duckdb::QueryResult, anyhow::Error> {
    let is_r2 = std::env::var("ICEBERG_TEST_TYPE").ok().as_deref() == Some("r2");
    let max_retries = if is_r2 { max_retries.max(10) } else { max_retries };
    
    for attempt in 0..max_retries {
        match engine.execute_query(sql).await {
            Ok(result) => {
                let count = result.rows[0][0].as_i64().unwrap_or(0);
                if count == expected_count {
                    return Ok(result);
                }
                if attempt < max_retries - 1 {
                    let delay_ms = 100 * (1 << attempt.min(5)); // Exponential backoff, max 3.2s
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
            }
            Err(e) => {
                if attempt < max_retries - 1 {
                    let delay_ms = 100 * (1 << attempt.min(5));
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                } else {
                    return Err(anyhow::anyhow!("Query failed after {} retries: {}", max_retries, e));
                }
            }
        }
    }
    
    // Final attempt
    let result = engine.execute_query(sql).await?;
    let count = result.rows[0][0].as_i64().unwrap_or(0);
    if count == expected_count {
        Ok(result)
    } else {
        Err(anyhow::anyhow!(
            "Query returned {} rows, expected {} after {} retries",
            count,
            expected_count,
            max_retries
        ))
    }
}

#[tokio::test]
async fn perf_union_read_latency() {
    let mut config = load_perf_config();
    if std::env::var("PERF_FORCE_SINGLE_WORKER").ok().as_deref() == Some("1") {
        config.duckdb.max_connections = 1;
    }
    ensure_wal_bucket(&mut config);

    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.pipeline;

    let per_session = std::env::var("PERF_EVENTS_PER_SESSION")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let days_back = std::env::var("PERF_DAYS_BACK")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(7);

    let now = Utc::now();
    let base_session = format!("perf-base-{}", uuid::Uuid::new_v4());
    let staged_session = format!("perf-staged-{}", uuid::Uuid::new_v4());
    let buffer_session = format!("perf-buffer-{}", uuid::Uuid::new_v4());

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
    let mut buffer_logs = Vec::new();
    for i in 0..per_session {
        buffer_logs.push(LogData {
            session_id: Some(buffer_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(20_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(20_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Buffer log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
        });
    }
    pipeline
        .add_logs(buffer_logs, per_session * 256)
        .await
        .expect("buffer add");

    let query_engine = test_pipeline.query_engine();
    maybe_log_cache_profile(&query_engine, "before_warmup").await;
    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM union_logs \
         WHERE session_id = '{}' AND record_date >= DATE '{}'",
        staged_session.replace('\'', "''"),
        record_date_start(days_back),
    );
    let warmup_workers = std::cmp::max(1, test_pipeline.config.duckdb.max_connections);
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }
    let warmup_iceberg_sql = format!(
        "SELECT COUNT(*) AS count FROM iceberg_logs \
         WHERE session_id = '{}' AND record_date >= DATE '{}'",
        base_session.replace('\'', "''"),
        record_date_start(days_back),
    );
    // Retry query to handle R2 eventual consistency after write_log_batches
    let warmup = retry_query_until_count(
        &query_engine,
        &warmup_iceberg_sql,
        per_session as i64,
        15,
    )
    .await
    .expect("iceberg warmup should eventually return data");
    assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    
    // Now run additional warmup queries (data should be visible now)
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_iceberg_sql)
            .await
            .expect("iceberg warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }
    let warmup_iceberg_sql = format!(
        "SELECT COUNT(*) AS count FROM iceberg_logs \
         WHERE session_id = '{}' AND record_date >= DATE '{}'",
        base_session.replace('\'', "''"),
        record_date_start(days_back),
    );
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_iceberg_sql)
            .await
            .expect("iceberg warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }
    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM union_logs \
         WHERE session_id = '{}' AND record_date >= DATE '{}'",
        buffer_session.replace('\'', "''"),
        record_date_start(days_back),
    );
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }

    let sql = format!(
        "SELECT COUNT(*) AS count FROM union_logs \
         WHERE session_id = '{}' AND record_date >= DATE '{}'",
        buffer_session.replace('\'', "''"),
        record_date_start(days_back),
    );
    for _ in 0..2 {
        let warm = query_engine.execute_query(&sql).await.expect("warmup");
        assert_eq!(warm.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }
    let iterations = 5;
    let mut durations = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let result = query_engine.execute_query(&sql).await.expect("query");
        let duration = start.elapsed();
        let count = result.rows[0][0].as_i64().unwrap_or(0);
        assert_eq!(count, per_session as i64);
        durations.push(duration);
    }
    durations.sort();
    let p95_idx = ((durations.len() as f64) * 0.95).ceil() as usize - 1;
    let p95 = durations[p95_idx.min(durations.len() - 1)];
    maybe_log_cache_profile(&query_engine, "after_latency").await;
    if diagnostics_enabled() {
        let base_sql = format!(
            "SELECT COUNT(*) AS count FROM iceberg_logs \
             WHERE session_id = '{}' AND record_date >= DATE '{}'",
            base_session.replace('\'', "''"),
            record_date_start(days_back),
        );
        let staged_sql = format!(
            "SELECT COUNT(*) AS count FROM staged_logs \
             WHERE session_id = '{}' AND record_date >= DATE '{}'",
            staged_session.replace('\'', "''"),
            record_date_start(days_back),
        );
        run_diagnostics(&query_engine, "iceberg_logs", &base_sql).await;
        run_diagnostics(&query_engine, "staged_logs", &staged_sql).await;
        let buffer_sql = format!(
            "SELECT COUNT(*) AS count FROM buffer_logs \
             WHERE session_id = '{}' AND record_date >= DATE '{}'",
            buffer_session.replace('\'', "''"),
            record_date_start(days_back),
        );
        run_diagnostics(&query_engine, "buffer_logs", &buffer_sql).await;
        run_diagnostics(&query_engine, "union_logs", &sql).await;
    }
    println!("p95 warm union_logs latency: {:?}", p95);
    assert!(
        p95 < perf_target(),
        "Expected p95 warm query under {:?}, got {:?}",
        perf_target(),
        p95
    );
}

#[tokio::test]
async fn perf_union_read_concurrency() {
    let mut config = load_perf_config();
    if std::env::var("PERF_FORCE_SINGLE_WORKER").ok().as_deref() == Some("1") {
        config.duckdb.max_connections = 1;
    }
    ensure_wal_bucket(&mut config);

    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.pipeline;

    let per_session = std::env::var("PERF_EVENTS_PER_SESSION")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let concurrency = std::env::var("PERF_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8);
    let days_back = std::env::var("PERF_DAYS_BACK")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(7);

    let now = Utc::now();
    let base_session = format!("perf-base-{}", uuid::Uuid::new_v4());
    let staged_session = format!("perf-staged-{}", uuid::Uuid::new_v4());
    let buffer_session = format!("perf-buffer-{}", uuid::Uuid::new_v4());

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
    let mut buffer_logs = Vec::new();
    for i in 0..per_session {
        buffer_logs.push(LogData {
            session_id: Some(buffer_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(20_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(20_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Buffer log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
        });
    }
    pipeline
        .add_logs(buffer_logs, per_session * 256)
        .await
        .expect("buffer add");

    let query_engine = test_pipeline.query_engine();
    maybe_log_cache_profile(&query_engine, "before_warmup").await;
    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM union_logs \
         WHERE session_id = '{}' AND record_date >= DATE '{}'",
        staged_session.replace('\'', "''"),
        record_date_start(days_back),
    );
    let warmup_workers = std::cmp::max(1, test_pipeline.config.duckdb.max_connections);
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }

    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM union_logs \
         WHERE session_id = '{}' AND record_date >= DATE '{}'",
        base_session.replace('\'', "''"),
        record_date_start(days_back),
    );
    // Retry query to handle R2 eventual consistency after write_log_batches
    let warmup = retry_query_until_count(
        &query_engine,
        &warmup_sql,
        per_session as i64,
        15,
    )
    .await
    .expect("warmup should eventually return data");
    assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    
    // Now run additional warmup queries (data should be visible now)
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }

    let warmup_sql = format!(
        "SELECT COUNT(*) AS count FROM union_logs \
         WHERE session_id = '{}' AND record_date >= DATE '{}'",
        buffer_session.replace('\'', "''"),
        record_date_start(days_back),
    );
    for _ in 0..warmup_workers {
        let warmup = query_engine
            .execute_query(&warmup_sql)
            .await
            .expect("warmup");
        assert_eq!(warmup.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    }

    let sessions = vec![
        base_session.clone(),
        staged_session.clone(),
        buffer_session.clone(),
    ];
    let mut handles = Vec::new();
    for i in 0..concurrency {
        let engine = query_engine.clone();
        let session_id = sessions[i % sessions.len()].clone();
        let date_filter = record_date_start(days_back);
        handles.push(tokio::spawn(async move {
            let sql = format!(
                "SELECT COUNT(*) AS count FROM union_logs \
                 WHERE session_id = '{}' AND record_date >= DATE '{}'",
                session_id.replace('\'', "''"),
                date_filter,
            );
            let _ = engine.execute_query(&sql).await.expect("warmup");
            let start = Instant::now();
            let result = engine.execute_query(&sql).await.expect("query");
            let duration = start.elapsed();
            let count = result.rows[0][0].as_i64().unwrap_or(0);
            (duration, count)
        }));
    }

    let mut durations = Vec::new();
    for handle in handles {
        let (duration, count) = handle.await.expect("task");
        assert_eq!(count, per_session as i64, "Expected {} rows", per_session);
        durations.push(duration);
    }
    durations.sort();
    let p95_idx = ((durations.len() as f64) * 0.95).ceil() as usize - 1;
    let p95 = durations[p95_idx.min(durations.len() - 1)];
    maybe_log_cache_profile(&query_engine, "after_concurrency").await;
    if diagnostics_enabled() {
        let base_sql = format!(
            "SELECT COUNT(*) AS count FROM iceberg_logs \
             WHERE session_id = '{}' AND record_date >= DATE '{}'",
            base_session.replace('\'', "''"),
            record_date_start(days_back),
        );
        let staged_sql = format!(
            "SELECT COUNT(*) AS count FROM staged_logs \
             WHERE session_id = '{}' AND record_date >= DATE '{}'",
            staged_session.replace('\'', "''"),
            record_date_start(days_back),
        );
        let buffer_sql = format!(
            "SELECT COUNT(*) AS count FROM buffer_logs \
             WHERE session_id = '{}' AND record_date >= DATE '{}'",
            buffer_session.replace('\'', "''"),
            record_date_start(days_back),
        );
        let union_sql = format!(
            "SELECT COUNT(*) AS count FROM union_logs \
             WHERE session_id = '{}' AND record_date >= DATE '{}'",
            buffer_session.replace('\'', "''"),
            record_date_start(days_back),
        );
        run_diagnostics(&query_engine, "iceberg_logs", &base_sql).await;
        run_diagnostics(&query_engine, "staged_logs", &staged_sql).await;
        run_diagnostics(&query_engine, "buffer_logs", &buffer_sql).await;
        run_diagnostics(&query_engine, "union_logs", &union_sql).await;
    }
    println!("p95 warm union_logs latency: {:?}", p95);
    assert!(
        p95 < perf_target(),
        "Expected p95 warm query under {:?}, got {:?}",
        perf_target(),
        p95
    );
}

#[tokio::test]
async fn perf_view_recreate_stability() {
    let mut config = load_perf_config();
    ensure_wal_bucket(&mut config);

    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.pipeline;

    let per_session = 1000;
    let now = Utc::now();
    let base_session = format!("perf-base-{}", uuid::Uuid::new_v4());
    let staged_session = format!("perf-staged-{}", uuid::Uuid::new_v4());
    let buffer_session = format!("perf-buffer-{}", uuid::Uuid::new_v4());

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
    let mut buffer_logs = Vec::new();
    for i in 0..per_session {
        buffer_logs.push(LogData {
            session_id: Some(buffer_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(20_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(20_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Buffer log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
        });
    }
    pipeline
        .add_logs(buffer_logs, per_session * 256)
        .await
        .expect("buffer add");

    let query_engine = test_pipeline.query_engine();
    reset_view_counters();
    let sql = format!(
        "SELECT COUNT(*) AS count FROM union_logs \
         WHERE session_id = '{}' AND record_date >= DATE '{}'",
        buffer_session.replace('\'', "''"),
        record_date_start(7),
    );
    let first = query_engine.execute_query(&sql).await.expect("query");
    assert_eq!(first.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    let snapshot_after_first = view_counters_snapshot();

    let second = query_engine.execute_query(&sql).await.expect("query");
    assert_eq!(second.rows[0][0].as_i64().unwrap_or(0), per_session as i64);
    let snapshot_after_second = view_counters_snapshot();

    assert_eq!(
        snapshot_after_first.iceberg_recreates, snapshot_after_second.iceberg_recreates,
        "Iceberg view should not be recreated between warm queries"
    );
    assert_eq!(
        snapshot_after_first.staged_recreates, snapshot_after_second.staged_recreates,
        "Staged view should not be recreated between warm queries"
    );
    assert_eq!(
        snapshot_after_first.union_recreates, snapshot_after_second.union_recreates,
        "Union view should not be recreated between warm queries"
    );
}

#[tokio::test]
async fn view_recreate_stability_local_stub() {
    let previous_config = std::env::var("CONFIG_FILE").ok();
    std::env::set_var("CONFIG_FILE", "tests/config/test.yaml");
    let mut config = Config::load().expect("config");
    let cache_dir = tempdir().expect("temp cache dir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
    config.ingest_engine.wal_dir =
        Some(cache_dir.path().join("wal").to_string_lossy().to_string());

    let stub_dir = tempdir().expect("stub dir");
    let stub_path = stub_dir.path().join("iceberg_stub.parquet");

    let now = Utc::now();
    let log = LogData {
        session_id: Some("stub-session".to_string()),
        timestamp: now,
        observed_timestamp: Some(now + chrono::Duration::milliseconds(1)),
        severity_number: 4,
        severity_text: "INFO".to_string(),
        body: "stub log".to_string(),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
    };
    let schema = storage::iceberg::OtlpLogsTable::schema(None);
    let record_batch = logs_to_record_batch(&[log], &schema).expect("record batch");
    let props = WriterProperties::builder().build();
    let file = std::fs::File::create(&stub_path).expect("stub file");
    let mut writer =
        ArrowWriter::try_new(file, record_batch.schema(), Some(props)).expect("writer");
    writer.write(&record_batch).expect("write");
    writer.close().expect("close");

    std::env::set_var(
        "DUCKDB_TEST_ICEBERG_FALLBACK_PATH",
        stub_path.to_string_lossy().to_string(),
    );
    reset_view_counters();

    let pipeline = IngestPipeline::new(&config)
        .await
        .expect("pipeline");
    let query_engine = query::create_query_engine(&config, std::sync::Arc::new(pipeline.storage.clone()))
        .await
        .expect("query engine");
    let sql = "SELECT COUNT(*) AS count FROM union_logs WHERE session_id = 'stub-session'";
    let first = query_engine.execute_query(sql).await.expect("query");
    assert_eq!(first.rows[0][0].as_i64().unwrap_or(0), 1);
    let after_first = view_counters_snapshot();

    let second = query_engine.execute_query(sql).await.expect("query");
    assert_eq!(second.rows[0][0].as_i64().unwrap_or(0), 1);
    let after_second = view_counters_snapshot();

    assert_eq!(
        after_first.iceberg_recreates,
        after_second.iceberg_recreates
    );
    assert_eq!(after_first.staged_recreates, after_second.staged_recreates);
    assert_eq!(after_first.union_recreates, after_second.union_recreates);

    std::env::remove_var("DUCKDB_TEST_ICEBERG_FALLBACK_PATH");
    if let Some(previous) = previous_config {
        std::env::set_var("CONFIG_FILE", previous);
    } else {
        std::env::remove_var("CONFIG_FILE");
    }
}
