//! Strict storage and SQL-shape checks that replace the removed ad hoc scripts
//! (`verify_e2e.sh`, `verify_iceberg.sql`, `verify_session.sql`). Assertions run against the
//! runtime **DuckLake** query path (`union_spans` / `union_logs`), not Iceberg REST scans.
//!
//! Requirements: `integration-e2e` feature, local MinIO on the configured S3 endpoint, and
//! `tests/config/test.yaml` (see `make test-local`).

use crate::util::pipeline::TestPipeline;
use crate::util::poll::wait_for;
use crate::util::storage_config::{ensure_wal_bucket, load_test_config};
use chrono::Utc;
use softprobe_runtime::config::Config;
use softprobe_runtime::models::{Log as LogData, Span as SpanData};
use std::collections::HashMap;
use std::time::Duration;

async fn build_test_pipeline(mut config: Config) -> TestPipeline {
    ensure_wal_bucket(&mut config);
    TestPipeline::new(config).await
}

/// DuckLake / union view contract: non-empty counts, HTTP columns, `record_date` partition,
/// and distinct session scope (replaces former ad hoc Iceberg SQL checks).
#[tokio::test]
async fn strict_trace_union_shape_ducklake_contract() {
    let mut config = load_test_config();
    config.span_buffering.max_buffer_spans = 1;
    config.span_buffering.flush_interval_seconds = 1;

    let session_id = format!("strict-trace-{}", uuid::Uuid::new_v4());
    let trace_id = format!("strict-tr-{}", uuid::Uuid::new_v4());
    let now = Utc::now();

    let test_pipeline = build_test_pipeline(config).await;
    let pipeline = &test_pipeline.pipeline;

    let span = SpanData {
        session_id: session_id.clone(),
        trace_id: trace_id.clone(),
        span_id: "strict-span-1".to_string(),
        parent_span_id: None,
        app_id: "strict-app".to_string(),
        organization_id: None,
        tenant_id: None,
        message_type: "server".to_string(),
        span_kind: Some("SERVER".to_string()),
        timestamp: now,
        end_timestamp: Some(now),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: Some("GET".to_string()),
        http_request_path: Some("/api/strict-contract".to_string()),
        http_request_headers: Some(r#"{"X-Test":"1"}"#.to_string()),
        http_request_body: None,
        http_response_status_code: Some(201),
        http_response_headers: Some(r#"{"X-Resp":"ok"}"#.to_string()),
        http_response_body: None,
        status_code: None,
        status_message: None,
    };

    pipeline
        .add_spans(vec![span], 4096)
        .await
        .expect("add span");
    pipeline.force_flush_spans().await.expect("flush spans");

    let escaped = session_id.replace('\'', "''");
    let count_sql =
        format!("SELECT COUNT(*)::BIGINT AS c FROM union_spans WHERE session_id = '{escaped}'");
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let r = test_pipeline.execute_query(&count_sql).await?;
            let c = r.rows[0][0].as_i64().unwrap_or(0);
            Ok(c >= 1)
        },
    )
    .await
    .expect("union_spans should show the flushed span");

    let detail_sql = format!(
        "SELECT \
            http_request_method, \
            http_request_path, \
            http_response_status_code, \
            record_date::VARCHAR AS rd \
         FROM union_spans \
         WHERE session_id = '{escaped}' \
         LIMIT 1"
    );
    let row = test_pipeline
        .execute_query(&detail_sql)
        .await
        .expect("detail query");
    assert_eq!(row.row_count, 1, "expected one row for session");
    assert_eq!(row.rows[0][0].as_str().unwrap_or(""), "GET");
    assert_eq!(
        row.rows[0][1].as_str().unwrap_or(""),
        "/api/strict-contract"
    );
    assert_eq!(row.rows[0][2].as_i64().unwrap_or(0), 201);
    let rd = row.rows[0][3].as_str().unwrap_or("");
    assert!(
        !rd.is_empty() && rd != "NULL",
        "record_date must be populated for partition pruning (got {rd:?})"
    );

    let part_sql = format!(
        "SELECT COUNT(*)::BIGINT AS partitions FROM ( \
            SELECT record_date FROM union_spans WHERE session_id = '{escaped}' GROUP BY record_date \
        ) s"
    );
    let pr = test_pipeline
        .execute_query(&part_sql)
        .await
        .expect("partition query");
    assert!(
        pr.rows[0][0].as_i64().unwrap_or(0) >= 1,
        "expected at least one record_date partition for the session"
    );

    let distinct_sql = format!(
        "SELECT COUNT(DISTINCT session_id)::BIGINT AS d FROM union_spans WHERE session_id = '{escaped}'"
    );
    let dr = test_pipeline
        .execute_query(&distinct_sql)
        .await
        .expect("distinct session");
    assert_eq!(
        dr.rows[0][0].as_i64().unwrap_or(0),
        1,
        "session filter must resolve to exactly one session id"
    );
}

/// Mirrors former `verify_session.sql`: same `session_id` must appear in both traces and logs with
/// shared trace correlation.
#[tokio::test]
async fn strict_session_correlates_traces_and_logs() {
    let mut config = load_test_config();
    config.span_buffering.max_buffer_spans = 2;
    config.span_buffering.flush_interval_seconds = 1;

    let session_id = format!("strict-sess-{}", uuid::Uuid::new_v4());
    let trace_id = format!("strict-tid-{}", uuid::Uuid::new_v4());
    let now = Utc::now();

    let test_pipeline = build_test_pipeline(config).await;
    let pipeline = &test_pipeline.pipeline;

    let span = SpanData {
        session_id: session_id.clone(),
        trace_id: trace_id.clone(),
        span_id: "strict-span-a".to_string(),
        parent_span_id: None,
        app_id: "strict-app".to_string(),
        organization_id: None,
        tenant_id: None,
        message_type: "server".to_string(),
        span_kind: Some("SERVER".to_string()),
        timestamp: now,
        end_timestamp: Some(now),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
        status_code: None,
        status_message: None,
    };

    let log = LogData {
        session_id: Some(session_id.clone()),
        timestamp: now + chrono::Duration::milliseconds(1),
        observed_timestamp: None,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: "strict contract log".to_string(),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        trace_id: Some(trace_id.clone()),
        span_id: Some("strict-span-a".to_string()),
    };

    pipeline
        .add_spans(vec![span], 4096)
        .await
        .expect("add span");
    pipeline.add_logs(vec![log], 4096).await.expect("add log");
    pipeline.force_flush_spans().await.expect("flush spans");
    pipeline.force_flush_logs().await.expect("flush logs");

    let esc = session_id.replace('\'', "''");
    let span_wait = format!("SELECT COUNT(*)::BIGINT FROM union_spans WHERE session_id = '{esc}'");
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let r = test_pipeline.execute_query(&span_wait).await?;
            Ok(r.rows[0][0].as_i64().unwrap_or(0) >= 1)
        },
    )
    .await
    .expect("union_spans row for session");

    let log_wait = format!("SELECT COUNT(*)::BIGINT FROM union_logs WHERE session_id = '{esc}'");
    wait_for(
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let r = test_pipeline.execute_query(&log_wait).await?;
            Ok(r.rows[0][0].as_i64().unwrap_or(0) >= 1)
        },
    )
    .await
    .expect("union_logs row for session");

    let trace_esc = trace_id.replace('\'', "''");
    let correlate_sql = format!(
        "SELECT COUNT(*)::BIGINT AS n FROM union_logs \
         WHERE session_id = '{esc}' AND trace_id = '{trace_esc}'"
    );
    let cr = test_pipeline
        .execute_query(&correlate_sql)
        .await
        .expect("correlate");
    assert!(
        cr.rows[0][0].as_i64().unwrap_or(0) >= 1,
        "log must carry the same trace_id as the span for session-level drill-down"
    );
}
