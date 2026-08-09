//! Verify DuckLake VARIANT shredding for hot attribute columns.

use chrono::Utc;
use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::models::{Log as LogData, Metric as MetricData, Span as SpanData};
use softprobe_runtime::query;
use softprobe_runtime::storage::schema::variant::variant_varchar;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use crate::util::config::file_backed_test_config;
use serde_json::Value;

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

fn attributes_object(value: &Value) -> serde_json::Map<String, Value> {
    match value {
        Value::Object(map) => map.clone(),
        Value::String(text) => serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default(),
        _ => serde_json::Map::new(),
    }
}

#[tokio::test]
async fn variant_shredding_hot_paths_and_nested_filters() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(0);

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let query_engine = query::create_query_engine(&config, Arc::new(pipeline.storage.clone()))
        .await
        .expect("query engine");

    let now = Utc::now();
    let session_id = format!("variant-sess-{}", uuid::Uuid::new_v4());
    let mut spans = Vec::new();
    for i in 0..80 {
        let mut attributes = HashMap::new();
        attributes.insert(
            "sp.observation.type".to_string(),
            if i % 2 == 0 {
                "generation".to_string()
            } else {
                "span".to_string()
            },
        );
        attributes.insert("gen_ai.request.model".to_string(), "gpt-test".to_string());
        attributes.insert(
            "gen_ai.usage.input_tokens".to_string(),
            ((i + 1) * 10).to_string(),
        );
        attributes.insert(
            "sp.cost.total".to_string(),
            format!("{:.2}", (i as f64) * 0.1),
        );
        attributes.insert("sp.user.id".to_string(), format!("user-{i}"));
        spans.push(SpanData {
            session_id: session_id.clone(),
            trace_id: format!("tr-{i}"),
            span_id: format!("sp-{i}"),
            parent_span_id: None,
            app_id: "variant-app".to_string(),
            organization_id: None,
            tenant_id: None,
            message_type: "chat".to_string(),
            span_kind: Some("INTERNAL".to_string()),
            timestamp: now + chrono::Duration::milliseconds(i),
            end_timestamp: Some(now + chrono::Duration::milliseconds(i + 1)),
            attributes,
            resource_attributes: HashMap::new(),
            events: Vec::new(),
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: Some("OK".to_string()),
            status_message: None,
        });
    }

    let mut log_attrs = HashMap::new();
    log_attrs.insert("sp.session.id".to_string(), session_id.clone());
    let mut log_resource = HashMap::new();
    log_resource.insert("service.name".to_string(), "variant-svc".to_string());
    let log = LogData {
        session_id: Some(session_id.clone()),
        timestamp: now,
        observed_timestamp: Some(now),
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: "hello".to_string(),
        attributes: log_attrs,
        resource_attributes: log_resource,
        trace_id: Some("tr-0".to_string()),
        span_id: Some("sp-0".to_string()),
    };

    let mut metric_attrs = HashMap::new();
    metric_attrs.insert("sp.session.id".to_string(), session_id.clone());
    let metric = MetricData {
        metric_name: "variant.metric".to_string(),
        description: "d".to_string(),
        unit: "1".to_string(),
        metric_type: "gauge".to_string(),
        timestamp: now,
        value: 1.0,
        attributes: metric_attrs,
        resource_attributes: HashMap::new(),
    };

    pipeline
        .write_span_batches(vec![spans])
        .await
        .expect("write spans");
    pipeline
        .write_log_batches(vec![vec![log]])
        .await
        .expect("write logs");
    pipeline
        .write_metric_batches(vec![vec![metric]])
        .await
        .expect("write metrics");

    let conn = attach(&config.ducklake.metadata_path, &config.ducklake.data_path);
    let mut describe = conn
        .prepare("DESCRIBE softprobe.traces;")
        .expect("describe");
    let types: HashMap<String, String> = describe
        .query_map([], |row| {
            let name: String = row.get(0)?;
            let dtype: String = row.get(1)?;
            Ok((name, dtype))
        })
        .expect("map")
        .map(|r| r.expect("row"))
        .collect();
    assert_eq!(
        types.get("attributes").map(String::as_str),
        Some("VARIANT"),
        "traces.attributes must be VARIANT, got {types:?}"
    );

    let mut stats = conn
        .prepare(
            "SELECT variant_path, shredded_type, value_count \
             FROM __ducklake_metadata_softprobe.ducklake_file_variant_stats \
             ORDER BY variant_path;",
        )
        .expect("stats");
    let rows: Vec<(String, String, i64)> = stats
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2)?))
        })
        .expect("query")
        .map(|r| r.expect("row"))
        .collect();
    assert!(
        rows.iter()
            .any(|(path, ty, _)| path == "\"sp.observation.type\"" && ty == "varchar"),
        "expected shredded observation type path, got {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|(path, ty, _)| path == "\"gen_ai.request.model\"" && ty == "varchar"),
        "expected shredded model path, got {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|(path, ty, _)| path == "\"sp.cost.total\"" && ty == "float64"),
        "expected shredded cost path, got {rows:?}"
    );

    let filter_sql = format!(
        "SELECT COUNT(*)::BIGINT AS c FROM union_spans \
         WHERE session_id = '{sess}' AND {obs} = 'generation'",
        sess = session_id.replace('\'', "''"),
        obs = variant_varchar("attributes", "sp.observation.type"),
    );
    let started = Instant::now();
    let result = query_engine
        .execute_query(&filter_sql)
        .await
        .expect("filter query");
    let elapsed = started.elapsed();
    assert_eq!(result.rows[0][0].as_i64(), Some(40));
    assert!(
        elapsed < Duration::from_secs(5),
        "nested VARIANT filter should complete quickly, took {elapsed:?}"
    );

    let detail_sql = format!(
        "SELECT CAST(attributes AS JSON) AS attributes FROM union_spans \
         WHERE session_id = '{sess}' AND {obs} = 'generation' LIMIT 1",
        sess = session_id.replace('\'', "''"),
        obs = variant_varchar("attributes", "sp.observation.type"),
    );
    let detail = query_engine
        .execute_query(&detail_sql)
        .await
        .expect("detail");
    let attrs = attributes_object(&detail.rows[0][0]);
    assert_eq!(
        attrs.get("sp.observation.type").and_then(|v| v.as_str()),
        Some("generation")
    );

    let metric_sql = format!(
        "SELECT COUNT(*)::BIGINT FROM union_metrics WHERE {pred} = '{sess}'",
        pred = variant_varchar("attributes", "sp.session.id"),
        sess = session_id.replace('\'', "''"),
    );
    let metrics = query_engine
        .execute_query(&metric_sql)
        .await
        .expect("metrics");
    assert_eq!(metrics.rows[0][0].as_i64(), Some(1));

    let log_sql = format!(
        "SELECT COUNT(*)::BIGINT FROM union_logs WHERE {pred} = '{sess}'",
        pred = variant_varchar("attributes", "sp.session.id"),
        sess = session_id.replace('\'', "''"),
    );
    let logs = query_engine.execute_query(&log_sql).await.expect("logs");
    assert_eq!(logs.rows[0][0].as_i64(), Some(1));
}

/// Cover every nested VARIANT key path used by LLM / telemetry SQL compilers.
#[tokio::test]
async fn variant_key_queries_cover_llm_telemetry_and_capture_paths() {
    use softprobe_runtime::api::llm::query::{
        compile_observation_search_sql, ObservationSearchRequest,
    };
    use softprobe_runtime::api::telemetry::{compile_details_sql, TelemetryDetailsTarget};
    use softprobe_runtime::storage::schema::variant::variant_try_cast;

    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(0);

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let query_engine = query::create_query_engine(&config, Arc::new(pipeline.storage.clone()))
        .await
        .expect("query engine");

    let now = Utc::now();
    let session_id = format!("vk-sess-{}", uuid::Uuid::new_v4());
    let capture_id = format!("cap-{}", uuid::Uuid::new_v4());
    let tenant_id = "tenant-variant-keys";
    let trace_id = "vk-trace-1";

    // Span with full LLM hot-key set + capture id.
    let mut attrs = HashMap::new();
    attrs.insert("sp.observation.type".into(), "generation".into());
    attrs.insert("gen_ai.request.model".into(), "gpt-4o-mini".into());
    attrs.insert("gen_ai.provider.name".into(), "openai".into());
    attrs.insert("sp.user.id".into(), "user-vk-1".into());
    attrs.insert("gen_ai.usage.input_tokens".into(), "11".into());
    attrs.insert("gen_ai.usage.output_tokens".into(), "22".into());
    attrs.insert("gen_ai.usage.total_tokens".into(), "33".into());
    attrs.insert("sp.cost.total".into(), "0.42".into());
    attrs.insert("sp.capture.id".into(), capture_id.clone());
    let span = SpanData {
        session_id: session_id.clone(),
        trace_id: trace_id.to_string(),
        span_id: "vk-span-1".into(),
        parent_span_id: None,
        app_id: "vk-app".into(),
        organization_id: None,
        tenant_id: Some(tenant_id.into()),
        message_type: "chat".into(),
        span_kind: Some("CLIENT".into()),
        timestamp: now,
        end_timestamp: Some(now + chrono::Duration::milliseconds(50)),
        attributes: attrs,
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
        status_code: Some("OK".into()),
        status_message: None,
    };

    // Span missing observation type / user id (COALESCE + enduser fallback).
    let mut attrs_fallback = HashMap::new();
    attrs_fallback.insert("enduser.id".into(), "enduser-vk".into());
    attrs_fallback.insert("gen_ai.request.model".into(), "other-model".into());
    let span_fallback = SpanData {
        session_id: session_id.clone(),
        trace_id: "vk-trace-2".into(),
        span_id: "vk-span-2".into(),
        parent_span_id: None,
        app_id: "vk-app".into(),
        organization_id: None,
        tenant_id: Some(tenant_id.into()),
        message_type: "tool".into(),
        span_kind: Some("INTERNAL".into()),
        timestamp: now + chrono::Duration::milliseconds(1),
        end_timestamp: Some(now + chrono::Duration::milliseconds(2)),
        attributes: attrs_fallback,
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
        status_code: Some("OK".into()),
        status_message: None,
    };

    // Metrics correlated via attributes and resource_attributes keys.
    let mut metric_attrs = HashMap::new();
    metric_attrs.insert("sp.session.id".into(), session_id.clone());
    metric_attrs.insert("trace.id".into(), trace_id.to_string());
    let mut metric_resource = HashMap::new();
    metric_resource.insert("session.id".into(), session_id.clone());
    metric_resource.insert("trace_id".into(), trace_id.to_string());
    let metric = MetricData {
        metric_name: "vk.metric".into(),
        description: "d".into(),
        unit: "1".into(),
        metric_type: "gauge".into(),
        timestamp: now,
        value: 3.0,
        attributes: metric_attrs,
        resource_attributes: metric_resource,
    };

    let mut log_attrs = HashMap::new();
    log_attrs.insert("sp.session.id".into(), session_id.clone());
    let mut log_resource = HashMap::new();
    log_resource.insert("service.name".into(), "vk-svc".into());
    let log = LogData {
        session_id: Some(session_id.clone()),
        timestamp: now,
        observed_timestamp: Some(now),
        severity_number: 9,
        severity_text: "INFO".into(),
        body: "vk".into(),
        attributes: log_attrs,
        resource_attributes: log_resource,
        trace_id: Some(trace_id.into()),
        span_id: Some("vk-span-1".into()),
    };

    pipeline
        .write_span_batches(vec![vec![span, span_fallback]])
        .await
        .expect("write spans");
    pipeline
        .write_metric_batches(vec![vec![metric]])
        .await
        .expect("write metrics");
    pipeline
        .write_log_batches(vec![vec![log]])
        .await
        .expect("write logs");

    // 1) Direct VARIANT key projections + typed casts.
    let proj_sql = format!(
        "SELECT \
            COALESCE({obs}, 'span') AS observation_type, \
            {model} AS model_name, \
            {provider} AS model_provider, \
            COALESCE({user}, {enduser}) AS user_id, \
            {input} AS input_tokens, \
            {output} AS output_tokens, \
            {total} AS total_tokens, \
            {cost} AS total_cost, \
            {capture} AS capture_id \
         FROM union_spans \
         WHERE session_id = '{sess}' AND span_id = 'vk-span-1'",
        obs = variant_varchar("attributes", "sp.observation.type"),
        model = variant_varchar("attributes", "gen_ai.request.model"),
        provider = variant_varchar("attributes", "gen_ai.provider.name"),
        user = variant_varchar("attributes", "sp.user.id"),
        enduser = variant_varchar("attributes", "enduser.id"),
        input = variant_try_cast("attributes", "gen_ai.usage.input_tokens", "BIGINT"),
        output = variant_try_cast("attributes", "gen_ai.usage.output_tokens", "BIGINT"),
        total = variant_try_cast("attributes", "gen_ai.usage.total_tokens", "BIGINT"),
        cost = variant_try_cast("attributes", "sp.cost.total", "DOUBLE"),
        capture = variant_varchar("attributes", "sp.capture.id"),
        sess = session_id.replace('\'', "''"),
    );
    let proj = query_engine.execute_query(&proj_sql).await.expect("proj");
    assert_eq!(proj.row_count, 1);
    assert_eq!(proj.rows[0][0].as_str(), Some("generation"));
    assert_eq!(proj.rows[0][1].as_str(), Some("gpt-4o-mini"));
    assert_eq!(proj.rows[0][2].as_str(), Some("openai"));
    assert_eq!(proj.rows[0][3].as_str(), Some("user-vk-1"));
    assert_eq!(proj.rows[0][4].as_i64(), Some(11));
    assert_eq!(proj.rows[0][5].as_i64(), Some(22));
    assert_eq!(proj.rows[0][6].as_i64(), Some(33));
    assert!(
        (proj.rows[0][7].as_f64().unwrap_or(0.0) - 0.42).abs() < 1e-9,
        "cost={}",
        proj.rows[0][7]
    );
    assert_eq!(proj.rows[0][8].as_str(), Some(capture_id.as_str()));

    // 2) COALESCE default + enduser.id fallback.
    let fallback_sql = format!(
        "SELECT COALESCE({obs}, 'span') AS observation_type, \
                COALESCE({user}, {enduser}) AS user_id \
         FROM union_spans WHERE span_id = 'vk-span-2'",
        obs = variant_varchar("attributes", "sp.observation.type"),
        user = variant_varchar("attributes", "sp.user.id"),
        enduser = variant_varchar("attributes", "enduser.id"),
    );
    let fallback = query_engine
        .execute_query(&fallback_sql)
        .await
        .expect("fallback");
    assert_eq!(fallback.rows[0][0].as_str(), Some("span"));
    assert_eq!(fallback.rows[0][1].as_str(), Some("enduser-vk"));

    // 3) Missing key is NULL (not an error).
    let missing_sql = format!(
        "SELECT {missing} IS NULL AS is_missing FROM union_spans WHERE span_id = 'vk-span-1'",
        missing = variant_varchar("attributes", "does.not.exist"),
    );
    let missing = query_engine
        .execute_query(&missing_sql)
        .await
        .expect("missing");
    assert_eq!(missing.rows[0][0].as_bool(), Some(true));

    // 4) Compiled LLM observation search SQL against live VARIANT data.
    let search = ObservationSearchRequest {
        from: now - chrono::Duration::hours(1),
        to: now + chrono::Duration::hours(1),
        observation_types: vec!["generation".into()],
        model_name: Some("gpt-4o-mini".into()),
        user_id: Some("user-vk-1".into()),
        session_id: Some(session_id.clone()),
        trace_id: None,
        limit: Some(10),
        cursor: None,
    };
    let search_sql = compile_observation_search_sql(&search).expect("compile search");
    assert!(search_sql.contains("CAST(attributes['sp.observation.type'] AS VARCHAR)"));
    assert!(search_sql.contains("CAST(attributes['gen_ai.request.model'] AS VARCHAR)"));
    assert!(search_sql.contains("CAST(attributes['sp.user.id'] AS VARCHAR)"));
    let search_result = query_engine
        .execute_query(&search_sql)
        .await
        .expect("run search sql");
    assert_eq!(search_result.row_count, 1);
    // columns include observation_type / model_name / tokens from projection
    let cols = &search_result.columns;
    let obs_idx = cols.iter().position(|c| c == "observation_type").unwrap();
    let model_idx = cols.iter().position(|c| c == "model_name").unwrap();
    let tokens_idx = cols.iter().position(|c| c == "total_tokens").unwrap();
    assert_eq!(search_result.rows[0][obs_idx].as_str(), Some("generation"));
    assert_eq!(
        search_result.rows[0][model_idx].as_str(),
        Some("gpt-4o-mini")
    );
    assert_eq!(search_result.rows[0][tokens_idx].as_i64(), Some(33));

    // Negative: wrong model filters out the generation span.
    let miss = ObservationSearchRequest {
        model_name: Some("no-such-model".into()),
        ..search.clone()
    };
    let miss_sql = compile_observation_search_sql(&miss).expect("compile miss");
    let miss_result = query_engine
        .execute_query(&miss_sql)
        .await
        .expect("run miss");
    assert_eq!(miss_result.row_count, 0);

    // 5) Nested VARIANT capture-id key (SoftProbe capture_export removed with Redis).
    let capture_sql = format!(
        "SELECT CAST(attributes AS JSON) AS attributes FROM union_spans \
         WHERE CAST(attributes['sp.capture.id'] AS VARCHAR) = '{cap}' \
           AND tenant_id = '{ten}'",
        cap = capture_id.replace('\'', "''"),
        ten = tenant_id.replace('\'', "''"),
    );
    let capture_result = query_engine
        .execute_query(&capture_sql)
        .await
        .expect("capture id filter");
    assert_eq!(capture_result.row_count, 1);
    let attr_idx = capture_result
        .columns
        .iter()
        .position(|c| c == "attributes")
        .expect("attributes column");
    let attrs_obj = attributes_object(&capture_result.rows[0][attr_idx]);
    assert_eq!(
        attrs_obj.get("sp.capture.id").and_then(|v| v.as_str()),
        Some(capture_id.as_str())
    );

    // 6) Telemetry details metric filters on attributes + resource_attributes keys.
    let details = compile_details_sql(
        &TelemetryDetailsTarget {
            kind: "session".into(),
            id: session_id.clone(),
        },
        None,
        100,
    )
    .expect("compile details");
    assert!(details
        .metrics
        .contains("CAST(attributes['sp.session.id'] AS VARCHAR)"));
    assert!(details
        .metrics
        .contains("CAST(resource_attributes['session.id'] AS VARCHAR)"));
    let metrics = query_engine
        .execute_query(&details.metrics)
        .await
        .expect("details metrics");
    assert_eq!(metrics.row_count, 1);

    let trace_details = compile_details_sql(
        &TelemetryDetailsTarget {
            kind: "trace".into(),
            id: trace_id.into(),
        },
        None,
        100,
    )
    .expect("compile trace details");
    assert!(trace_details
        .metrics
        .contains("CAST(attributes['trace.id'] AS VARCHAR)"));
    assert!(trace_details
        .metrics
        .contains("CAST(resource_attributes['trace_id'] AS VARCHAR)"));
    let trace_metrics = query_engine
        .execute_query(&trace_details.metrics)
        .await
        .expect("trace metrics");
    assert_eq!(trace_metrics.row_count, 1);

    let logs = query_engine
        .execute_query(&details.logs)
        .await
        .expect("details logs");
    assert_eq!(logs.row_count, 1);
    let log_attr_idx = logs
        .columns
        .iter()
        .position(|c| c == "attributes")
        .expect("log attributes");
    assert!(
        !attributes_object(&logs.rows[0][log_attr_idx]).is_empty()
            || logs.rows[0][log_attr_idx].as_str().is_some(),
        "log attributes should deserialize as JSON object or JSON text"
    );
}

#[tokio::test]
async fn variant_write_fails_fast_on_legacy_map_table() {
    // make test-local exports SPLAKE_RESET_DUCKLAKE=1; that path drops tables for local
    // iteration only. This test must not rely on DROP and must not fight that reset.
    let previous_reset = std::env::var_os("SPLAKE_RESET_DUCKLAKE");
    std::env::remove_var("SPLAKE_RESET_DUCKLAKE");

    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(0);

    // Fresh catalog: create legacy MAP table first (no DROP). Writer CREATE IF NOT EXISTS
    // leaves it alone; ensure_variant_column_types must then fail fast.
    {
        let conn = attach(&config.ducklake.metadata_path, &config.ducklake.data_path);
        conn.execute_batch("CREATE TABLE softprobe.traces AS SELECT MAP {'a':'1'} AS attributes;")
            .expect("create legacy map table");
        let dtype: String = conn
            .query_row(
                "SELECT column_type FROM (DESCRIBE softprobe.traces) WHERE column_name = 'attributes';",
                [],
                |row| row.get(0),
            )
            .expect("describe attributes type");
        assert!(
            dtype.to_ascii_uppercase().contains("MAP"),
            "precondition: legacy table should be MAP, got {dtype}"
        );
    }

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let now = Utc::now();
    let mut attributes = HashMap::new();
    attributes.insert("sp.observation.type".to_string(), "generation".to_string());
    let span = SpanData {
        session_id: "legacy-map".to_string(),
        trace_id: "tr-legacy".to_string(),
        span_id: "sp-legacy".to_string(),
        parent_span_id: None,
        app_id: "variant-app".to_string(),
        organization_id: None,
        tenant_id: None,
        message_type: "chat".to_string(),
        span_kind: Some("INTERNAL".to_string()),
        timestamp: now,
        end_timestamp: Some(now),
        attributes,
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
        status_code: Some("OK".to_string()),
        status_message: None,
    };

    let write_result = pipeline.write_span_batches(vec![vec![span]]).await;
    match previous_reset {
        Some(value) => std::env::set_var("SPLAKE_RESET_DUCKLAKE", value),
        None => std::env::remove_var("SPLAKE_RESET_DUCKLAKE"),
    }

    let err = write_result.expect_err("legacy MAP table must fail fast");
    let message = err.to_string();
    assert!(
        message.contains("expected VARIANT") || message.contains("VARIANT"),
        "error should mention VARIANT migration, got: {message}"
    );
    assert!(
        message.contains("rebuild") || message.contains("migrate"),
        "error should tell operators migration is required, got: {message}"
    );
}
