use chrono::Utc;
use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::models::Span;
use softprobe_runtime::promotion::ensure_promotion_metadata_tables;
use softprobe_runtime::runtime_engine::{DuckLakeScopeResolver, ScopeProvisioningRequest};
use std::collections::HashMap;
use tempfile::TempDir;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
async fn promoted_service_and_division_columns_are_queryable_after_ingest() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = Config::default();
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let mut ducklake = config.ducklake_or_default();
    ducklake.catalog_type = "postgres".to_string();
    ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    ducklake.catalog_alias = "softprobe".to_string();
    ducklake.metadata_schema = format!("softprobe_test_{suffix}");
    let tenant_data_path = temp
        .path()
        .join("tenant-data")
        .to_string_lossy()
        .to_string();
    ducklake.data_path = tenant_data_path.clone();
    ducklake.data_inlining_row_limit = Some(0);
    let data_path = ducklake.data_path.clone();
    let metadata_path = ducklake.metadata_path.clone();
    config.ducklake = Some(ducklake);
    config.ingest_engine.cache_dir = Some(temp.path().join("cache").to_string_lossy().to_string());
    config.ingest_engine.wal_dir = Some(temp.path().join("wal").to_string_lossy().to_string());

    let tenant_id = format!("tenant-promoted-{suffix}");
    let resolver = DuckLakeScopeResolver::connect(&config)
        .await
        .expect("resolver")
        .expect("postgres resolver");
    resolver
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_id.clone(),
            metadata_schema: format!("softprobe_promoted_data_{suffix}"),
            data_path: tenant_data_path,
        })
        .await
        .expect("provision tenant");
    let scope = resolver
        .resolve_scope(&tenant_id)
        .await
        .expect("tenant scope");
    insert_active_trace_promotion_spec(&scope.metadata_schema).await;

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    pipeline
        .write_span_batches(vec![vec![promoted_span(&tenant_id)]])
        .await
        .expect("write promoted span");

    let conn = duckdb::Connection::open_in_memory().expect("duckdb");
    conn.execute_batch("INSTALL ducklake; INSTALL postgres; LOAD postgres;")
        .expect("ducklake extensions");
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:postgres:{}' AS softprobe (DATA_PATH '{}', METADATA_SCHEMA '{}', META_SCHEMA '{}', DATA_INLINING_ROW_LIMIT 0);",
        metadata_path.replace('\'', "''"),
        data_path.replace('\'', "''"),
        scope.metadata_schema.replace('\'', "''"),
        scope.metadata_schema.replace('\'', "''"),
    ))
    .expect("attach tenant ducklake");
    let sql = format!(
        r#"SELECT service_name, division_name FROM softprobe.{}.traces WHERE session_id = 's-promoted'"#,
        scope.metadata_schema
    );
    let (service_name, division_name): (String, String) = conn
        .query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query promoted columns");

    assert_eq!(service_name, "checkout-api");
    assert_eq!(division_name, "payments");
}

async fn insert_active_trace_promotion_spec(schema: &str) {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
        NoTls,
    )
    .await
    .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    ensure_promotion_metadata_tables(&client, schema)
        .await
        .expect("metadata tables");
    let manifest = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: service_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: service.name
  - name: division_name
    type: string
    nullable: true
    source:
      from: attribute
      key: division.name
"#;
    client
        .execute(
            &format!(
                r#"INSERT INTO "{}".promotion_specs
(spec_id, spec_version, target_kind, manifest_json, manifest_hash, status)
VALUES ('trace-promoted-columns', 'softprobe.promotion.v1', 'telemetry_columns', $1, 'hash-trace-promoted-columns', 'active');"#,
                schema
            ),
            &[&manifest],
        )
        .await
        .expect("insert promotion spec");
}

fn promoted_span(tenant_id: &str) -> Span {
    let mut attributes = HashMap::new();
    attributes.insert("division.name".to_string(), "payments".to_string());
    let mut resource_attributes = HashMap::new();
    resource_attributes.insert("service.name".to_string(), "checkout-api".to_string());

    Span {
        session_id: "s-promoted".to_string(),
        trace_id: "trace-promoted".to_string(),
        span_id: "span-promoted".to_string(),
        parent_span_id: None,
        app_id: "checkout-api".to_string(),
        organization_id: None,
        tenant_id: Some(tenant_id.to_string()),
        message_type: "checkout".to_string(),
        span_kind: Some("SPAN_KIND_INTERNAL".to_string()),
        timestamp: Utc::now(),
        end_timestamp: None,
        attributes,
        resource_attributes,
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
    }
}
