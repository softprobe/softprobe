use softprobe_runtime::config::Config;
use softprobe_runtime::promotion::{
    business_table_create_ddls, parse_promotion_manifest, PromotionManifest,
};
use softprobe_runtime::storage::ducklake::DuckLakeWriter;
use softprobe_runtime::runtime_engine::{DuckLakeScopeResolver, ScopeProvisioningRequest};
use tempfile::TempDir;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
async fn creates_versioned_business_table_and_current_view_from_manifest() {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
        NoTls,
    )
    .await
    .expect("connect ducklake postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let schema = format!("tenant_business_ddl_{suffix}");
    client
        .execute(&format!(r#"CREATE SCHEMA "{}";"#, schema), &[])
        .await
        .expect("create tenant schema");

    let manifest = parse_promotion_manifest(
        r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: order_id
    type: string
    nullable: false
    source:
      from: http_response_body
      json_path: $.order.id
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#,
    )
    .expect("valid manifest");
    let PromotionManifest::BusinessTable(spec) = manifest else {
        panic!("expected business table manifest");
    };

    for ddl in business_table_create_ddls(&format!(r#""{}""#, schema), &spec).expect("ddl") {
        client.execute(&ddl, &[]).await.expect("apply business ddl");
    }

    assert!(relation_exists(&client, &schema, "checkout_orders_v1", "BASE TABLE").await);
    assert!(relation_exists(&client, &schema, "checkout_orders_current", "VIEW").await);
    assert!(column_exists(&client, &schema, "checkout_orders_v1", "session_id").await);
    assert!(column_exists(&client, &schema, "checkout_orders_v1", "order_id").await);
    assert!(column_exists(&client, &schema, "checkout_orders_v1", "total_cents").await);
}

#[tokio::test]
async fn ducklake_writer_applies_business_table_to_tenant_scope() {
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let mut config = Config::default();
    let mut ducklake = config.ducklake_or_default();
    ducklake.catalog_type = "postgres".to_string();
    ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    ducklake.catalog_alias = "softprobe".to_string();
    ducklake.metadata_schema = format!("softprobe_business_apply_{suffix}");
    let business_data_path = temp.path().join("data").to_string_lossy().to_string();
    ducklake.data_path = business_data_path.clone();
    ducklake.data_inlining_row_limit = Some(0);
    config.ducklake = Some(ducklake);
    config.ingest_engine.cache_dir = Some(temp.path().join("cache").to_string_lossy().to_string());
    config.ingest_engine.wal_dir = Some(temp.path().join("wal").to_string_lossy().to_string());

    let resolver = DuckLakeScopeResolver::connect(&config)
        .await
        .expect("resolver")
        .expect("postgres resolver");
    let business_tenant_id = format!("tenant-business-apply-{suffix}");
    let business_metadata_schema = format!("softprobe_business_apply_data_{suffix}");
    resolver
        .provision_scope(ScopeProvisioningRequest {
            tenant_id: business_tenant_id.clone(),
            metadata_schema: business_metadata_schema.clone(),
            data_path: business_data_path,
        })
        .await
        .expect("provision tenant");
    let scope = resolver
        .resolve_scope(&business_tenant_id)
        .await
        .expect("tenant scope");
    let writer = DuckLakeWriter::new(&config, None, Some(resolver))
        .await
        .expect("writer");
    let manifest = parse_promotion_manifest(BUSINESS_MANIFEST).expect("valid manifest");
    let PromotionManifest::BusinessTable(spec) = manifest else {
        panic!("expected business table manifest");
    };

    let ddls = writer
        .apply_business_table_promotion(&scope, &spec)
        .await
        .expect("apply business table promotion");

    assert_eq!(ddls.len(), 2);
    assert_ducklake_table_exists(&scope.metadata_schema, "checkout_orders_v1").await;
    assert_ducklake_view_exists(&scope.metadata_schema, "checkout_orders_current").await;
}

async fn relation_exists(
    client: &tokio_postgres::Client,
    schema: &str,
    relation: &str,
    relation_type: &str,
) -> bool {
    client
        .query_one(
            r#"SELECT count(*)
FROM information_schema.tables
WHERE table_schema = $1 AND table_name = $2 AND table_type = $3;"#,
            &[&schema, &relation, &relation_type],
        )
        .await
        .expect("relation exists query")
        .get::<_, i64>(0)
        == 1
}

async fn column_exists(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
    column: &str,
) -> bool {
    client
        .query_one(
            r#"SELECT count(*)
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2 AND column_name = $3;"#,
            &[&schema, &table, &column],
        )
        .await
        .expect("column exists query")
        .get::<_, i64>(0)
        == 1
}

async fn assert_ducklake_table_exists(schema: &str, table: &str) {
    assert_ducklake_relation_exists(schema, "ducklake_table", "table_name", table).await;
}

async fn assert_ducklake_view_exists(schema: &str, view: &str) {
    assert_ducklake_relation_exists(schema, "ducklake_view", "view_name", view).await;
}

async fn assert_ducklake_relation_exists(
    schema: &str,
    metadata_table: &str,
    name_column: &str,
    relation: &str,
) {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
        NoTls,
    )
    .await
    .expect("connect ducklake postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let sql = format!(
        r#"SELECT count(*) FROM "{}".{} WHERE {} = $1;"#,
        schema, metadata_table, name_column
    );
    let count: i64 = client
        .query_one(&sql, &[&relation])
        .await
        .expect("query DuckLake relation metadata")
        .get(0);
    assert_eq!(count, 1, "missing DuckLake relation {schema}.{relation}");
}

const BUSINESS_MANIFEST: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: order_id
    type: string
    nullable: false
    source:
      from: http_response_body
      json_path: $.order.id
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#;
