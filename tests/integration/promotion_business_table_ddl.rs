use softprobe_runtime::promotion::{
    business_table_create_ddls, parse_promotion_manifest, PromotionManifest,
};
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
