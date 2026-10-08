use softprobe_runtime::promotion::ensure_promotion_metadata_tables;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
async fn creates_promotion_metadata_tables_in_separate_tenant_schemas() {
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
    let first_schema = format!("tenant_promotion_a_{suffix}");
    let second_schema = format!("tenant_promotion_b_{suffix}");

    ensure_promotion_metadata_tables(&client, &first_schema)
        .await
        .expect("first tenant metadata tables");
    ensure_promotion_metadata_tables(&client, &second_schema)
        .await
        .expect("second tenant metadata tables");

    insert_spec(&client, &first_schema, "spec-a").await;
    insert_error(&client, &second_schema, "spec-b").await;

    assert_eq!(
        count_rows(&client, &first_schema, "promotion_specs").await,
        1
    );
    assert_eq!(
        count_rows(&client, &second_schema, "promotion_specs").await,
        0
    );
    assert_eq!(
        count_rows(&client, &first_schema, "promotion_errors").await,
        0
    );
    assert_eq!(
        count_rows(&client, &second_schema, "promotion_errors").await,
        1
    );
}

async fn insert_spec(client: &tokio_postgres::Client, schema: &str, spec_id: &str) {
    let sql = format!(
        r#"INSERT INTO "{}".promotion_specs
(spec_id, spec_version, target_kind, manifest_json, manifest_hash, status)
VALUES ($1, 'softprobe.promotion.v1', 'telemetry_columns', '{{}}', 'hash-a', 'active');"#,
        schema
    );
    client
        .execute(&sql, &[&spec_id])
        .await
        .expect("insert spec");
}

async fn insert_error(client: &tokio_postgres::Client, schema: &str, spec_id: &str) {
    let sql = format!(
        r#"INSERT INTO "{}".promotion_errors
(spec_id, target_kind, target_column, source_signal, source_path, error_code, error_message)
VALUES ($1, 'business_table', 'order_id', 'trace', '$.order.id', 'missing_required', 'missing order id');"#,
        schema
    );
    client
        .execute(&sql, &[&spec_id])
        .await
        .expect("insert error");
}

async fn count_rows(client: &tokio_postgres::Client, schema: &str, table: &str) -> i64 {
    let sql = format!(r#"SELECT count(*) FROM "{}".{};"#, schema, table);
    client
        .query_one(&sql, &[])
        .await
        .expect("count rows")
        .get(0)
}
