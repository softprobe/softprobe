use softprobe_runtime::promotion::{
    parse_promotion_manifest, telemetry_column_add_ddls, PromotionManifest,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
async fn applies_different_telemetry_columns_to_isolated_tenant_tables() {
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
    let first_schema = format!("tenant_tel_a_{suffix}");
    let second_schema = format!("tenant_tel_b_{suffix}");
    create_minimal_trace_tables(&client, &first_schema).await;
    create_minimal_trace_tables(&client, &second_schema).await;

    apply_telemetry_manifest(&client, &first_schema, "division_name", "division.name").await;
    apply_telemetry_manifest(&client, &second_schema, "team_name", "team.name").await;

    client
        .execute(
            &format!(
                r#"INSERT INTO "{}".traces (session_id, trace_id, span_id, division_name)
VALUES ('s1', 't1', 'sp1', 'payments');"#,
                first_schema
            ),
            &[],
        )
        .await
        .expect("insert first tenant row");
    client
        .execute(
            &format!(
                r#"INSERT INTO "{}".traces (session_id, trace_id, span_id, team_name)
VALUES ('s2', 't2', 'sp2', 'checkout');"#,
                second_schema
            ),
            &[],
        )
        .await
        .expect("insert second tenant row");

    assert_eq!(
        query_string(&client, &first_schema, "division_name").await,
        "payments"
    );
    assert_eq!(
        query_string(&client, &second_schema, "team_name").await,
        "checkout"
    );
    assert!(!column_exists(&client, &first_schema, "traces", "team_name").await);
    assert!(!column_exists(&client, &second_schema, "traces", "division_name").await);
}

async fn create_minimal_trace_tables(client: &tokio_postgres::Client, schema: &str) {
    client
        .execute(&format!(r#"CREATE SCHEMA "{}";"#, schema), &[])
        .await
        .expect("create schema");
    for table in ["traces", "logs", "metric_samples"] {
        client
            .execute(
                &format!(
                    r#"CREATE TABLE "{}".{} (
  session_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  span_id TEXT NOT NULL
);"#,
                    schema, table
                ),
                &[],
            )
            .await
            .expect("create telemetry table");
    }
}

async fn apply_telemetry_manifest(
    client: &tokio_postgres::Client,
    schema: &str,
    column_name: &str,
    attribute_key: &str,
) {
    let manifest = parse_promotion_manifest(&format!(
        r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: {column_name}
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: {attribute_key}
"#
    ))
    .expect("valid manifest");
    let PromotionManifest::TelemetryColumns(spec) = manifest else {
        panic!("expected telemetry manifest");
    };

    for ddl in telemetry_column_add_ddls(&format!(r#""{}""#, schema), &spec).expect("ddl") {
        client
            .execute(&ddl, &[])
            .await
            .expect("apply telemetry ddl");
    }
}

async fn query_string(client: &tokio_postgres::Client, schema: &str, column: &str) -> String {
    client
        .query_one(
            &format!(r#"SELECT {} FROM "{}".traces LIMIT 1;"#, column, schema),
            &[],
        )
        .await
        .expect("query promoted column")
        .get(0)
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
        .expect("column lookup")
        .get::<_, i64>(0)
        == 1
}
