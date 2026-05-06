//! Promotion spec loading is isolated per **Postgres metadata schema**: two schemas in one DB see different active telemetry manifests (runtime deploys use one configured schema per process).

use softprobe_runtime::promotion::{
    ensure_promotion_metadata_tables, load_active_telemetry_columns_manifests,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

const MANIFEST_DIVISION: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: division_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: division.name
"#;

const MANIFEST_REGION: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces, logs]
columns:
  - name: region_code
    type: string
    nullable: true
    source:
      from: attribute
      key: region.code
"#;

#[tokio::test]
async fn active_telemetry_manifests_are_isolated_per_tenant_schema() {
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
    let first_schema = format!("tenant_promo_a_{suffix}");
    let second_schema = format!("tenant_promo_b_{suffix}");

    ensure_promotion_metadata_tables(&client, &first_schema)
        .await
        .expect("first tenant metadata tables");
    ensure_promotion_metadata_tables(&client, &second_schema)
        .await
        .expect("second tenant metadata tables");

    insert_telemetry_spec(
        &client,
        &first_schema,
        "spec-a",
        MANIFEST_DIVISION,
        "hash-a",
    )
    .await;
    insert_telemetry_spec(&client, &second_schema, "spec-b", MANIFEST_REGION, "hash-b").await;
    insert_business_spec_placeholder(&client, &first_schema, "biz-a").await;

    let first = load_active_telemetry_columns_manifests(&client, &first_schema)
        .await
        .expect("load first tenant");
    let second = load_active_telemetry_columns_manifests(&client, &second_schema)
        .await
        .expect("load second tenant");

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].columns[0].name, "division_name");
    assert_eq!(second[0].columns[0].name, "region_code");
    assert_eq!(first[0].target.tables.len(), 1);
    assert_eq!(second[0].target.tables.len(), 2);

    let first_names: Vec<_> = first[0].columns.iter().map(|c| c.name.as_str()).collect();
    assert!(
        !first_names.contains(&"region_code"),
        "first tenant must not see second tenant's promoted column"
    );
    let second_names: Vec<_> = second[0].columns.iter().map(|c| c.name.as_str()).collect();
    assert!(
        !second_names.contains(&"division_name"),
        "second tenant must not see first tenant's promoted column"
    );
}

async fn insert_telemetry_spec(
    client: &tokio_postgres::Client,
    schema: &str,
    spec_id: &str,
    manifest_yaml: &str,
    manifest_hash: &str,
) {
    let sql = format!(
        r#"INSERT INTO "{}".promotion_specs
(spec_id, spec_version, target_kind, manifest_json, manifest_hash, status)
VALUES ($1, 'softprobe.promotion.v1', 'telemetry_columns', $2, $3, 'active');"#,
        schema
    );
    client
        .execute(&sql, &[&spec_id, &manifest_yaml, &manifest_hash])
        .await
        .expect("insert telemetry spec");
}

/// Business-table row must not appear in telemetry manifest loads.
async fn insert_business_spec_placeholder(
    client: &tokio_postgres::Client,
    schema: &str,
    spec_id: &str,
) {
    let manifest = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: orders
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
      from: attribute
      key: order.id
"#;
    let sql = format!(
        r#"INSERT INTO "{}".promotion_specs
(spec_id, spec_version, target_kind, manifest_json, manifest_hash, status)
VALUES ($1, 'softprobe.promotion.v1', 'business_table', $2, 'hash-biz', 'active');"#,
        schema
    );
    client
        .execute(&sql, &[&spec_id, &manifest])
        .await
        .expect("insert business spec");
}
