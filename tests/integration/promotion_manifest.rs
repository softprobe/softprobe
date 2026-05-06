use softprobe_runtime::promotion::{parse_promotion_manifest, PromotionManifest};

#[test]
fn parses_representative_telemetry_manifest() {
    let manifest = parse_promotion_manifest(
        r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces, logs, metrics]
columns:
  - name: division_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: division.name
  - name: checkout_status
    type: string
    nullable: true
    source:
      from: event_attribute
      event_name: checkout.completed
      key: status
"#,
    )
    .expect("telemetry manifest should parse");

    match manifest {
        PromotionManifest::TelemetryColumns(spec) => {
            assert_eq!(spec.target.tables.len(), 3);
            assert_eq!(spec.columns[0].name, "division_name");
            assert_eq!(spec.columns[1].name, "checkout_status");
        }
        other => panic!("expected telemetry manifest, got {other:?}"),
    }
}

#[test]
fn parses_representative_business_table_manifest() {
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
      json_path: $.order.totalCents
"#,
    )
    .expect("business manifest should parse");

    match manifest {
        PromotionManifest::BusinessTable(spec) => {
            assert_eq!(spec.target.table, "checkout_orders");
            assert_eq!(spec.target.version, 1);
            assert_eq!(spec.columns[0].name, "order_id");
            assert_eq!(spec.columns[1].name, "total_cents");
        }
        other => panic!("expected business manifest, got {other:?}"),
    }
}
