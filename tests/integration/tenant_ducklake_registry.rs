use softprobe_runtime::config::Config;
use softprobe_runtime::runtime_engine::{DuckLakeScopeResolver, ScopeProvisioningRequest};
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
  tables: [traces]
columns:
  - name: region_code
    type: string
    nullable: true
    source:
      from: attribute
      key: region.code
"#;

#[tokio::test]
async fn resolve_scope_is_registry_strict_and_idempotent() {
    let resolver = postgres_resolver().await;
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let tenant_id = format!("tenant_registry_{suffix}");
    let metadata_schema = format!("tenant_registry_scope_{suffix}");
    let data_path = format!("s3://warehouse/tenants/{tenant_id}/ducklake/data/");

    let unknown = resolver
        .resolve_scope(&tenant_id)
        .await
        .expect_err("unknown scopes must not be lazily provisioned");
    assert!(
        unknown.to_string().contains("unknown scope"),
        "unexpected unknown scope error: {unknown}"
    );

    let request = ScopeProvisioningRequest {
        scope_id: tenant_id.clone(),
        metadata_schema: metadata_schema.clone(),
        data_path: data_path.clone(),
    };
    let created = resolver
        .provision_scope(request.clone())
        .await
        .expect("provision tenant");
    let first = resolver
        .resolve_scope(&tenant_id)
        .await
        .expect("first resolve");
    let second = resolver
        .resolve_scope(&tenant_id)
        .await
        .expect("second resolve");

    assert_eq!(created, first);
    assert_eq!(first, second);
    assert_eq!(first.metadata_schema, metadata_schema);
    assert_eq!(first.data_path, data_path);

    let repeated = resolver
        .provision_scope(request)
        .await
        .expect("idempotent provision");
    assert_eq!(repeated, first);
}

#[tokio::test]
async fn resolver_loads_active_promotion_specs_from_only_the_resolved_tenant_schema() {
    let resolver = postgres_resolver().await;
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let tenant_a = format!("tenant_promo_registry_a_{suffix}");
    let tenant_b = format!("tenant_promo_registry_b_{suffix}");

    let scope_a = resolver
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_a.clone(),
            metadata_schema: format!("tenant_promo_registry_a_scope_{suffix}"),
            data_path: format!("s3://warehouse/tenants/{tenant_a}/ducklake/data/"),
        })
        .await
        .expect("provision tenant A");
    let scope_b = resolver
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_b.clone(),
            metadata_schema: format!("tenant_promo_registry_b_scope_{suffix}"),
            data_path: format!("s3://warehouse/tenants/{tenant_b}/ducklake/data/"),
        })
        .await
        .expect("provision tenant B");

    resolver
        .record_active_telemetry_promotion_spec(
            &scope_a,
            MANIFEST_DIVISION,
            &["traces".to_string()],
        )
        .await
        .expect("record tenant A spec");
    resolver
        .record_active_telemetry_promotion_spec(&scope_b, MANIFEST_REGION, &["traces".to_string()])
        .await
        .expect("record tenant B spec");

    let (resolved_a, manifests_a) = resolver
        .load_active_telemetry_columns_manifests(&tenant_a)
        .await
        .expect("load tenant A manifests");
    let (resolved_b, manifests_b) = resolver
        .load_active_telemetry_columns_manifests(&tenant_b)
        .await
        .expect("load tenant B manifests");

    assert_eq!(resolved_a, scope_a);
    assert_eq!(resolved_b, scope_b);
    assert_eq!(manifests_a.len(), 1);
    assert_eq!(manifests_b.len(), 1);
    assert_eq!(manifests_a[0].columns[0].name, "division_name");
    assert_eq!(manifests_b[0].columns[0].name, "region_code");
}

async fn postgres_resolver() -> DuckLakeScopeResolver {
    let mut config = Config::default();
    config.ducklake.catalog_type = "postgres".to_string();
    config.ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    config.ducklake.metadata_schema = "softprobe_registry_test".to_string();
    config.ducklake.data_path = "s3://warehouse/ducklake/data/".to_string();

    DuckLakeScopeResolver::connect(&config)
        .await
        .expect("connect tenant resolver")
        .expect("postgres tenant resolver")
}
