//! PostgreSQL adapter for the shared metrics fidelity migration contract.

use async_trait::async_trait;
use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::runtime_engine::{
    DuckLakeScope, DuckLakeScopeResolver, ScopeProvisioningRequest,
};
use softprobe_runtime::storage::Storage;
use tempfile::TempDir;
use uuid::Uuid;

use crate::util::metrics_fidelity_contract::{
    contract_legacy_metrics_table_widens_on_gauge_ingest, MetricsFidelityBackend,
};

const POSTGRES_DSN: &str =
    "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake";

struct PostgresBackend {
    _temp: TempDir,
    storage: Storage,
    metadata_path: String,
    data_path: String,
    metadata_schema: String,
}

fn attach(metadata_path: &str, data_path: &str, metadata_schema: &str) -> duckdb::Connection {
    let connection = duckdb::Connection::open_in_memory().expect("duckdb");
    connection
        .execute_batch("INSTALL ducklake; INSTALL postgres; LOAD postgres;")
        .expect("extensions");
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:postgres:{}' AS softprobe \
             (DATA_PATH '{}', METADATA_SCHEMA '{}', META_SCHEMA '{}', \
              DATA_INLINING_ROW_LIMIT 0);",
            metadata_path.replace('\'', "''"),
            data_path.replace('\'', "''"),
            metadata_schema.replace('\'', "''"),
            metadata_schema.replace('\'', "''"),
        ))
        .expect("attach");
    connection
}

async fn setup() -> PostgresBackend {
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().simple().to_string();
    let short = &suffix[..8];
    let tenant_id = format!("tenant-mfidelity-{short}");
    let metadata_schema = format!("sp_mfidelity_data_{short}");
    let data_path = temp
        .path()
        .join("tenant-data")
        .to_string_lossy()
        .to_string();
    std::fs::create_dir_all(&data_path).expect("data dir");

    let mut config = Config::default();
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.shrink_pools_for_tests();
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().into());
    config.ducklake.catalog_type = "postgres".to_string();
    config.ducklake.metadata_path = POSTGRES_DSN.to_string();
    config.ducklake.catalog_alias = "softprobe".to_string();
    config.ducklake.metadata_schema = format!("sp_mfidelity_reg_{short}");
    config.ducklake.data_path = data_path.clone();
    config.ducklake.data_inlining_row_limit = Some(0);

    let resolver = DuckLakeScopeResolver::connect(&config)
        .await
        .expect("resolver")
        .expect("postgres resolver");
    let scope = resolver
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_id.clone(),
            metadata_schema: metadata_schema.clone(),
            data_path: data_path.clone(),
        })
        .await
        .expect("provision tenant");

    // Scope-bound writer (no registry tenant resolution) so ALTER/INSERT hit this tenant's catalog.
    let storage = IngestPipeline::build_tenant_storage(
        &config,
        None,
        None,
        tenant_id,
        DuckLakeScope {
            metadata_schema: scope.metadata_schema.clone(),
            data_path: scope.data_path.clone(),
        },
    )
    .await
    .expect("scope-bound storage");

    PostgresBackend {
        _temp: temp,
        storage,
        metadata_path: config.ducklake.metadata_path.clone(),
        data_path: scope.data_path,
        metadata_schema: scope.metadata_schema,
    }
}

#[async_trait]
impl MetricsFidelityBackend for PostgresBackend {
    fn metrics_table(&self) -> String {
        // Writer prefers catalog.schema.table when metadata_schema != main.
        format!("softprobe.{}.metrics", self.metadata_schema)
    }

    fn attach(&self) -> duckdb::Connection {
        attach(&self.metadata_path, &self.data_path, &self.metadata_schema)
    }

    async fn write_metric_batches(
        &self,
        batches: Vec<Vec<softprobe_runtime::models::Metric>>,
    ) -> anyhow::Result<()> {
        self.storage.writer.write_metric_batches(batches).await
    }

    fn create_legacy_metrics_table(&self) {
        let conn = self.attach();
        let table = self.metrics_table();
        conn.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS softprobe.{schema};
             CREATE TABLE {table} (
                metric_name VARCHAR,
                description VARCHAR,
                unit VARCHAR,
                metric_type VARCHAR,
                timestamp TIMESTAMPTZ,
                value DOUBLE,
                attributes VARIANT,
                resource_attributes VARIANT,
                record_date DATE
            );",
            schema = self.metadata_schema,
            table = table,
        ))
        .expect("legacy create on postgres ducklake");
    }
}

#[tokio::test]
async fn postgres_legacy_metrics_table_widens_on_gauge_ingest() {
    let backend = setup().await;
    contract_legacy_metrics_table_widens_on_gauge_ingest(&backend).await;
}
