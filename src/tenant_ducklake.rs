//! Runtime-owned DuckLake scope resolution.
//!
//! Hosted runtime instances are single-tenant from a storage-routing perspective: one runtime owns
//! one configured DuckLake schema and data path. Auth metadata may still include `tenantId` for
//! account context, but runtime ingest/query/promotion paths resolve a single storage scope.

use crate::config::{Config, DuckLakeConfig};
use crate::promotion::{
    ensure_promotion_metadata_tables, load_active_telemetry_columns_manifests,
    PromotionSpecLoadError, TelemetryColumnsManifest,
};
use anyhow::{anyhow, Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio_postgres::NoTls;

/// DuckLake storage scope for this runtime process (configured in `ducklake.*`, not derived from auth tenant id).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantDuckLakeScope {
    /// SQL schema inside the shared Postgres DuckLake metadata database.
    pub metadata_schema: String,
    /// Object-store data path for this deployment (from config).
    pub data_path: String,
}

#[derive(Clone)]
pub struct TenantDuckLakeResolver {
    pool: Pool,
    scope: TenantDuckLakeScope,
}

impl TenantDuckLakeResolver {
    pub async fn connect(config: &Config) -> Result<Option<Self>> {
        let dl = config.ducklake_or_default();
        if dl.catalog_type != "postgres" {
            return Ok(None);
        }
        let resolver = Self::build_pool(&dl)?;
        resolver.ensure_scope().await?;
        Ok(Some(resolver))
    }

    fn build_pool(dl: &DuckLakeConfig) -> Result<Self> {
        let mut pg = tokio_postgres::Config::new();
        parse_postgres_kv_config(&mut pg, &dl.metadata_path)?;
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
            ..Default::default()
        };
        let mgr = Manager::from_config(pg, NoTls, mgr_config);
        let pool = Pool::builder(mgr).max_size(8).build()?;
        let scope = TenantDuckLakeScope {
            metadata_schema: dl.metadata_schema.clone(),
            data_path: dl.data_path.clone(),
        };
        Ok(Self {
            pool,
            scope,
        })
    }

    async fn ensure_scope(&self) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                &format!(
                    "CREATE SCHEMA IF NOT EXISTS {};",
                    quote_pg_ident(&self.scope.metadata_schema)
                ),
                &[],
            )
            .await?;
        ensure_promotion_metadata_tables(&client, &self.scope.metadata_schema).await?;
        Ok(())
    }

    /// Resolve the runtime-owned DuckLake scope.
    pub async fn resolve_or_create(&self, _tenant_id: &str) -> Result<TenantDuckLakeScope> {
        self.ensure_scope().await?;
        Ok(self.scope.clone())
    }

    /// Resolve the tenant scope and load its active telemetry column manifests from Postgres.
    pub async fn load_active_telemetry_columns_manifests(
        &self,
        tenant_id: &str,
    ) -> Result<(TenantDuckLakeScope, Vec<TelemetryColumnsManifest>)> {
        let _ = tenant_id;
        let scope = self.resolve_or_create(tenant_id).await?;
        let client = self.pool.get().await?;
        let manifests = load_active_telemetry_columns_manifests(&client, &scope.metadata_schema)
            .await
            .map_err(|err| match err {
                PromotionSpecLoadError::Postgres(e) => anyhow!(e),
                PromotionSpecLoadError::InvalidRowManifest { spec_id, source } => {
                    anyhow!("promotion spec {spec_id} is invalid: {source}")
                }
            })?;
        Ok((scope, manifests))
    }

    /// Record one successfully applied telemetry promotion manifest in the tenant metadata schema.
    ///
    /// The manifest is stored in `promotion_specs` in the configured metadata schema because ingest
    /// loads active specs on each write. The deterministic id keeps repeated `promotion apply` calls
    /// for the same manifest from creating duplicate active specs.
    pub async fn record_active_telemetry_promotion_spec(
        &self,
        scope: &TenantDuckLakeScope,
        manifest_yaml: &str,
        target_tables: &[String],
    ) -> Result<String> {
        let mut hasher = DefaultHasher::new();
        manifest_yaml.hash(&mut hasher);
        let manifest_hash = format!("{:016x}", hasher.finish());
        let spec_id = format!("telemetry_columns_{manifest_hash}");
        let client = self.pool.get().await?;
        client
            .execute(
                &format!(
                    r#"INSERT INTO "{}".promotion_specs
  (spec_id, spec_version, target_kind, target_tables, manifest_json, manifest_hash, status)
VALUES ($1, 'softprobe.promotion.v1', 'telemetry_columns', $2, $3, $4, 'active')
ON CONFLICT (spec_id) DO UPDATE SET
  target_tables = EXCLUDED.target_tables,
  manifest_json = EXCLUDED.manifest_json,
  manifest_hash = EXCLUDED.manifest_hash,
  status = 'active',
  applied_at = NOW();"#,
                    scope.metadata_schema.replace('"', "\"\"")
                ),
                &[
                    &spec_id,
                    &target_tables.join(","),
                    &manifest_yaml,
                    &manifest_hash,
                ],
            )
            .await?;
        Ok(spec_id)
    }

    /// Record one successfully applied business table promotion manifest in tenant metadata.
    ///
    /// Business tables are tenant-local physical tables/views, so the active manifest is stored
    /// beside the generated relations for agents and later ingest workers to resolve.
    pub async fn record_active_business_promotion_spec(
        &self,
        scope: &TenantDuckLakeScope,
        manifest_yaml: &str,
        table_name: &str,
    ) -> Result<String> {
        let mut hasher = DefaultHasher::new();
        manifest_yaml.hash(&mut hasher);
        let manifest_hash = format!("{:016x}", hasher.finish());
        let spec_id = format!("business_table_{}_{}", table_name, manifest_hash);
        let client = self.pool.get().await?;
        client
            .execute(
                &format!(
                    r#"INSERT INTO "{}".promotion_specs
  (spec_id, spec_version, target_kind, target_tables, manifest_json, manifest_hash, status)
VALUES ($1, 'softprobe.promotion.v1', 'business_table', $2, $3, $4, 'active')
ON CONFLICT (spec_id) DO UPDATE SET
  target_tables = EXCLUDED.target_tables,
  manifest_json = EXCLUDED.manifest_json,
  manifest_hash = EXCLUDED.manifest_hash,
  status = 'active',
  applied_at = NOW();"#,
                    scope.metadata_schema.replace('"', "\"\"")
                ),
                &[&spec_id, &table_name, &manifest_yaml, &manifest_hash],
            )
            .await?;
        Ok(spec_id)
    }
}

fn parse_postgres_kv_config(pg: &mut tokio_postgres::Config, metadata_path: &str) -> Result<()> {
    let conn_str = metadata_path
        .trim()
        .strip_prefix("postgres:")
        .unwrap_or(metadata_path.trim())
        .trim();
    for part in conn_str.split_whitespace() {
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid postgres kv segment in metadata_path: {}", part))?;
        let v = v.trim_matches('\'');
        match k {
            "host" => {
                pg.host(v);
            }
            "port" => {
                pg.port(v.parse().context("metadata_path port")?);
            }
            "dbname" | "database" => {
                pg.dbname(v);
            }
            "user" | "username" => {
                pg.user(v);
            }
            "password" => {
                pg.password(v);
            }
            _ => {}
        }
    }
    Ok(())
}

fn quote_pg_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::quote_pg_ident;

    #[test]
    fn quote_pg_ident_escapes_double_quotes() {
        assert_eq!(quote_pg_ident("abc"), "\"abc\"");
        assert_eq!(quote_pg_ident("a\"b"), "\"a\"\"b\"");
    }
}
