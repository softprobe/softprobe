//! Tenant DuckLake scope resolution.
//!
//! Hosted Softprobe uses one Postgres metadata database, but each tenant owns a separate DuckLake
//! SQL schema inside that database. This module resolves that tenant scope from control metadata
//! instead of requiring one YAML config block per tenant.

use crate::config::{Config, DuckLakeConfig};
use anyhow::{anyhow, Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

/// DuckLake storage scope assigned to one tenant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantDuckLakeScope {
    /// SQL schema inside the shared Postgres DuckLake metadata database.
    pub metadata_schema: String,
    /// Data path assigned to this tenant. v1 uses the shared configured path; keeping it in the
    /// mapping table lets us introduce tenant-specific paths later without changing the resolver API.
    pub data_path: String,
}

#[derive(Clone)]
pub struct TenantDuckLakeResolver {
    pool: Pool,
    control_schema: String,
    qualified_table: String,
    default_data_path: String,
}

impl TenantDuckLakeResolver {
    pub async fn connect(config: &Config) -> Result<Option<Self>> {
        let dl = config.ducklake_or_default();
        if dl.catalog_type != "postgres" {
            return Ok(None);
        }
        let resolver = Self::build_pool(&dl)?;
        resolver.ensure_table().await?;
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
        let control_schema = control_schema_name(dl);
        let qualified_table = format!("{}.tenant_ducklake_scopes", quote_pg_ident(&control_schema));
        Ok(Self {
            pool,
            control_schema,
            qualified_table,
            default_data_path: dl.data_path.clone(),
        })
    }

    async fn ensure_table(&self) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                &format!(
                    "CREATE SCHEMA IF NOT EXISTS {};",
                    quote_pg_ident(&self.control_schema)
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS {} (
  tenant_id TEXT PRIMARY KEY,
  ducklake_schema TEXT NOT NULL UNIQUE,
  ducklake_data_path TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);"#,
                    self.qualified_table
                ),
                &[],
            )
            .await?;
        Ok(())
    }

    /// Resolve or create the DuckLake scope for a tenant.
    ///
    /// The deterministic schema name is only used when inserting the first mapping row. After that,
    /// Postgres is the source of truth so future scope changes are explicit and auditable.
    pub async fn resolve_or_create(&self, tenant_id: &str) -> Result<TenantDuckLakeScope> {
        let tenant_id = tenant_id.trim();
        if tenant_id.is_empty() {
            return Err(anyhow!("tenant DuckLake scope requires tenant_id"));
        }
        let candidate_schema = tenant_ducklake_schema_name(tenant_id);
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                &format!(
                    r#"INSERT INTO {} (tenant_id, ducklake_schema, ducklake_data_path)
VALUES ($1, $2, $3)
ON CONFLICT (tenant_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id
RETURNING ducklake_schema, ducklake_data_path;"#,
                    self.qualified_table
                ),
                &[&tenant_id, &candidate_schema, &self.default_data_path],
            )
            .await?;
        let scope = TenantDuckLakeScope {
            metadata_schema: row.get(0),
            data_path: row.get(1),
        };
        // Create the tenant metadata schema eagerly so data setup can hand agents a scope that is
        // immediately ready for DuckLake table creation and future promotion DDL.
        client
            .execute(
                &format!(
                    "CREATE SCHEMA IF NOT EXISTS {};",
                    quote_pg_ident(&scope.metadata_schema)
                ),
                &[],
            )
            .await?;
        Ok(scope)
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

fn control_schema_name(dl: &DuckLakeConfig) -> String {
    // Keep control metadata out of tenant DuckLake schemas. Operators configure the shared
    // Postgres catalog connection in YAML, but the tenant mapping itself always lives here.
    let _ = dl;
    "softprobe_control".to_string()
}

/// Convert tenant ids into conservative unquoted SQL identifiers for DuckLake schemas.
pub(crate) fn tenant_ducklake_schema_name(tenant_id: &str) -> String {
    let mut out = String::from("tenant_");
    let mut last_underscore = false;
    for ch in tenant_id.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_underscore = false;
        } else if !last_underscore {
            out.push('_');
            last_underscore = true;
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    if out == "tenant" {
        out.push_str("_unknown");
    }
    out
}

fn quote_pg_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::tenant_ducklake_schema_name;

    #[test]
    fn tenant_schema_name_is_stable_sql_identifier() {
        assert_eq!(
            tenant_ducklake_schema_name("tenant-123"),
            "tenant_tenant_123"
        );
        assert_eq!(tenant_ducklake_schema_name("Acme Prod"), "tenant_acme_prod");
        assert_eq!(tenant_ducklake_schema_name("a/b:c"), "tenant_a_b_c");
    }
}
