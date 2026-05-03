//! Tenant-scoped distinct values for telemetry dimensions (`ui_dropdown_catalog`).

use crate::config::{Config, DropdownCatalogConfig, DuckLakeConfig};
use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use std::collections::{HashMap, HashSet};
use tokio_postgres::NoTls;
use tracing::{debug, warn};

/// Application table next to DuckLake metadata (same Postgres database / schema).
#[derive(Debug)]
pub struct DropdownCatalog {
    pool: Pool,
    /// Unquoted schema name (e.g. `softprobe` or `public`).
    schema_name: String,
    qualified_table: String,
    cfg: DropdownCatalogConfig,
}

impl DropdownCatalog {
    pub fn active_values_days(&self) -> u32 {
        self.cfg.active_values_days
    }

    /// Connect when [`crate::config::DropdownCatalogConfig::enabled`] and DuckLake uses Postgres metadata.
    pub async fn connect(config: &Config) -> Result<Option<std::sync::Arc<Self>>> {
        if !config.dropdown_catalog.enabled {
            return Ok(None);
        }
        let dl = config.ducklake_or_default();
        if dl.catalog_type != "postgres" {
            warn!("dropdown catalog enabled but ducklake.catalog_type is not postgres; skipping");
            return Ok(None);
        }
        let cat = std::sync::Arc::new(Self::build_pool(config, &dl)?);
        cat.ensure_table().await?;
        Ok(Some(cat))
    }

    fn build_pool(config: &Config, dl: &DuckLakeConfig) -> Result<Self> {
        let conn_str = dl.metadata_path.trim();
        let conn_str = conn_str
            .strip_prefix("postgres:")
            .unwrap_or(conn_str)
            .trim();
        let mut pg = tokio_postgres::Config::new();
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
                other => {
                    debug!("dropdown catalog: ignoring unknown postgres config key {}", other);
                }
            }
        }
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
            ..Default::default()
        };
        let mgr = Manager::from_config(pg, NoTls, mgr_config);
        let pool = Pool::builder(mgr).max_size(8).build()?;
        let schema_name = catalog_schema_name(dl);
        let qualified_table = format!(
            "{}.ui_dropdown_catalog",
            quote_pg_ident(&schema_name)
        );
        Ok(Self {
            pool,
            schema_name,
            qualified_table,
            cfg: config.dropdown_catalog.clone(),
        })
    }

    async fn ensure_table(&self) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                &format!(
                    "CREATE SCHEMA IF NOT EXISTS {};",
                    quote_pg_ident(&self.schema_name)
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS {} (
  tenant_id TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_value TEXT NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, entity_type, entity_value)
);"#,
                    self.qualified_table
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    r#"CREATE INDEX IF NOT EXISTS ui_dropdown_catalog_lookup_idx
ON {} (tenant_id, entity_type, last_seen_at DESC);"#,
                    self.qualified_table
                ),
                &[],
            )
            .await?;
        Ok(())
    }

    /// Upsert distinct (entity_type, entity_value) pairs for traces from Arrow batches.
    /// Call **before** DuckLake INSERT so inlined rows are covered.
    pub async fn upsert_trace_batches(
        &self,
        tenant_id: &str,
        batches: &[RecordBatch],
    ) -> Result<()> {
        let pairs = collect_trace_catalog_pairs(batches, &self.cfg)?;
        self.upsert_pairs_sql(tenant_id, pairs).await
    }

    async fn upsert_pairs_sql(
        &self,
        tenant_id: &str,
        pairs: Vec<(String, String)>,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        for (etype, eval) in pairs {
            client
                .execute(
                    &format!(
                        r#"INSERT INTO {} (tenant_id, entity_type, entity_value, last_seen_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (tenant_id, entity_type, entity_value) DO UPDATE SET
last_seen_at = GREATEST({}.last_seen_at, EXCLUDED.last_seen_at)"#,
                        self.qualified_table, self.qualified_table
                    ),
                    &[&tenant_id, &etype, &eval],
                )
                .await?;
        }
        Ok(())
    }

    /// List entity types seen recently for a tenant.
    pub async fn list_entity_types(
        &self,
        tenant_id: &str,
        active_within_days: u32,
    ) -> Result<Vec<String>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                &format!(
                    r#"SELECT DISTINCT entity_type FROM {}
WHERE tenant_id = $1
  AND last_seen_at > NOW() - ($2::integer * INTERVAL '1 day')
ORDER BY entity_type"#,
                    self.qualified_table
                ),
                &[&tenant_id, &(active_within_days as i32)],
            )
            .await?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    /// List distinct values for one entity type (recent).
    pub async fn list_entity_values(
        &self,
        tenant_id: &str,
        entity_type: &str,
        active_within_days: u32,
        limit: i64,
    ) -> Result<Vec<String>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                &format!(
                    r#"SELECT entity_value FROM (
  SELECT entity_value, max(last_seen_at) AS mx
  FROM {}
  WHERE tenant_id = $1 AND entity_type = $2
    AND last_seen_at > NOW() - ($3::integer * INTERVAL '1 day')
  GROUP BY entity_value
) t
ORDER BY mx DESC
LIMIT $4"#,
                    self.qualified_table
                ),
                &[
                    &tenant_id,
                    &entity_type,
                    &(active_within_days as i32),
                    &limit,
                ],
            )
            .await?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    /// Delete stale rows (TTL maintenance).
    pub async fn prune_older_than_days(&self, days: u32) -> Result<u64> {
        let client = self.pool.get().await?;
        let n = client
            .execute(
                &format!(
                    r#"DELETE FROM {} WHERE last_seen_at < NOW() - ($1::integer * INTERVAL '1 day')"#,
                    self.qualified_table
                ),
                &[&(days as i32)],
            )
            .await?;
        Ok(n)
    }
}

fn catalog_schema_name(dl: &DuckLakeConfig) -> String {
    if dl.metadata_schema == "main" {
        "public".to_string()
    } else {
        dl.metadata_schema.clone()
    }
}

fn quote_pg_ident(id: &str) -> String {
    format!("\"{}\"", id.replace('"', "\"\""))
}

/// Default columns excluded (high-cardinality IDs or tenant scope key).
fn default_skip_columns(cfg: &DropdownCatalogConfig) -> HashSet<String> {
    let mut s: HashSet<String> = [
        "trace_id",
        "span_id",
        "parent_span_id",
        "session_id",
        "tenant_id",
        "attributes",
        "events",
        "record_date",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    for x in &cfg.skip_entity_columns {
        s.insert(x.clone());
    }
    s
}

fn collect_trace_catalog_pairs(
    batches: &[RecordBatch],
    cfg: &DropdownCatalogConfig,
) -> Result<Vec<(String, String)>> {
    let skip = default_skip_columns(cfg);
    let mut uniq: HashMap<(String, String), ()> = HashMap::new();
    for batch in batches {
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let name = field.name().clone();
            if skip.contains(&name) {
                continue;
            }
            let col = batch.column(col_idx);
            match field.data_type() {
                DataType::Utf8 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| anyhow!("expected Utf8 for {}", name))?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let v = arr.value(i);
                        if v.is_empty() {
                            continue;
                        }
                        uniq.insert((name.clone(), v.to_string()), ());
                    }
                }
                DataType::LargeUtf8 => {
                    use arrow::array::LargeStringArray;
                    let arr = col
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| anyhow!("expected LargeUtf8 for {}", name))?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let v = arr.value(i);
                        if v.is_empty() {
                            continue;
                        }
                        uniq.insert((name.clone(), v.to_string()), ());
                    }
                }
                DataType::Int32 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| anyhow!("expected Int32 for {}", name))?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        uniq.insert((name.clone(), arr.value(i).to_string()), ());
                    }
                }
                _ => {}
            }
        }
    }
    Ok(uniq.into_keys().collect())
}

/// Returns Some(tenant id) when all non-null `tenant_id` cells agree; None when absent or inconsistent (catalog upsert skipped).
pub fn resolve_trace_tenant_id(batches: &[RecordBatch]) -> Option<String> {
    let mut out: Option<String> = None;
    for batch in batches {
        let Some((idx, _)) = batch.schema().column_with_name("tenant_id") else {
            continue;
        };
        let col = batch.column(idx);
        let Some(arr) = col.as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = arr.value(i).to_string();
            if out.is_none() {
                out = Some(v);
            } else if out.as_ref() != Some(&v) {
                warn!("dropdown catalog: mixed tenant_id in batch; skipping catalog upsert");
                return None;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::iceberg::tables::TraceTable;
    use crate::models::Span;
    use iceberg::spec::Schema as IcebergSchema;

    fn sample_span(tenant: Option<&str>) -> Span {
        Span {
            tenant_id: tenant.map(|s| s.to_string()),
            session_id: "s1".into(),
            trace_id: "t1".into(),
            span_id: "p1".into(),
            parent_span_id: None,
            app_id: "svc-a".into(),
            organization_id: Some("org1".into()),
            message_type: "GET /x".into(),
            span_kind: Some("SPAN_KIND_SERVER".into()),
            timestamp: chrono::Utc::now(),
            end_timestamp: None,
            attributes: Default::default(),
            events: vec![],
            status_code: None,
            status_message: None,
            http_request_method: Some("GET".into()),
            http_request_path: Some("/x".into()),
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: Some(200),
            http_response_headers: None,
            http_response_body: None,
        }
    }

    #[test]
    fn collect_pairs_skips_ids_and_collects_dimensions() {
        let schema: IcebergSchema = TraceTable::schema(None);
        let spans = vec![sample_span(Some("ten-1"))];
        let batch =
            crate::storage::iceberg::arrow::spans_to_record_batch(&spans, &schema).unwrap();
        let cfg = DropdownCatalogConfig::default();
        let pairs = collect_trace_catalog_pairs(std::slice::from_ref(&batch), &cfg).unwrap();
        let types: HashSet<_> = pairs.iter().map(|(a, _)| a.as_str()).collect();
        assert!(!types.contains("trace_id"));
        assert!(!types.contains("tenant_id"));
        assert!(types.contains("app_id"));
        assert!(types.contains("message_type"));
        assert!(pairs.contains(&("http_response_status_code".into(), "200".into())));
    }

    #[test]
    fn resolve_tenant_roundtrip() {
        let schema: IcebergSchema = TraceTable::schema(None);
        let spans = vec![sample_span(Some("tid"))];
        let batch =
            crate::storage::iceberg::arrow::spans_to_record_batch(&spans, &schema).unwrap();
        assert_eq!(
            resolve_trace_tenant_id(std::slice::from_ref(&batch)).as_deref(),
            Some("tid")
        );
    }
}
