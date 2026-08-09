//! Promotion specs store (local SQLite) and DuckLakeWriter apply/load methods.
//!
//! Specs persist as `{catalog_alias}.promotion_specs` through the writer's attached DuckDB
//! connection. Apply is serialized with a process-global mutex (required because each tenant id
//! gets its own RuntimeEngine/writer pointing at the same SQLite file).

use crate::promotion::{
    business_manifest_from_row, business_spec_activation, business_table_create_ddls,
    local_promotion_specs_table_ddl, run_business_apply, run_telemetry_apply,
    telemetry_column_add_ddls, telemetry_manifest_from_row, telemetry_spec_activation,
    BusinessApplyError, BusinessTableManifest, PromotionSpecActivation, PromotionSpecLoadError,
    TelemetryColumnsManifest,
};
use crate::runtime_engine::DuckLakeScope;
use anyhow::{anyhow, Result};
use duckdb::Connection;
use std::sync::OnceLock;
use tokio::sync::Mutex as AsyncMutex;

use super::util::quote_duckdb_ident;
use super::DuckLakeWriter;

/// Process-global serialize for local promotion apply (cross-writer, same SQLite file).
pub(super) fn local_apply_mutex() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

fn table_missing(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("does not exist") || msg.contains("not found") || msg.contains("catalog error")
}

fn ensure_specs_table(conn: &Connection, catalog_alias: &str) -> Result<()> {
    conn.execute_batch(&local_promotion_specs_table_ddl(catalog_alias))?;
    Ok(())
}

/// Load active telemetry manifests from the local DuckLake catalog.
///
/// A missing `promotion_specs` table means no promotions have been applied yet — returns empty.
pub(super) fn load_active_telemetry_manifests(
    conn: &Connection,
    catalog_alias: &str,
) -> Result<Vec<TelemetryColumnsManifest>, PromotionSpecLoadError> {
    let catalog = quote_duckdb_ident(catalog_alias);
    let sql = format!(
        "SELECT spec_id, manifest_json FROM {catalog}.promotion_specs \
WHERE status = 'active' AND target_kind = 'telemetry_columns';"
    );
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(err) if table_missing(&err) => return Ok(Vec::new()),
        Err(err) => return Err(PromotionSpecLoadError::Backend(err.to_string())),
    };
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|err| PromotionSpecLoadError::Backend(err.to_string()))?;
    let mut out = Vec::new();
    for row in rows {
        let (spec_id, manifest_json) =
            row.map_err(|err| PromotionSpecLoadError::Backend(err.to_string()))?;
        if let Some(m) = telemetry_manifest_from_row(&spec_id, &manifest_json)? {
            out.push(m);
        }
    }
    Ok(out)
}

/// Load the active business-table manifest for one logical table, if any.
pub(super) fn load_active_business_manifest(
    conn: &Connection,
    catalog_alias: &str,
    table_name: &str,
) -> Result<Option<BusinessTableManifest>, PromotionSpecLoadError> {
    let catalog = quote_duckdb_ident(catalog_alias);
    let sql = format!(
        "SELECT spec_id, manifest_json FROM {catalog}.promotion_specs \
WHERE status = 'active' AND target_kind = 'business_table' AND target_tables = ? \
ORDER BY applied_at DESC LIMIT 1;"
    );
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(err) if table_missing(&err) => return Ok(None),
        Err(err) => return Err(PromotionSpecLoadError::Backend(err.to_string())),
    };
    let mut rows = stmt
        .query([table_name])
        .map_err(|err| PromotionSpecLoadError::Backend(err.to_string()))?;
    let Some(row) = rows
        .next()
        .map_err(|err| PromotionSpecLoadError::Backend(err.to_string()))?
    else {
        return Ok(None);
    };
    let spec_id: String = row
        .get(0)
        .map_err(|err| PromotionSpecLoadError::Backend(err.to_string()))?;
    let manifest_json: String = row
        .get(1)
        .map_err(|err| PromotionSpecLoadError::Backend(err.to_string()))?;
    business_manifest_from_row(&spec_id, &manifest_json)
}

/// Activate one spec using the lifecycle shared by telemetry and business promotions.
///
/// DuckLake tables do not support unique constraints, so UPDATE-then-INSERT runs under the
/// process-global apply mutex held by the coordinator.
pub(super) fn record_active_spec(
    conn: &Connection,
    catalog_alias: &str,
    manifest_yaml: &str,
    activation: &PromotionSpecActivation,
) -> Result<String> {
    ensure_specs_table(conn, catalog_alias)?;
    let catalog = quote_duckdb_ident(catalog_alias);
    conn.execute_batch("BEGIN TRANSACTION;")?;
    let result = (|| -> Result<String> {
        conn.execute(
            &format!(
                "UPDATE {catalog}.promotion_specs SET status = 'inactive' \
WHERE status = 'active' AND target_kind = ? \
AND (? <> 'business_table' OR target_tables = ?) AND spec_id <> ?"
            ),
            duckdb::params![
                activation.target_kind,
                activation.target_kind,
                activation.target_tables,
                activation.spec_id
            ],
        )?;
        let updated = conn.execute(
            &format!(
                "UPDATE {catalog}.promotion_specs SET \
target_tables = ?, manifest_json = ?, manifest_hash = ?, status = 'active', applied_at = NOW() \
WHERE spec_id = ?"
            ),
            duckdb::params![
                activation.target_tables,
                manifest_yaml,
                activation.manifest_hash,
                activation.spec_id
            ],
        )?;
        if updated == 0 {
            conn.execute(
                &format!(
                    "INSERT INTO {catalog}.promotion_specs \
(spec_id, spec_version, target_kind, target_tables, manifest_json, manifest_hash, status) \
VALUES (?, 'softprobe.promotion.v1', ?, ?, ?, ?, 'active')"
                ),
                duckdb::params![
                    activation.spec_id,
                    activation.target_kind,
                    activation.target_tables,
                    manifest_yaml,
                    activation.manifest_hash
                ],
            )?;
        }
        Ok(activation.spec_id.clone())
    })();
    match result {
        Ok(id) => {
            conn.execute_batch("COMMIT;")?;
            Ok(id)
        }
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK;");
            Err(err)
        }
    }
}

impl DuckLakeWriter {
    pub(super) fn map_spec_load(err: PromotionSpecLoadError) -> anyhow::Error {
        match err {
            PromotionSpecLoadError::Postgres(e) => anyhow!(e),
            PromotionSpecLoadError::Backend(e) => anyhow!(e),
            PromotionSpecLoadError::InvalidRowManifest { spec_id, source } => {
                anyhow!("promotion spec {spec_id} is invalid: {source}")
            }
        }
    }

    pub(super) fn load_active_telemetry_manifests_local(
        &self,
    ) -> Result<Vec<TelemetryColumnsManifest>> {
        let dk = &self.ducklake;
        self.with_attached_conn(dk, |conn| {
            load_active_telemetry_manifests(conn, &dk.catalog_alias).map_err(Self::map_spec_load)
        })
    }

    /// Backend-neutral load of active telemetry promotion manifests for this writer's scope.
    pub async fn load_active_telemetry_manifests(
        &self,
        scope: &DuckLakeScope,
    ) -> Result<Vec<TelemetryColumnsManifest>> {
        if self.ducklake.catalog_type == "postgres" {
            let resolver = self
                .tenant_ducklake
                .as_ref()
                .ok_or_else(|| anyhow!("postgres promotion requires a tenant DuckLake resolver"))?;
            return resolver
                .load_active_telemetry_columns_manifests_for_scope(scope)
                .await;
        }
        if self.ducklake.catalog_type == "sqlite" {
            return self.load_active_telemetry_manifests_local();
        }
        Ok(Vec::new())
    }

    pub(super) fn activate_spec_local_unlocked(
        &self,
        scope: &DuckLakeScope,
        manifest_yaml: &str,
        activation: &crate::promotion::PromotionSpecActivation,
    ) -> Result<String> {
        let dk = self.effective_ducklake(scope);
        self.with_attached_conn(&dk, |conn| {
            record_active_spec(conn, &dk.catalog_alias, manifest_yaml, activation)
        })
    }

    /// Apply telemetry DDL and activate the spec under one backend-specific critical section
    /// (Postgres advisory lock / SQLite process-global mutex), so concurrent applies cannot
    /// interleave DDL with a different manifest's activation.
    pub async fn apply_and_record_telemetry_promotion(
        &self,
        scope: &DuckLakeScope,
        manifest_yaml: &str,
        spec: &TelemetryColumnsManifest,
        target_tables: &[String],
    ) -> Result<String> {
        if self.ducklake.catalog_type == "postgres" {
            let resolver = self
                .tenant_ducklake
                .as_ref()
                .ok_or_else(|| anyhow!("postgres promotion requires a tenant DuckLake resolver"))?;
            return resolver
                .apply_telemetry_promotion_guarded(scope, manifest_yaml, target_tables, || async {
                    self.apply_telemetry_column_promotion(scope, spec)
                        .await
                        .map(|_| ())
                })
                .await;
        }
        if self.ducklake.catalog_type == "sqlite" {
            let _guard = local_apply_mutex().lock().await;
            let activation = telemetry_spec_activation(manifest_yaml, target_tables);
            return run_telemetry_apply(
                || async {
                    self.apply_telemetry_column_promotion(scope, spec)
                        .await
                        .map(|_| ())
                },
                || async { self.activate_spec_local_unlocked(scope, manifest_yaml, &activation) },
            )
            .await;
        }
        Err(anyhow!(
            "promotion specs are unsupported for catalog_type={}",
            self.ducklake.catalog_type
        ))
    }

    /// Backend-neutral guarded business-table apply (load → validate → DDL → record).
    pub async fn apply_business_promotion_guarded(
        &self,
        scope: &DuckLakeScope,
        manifest_yaml: &str,
        spec: &BusinessTableManifest,
    ) -> std::result::Result<String, BusinessApplyError> {
        if self.ducklake.catalog_type == "postgres" {
            let resolver = self.tenant_ducklake.as_ref().ok_or_else(|| {
                BusinessApplyError::Other(anyhow!(
                    "postgres promotion requires a tenant DuckLake resolver"
                ))
            })?;
            return resolver
                .apply_business_promotion_guarded(scope, manifest_yaml, spec, || async {
                    self.apply_business_table_promotion(scope, spec)
                        .await
                        .map(|_| ())
                })
                .await;
        }
        if self.ducklake.catalog_type == "sqlite" {
            let _guard = local_apply_mutex().lock().await;
            let dk = self.effective_ducklake(scope);
            let activation = business_spec_activation(&spec.target.table, manifest_yaml);
            return run_business_apply(
                spec,
                || async {
                    self.with_attached_conn(&dk, |conn| {
                        load_active_business_manifest(conn, &dk.catalog_alias, &spec.target.table)
                            .map_err(Self::map_spec_load)
                    })
                },
                || async {
                    self.apply_business_table_promotion(scope, spec)
                        .await
                        .map(|_| ())
                },
                || async { self.activate_spec_local_unlocked(scope, manifest_yaml, &activation) },
            )
            .await;
        }
        Err(BusinessApplyError::Other(anyhow!(
            "promotion specs are unsupported for catalog_type={}",
            self.ducklake.catalog_type
        )))
    }

    /// Apply additive telemetry promotion DDL inside one tenant DuckLake scope.
    ///
    /// `promotion apply` owns schema changes for promoted telemetry columns. It first materializes
    /// the hardcoded canonical telemetry tables if they do not exist, then runs the nullable
    /// `ALTER TABLE ADD COLUMN IF NOT EXISTS` statements generated from the tenant manifest.
    pub async fn apply_telemetry_column_promotion(
        &self,
        scope: &DuckLakeScope,
        spec: &TelemetryColumnsManifest,
    ) -> Result<Vec<String>> {
        let dk = self.effective_ducklake(scope);
        for table in &spec.target.tables {
            self.ensure_telemetry_table_for(&dk, table).await?;
        }
        let ddls = self.with_attached_conn(&dk, |conn| {
            let prefix = if dk.metadata_schema == "main" {
                dk.catalog_alias.clone()
            } else {
                format!(
                    "{}.{}",
                    quote_duckdb_ident(&dk.catalog_alias),
                    quote_duckdb_ident(&dk.metadata_schema)
                )
            };
            let ddls = telemetry_column_add_ddls(&prefix, spec)
                .map_err(|err| anyhow!("telemetry promotion validation failed: {err}"))?;
            for ddl in &ddls {
                conn.execute_batch(ddl)?;
            }
            Ok(ddls)
        })?;
        Ok(ddls)
    }

    /// Apply generated business table DDL inside one tenant DuckLake scope.
    ///
    /// Business promotion manifests own the physical table and current view. The runtime executes
    /// generated DDL in order so agents do not need to write tenant-specific `CREATE TABLE` SQL.
    pub async fn apply_business_table_promotion(
        &self,
        scope: &DuckLakeScope,
        spec: &BusinessTableManifest,
    ) -> Result<Vec<String>> {
        let dk = self.effective_ducklake(scope);
        let ddls = self.with_attached_conn(&dk, |conn| {
            // Prefer catalog.schema when metadata lives outside `main`; fall back to catalog-only
            // (matches write-path table name candidates when ATTACH uses METADATA_SCHEMA).
            let prefixes = if dk.metadata_schema == "main" {
                vec![dk.catalog_alias.clone()]
            } else {
                vec![
                    format!(
                        "{}.{}",
                        quote_duckdb_ident(&dk.catalog_alias),
                        quote_duckdb_ident(&dk.metadata_schema)
                    ),
                    dk.catalog_alias.clone(),
                ]
            };
            let mut last_err: Option<anyhow::Error> = None;
            for prefix in prefixes {
                let ddls = business_table_create_ddls(&prefix, spec)
                    .map_err(|err| anyhow!("business table promotion validation failed: {err}"))?;
                match ddls
                    .iter()
                    .try_for_each(|ddl| conn.execute_batch(ddl).map(|_| ()))
                {
                    Ok(()) => return Ok(ddls),
                    Err(err) => {
                        last_err = Some(anyhow!(
                            "business table promotion failed with prefix {prefix}: {err}"
                        ));
                    }
                }
            }
            Err(last_err.unwrap_or_else(|| anyhow!("business table promotion failed")))
        })?;
        Ok(ddls)
    }
}
