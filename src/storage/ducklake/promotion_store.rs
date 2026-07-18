//! Single-scope local promotion_specs store for SQLite DuckLake catalogs.
//!
//! Specs persist as `{catalog_alias}.promotion_specs` through the writer's attached DuckDB
//! connection. Apply is serialized with a process-global mutex (required because each tenant id
//! gets its own RuntimeEngine/writer pointing at the same SQLite file).

use crate::promotion::{
    business_manifest_from_row, local_promotion_specs_table_ddl, telemetry_manifest_from_row,
    BusinessTableManifest, PromotionSpecActivation, PromotionSpecLoadError,
    TelemetryColumnsManifest,
};
use anyhow::Result;
use duckdb::Connection;
use std::sync::OnceLock;
use tokio::sync::Mutex as AsyncMutex;

/// Process-global serialize for local promotion apply (cross-writer, same SQLite file).
pub fn local_apply_mutex() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

fn quote_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
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
pub fn load_active_telemetry_manifests(
    conn: &Connection,
    catalog_alias: &str,
) -> Result<Vec<TelemetryColumnsManifest>, PromotionSpecLoadError> {
    let catalog = quote_ident(catalog_alias);
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
pub fn load_active_business_manifest(
    conn: &Connection,
    catalog_alias: &str,
    table_name: &str,
) -> Result<Option<BusinessTableManifest>, PromotionSpecLoadError> {
    let catalog = quote_ident(catalog_alias);
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
pub fn record_active_spec(
    conn: &Connection,
    catalog_alias: &str,
    manifest_yaml: &str,
    activation: &PromotionSpecActivation,
) -> Result<String> {
    ensure_specs_table(conn, catalog_alias)?;
    let catalog = quote_ident(catalog_alias);
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
