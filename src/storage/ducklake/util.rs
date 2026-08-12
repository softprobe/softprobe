use crate::storage::schema::variant::hot_variant_columns;
use anyhow::{anyhow, Result};
use duckdb::Connection;
use std::collections::HashMap;
use tracing::warn;

pub(crate) fn escape_sql_literal(input: &str) -> String {
    input.replace('\'', "''")
}

/// Write attempt outcome: only qualification/engine errors may try the next table name candidate.
pub(super) enum WriteAttemptError {
    /// Schema incompatibility (legacy MAP, missing VARIANT column) — fail the write immediately.
    Fatal(anyhow::Error),
    /// Likely three-part name / catalog issues — try `catalog.table` fallback.
    Retryable(anyhow::Error),
}

impl WriteAttemptError {
    pub(super) fn from_variant_guard(err: anyhow::Error) -> Self {
        let msg = err.to_string();
        if msg.contains("expected VARIANT") || msg.contains("missing required VARIANT") {
            Self::Fatal(err)
        } else {
            // DESCRIBE / prepare failures can mean the three-part name is unsupported.
            Self::Retryable(err)
        }
    }
}

/// Fail fast when an existing DuckLake table still uses MAP for hot VARIANT columns.
pub(super) fn ensure_variant_column_types(
    conn: &Connection,
    qualified_table: &str,
    table_name: &str,
) -> Result<()> {
    let expected = hot_variant_columns(table_name);
    if expected.is_empty() {
        return Ok(());
    }
    let found = describe_columns(conn, qualified_table)?;

    for col in expected {
        let Some(dtype) = found.get(*col) else {
            return Err(anyhow!(
                "table {qualified_table} is missing required VARIANT column '{col}'"
            ));
        };
        let normalized = dtype.to_ascii_uppercase();
        if normalized != "VARIANT" {
            return Err(anyhow!(
                "table {qualified_table} column '{col}' has type {dtype}, expected VARIANT. \
                 Hot MAP columns were migrated to Iceberg/DuckLake VARIANT shredding; \
                 rebuild/migrate this DuckLake table via operations (do not auto-drop in-process), \
                 then re-ingest."
            ));
        }
    }
    Ok(())
}

/// Canonical metrics fidelity columns (Phase 0 classic histogram/summary).
/// Added with `ALTER TABLE … ADD COLUMN IF NOT EXISTS` so existing catalogs keep ingesting.
pub(super) const METRICS_FIDELITY_COLUMNS: &[(&str, &str)] = &[
    ("count", "UBIGINT"),
    ("sum", "DOUBLE"),
    ("bucket_counts", "UBIGINT[]"),
    ("explicit_bounds", "DOUBLE[]"),
    ("quantiles", "STRUCT(quantile DOUBLE, value DOUBLE)[]"),
    ("aggregation_temporality", "VARCHAR"),
    ("exemplars_json", "VARCHAR"),
];

/// Widen existing `metrics` tables with nullable fidelity columns before INSERT BY NAME.
pub(super) fn ensure_metrics_fidelity_columns(
    conn: &Connection,
    qualified_table: &str,
) -> Result<()> {
    let found = describe_columns(conn, qualified_table)?;
    let mut ddls = Vec::new();
    for (name, sql_type) in METRICS_FIDELITY_COLUMNS {
        if !found.contains_key(*name) {
            ddls.push(format!(
                "ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {name} {sql_type};"
            ));
        }
    }
    if ddls.is_empty() {
        return Ok(());
    }
    conn.execute_batch(&ddls.join("\n")).map_err(|e| {
        anyhow!(
            "failed to add metrics fidelity columns on {qualified_table}: {e}. \
             Classic histogram/summary columns are required for Grafana/Prometheus compatibility; \
             fix DDL permissions or rebuild the metrics table, then retry ingest."
        )
    })?;
    Ok(())
}

fn describe_columns(conn: &Connection, qualified_table: &str) -> Result<HashMap<String, String>> {
    let sql = format!("DESCRIBE {qualified_table};");
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| anyhow!("DESCRIBE {qualified_table} failed: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            let name: String = row.get(0)?;
            let dtype: String = row.get(1)?;
            Ok((name, dtype))
        })
        .map_err(|e| anyhow!("DESCRIBE {qualified_table} query failed: {e}"))?;

    let mut found: HashMap<String, String> = HashMap::new();
    for row in rows {
        let (name, dtype) = row.map_err(|e| anyhow!("DESCRIBE row failed: {e}"))?;
        found.insert(name, dtype);
    }
    Ok(found)
}

pub(super) fn quote_duckdb_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

pub(crate) fn size_literal(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;
    if bytes >= GB && bytes.is_multiple_of(GB) {
        format!("{}GB", bytes / GB)
    } else if bytes >= MB && bytes.is_multiple_of(MB) {
        format!("{}MB", bytes / MB)
    } else if bytes >= KB && bytes.is_multiple_of(KB) {
        format!("{}KB", bytes / KB)
    } else {
        warn!(
            "target_file_size_bytes={} is not power-of-1024 aligned; using byte literal",
            bytes
        );
        format!("{}B", bytes)
    }
}
