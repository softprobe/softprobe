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

/// Ensure log timestamps use DuckDB's nanosecond timestamp type.
///
/// Origin/v0.2 tables may have timezone-bearing microsecond columns. Migrate those
/// columns in-place using their exact representable epoch nanoseconds. Any other
/// schema is refused before issuing DDL so a legacy table cannot silently truncate
/// the new Loki nanosecond contract.
pub(super) fn ensure_log_timestamp_precision(
    conn: &Connection,
    qualified_table: &str,
) -> Result<()> {
    ensure_timestamp_precision(
        conn,
        qualified_table,
        &["timestamp", "observed_timestamp"],
        "log",
    )
}

pub(super) fn ensure_trace_timestamp_precision(
    conn: &Connection,
    qualified_table: &str,
) -> Result<()> {
    ensure_timestamp_precision(
        conn,
        qualified_table,
        &["timestamp", "end_timestamp"],
        "trace",
    )
}

fn ensure_timestamp_precision(
    conn: &Connection,
    qualified_table: &str,
    columns: &[&str],
    kind: &str,
) -> Result<()> {
    let found = describe_columns(conn, qualified_table)?;
    let mut ddls = Vec::new();

    for &column in columns {
        let Some(dtype) = found.get(column) else {
            return Err(anyhow!(
                "table {qualified_table} cannot safely migrate {kind} timestamps: missing column '{column}'"
            ));
        };
        let normalized = dtype.to_ascii_uppercase();
        if normalized == "TIMESTAMP_NS" {
            continue;
        }
        if !matches!(
            normalized.as_str(),
            "TIMESTAMP" | "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE"
        ) {
            return Err(anyhow!(
                "table {qualified_table} cannot safely migrate {kind} timestamps: column '{column}' has unsupported type {dtype}"
            ));
        }
        ddls.push(format!(
            "ALTER TABLE {qualified_table} ALTER COLUMN {column} SET DATA TYPE TIMESTAMP_NS \
             USING make_timestamp_ns(epoch_ns({column}));"
        ));
    }

    if !ddls.is_empty() {
        conn.execute_batch(&ddls.join("\n")).map_err(|e| {
            anyhow!(
                "failed to migrate {kind} timestamps on {qualified_table} to TIMESTAMP_NS; refusing write to prevent truncation: {e}"
            )
        })?;
    }

    let verified = describe_columns(conn, qualified_table)?;
    for &column in columns {
        if verified.get(column).map(|dtype| dtype.as_str()) != Some("TIMESTAMP_NS") {
            return Err(anyhow!(
                "table {qualified_table} cannot safely migrate {kind} timestamps: column '{column}' is not TIMESTAMP_NS after migration"
            ));
        }
    }
    Ok(())
}

pub(super) fn ensure_trace_fidelity_columns(
    conn: &Connection,
    qualified_table: &str,
) -> Result<()> {
    let found = describe_columns(conn, qualified_table)?;
    let ddls = [
        ("resource_attributes", "VARIANT"),
        ("instrumentation_scope", "VARIANT"),
        ("links", "VARIANT"),
    ]
    .into_iter()
    .filter(|(name, _)| !found.contains_key(*name))
    .map(|(name, sql_type)| {
        format!("ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {name} {sql_type};")
    })
    .collect::<Vec<_>>();
    if ddls.is_empty() {
        return Ok(());
    }
    conn.execute_batch(&ddls.join("\n")).map_err(|e| {
        anyhow!(
            "failed to add Tempo trace fidelity columns on {qualified_table}; refusing write: {e}"
        )
    })
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
