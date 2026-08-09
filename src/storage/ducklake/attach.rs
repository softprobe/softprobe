use crate::config::DuckLakeConfig;
use anyhow::Result;
use duckdb::Connection;

use super::util::escape_sql_literal;

pub(super) fn catalog_is_attached(conn: &Connection, alias: &str) -> bool {
    let sql = format!(
        "SELECT 1 FROM duckdb_databases() WHERE database_name = '{}' LIMIT 1;",
        escape_sql_literal(alias)
    );
    conn.query_row(&sql, [], |_| Ok(())).is_ok()
}

/// Pin DuckLake extension conflict-retry defaults (official concurrent-write mechanism).
pub(super) fn apply_ducklake_retry_settings(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "SET ducklake_max_retry_count = 10;\n\
         SET ducklake_retry_backoff = 1.5;\n\
         SET ducklake_retry_wait_ms = 100;",
    )?;
    Ok(())
}

pub(crate) fn ducklake_attach_target(dk: &DuckLakeConfig) -> String {
    match dk.catalog_type.as_str() {
        "postgres" => {
            if dk.metadata_path.starts_with("postgres:") {
                dk.metadata_path.clone()
            } else {
                format!("postgres:{}", dk.metadata_path)
            }
        }
        "sqlite" => {
            if dk.metadata_path.starts_with("sqlite:") {
                dk.metadata_path.clone()
            } else {
                format!("sqlite:{}", dk.metadata_path)
            }
        }
        _ => dk.metadata_path.clone(),
    }
}

/// ATTACH options shared by writer / query / compaction.
pub(crate) fn ducklake_attach_options(dk: &DuckLakeConfig) -> Vec<String> {
    let mut options = vec![format!("DATA_PATH '{}'", escape_sql_literal(&dk.data_path))];
    if dk.catalog_type == "postgres" && dk.metadata_schema != "main" {
        let schema = escape_sql_literal(&dk.metadata_schema);
        options.push(format!("METADATA_SCHEMA '{}'", schema));
        options.push(format!("META_SCHEMA '{}'", schema));
    }
    // Official SQLite multi-client guidance: WAL + busy timeout (DuckLake / sqlite extension).
    // 5s absorbs concurrent query-worker ATTACH / snapshot races better than 500ms.
    if dk.catalog_type == "sqlite" {
        options.push("META_JOURNAL_MODE 'WAL'".to_string());
        options.push("META_BUSY_TIMEOUT 5000".to_string());
    }
    if let Some(limit) = dk.data_inlining_row_limit {
        options.push(format!("DATA_INLINING_ROW_LIMIT {}", limit));
    }
    options
}

/// Ensure local filesystem paths exist before ATTACH.
///
/// SQLite catalogs need the metadata DB parent directory. Local (non-URI)
/// `DATA_PATH` must exist for both sqlite and postgres catalogs so DuckLake can
/// create files under it.
pub(crate) fn prepare_local_ducklake_paths(
    dk: &DuckLakeConfig,
    attach_target: &str,
) -> Result<()> {
    if dk.catalog_type == "sqlite" {
        let raw = attach_target
            .strip_prefix("sqlite:")
            .unwrap_or(attach_target);
        let metadata_path = std::path::PathBuf::from(raw);
        if let Some(parent) = metadata_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
    }
    if !dk.data_path.contains("://") {
        std::fs::create_dir_all(&dk.data_path)?;
    }
    Ok(())
}

/// Fully qualified DuckLake table name used for CREATE / INSERT (`catalog.table` when
/// `metadata_schema` is `main`, else `catalog.metadata_schema.table`).
pub(crate) fn ducklake_qualified_table_name(cfg: &DuckLakeConfig, bare_table: &str) -> String {
    if cfg.metadata_schema == "main" {
        format!("{}.{}", cfg.catalog_alias, bare_table)
    } else {
        format!(
            "{}.{}.{}",
            cfg.catalog_alias, cfg.metadata_schema, bare_table
        )
    }
}

/// Scoping clause for `CALL <catalog>.set_option(...)` matching a qualified table name.
/// Two-part `catalog.table` → `table_name` only; three-part → `schema` + `table_name`.
pub(crate) fn ducklake_set_option_scope_for_qualified(qualified_table: &str) -> String {
    let parts: Vec<&str> = qualified_table.split('.').collect();
    match parts.len() {
        3 => {
            let s = escape_sql_literal(parts[1]);
            let t = escape_sql_literal(parts[2]);
            format!("schema => '{s}', table_name => '{t}'")
        }
        2 => {
            let t = escape_sql_literal(parts[1]);
            format!("table_name => '{t}'")
        }
        _ => {
            let t = escape_sql_literal(parts.last().copied().unwrap_or(""));
            format!("table_name => '{t}'")
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_option_scope_matches_qualified_table_shape() {
        assert_eq!(
            ducklake_set_option_scope_for_qualified("softprobe.traces"),
            "table_name => 'traces'"
        );
        assert_eq!(
            ducklake_set_option_scope_for_qualified("softprobe.tenant_a.traces"),
            "schema => 'tenant_a', table_name => 'traces'"
        );
    }

}
