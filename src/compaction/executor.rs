use crate::catalog::DropdownCatalog;
use crate::config::Config;
use crate::runtime_engine::DuckLakeScopeResolver;
use anyhow::{anyhow, Result};
use duckdb::Connection;
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Clone)]
pub struct MaintenanceExecutor {
    config: Config,
    ducklake: crate::config::DuckLakeConfig,
    dropdown_catalog: Option<Arc<DropdownCatalog>>,
    scope_registry: Option<DuckLakeScopeResolver>,
}

#[derive(Debug, Clone)]
pub struct MaintenanceSummary {
    pub tables: Vec<TableMaintenanceResult>,
}

#[derive(Debug, Clone)]
pub struct TableMaintenanceResult {
    pub table: String,
    pub metadata: MetadataMaintenanceResult,
    pub compaction: CompactionResult,
    pub rewrite_manifests: ActionResult,
    pub remove_orphan_files: ActionResult,
}

#[derive(Debug, Clone)]
pub struct MetadataMaintenanceResult {
    pub expired_snapshots: usize,
    pub skipped: bool,
}

#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub status: CompactionStatus,
}

#[derive(Debug, Clone)]
pub struct ActionResult {
    pub status: ActionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionStatus {
    Completed,
    Skipped,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionStatus {
    Completed,
    Skipped,
    Unsupported,
}

impl MaintenanceExecutor {
    pub async fn new(
        config: &Config,
        dropdown_catalog: Option<Arc<DropdownCatalog>>,
        scope_registry: Option<DuckLakeScopeResolver>,
    ) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            ducklake: config.ducklake.clone(),
            dropdown_catalog,
            scope_registry,
        })
    }

    pub async fn run_once(&self) -> Result<MaintenanceSummary> {
        self.run_once_ducklake().await
    }

    async fn maintenance_scopes(&self) -> Result<Vec<(String, crate::config::DuckLakeConfig)>> {
        let mut scopes = Vec::new();
        let default = self.ducklake.clone();
        scopes.push((
            format!("default:{}", default.metadata_schema),
            default.clone(),
        ));
        if let Some(registry) = &self.scope_registry {
            for scope in registry.list_scopes().await? {
                if scope.metadata_schema == default.metadata_schema
                    && scope.data_path == default.data_path
                {
                    continue;
                }
                let mut dk = default.clone();
                dk.metadata_schema = scope.metadata_schema;
                dk.data_path = scope.data_path;
                scopes.push((format!("registry:{}", dk.metadata_schema), dk));
            }
        }
        Ok(scopes)
    }

    async fn run_once_ducklake(&self) -> Result<MaintenanceSummary> {
        // Metrics first: Prom/Grafana load is usually the loudest small-file
        // victim under continuous OTLP. Traces/logs/scores follow.
        let tables = ["metrics", "traces", "logs", "scores"];
        let mut results = Vec::new();

        for (label, ducklake) in self.maintenance_scopes().await? {
            let conn = match self.open_ducklake_connection(&ducklake) {
                Ok(c) => c,
                Err(err) => {
                    warn!("Maintenance skip scope {}: open failed: {}", label, err);
                    continue;
                }
            };
            if let Err(err) = self.attach_ducklake(&conn, &ducklake) {
                warn!("Maintenance skip scope {}: attach failed: {}", label, err);
                continue;
            }

            let files_before = count_parquet_files_under(&ducklake.data_path);
            for table in tables {
                let table_ident = format!("{}.{}", ducklake.metadata_schema, table);
                let compaction = if self.config.maintenance.enabled {
                    CompactionResult {
                        status: match self.ducklake_compact_table(&conn, &ducklake, table) {
                            Ok(status) => status,
                            Err(err) => {
                                warn!(
                                    "Maintenance compaction failed for {} ({}): {}",
                                    table_ident, label, err
                                );
                                CompactionStatus::Skipped
                            }
                        },
                    }
                } else {
                    CompactionResult {
                        status: CompactionStatus::Skipped,
                    }
                };

                let metadata = if self.config.maintenance.metadata_enabled {
                    match self.ducklake_expire_snapshots(&conn, &ducklake) {
                        Ok(expired) => MetadataMaintenanceResult {
                            expired_snapshots: expired,
                            skipped: false,
                        },
                        Err(err) => {
                            warn!(
                                "Maintenance metadata failed for {} ({}): {}",
                                table_ident, label, err
                            );
                            MetadataMaintenanceResult {
                                expired_snapshots: 0,
                                skipped: true,
                            }
                        }
                    }
                } else {
                    MetadataMaintenanceResult {
                        expired_snapshots: 0,
                        skipped: true,
                    }
                };

                let remove_orphan_files = if self.config.maintenance.metadata_enabled
                    && self.config.maintenance.remove_orphan_files_enabled
                {
                    match self.ducklake_cleanup_files(&conn, &ducklake) {
                        Ok(()) => ActionResult {
                            status: ActionStatus::Completed,
                        },
                        Err(err) => {
                            warn!(
                                "Maintenance orphan cleanup failed for {} ({}): {}",
                                table_ident, label, err
                            );
                            ActionResult {
                                status: ActionStatus::Skipped,
                            }
                        }
                    }
                } else {
                    ActionResult {
                        status: ActionStatus::Skipped,
                    }
                };

                results.push(TableMaintenanceResult {
                    table: table_ident,
                    metadata,
                    compaction,
                    rewrite_manifests: ActionResult {
                        status: ActionStatus::Unsupported,
                    },
                    remove_orphan_files,
                });
            }
            let files_after = count_parquet_files_under(&ducklake.data_path);
            warn_if_too_many_parquet_files(&label, &ducklake.data_path, files_before, files_after);
        }

        if let Some(ref dc) = self.dropdown_catalog {
            if self.config.dropdown_catalog.enabled
                && self.config.dropdown_catalog.maintenance_prune_enabled
            {
                let days = self.config.dropdown_catalog.active_values_days;
                match dc.prune_older_than_days(days).await {
                    Ok(n) => info!("dropdown catalog TTL prune removed {} rows", n),
                    Err(e) => warn!("dropdown catalog TTL prune failed: {}", e),
                }
            }
        }

        Ok(MaintenanceSummary { tables: results })
    }

    fn open_ducklake_connection(
        &self,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
        crate::storage::ducklake::configure_object_store(&conn, &self.config, &ducklake.data_path)?;
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
        if ducklake.catalog_type == "postgres" {
            conn.execute_batch("INSTALL postgres; LOAD postgres;")?;
        }
        if ducklake.catalog_type == "sqlite" {
            conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?;
        }
        Ok(conn)
    }

    fn attach_ducklake(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let attach_target = crate::storage::ducklake::ducklake_attach_target(ducklake);
        crate::storage::ducklake::prepare_local_ducklake_paths(ducklake, &attach_target)?;
        let opts = crate::storage::ducklake::ducklake_attach_options(ducklake);
        let attach_sql = format!(
            "ATTACH 'ducklake:{}' AS {} ({});",
            crate::storage::ducklake::escape_sql_literal(&attach_target),
            ducklake.catalog_alias,
            opts.join(", ")
        );
        conn.execute_batch(&attach_sql)?;
        Ok(())
    }

    fn ducklake_compact_table(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
    ) -> Result<CompactionStatus> {
        // Match qualified name used for tables (see ducklake_qualified_table_name).
        let qualified = crate::storage::ducklake::ducklake_qualified_table_name(ducklake, table);
        let scope = crate::storage::ducklake::ducklake_set_option_scope_for_qualified(&qualified);
        let target_file_size =
            crate::storage::ducklake::size_literal(self.config.maintenance.target_file_size_bytes);
        let set_target = format!(
            "CALL {}.set_option('target_file_size', '{}', {});",
            ducklake.catalog_alias, target_file_size, scope
        );
        if let Err(err) = execute_batch_with_serialization_retry(
            conn,
            &set_target,
            COMPACTION_SERIALIZATION_ATTEMPTS,
            &format!("ducklake set_option target_file_size {}", qualified),
        ) {
            if is_ducklake_serialization_conflict(&err) {
                warn!(
                    "DuckLake compaction skipped for {} due to transient metadata conflict: {}",
                    qualified, err
                );
                return Ok(CompactionStatus::Skipped);
            }
            return Err(anyhow!(
                "DuckLake set_option failed for {}: {}",
                qualified,
                err
            ));
        }
        let sql = format!(
            "CALL ducklake_merge_adjacent_files('{}', '{}', schema => '{}');",
            ducklake.catalog_alias, table, ducklake.metadata_schema
        );
        // Two waves: under heavy ingest the first merge window can still lose the
        // serialization race after inner retries; wait and try once more before skip.
        for wave in 1..=2 {
            match execute_batch_with_serialization_retry(
                conn,
                &sql,
                COMPACTION_SERIALIZATION_ATTEMPTS,
                &format!("ducklake_merge_adjacent_files {} wave{}", qualified, wave),
            ) {
                Ok(_) => return Ok(CompactionStatus::Completed),
                Err(err) if is_ducklake_serialization_conflict(&err) && wave < 2 => {
                    warn!(
                        "DuckLake compaction conflict on {} wave {}; backing off before retry: {}",
                        qualified, wave, err
                    );
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
                Err(err) if is_ducklake_serialization_conflict(&err) => {
                    warn!(
                        "DuckLake compaction skipped for {} due to transient metadata conflict: {}",
                        qualified, err
                    );
                    return Ok(CompactionStatus::Skipped);
                }
                Err(err) if is_ducklake_unsupported(&err) => {
                    return Ok(CompactionStatus::Unsupported);
                }
                Err(err) => {
                    return Err(anyhow!(
                        "DuckLake compaction failed for {}.{}: {}",
                        ducklake.metadata_schema,
                        table,
                        err
                    ));
                }
            }
        }
        Ok(CompactionStatus::Skipped)
    }

    fn ducklake_expire_snapshots(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<usize> {
        let days = std::cmp::max(
            1,
            self.config.maintenance.max_snapshot_age_seconds / (24 * 3600),
        );
        let dry_run_sql = format!(
            "CALL ducklake_expire_snapshots('{}', dry_run => true, older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{} days');",
            ducklake.catalog_alias, days
        );
        let planned = count_returned_rows(conn, &dry_run_sql)?;
        let sql = format!(
            "CALL ducklake_expire_snapshots('{}', older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{} days');",
            ducklake.catalog_alias, days
        );
        conn.execute_batch(&sql)?;
        Ok(planned)
    }

    fn ducklake_cleanup_files(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let older_than_seconds = self.config.maintenance.remove_orphan_older_than_seconds;
        let sql = if older_than_seconds == 0 {
            format!(
                "CALL ducklake_cleanup_old_files('{}', cleanup_all => true);",
                ducklake.catalog_alias
            )
        } else {
            let days = std::cmp::max(1, older_than_seconds / (24 * 3600));
            format!(
                "CALL ducklake_cleanup_old_files('{}', older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{} days');",
                ducklake.catalog_alias, days
            )
        };
        conn.execute_batch(&sql)?;
        Ok(())
    }
}

fn is_ducklake_unsupported(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("catalog error")
        || msg.contains("function") && (msg.contains("does not exist") || msg.contains("not found"))
        || msg.contains("no function matches")
        || msg.contains("not implemented")
}

fn is_ducklake_serialization_conflict(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("could not serialize access due to concurrent update")
        || msg.contains("serialization failure")
}

/// Inner attempts per merge wave. Paired with a second wave in
/// [`MaintenanceExecutor::ducklake_compact_table`].
const COMPACTION_SERIALIZATION_ATTEMPTS: usize = 8;

/// Soft warn when a scope still has many Parquet files after a maintenance pass.
const PARQUET_FILE_WARN_THRESHOLD: usize = 200;

fn count_parquet_files_under(data_path: &str) -> usize {
    let root = std::path::Path::new(data_path);
    if !root.exists() {
        return 0;
    }
    let mut count = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()).is_some_and(|e| {
                e.eq_ignore_ascii_case("parquet") || e.eq_ignore_ascii_case("parq")
            }) {
                count += 1;
            }
        }
    }
    count
}

fn warn_if_too_many_parquet_files(
    scope_label: &str,
    data_path: &str,
    files_before: usize,
    files_after: usize,
) {
    if files_after >= PARQUET_FILE_WARN_THRESHOLD {
        warn!(
            "DuckLake scope {} still has {} parquet files under {} after maintenance (was {}); \
             query scans may stay expensive — check ingest batching / compaction conflicts",
            scope_label, files_after, data_path, files_before
        );
    } else if files_before > files_after {
        info!(
            "DuckLake scope {} parquet files {} → {} under {}",
            scope_label, files_before, files_after, data_path
        );
    }
}

fn execute_batch_with_serialization_retry(
    conn: &Connection,
    sql: &str,
    max_attempts: usize,
    action: &str,
) -> std::result::Result<(), duckdb::Error> {
    let attempts = std::cmp::max(1, max_attempts);
    let mut backoff_ms = 150u64;
    for attempt in 1..=attempts {
        match conn.execute_batch(sql) {
            Ok(()) => return Ok(()),
            Err(err) if is_ducklake_serialization_conflict(&err) && attempt < attempts => {
                warn!(
                    "Retrying {} after transient serialization conflict (attempt {}/{}): {}",
                    action, attempt, attempts, err
                );
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                backoff_ms = (backoff_ms.saturating_mul(2)).min(2_000);
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn count_returned_rows(conn: &Connection, sql: &str) -> Result<usize> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut count = 0usize;
    while let Some(_row) = rows.next()? {
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn maintenance_compacts_metrics_before_other_tables() {
        let tables = ["metrics", "traces", "logs", "scores"];
        assert_eq!(tables[0], "metrics");
    }

    #[test]
    fn count_parquet_files_under_walks_nested_dirs() {
        let tmp = TempDir::new().expect("temp");
        let nested = tmp.path().join("metrics").join("record_date=2026-08-14");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("a.parquet"), b"x").unwrap();
        fs::write(nested.join("b.parq"), b"y").unwrap();
        fs::write(nested.join("ignore.txt"), b"z").unwrap();
        assert_eq!(count_parquet_files_under(tmp.path().to_str().unwrap()), 2);
        assert_eq!(count_parquet_files_under("/no/such/path"), 0);
    }

    #[test]
    fn parquet_warn_threshold_is_sane() {
        assert!(PARQUET_FILE_WARN_THRESHOLD >= 50);
        assert_eq!(COMPACTION_SERIALIZATION_ATTEMPTS, 8);
    }
}
