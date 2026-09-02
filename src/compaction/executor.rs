use crate::catalog::DropdownCatalog;
use crate::compaction::collapse::{collapse_job_1h_from_raw_sql, collapse_job_1h_sql};
use crate::compaction::downsample::{
    downsample_1h_from_5m_sql, downsample_1h_from_raw_sql, downsample_5m_sql,
    hist_downsample_1h_from_5m_for_day_sql, hist_downsample_1h_from_5m_pending_days_sql,
    hist_downsample_1h_from_raw_for_day_sql, hist_downsample_5m_for_day_sql,
    hist_downsample_5m_pending_days_sql, HIST_DOWNSAMPLE_MAX_DAYS_PER_PASS,
};
use crate::compaction::twcs::{
    closed_day_live_file_count, closed_days_need_complete_merge, day_kind,
    ducklake_merge_adjacent_files_sql, live_file_count_sql, open_day_files_for_merge,
    open_day_max_compacted_files, partition_live_file_stats_sql, plan_twcs_merges,
    should_merge_partition, DayKind, PartitionFileStats, TwcsMergePlan, TwcsPolicy,
};
use crate::config::Config;
use crate::runtime_engine::DuckLakeScopeResolver;
use crate::storage::schema::metrics_layout::ensure_metrics_layout_family_tables;
use crate::storage::schema::MAINTENANCE_METRICS_FAMILY_TABLES;
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, Utc};
use duckdb::Connection;
use std::sync::Arc;
use tracing::{info, warn};

/// Metrics-family tables compacted/expired before traces/logs/scores (AC-M1).
pub fn maintenance_metrics_family_tables() -> &'static [&'static str] {
    MAINTENANCE_METRICS_FAMILY_TABLES
}

/// Full ordered maintenance table list: metrics family first, then other telemetry.
pub fn maintenance_table_names() -> Vec<&'static str> {
    let mut tables = Vec::with_capacity(MAINTENANCE_METRICS_FAMILY_TABLES.len() + 3);
    tables.extend_from_slice(MAINTENANCE_METRICS_FAMILY_TABLES);
    tables.extend_from_slice(&["traces", "logs", "scores"]);
    tables
}

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
        self.run_pass(true).await
    }

    /// Metadata (expire + DuckLake file cleanup) always; TWCS/ladder only when
    /// `run_compaction` is true so the scheduler can expire every `A` without
    /// merging every metadata tick.
    pub async fn run_pass(&self, run_compaction: bool) -> Result<MaintenanceSummary> {
        self.run_once_ducklake(run_compaction).await
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

    async fn run_once_ducklake(&self, run_compaction: bool) -> Result<MaintenanceSummary> {
        // §7.2 pass order per tenant scope:
        // 1 ensure PARTITIONED BY / SORTED BY
        // 2 TWCS merge (metrics family first, partition-scoped plans)
        // 3–5 downsample 5m → 1h → collapse
        // 6–7 expire snapshots + orphan cleanup (once per scope)
        let tables = maintenance_table_names();
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

            // §7.2 step 1 — idempotent layout DDL for metrics family.
            let layout_catalog = crate::storage::ducklake::layout_catalog_prefix(
                &ducklake.catalog_alias,
                &ducklake.metadata_schema,
            );
            if let Err(err) = ensure_metrics_layout_family_tables(&conn, &layout_catalog) {
                warn!(
                    "Maintenance ensure layout tables failed ({}): {}",
                    label, err
                );
            }

            let mut compact_status: std::collections::HashMap<String, CompactionStatus> =
                std::collections::HashMap::new();

            if self.config.maintenance.enabled && run_compaction {
                for table in MAINTENANCE_METRICS_FAMILY_TABLES {
                    if let Err(err) = self.ducklake_flush_inlined_table(&conn, &ducklake, table) {
                        warn!(
                            "Maintenance flush inlined failed for {}.{} ({}); TWCS still runs: {}",
                            ducklake.metadata_schema, table, label, err
                        );
                    }
                    let status = match self.ducklake_twcs_compact_table(&conn, &ducklake, table) {
                        Ok(s) => s,
                        Err(err) => {
                            warn!(
                                "Maintenance TWCS merge failed for {}.{} ({}): {}",
                                ducklake.metadata_schema, table, label, err
                            );
                            CompactionStatus::Skipped
                        }
                    };
                    compact_status.insert((*table).to_string(), status);
                }

                if let Err(err) = self.run_metrics_ladder(&conn, &ducklake) {
                    warn!(
                        "Maintenance downsample/collapse ladder failed ({}): {}",
                        label, err
                    );
                }

                // Metrics-layout demos have no traces/logs/scores tables.
                // Only compact when the table exists so we do not ERROR/spam every
                // minute and contend with PromQL (Grafana 100ms SLO).
                for table in ["traces", "logs", "scores"] {
                    let status = if self
                        .ducklake_table_exists(&conn, &ducklake, table)
                        .unwrap_or(false)
                    {
                        match self.ducklake_compact_table(&conn, &ducklake, table) {
                            Ok(s) => s,
                            Err(err) => {
                                warn!(
                                    "Maintenance compaction failed for {}.{} ({}): {}",
                                    ducklake.metadata_schema, table, label, err
                                );
                                CompactionStatus::Skipped
                            }
                        }
                    } else {
                        CompactionStatus::Skipped
                    };
                    compact_status.insert(table.to_string(), status);
                }
            }

            // Expire + orphan cleanup once per scope (not once per table).
            let (metadata, remove_orphan_files) =
                self.run_scope_metadata_cleanup(&conn, &ducklake, &label);

            for table in &tables {
                let table_ident = format!("{}.{}", ducklake.metadata_schema, table);
                let compaction = CompactionResult {
                    status: if self.config.maintenance.enabled {
                        compact_status
                            .get(*table)
                            .cloned()
                            .unwrap_or(CompactionStatus::Skipped)
                    } else {
                        CompactionStatus::Skipped
                    },
                };
                results.push(TableMaintenanceResult {
                    table: table_ident,
                    metadata: metadata.clone(),
                    compaction,
                    rewrite_manifests: ActionResult {
                        status: ActionStatus::Unsupported,
                    },
                    remove_orphan_files: remove_orphan_files.clone(),
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

    fn run_scope_metadata_cleanup(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        label: &str,
    ) -> (MetadataMaintenanceResult, ActionResult) {
        let metadata = if self.config.maintenance.metadata_enabled {
            match self.ducklake_expire_snapshots(conn, ducklake) {
                Ok(expired) => MetadataMaintenanceResult {
                    expired_snapshots: expired,
                    skipped: false,
                },
                Err(err) => {
                    warn!("Maintenance metadata failed ({}): {}", label, err);
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
            match self.ducklake_cleanup_files(conn, ducklake) {
                Ok(()) => ActionResult {
                    status: ActionStatus::Completed,
                },
                Err(err) => {
                    warn!("Maintenance orphan cleanup failed ({}): {}", label, err);
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
        (metadata, remove_orphan_files)
    }

    /// §7.2 steps 3–5: incremental 5m → 1h → collapse (AC-S2 / AC-M2).
    fn run_metrics_ladder(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let catalog = crate::storage::ducklake::layout_catalog_prefix(
            &ducklake.catalog_alias,
            &ducklake.metadata_schema,
        );
        let run_tx = |sql: &str| -> Result<()> {
            let body = sql.trim().trim_end_matches(';');
            if let Err(err) = conn.execute_batch(&format!("BEGIN TRANSACTION;\n{body};\nCOMMIT;")) {
                let _ = conn.execute_batch("ROLLBACK;");
                return Err(err.into());
            }
            Ok(())
        };
        let run_step = |label: &str, sql: &str| {
            if let Err(err) = run_tx(sql) {
                warn!(
                    "Metrics ladder step {} soft-failed (will try fallback if any): {}",
                    label, err
                );
                return Err(err);
            }
            Ok(())
        };

        let _ = run_step("downsample_5m", &downsample_5m_sql(&catalog));

        self.run_hist_downsample_5m_batched(conn, &catalog, &run_step);

        if run_step(
            "downsample_1h_from_5m",
            &downsample_1h_from_5m_sql(&catalog),
        )
        .is_err()
        {
            let fb = downsample_1h_from_raw_sql(&catalog);
            if let Err(err2) = run_tx(&fb) {
                warn!("downsample_1h_from_raw fallback failed: {}", err2);
            }
        }

        if self
            .run_hist_downsample_1h_from_5m_batched(conn, &catalog, &run_step)
            .is_err()
        {
            self.run_hist_downsample_1h_from_raw_batched(conn, &catalog, &run_step);
        }

        if run_step("collapse_job_1h", &collapse_job_1h_sql(&catalog)).is_err() {
            let fb = collapse_job_1h_from_raw_sql(&catalog);
            if let Err(err2) = run_tx(&fb) {
                warn!("collapse_job_1h_from_raw fallback failed: {}", err2);
            }
        }
        Ok(())
    }

    fn query_pending_downsample_days(
        &self,
        conn: &Connection,
        sql: &str,
    ) -> Result<Vec<NaiveDate>> {
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            let raw: String = row.get(0)?;
            Ok(raw)
        })?;
        let mut days = Vec::new();
        for row in rows {
            let raw = row?;
            days.push(
                NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d").map_err(|e| {
                    anyhow!("invalid record_date {raw:?} from downsample pending probe: {e}")
                })?,
            );
        }
        Ok(days)
    }

    fn run_hist_downsample_5m_batched(
        &self,
        conn: &Connection,
        catalog: &str,
        run_step: &dyn Fn(&str, &str) -> Result<()>,
    ) {
        let pending =
            hist_downsample_5m_pending_days_sql(catalog, HIST_DOWNSAMPLE_MAX_DAYS_PER_PASS);
        let days = match self.query_pending_downsample_days(conn, &pending) {
            Ok(d) => d,
            Err(err) => {
                warn!("hist_downsample_5m pending-day probe failed: {}", err);
                return;
            }
        };
        if days.is_empty() {
            return;
        }
        for day in days {
            let label = format!("hist_downsample_5m[{day}]");
            let sql = hist_downsample_5m_for_day_sql(catalog, Some(day));
            if let Err(err) = run_step(&label, &sql) {
                warn!("hist_downsample_5m day {} failed: {}", day, err);
            }
        }
    }

    fn run_hist_downsample_1h_from_5m_batched(
        &self,
        conn: &Connection,
        catalog: &str,
        run_step: &dyn Fn(&str, &str) -> Result<()>,
    ) -> Result<()> {
        let pending =
            hist_downsample_1h_from_5m_pending_days_sql(catalog, HIST_DOWNSAMPLE_MAX_DAYS_PER_PASS);
        let days = self.query_pending_downsample_days(conn, &pending)?;
        if days.is_empty() {
            return Ok(());
        }
        for day in days {
            let label = format!("hist_downsample_1h_from_5m[{day}]");
            let sql = hist_downsample_1h_from_5m_for_day_sql(catalog, Some(day));
            run_step(&label, &sql)?;
        }
        Ok(())
    }

    fn run_hist_downsample_1h_from_raw_batched(
        &self,
        _conn: &Connection,
        catalog: &str,
        run_step: &dyn Fn(&str, &str) -> Result<()>,
    ) {
        let sql = hist_downsample_1h_from_raw_for_day_sql(catalog, None);
        if let Err(err) = run_step("hist_downsample_1h_from_raw", &sql) {
            warn!("hist_downsample_1h_from_raw fallback failed: {}", err);
        }
    }

    /// Materialize catalog-inlined rows to Parquet so TWCS can merge (AC-F7).
    /// Empty-vector INTERNAL from DuckLake is skippable when there is nothing to flush.
    fn ducklake_flush_inlined_table(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
    ) -> Result<()> {
        let sql = flush_inlined_sql(&ducklake.catalog_alias, &ducklake.metadata_schema, table);
        match execute_batch_with_serialization_retry(
            conn,
            &sql,
            COMPACTION_SERIALIZATION_ATTEMPTS,
            &format!(
                "ducklake_flush_inlined_data {}.{}",
                ducklake.metadata_schema, table
            ),
        ) {
            Ok(()) => Ok(()),
            Err(err) if is_skippable_empty_inlined_flush_error(&err.to_string()) => {
                info!(
                    "Maintenance flush inlined skipped for {}.{} (empty inlined data): {}",
                    ducklake.metadata_schema, table, err
                );
                Ok(())
            }
            Err(err) => Err(anyhow!(
                "DuckLake flush inlined failed for {}.{}: {}",
                ducklake.metadata_schema,
                table,
                err
            )),
        }
    }

    fn twcs_policy(&self) -> TwcsPolicy {
        TwcsPolicy::from(&self.config.maintenance)
    }

    /// TWCS-shaped merge: closed days loop until the AC-F8 file bar (high but
    /// finite cap); open day stays bounded (AC-F4 / AC-Q9).
    ///
    /// Softprobe plans which calendar days need merge. Execution is unscoped
    /// `ducklake_merge_adjacent_files` — DuckLake has no day filter API here,
    /// and merges within `PARTITIONED BY (record_date)` (T-F6).
    fn ducklake_twcs_compact_table(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
    ) -> Result<CompactionStatus> {
        let policy = self.twcs_policy();
        let today = Utc::now().date_naive();
        let mut last = CompactionStatus::Skipped;

        let initial = self
            .load_partition_stats(conn, &ducklake.catalog_alias, table)
            .unwrap_or_default();
        if initial.is_empty() {
            // Inline-only / empty stats: one merge may materialize Parquet.
            last = self.ducklake_compact_table_wave(
                conn,
                ducklake,
                table,
                policy.closed_day_max_compacted_files,
                policy.max_merge_file_size_bytes,
            )?;
        }

        last = self.twcs_compact_closed_days(conn, ducklake, table, today, last, &policy)?;
        last = self.twcs_compact_open_day(conn, ducklake, table, today, last, &policy)?;
        Ok(last)
    }

    fn twcs_compact_closed_days(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
        today: NaiveDate,
        mut last: CompactionStatus,
        policy: &TwcsPolicy,
    ) -> Result<CompactionStatus> {
        for wave in 0..policy.closed_day_max_waves {
            let partitions = self
                .load_partition_stats(conn, &ducklake.catalog_alias, table)
                .unwrap_or_default();
            if !closed_days_need_complete_merge(&partitions, today) {
                return Ok(last);
            }
            let size_pressure = partitions.iter().any(|p| {
                p.total_bytes > 0
                    && p.live_file_count > 1
                    && p.total_bytes < policy.max_merge_file_size_bytes
            });
            let actions = plan_twcs_merges(&TwcsMergePlan {
                table,
                catalog_alias: &ducklake.catalog_alias,
                schema: &ducklake.metadata_schema,
                partitions: &partitions,
                today,
                size_pressure,
                max_compacted_files: policy.closed_day_max_compacted_files,
                policy,
            });
            let files_before = closed_day_live_file_count(&partitions, today);
            info!(
                "TWCS closed-day wave {}/{}: {} day(s) need work for {}.{} ({} closed files); max_compacted_files={}",
                wave + 1,
                policy.closed_day_max_waves,
                actions.len(),
                ducklake.metadata_schema,
                table,
                files_before,
                policy.closed_day_max_compacted_files
            );
            last = self.ducklake_compact_table_wave(
                conn,
                ducklake,
                table,
                policy.closed_day_max_compacted_files,
                policy.max_merge_file_size_bytes,
            )?;
            if last != CompactionStatus::Completed {
                return Ok(last);
            }
            let files_after = closed_day_live_file_count(
                &self
                    .load_partition_stats(conn, &ducklake.catalog_alias, table)
                    .unwrap_or_default(),
                today,
            );
            if files_after >= files_before {
                info!(
                    "TWCS closed-day merge {} made no file-count progress ({}); stopping waves",
                    table, files_after
                );
                break;
            }
        }
        Ok(last)
    }

    fn twcs_compact_open_day(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
        today: NaiveDate,
        mut last: CompactionStatus,
        policy: &TwcsPolicy,
    ) -> Result<CompactionStatus> {
        for wave in 0..policy.max_waves_per_table {
            let partitions = self
                .load_partition_stats(conn, &ducklake.catalog_alias, table)
                .unwrap_or_default();
            let fallback = self
                .load_live_file_count(conn, &ducklake.catalog_alias, table)
                .ok();
            let files_before = open_day_files_for_merge(&partitions, today, fallback);
            let size_pressure = partitions.iter().any(|p| {
                p.total_bytes > 0
                    && p.live_file_count > 1
                    && p.total_bytes < policy.max_merge_file_size_bytes
            });
            let open_needs_merge = partitions.iter().any(|p| {
                day_kind(p.record_date, today) == DayKind::Open
                    && should_merge_partition(p, DayKind::Open, size_pressure, policy)
            });
            // Empty partition stats after a merge used to look like "done" and
            // stop at wave 1 while thousands of live files remained.
            if files_before <= policy.open_day_file_cap && !open_needs_merge {
                return Ok(last);
            }
            let max_compacted = open_day_max_compacted_files(files_before, policy);
            info!(
                "TWCS open-day wave {}/{}: {}.{} has {} live files (cap {}); max_compacted_files={}",
                wave + 1,
                policy.max_waves_per_table,
                ducklake.metadata_schema,
                table,
                files_before,
                policy.open_day_file_cap,
                max_compacted
            );
            last = self.ducklake_compact_table_wave(
                conn,
                ducklake,
                table,
                max_compacted,
                policy.max_merge_file_size_bytes,
            )?;
            let partitions_after = self
                .load_partition_stats(conn, &ducklake.catalog_alias, table)
                .unwrap_or_default();
            let fallback_after = self
                .load_live_file_count(conn, &ducklake.catalog_alias, table)
                .ok();
            let files_after = open_day_files_for_merge(&partitions_after, today, fallback_after);
            info!(
                "TWCS open-day wave {}/{} {}: status={:?} files {} → {}",
                wave + 1,
                policy.max_waves_per_table,
                table,
                last,
                files_before,
                files_after
            );
            if last == CompactionStatus::Unsupported {
                warn!(
                    "TWCS open-day merge {} unsupported; stopping waves at {} files",
                    table, files_after
                );
                return Ok(last);
            }
        }
        Ok(last)
    }

    fn load_live_file_count(
        &self,
        conn: &Connection,
        catalog_alias: &str,
        table: &str,
    ) -> Result<usize> {
        let sql = live_file_count_sql(catalog_alias, table);
        let n: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
        Ok(n.max(0) as usize)
    }

    fn load_partition_stats(
        &self,
        conn: &Connection,
        catalog_alias: &str,
        table: &str,
    ) -> Result<Vec<PartitionFileStats>> {
        let sql = partition_live_file_stats_sql(catalog_alias, table);
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            let date_str: String = row.get(0)?;
            let record_date = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d").map_err(|e| {
                duckdb::Error::FromSqlConversionFailure(0, duckdb::types::Type::Text, Box::new(e))
            })?;
            let live_file_count: i64 = row.get(1)?;
            let total_bytes: i64 = row.get(2)?;
            Ok(PartitionFileStats {
                record_date,
                live_file_count: live_file_count.max(0) as usize,
                total_bytes: total_bytes.max(0) as u64,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn ducklake_compact_table_wave(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
        max_compacted_files: u64,
        max_file_size_bytes: u64,
    ) -> Result<CompactionStatus> {
        let policy = self.twcs_policy();
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
        let sql = ducklake_merge_adjacent_files_sql(
            &ducklake.catalog_alias,
            table,
            &ducklake.metadata_schema,
            Some(max_compacted_files),
            Some(max_file_size_bytes),
        );
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
                    warn!(
                        "DuckLake merge unsupported for {} (max_compacted_files={}): {}",
                        qualified, max_compacted_files, err
                    );
                    return Ok(CompactionStatus::Unsupported);
                }
                Err(err)
                    if is_ducklake_oom(&err)
                        && max_compacted_files > policy.max_compacted_files_per_wave =>
                {
                    warn!(
                        "DuckLake compaction OOM for {} at max_compacted_files={}; retrying with {}",
                        qualified, max_compacted_files, policy.max_compacted_files_per_wave
                    );
                    return self.ducklake_compact_table_wave(
                        conn,
                        ducklake,
                        table,
                        policy.max_compacted_files_per_wave,
                        max_file_size_bytes,
                    );
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
        if let Err(err) = crate::storage::ducklake::configure_duckdb_resources(
            &conn,
            crate::storage::ducklake::COMPACTION_DUCKDB_THREADS,
            crate::storage::ducklake::COMPACTION_DUCKDB_MEMORY,
        ) {
            warn!("Failed to cap DuckDB compaction threads/memory: {}", err);
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

    fn ducklake_table_exists(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
    ) -> Result<bool> {
        let qualified = crate::storage::ducklake::ducklake_qualified_table_name(ducklake, table);
        let sql = format!("SELECT 1 FROM {qualified} LIMIT 0;");
        Ok(conn.execute_batch(&sql).is_ok())
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
        let policy = self.twcs_policy();
        let sql = ducklake_merge_adjacent_files_sql(
            &ducklake.catalog_alias,
            table,
            &ducklake.metadata_schema,
            None,
            Some(policy.max_merge_file_size_bytes),
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
        let age_seconds = self.config.maintenance.max_snapshot_age_seconds;
        let dry_run_sql = expire_snapshots_sql(&ducklake.catalog_alias, age_seconds, true);
        let planned = count_returned_rows(conn, &dry_run_sql)?;
        let sql = expire_snapshots_sql(&ducklake.catalog_alias, age_seconds, false);
        conn.execute_batch(&sql)?;
        Ok(planned)
    }

    fn ducklake_cleanup_files(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let age = self.config.maintenance.remove_orphan_older_than_seconds;
        // Only drain ducklake_files_scheduled_for_deletion. Do NOT CALL
        // ducklake_delete_orphaned_files here: with hive_file_pattern it treats
        // live tenant parquet as untracked and deletes them while catalog rows
        // remain, so Prom/Grafana fail with "Cannot open file".
        conn.execute_batch(&cleanup_old_files_sql(&ducklake.catalog_alias, age))?;
        Ok(())
    }
}

pub(crate) fn flush_inlined_sql(catalog_alias: &str, schema: &str, table: &str) -> String {
    format!(
        "CALL ducklake_flush_inlined_data('{catalog_alias}', schema_name => '{schema}', table_name => '{table}');"
    )
}

/// DuckLake INTERNAL empty-vector on `ducklake_flush_inlined_data` when the
/// table has nothing left to flush (AC-F7). Do not treat other flush errors
/// as skippable — those can hide leftover inlined bytes.
pub(crate) fn is_skippable_empty_inlined_flush_error(message: &str) -> bool {
    let m = message.to_lowercase();
    m.contains("attempted to access index 0 within vector of size 0")
}

/// AC-N6: after a maintenance pass, live `ducklake_snapshot` count must be ≤ this.
pub const SNAPSHOT_COUNT_BAR_AFTER_PASS: usize = 50;

/// AC-N6 age bar: no live snapshot older than `A + I`.
pub fn snapshot_max_age_after_pass_seconds(
    max_snapshot_age_seconds: u64,
    metadata_interval_seconds: u64,
) -> u64 {
    max_snapshot_age_seconds.saturating_add(metadata_interval_seconds)
}

/// DuckLake `older_than` interval from an age in seconds (no day flooring).
fn ducklake_older_than_interval(age_seconds: u64) -> String {
    format!("INTERVAL '{} seconds'", age_seconds)
}

pub(crate) fn expire_snapshots_sql(
    catalog_alias: &str,
    max_snapshot_age_seconds: u64,
    dry_run: bool,
) -> String {
    let interval = ducklake_older_than_interval(max_snapshot_age_seconds);
    // older_than is TIMESTAMP WITH TIME ZONE — use now(), not CAST(... AS TIMESTAMP).
    if dry_run {
        format!(
            "CALL ducklake_expire_snapshots('{}', dry_run => true, older_than => now() - {});",
            catalog_alias, interval
        )
    } else {
        format!(
            "CALL ducklake_expire_snapshots('{}', older_than => now() - {});",
            catalog_alias, interval
        )
    }
}

pub(crate) fn cleanup_old_files_sql(catalog_alias: &str, older_than_seconds: u64) -> String {
    ducklake_file_cleanup_sql(
        "ducklake_cleanup_old_files",
        catalog_alias,
        older_than_seconds,
    )
}

/// Untracked parquet on the data path (`ducklake_delete_orphaned_files`).
/// Not invoked by the scheduler — hive live files look untracked (see cleanup).
#[allow(dead_code)]
pub(crate) fn delete_orphaned_files_sql(catalog_alias: &str, older_than_seconds: u64) -> String {
    ducklake_file_cleanup_sql(
        "ducklake_delete_orphaned_files",
        catalog_alias,
        older_than_seconds,
    )
}

fn ducklake_file_cleanup_sql(
    function: &str,
    catalog_alias: &str,
    older_than_seconds: u64,
) -> String {
    if older_than_seconds == 0 {
        format!("CALL {function}('{catalog_alias}', cleanup_all => true);")
    } else {
        let interval = ducklake_older_than_interval(older_than_seconds);
        format!("CALL {function}('{catalog_alias}', older_than => now() - {interval});")
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

fn is_ducklake_oom(err: &duckdb::Error) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("out of memory")
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
        let tables = maintenance_table_names();
        assert_eq!(tables[0], "metric_samples");
        assert!(tables.contains(&"traces"));
        assert!(
            tables.iter().position(|t| *t == "metric_samples").unwrap()
                < tables.iter().position(|t| *t == "traces").unwrap()
        );
    }

    #[test]
    fn maintenance_tables_include_metric_family() {
        assert_eq!(
            maintenance_metrics_family_tables(),
            &[
                "metric_samples",
                "metric_postings",
                "metric_series",
                "metric_hist_samples",
                "metric_samples_5m",
                "metric_samples_1h",
                "metric_hist_samples_5m",
                "metric_hist_samples_1h",
                "metric_collapse_job_1h",
            ]
        );
        assert!(!maintenance_metrics_family_tables().contains(&"metrics"));
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
        assert_eq!(COMPACTION_SERIALIZATION_ATTEMPTS, 8);
    }

    /// AC-F7: flush leftover inlined skinny rows before TWCS merge.
    #[test]
    fn flush_inlined_sql_targets_schema_and_table() {
        let sql = flush_inlined_sql("softprobe", "main", "metric_samples");
        assert!(sql.contains("ducklake_flush_inlined_data"));
        assert!(sql.contains("schema_name => 'main'"));
        assert!(sql.contains("table_name => 'metric_samples'"));
    }

    /// AC-F7: DuckLake INTERNAL empty-vector is skippable; real flush errors are not.
    #[test]
    fn flush_inlined_empty_vector_error_is_skippable() {
        assert!(is_skippable_empty_inlined_flush_error(
            r#"Invalid Input Error: INTERNAL Error: Attempted to access index 0 within vector of size 0"#
        ));
        assert!(is_skippable_empty_inlined_flush_error(
            "INTERNAL Error: Attempted to access index 0 within vector of size 0"
        ));
        assert!(!is_skippable_empty_inlined_flush_error(
            "IO Error: could not write parquet"
        ));
        assert!(!is_skippable_empty_inlined_flush_error(
            "could not serialize access due to concurrent update"
        ));
        assert!(!is_skippable_empty_inlined_flush_error(
            "INTERNAL Error: unexpected catalog conflict"
        ));
        assert!(!is_skippable_empty_inlined_flush_error(""));
    }

    /// AC-N2 / T-N2: 3600s must become a seconds interval, not `INTERVAL '1 days'`.
    #[test]
    fn expire_snapshots_sql_honors_seconds() {
        let dry = expire_snapshots_sql("softprobe", 3600, true);
        let live = expire_snapshots_sql("softprobe", 3600, false);
        for sql in [&dry, &live] {
            assert!(
                sql.contains("INTERVAL '3600 seconds'"),
                "expected seconds interval, got: {sql}"
            );
            assert!(
                !sql.contains("days"),
                "must not day-floor snapshot expiry: {sql}"
            );
        }
        assert!(dry.contains("dry_run => true"));
        assert!(!live.contains("dry_run"));

        // Strengthen AC-N2/N5: sub-hour ages stay in seconds (no day floor).
        for age in [60u64, 1u64] {
            let sql = expire_snapshots_sql("softprobe", age, false);
            assert!(
                sql.contains(&format!("INTERVAL '{age} seconds'")),
                "expected INTERVAL '{age} seconds', got: {sql}"
            );
            assert!(!sql.contains("days"), "must not contain days: {sql}");
        }
    }

    /// AC-N6 / T-N6: expire uses A seconds; count bar is 50; remaining age < A+I.
    #[test]
    fn expire_snapshots_sql_honors_n6_count_and_age_bars() {
        let cfg = crate::config::Config::default();
        assert_eq!(cfg.maintenance.max_snapshot_age_seconds, 60);
        assert_eq!(cfg.maintenance.metadata_interval_seconds, 60);
        assert_eq!(SNAPSHOT_COUNT_BAR_AFTER_PASS, 50);
        assert_eq!(
            snapshot_max_age_after_pass_seconds(
                cfg.maintenance.max_snapshot_age_seconds,
                cfg.maintenance.metadata_interval_seconds,
            ),
            120
        );
        let sql =
            expire_snapshots_sql("softprobe", cfg.maintenance.max_snapshot_age_seconds, false);
        assert!(
            sql.contains("INTERVAL '60 seconds'"),
            "AC-N6 expiry must use A=60 seconds, got: {sql}"
        );
        assert!(
            !sql.contains("days"),
            "must not day-floor snapshot expiry: {sql}"
        );
        assert!(
            cfg.maintenance.max_snapshot_age_seconds < 3600,
            "AC-N1/N6: default A must stay 60s, not 1h"
        );
    }

    /// AC-N5 / T-N5: orphan cleanup older_than uses seconds, not day floor.
    #[test]
    fn cleanup_old_files_sql_honors_seconds() {
        let sql = cleanup_old_files_sql("softprobe", 3600);
        assert!(
            sql.contains("INTERVAL '3600 seconds'"),
            "expected seconds interval, got: {sql}"
        );
        assert!(
            !sql.contains("days"),
            "must not day-floor orphan cleanup: {sql}"
        );
        let all = cleanup_old_files_sql("softprobe", 0);
        assert!(all.contains("cleanup_all => true"));
        assert!(!all.contains("older_than"));

        for age in [60u64, 1u64] {
            let sql = cleanup_old_files_sql("softprobe", age);
            assert!(
                sql.contains(&format!("INTERVAL '{age} seconds'")),
                "expected INTERVAL '{age} seconds', got: {sql}"
            );
            assert!(!sql.contains("days"), "must not contain days: {sql}");
        }
    }

    /// Maintenance must use scheduled-file cleanup only — never the orphan
    /// sweeper (hive live files look untracked and get deleted).
    #[test]
    fn maintenance_file_cleanup_is_scheduled_only() {
        let scheduled = cleanup_old_files_sql("softprobe", 60);
        let orphan = delete_orphaned_files_sql("softprobe", 60);
        assert!(scheduled.contains("ducklake_cleanup_old_files"));
        assert!(!scheduled.contains("delete_orphaned"));
        assert!(
            orphan.contains("ducklake_delete_orphaned_files"),
            "helper exists for manual/ops use, not the scheduler"
        );
    }

    /// DuckLake leaves untracked parquet until this CALL (not automatic).
    #[test]
    fn delete_orphaned_files_sql_honors_seconds() {
        let sql = delete_orphaned_files_sql("softprobe", 3600);
        assert!(
            sql.contains("ducklake_delete_orphaned_files"),
            "expected DuckLake orphan API, got: {sql}"
        );
        assert!(
            sql.contains("INTERVAL '3600 seconds'"),
            "expected seconds interval, got: {sql}"
        );
        assert!(
            !sql.contains("days"),
            "must not day-floor orphan delete: {sql}"
        );
        let all = delete_orphaned_files_sql("softprobe", 0);
        assert!(all.contains("cleanup_all => true"));
        assert!(!all.contains("older_than"));
        for age in [60u64, 1u64] {
            let sql = delete_orphaned_files_sql("softprobe", age);
            assert!(
                sql.contains(&format!("INTERVAL '{age} seconds'")),
                "expected INTERVAL '{age} seconds', got: {sql}"
            );
            assert!(!sql.contains("days"), "must not contain days: {sql}");
        }
    }
}
