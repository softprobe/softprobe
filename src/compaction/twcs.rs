//! Softprobe TWCS merge policy (§7.1).
//!
//! Time window = calendar day (`record_date`). Softprobe **plans** merges per day
//! (AC-F6) and never schedules a cross-day rewrite intent. DuckLake's
//! `ducklake_merge_adjacent_files` has no `partition_filter` in the versions we
//! ship; Softprobe therefore executes a bounded unscoped CALL and **relies on
//! DuckLake partition-local merge** when the table is `PARTITIONED BY (record_date)`.
//! Integration `T-F6` proves live files stay single-day after merge; if that fails,
//! do not claim AC-F6.

use crate::config::MaintenanceConfig;
use chrono::NaiveDate;

/// Closed-day merge if more than this many live files (complete compact → 1 file).
pub const TWCS_TRIGGER_FILE_NUM: usize = 2;
/// Default open-day live file soft cap (AC-F4). Override via `MaintenanceConfig`.
pub const TWCS_OPEN_DAY_FILE_CAP: usize = 2;

/// Live Parquet stats for one `record_date` partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFileStats {
    pub record_date: NaiveDate,
    pub live_file_count: usize,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayKind {
    /// `record_date < today` — may fully merge toward target size.
    Closed,
    /// `record_date == today` — soft cap only; do not force single-file merge.
    Open,
}

/// Classify a partition relative to the maintenance "today" date.
pub fn day_kind(record_date: NaiveDate, today: NaiveDate) -> DayKind {
    if record_date < today {
        DayKind::Closed
    } else {
        DayKind::Open
    }
}

/// Whether TWCS should merge this partition on this pass (§7.1).
///
/// Closed days keep merging until the AC-F8 file bar (1 file, or 2 if that
/// day's bytes exceed 64 MiB). Open day is a soft file-count cap only (AC-F4) —
/// do **not** treat "many tiny files under 8MiB" as open-day size pressure:
/// that caused endless merge waves on the Grafana demo (CPU pegged, OTLP
/// ingest starved, PromQL queue times blew the 100ms SLO).
pub fn should_merge_partition(
    stats: &PartitionFileStats,
    kind: DayKind,
    size_pressure: bool,
    policy: &TwcsPolicy,
) -> bool {
    match kind {
        DayKind::Closed => {
            !closed_day_meets_file_bar(stats.live_file_count, stats.total_bytes) || size_pressure
        }
        DayKind::Open => stats.live_file_count > policy.open_day_file_cap,
    }
}

/// One partition-scoped merge intent (never spans two `record_date`s).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwcsMergeAction {
    pub table: String,
    pub record_date: NaiveDate,
    /// Executable merge SQL for this maintenance wave (bounded `max_compacted_files`).
    /// DuckLake has no per-day filter API here; partition locality is enforced by
    /// `PARTITIONED BY (record_date)` + proven by T-F6.
    pub sql: String,
}

pub fn ducklake_merge_adjacent_files_sql(
    catalog_alias: &str,
    table: &str,
    schema: &str,
    max_compacted_files: Option<u64>,
    max_file_size_bytes: Option<u64>,
) -> String {
    let mut args = format!("schema => '{schema}'");
    if let Some(max_compacted_files) = max_compacted_files {
        args.push_str(&format!(", max_compacted_files => {max_compacted_files}"));
    }
    if let Some(max_file_size_bytes) = max_file_size_bytes {
        args.push_str(&format!(", max_file_size => {max_file_size_bytes}"));
    }
    format!("CALL ducklake_merge_adjacent_files('{catalog_alias}', '{table}', {args});")
}

/// Bounded merge CALL Softprobe actually executes (AC-Q9 wave size).
pub fn twcs_merge_sql(
    catalog_alias: &str,
    table: &str,
    schema: &str,
    max_compacted_files: u64,
    policy: &TwcsPolicy,
) -> String {
    ducklake_merge_adjacent_files_sql(
        catalog_alias,
        table,
        schema,
        Some(max_compacted_files),
        Some(policy.max_merge_file_size_bytes),
    )
}

/// Inputs for [`plan_twcs_merges`].
#[derive(Debug, Clone, Copy)]
pub struct TwcsMergePlan<'a> {
    pub table: &'a str,
    pub catalog_alias: &'a str,
    pub schema: &'a str,
    pub partitions: &'a [PartitionFileStats],
    pub today: NaiveDate,
    pub size_pressure: bool,
    pub max_compacted_files: u64,
    pub policy: &'a TwcsPolicy,
}

/// Build per-day merge actions for one table. Each action covers exactly one
/// `record_date` — planning never combines two days into one intent (AC-F6).
pub fn plan_twcs_merges(plan: &TwcsMergePlan<'_>) -> Vec<TwcsMergeAction> {
    let sql = twcs_merge_sql(
        plan.catalog_alias,
        plan.table,
        plan.schema,
        plan.max_compacted_files,
        plan.policy,
    );
    let mut actions = Vec::new();
    for stats in plan.partitions {
        let kind = day_kind(stats.record_date, plan.today);
        if !should_merge_partition(stats, kind, plan.size_pressure, plan.policy) {
            continue;
        }
        actions.push(TwcsMergeAction {
            table: plan.table.to_string(),
            record_date: stats.record_date,
            sql: sql.clone(),
        });
    }
    actions
}

/// SQL to list live file counts / bytes per `record_date` for a metrics table.
///
/// Uses DuckLake metadata (`ducklake_data_file` + `ducklake_file_partition_value`).
pub fn partition_live_file_stats_sql(catalog_alias: &str, table: &str) -> String {
    let meta = format!("__ducklake_metadata_{catalog_alias}");
    format!(
        "SELECT CAST(fp.partition_value AS VARCHAR) AS record_date, \
                count(*)::BIGINT AS live_file_count, \
                coalesce(sum(df.file_size_bytes), 0)::BIGINT AS total_bytes \
         FROM {meta}.ducklake_data_file df \
         JOIN {meta}.ducklake_table t \
           ON df.table_id = t.table_id \
         JOIN {meta}.ducklake_file_partition_value fp \
           ON fp.data_file_id = df.data_file_id AND fp.table_id = t.table_id \
         WHERE t.table_name = '{table}' \
           AND t.end_snapshot IS NULL \
           AND df.end_snapshot IS NULL \
         GROUP BY 1 \
         ORDER BY 1"
    )
}

/// T-F6: live sample files that map to more than one `record_date` (must be empty).
pub fn live_files_spanning_record_dates_sql(catalog_alias: &str, table: &str) -> String {
    let meta = format!("__ducklake_metadata_{catalog_alias}");
    format!(
        "SELECT df.data_file_id, count(DISTINCT CAST(fp.partition_value AS VARCHAR)) AS n_dates \
         FROM {meta}.ducklake_data_file df \
         JOIN {meta}.ducklake_table t \
           ON df.table_id = t.table_id \
         JOIN {meta}.ducklake_file_partition_value fp \
           ON fp.data_file_id = df.data_file_id AND fp.table_id = t.table_id \
         WHERE t.table_name = '{table}' \
           AND t.end_snapshot IS NULL \
           AND df.end_snapshot IS NULL \
         GROUP BY df.data_file_id \
         HAVING count(DISTINCT CAST(fp.partition_value AS VARCHAR)) > 1"
    )
}

/// Paths of live data files for a table (T-F6 content check in Rust).
pub fn live_data_file_paths_sql(catalog_alias: &str, table: &str) -> String {
    let meta = format!("__ducklake_metadata_{catalog_alias}");
    format!(
        "SELECT df.path \
         FROM {meta}.ducklake_data_file df \
         JOIN {meta}.ducklake_table t \
           ON df.table_id = t.table_id \
         WHERE t.table_name = '{table}' \
           AND t.end_snapshot IS NULL \
           AND df.end_snapshot IS NULL"
    )
}

/// Small open-day CALL when already close to the AC-F4 cap (AC-Q9).
pub const TWCS_MAX_COMPACTED_FILES_PER_WAVE: u64 = 32;
/// Open-day wave cap per table per maintenance pass (AC-F4).
pub const TWCS_MAX_WAVES_PER_TABLE: usize = 32;

/// Closed-day files per merge CALL. Must be ≥ open-day 32 so leftover thousands
/// can finish in one pass (AC-F8). DuckLake merge is still bounded per CALL.
pub const TWCS_CLOSED_DAY_MAX_COMPACTED_FILES: u64 = 256;
/// Closed-day waves per table per pass. `64 × 256 = 16384` covers ~10k leftover
/// files in one maintenance pass without an unbounded loop.
pub const TWCS_CLOSED_DAY_MAX_WAVES: usize = 64;

/// TWCS merge policy knobs (from `MaintenanceConfig` at runtime).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TwcsPolicy {
    pub open_day_file_cap: usize,
    pub max_waves_per_table: usize,
    pub max_compacted_files_per_wave: u64,
    pub closed_day_max_compacted_files: u64,
    pub closed_day_max_waves: usize,
    pub max_merge_file_size_bytes: u64,
}

impl Default for TwcsPolicy {
    fn default() -> Self {
        Self {
            open_day_file_cap: TWCS_OPEN_DAY_FILE_CAP,
            max_waves_per_table: TWCS_MAX_WAVES_PER_TABLE,
            max_compacted_files_per_wave: TWCS_MAX_COMPACTED_FILES_PER_WAVE,
            closed_day_max_compacted_files: TWCS_CLOSED_DAY_MAX_COMPACTED_FILES,
            closed_day_max_waves: TWCS_CLOSED_DAY_MAX_WAVES,
            max_merge_file_size_bytes: 8 * 1024 * 1024,
        }
    }
}

impl From<&MaintenanceConfig> for TwcsPolicy {
    fn from(m: &MaintenanceConfig) -> Self {
        Self {
            open_day_file_cap: m.open_day_file_cap,
            max_waves_per_table: m.max_waves_per_table,
            max_compacted_files_per_wave: m.max_compacted_files_per_wave,
            closed_day_max_compacted_files: m.closed_day_max_compacted_files,
            closed_day_max_waves: m.closed_day_max_waves,
            max_merge_file_size_bytes: m.max_merge_file_size_bytes,
        }
    }
}

/// Files one closed-day pass can compact (`waves × files/wave`).
pub fn closed_day_file_capacity(policy: &TwcsPolicy) -> u64 {
    policy.closed_day_max_waves as u64 * policy.closed_day_max_compacted_files
}

/// Per-CALL bound for an open-day wave. Tiny leftover (≤256 files) keeps the
/// small CALL size; a storm uses the closed-day CALL size.
pub fn open_day_max_compacted_files(live_files: usize, policy: &TwcsPolicy) -> u64 {
    if live_files > policy.closed_day_max_compacted_files as usize {
        policy.closed_day_max_compacted_files
    } else {
        policy.max_compacted_files_per_wave
    }
}

/// Files one open-day pass can compact in a single maintenance pass.
pub fn open_day_file_capacity(policy: &TwcsPolicy) -> u64 {
    policy.max_waves_per_table as u64 * policy.closed_day_max_compacted_files
}

/// Live open-day file total. Per-day partition stats can undercount right
/// after a merge (JOIN miss); use max(open-from-parts, live − closed) so a
/// stale undercount cannot look like the AC-F4 cap is already met.
pub fn open_day_files_for_merge(
    partitions: &[PartitionFileStats],
    today: NaiveDate,
    live_file_fallback: Option<usize>,
) -> usize {
    let open = open_day_live_file_count(partitions, today);
    let closed = closed_day_live_file_count(partitions, today);
    match live_file_fallback {
        None => open,
        Some(live) => open.max(live.saturating_sub(closed)),
    }
}

/// Count live data files for a table (no partition JOIN). Fallback when
/// per-day stats come back empty after merge.
pub fn live_file_count_sql(catalog_alias: &str, table: &str) -> String {
    let meta = format!("__ducklake_metadata_{catalog_alias}");
    format!(
        "SELECT count(*)::BIGINT \
         FROM {meta}.ducklake_data_file df \
         JOIN {meta}.ducklake_table t \
           ON df.table_id = t.table_id \
         WHERE t.table_name = '{table}' \
           AND t.end_snapshot IS NULL \
           AND df.end_snapshot IS NULL"
    )
}

/// AC-F8: closed-day live files are 1, or 2 when that day's bytes exceed 64 MiB.
pub fn closed_day_meets_file_bar(live_file_count: usize, total_bytes: u64) -> bool {
    const TARGET: u64 = 64 * 1024 * 1024;
    match live_file_count {
        1 => true,
        2 => total_bytes > TARGET,
        _ => false,
    }
}

/// True when any closed `record_date` still fails the AC-F8 file bar.
pub fn closed_days_need_complete_merge(
    partitions: &[PartitionFileStats],
    today: NaiveDate,
) -> bool {
    partitions.iter().any(|p| {
        day_kind(p.record_date, today) == DayKind::Closed
            && !closed_day_meets_file_bar(p.live_file_count, p.total_bytes)
    })
}

/// Live-file total for closed partitions only (progress guard ignores open-day ingest).
pub fn closed_day_live_file_count(partitions: &[PartitionFileStats], today: NaiveDate) -> usize {
    partitions
        .iter()
        .filter(|p| day_kind(p.record_date, today) == DayKind::Closed)
        .map(|p| p.live_file_count)
        .sum()
}

/// Live-file total for the open day only.
pub fn open_day_live_file_count(partitions: &[PartitionFileStats], today: NaiveDate) -> usize {
    partitions
        .iter()
        .filter(|p| day_kind(p.record_date, today) == DayKind::Open)
        .map(|p| p.live_file_count)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn policy() -> TwcsPolicy {
        TwcsPolicy::default()
    }

    #[test]
    fn twcs_policy_matches_maintenance_config_defaults() {
        assert_eq!(
            TwcsPolicy::default(),
            TwcsPolicy::from(&MaintenanceConfig::default())
        );
    }

    #[test]
    fn closed_day_triggers_at_two_files() {
        let today = d(2026, 8, 15);
        let day = d(2026, 8, 14);
        let p = policy();
        let one = PartitionFileStats {
            record_date: day,
            live_file_count: 1,
            total_bytes: 1_000,
        };
        let two = PartitionFileStats {
            record_date: day,
            live_file_count: 2,
            total_bytes: 1_000,
        };
        assert_eq!(TWCS_TRIGGER_FILE_NUM, 2);
        assert!(!should_merge_partition(
            &one,
            day_kind(day, today),
            false,
            &p
        ));
        assert!(should_merge_partition(
            &two,
            day_kind(day, today),
            false,
            &p
        ));
        assert!(should_merge_partition(&one, day_kind(day, today), true, &p));
        let two_over_target = PartitionFileStats {
            record_date: day,
            live_file_count: 2,
            total_bytes: 65 * 1024 * 1024,
        };
        assert!(
            !should_merge_partition(&two_over_target, day_kind(day, today), false, &p),
            "AC-F8: two files over 64 MiB already meet the bar"
        );
    }

    #[test]
    fn closed_day_file_bar_allows_two_only_over_target() {
        assert!(closed_day_meets_file_bar(1, 1_000));
        assert!(!closed_day_meets_file_bar(2, 1_000));
        assert!(closed_day_meets_file_bar(2, 65 * 1024 * 1024));
        assert!(!closed_day_meets_file_bar(3, 100_000_000));
    }

    #[test]
    fn open_day_triggers_only_above_cap() {
        let today = d(2026, 8, 15);
        let p = policy();
        let under = PartitionFileStats {
            record_date: today,
            live_file_count: TWCS_OPEN_DAY_FILE_CAP,
            total_bytes: 1_000,
        };
        let over = PartitionFileStats {
            record_date: today,
            live_file_count: TWCS_OPEN_DAY_FILE_CAP + 1,
            total_bytes: 1_000,
        };
        assert_eq!(day_kind(today, today), DayKind::Open);
        assert!(!should_merge_partition(&under, DayKind::Open, false, &p));
        assert!(should_merge_partition(&over, DayKind::Open, false, &p));
    }

    /// AC-F6 planner: one action per `record_date`; never a combined multi-day intent.
    #[test]
    fn twcs_merge_does_not_cross_record_date() {
        let today = d(2026, 8, 15);
        let parts = vec![
            PartitionFileStats {
                record_date: d(2026, 8, 13),
                live_file_count: 5,
                total_bytes: 20_000_000,
            },
            PartitionFileStats {
                record_date: d(2026, 8, 14),
                live_file_count: 6,
                total_bytes: 20_000_000,
            },
        ];
        let p = policy();
        let actions = plan_twcs_merges(&TwcsMergePlan {
            table: "metric_samples",
            catalog_alias: "softprobe",
            schema: "main",
            partitions: &parts,
            today,
            size_pressure: false,
            max_compacted_files: TWCS_MAX_COMPACTED_FILES_PER_WAVE,
            policy: &p,
        });
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].record_date, d(2026, 8, 13));
        assert_eq!(actions[1].record_date, d(2026, 8, 14));
        for action in &actions {
            assert!(
                action.sql.contains("ducklake_merge_adjacent_files"),
                "expected real merge CALL, got {}",
                action.sql
            );
            assert!(
                action.sql.contains("max_compacted_files"),
                "AC-Q9: wave must be bounded"
            );
            assert!(
                action.sql.contains("max_file_size"),
                "merge must skip already-sized files"
            );
            // Honesty: no fake partition_filter / comment claiming a day filter.
            assert!(
                !action.sql.contains("partition_filter"),
                "do not pretend DuckLake accepts partition_filter"
            );
            assert!(
                !action.sql.contains("record_date ="),
                "do not emit unused record_date= filter theater"
            );
        }
        let closed_actions = plan_twcs_merges(&TwcsMergePlan {
            table: "metric_samples",
            catalog_alias: "softprobe",
            schema: "main",
            partitions: &parts,
            today,
            size_pressure: false,
            max_compacted_files: TWCS_CLOSED_DAY_MAX_COMPACTED_FILES,
            policy: &p,
        });
        assert_eq!(
            closed_actions.len(),
            2,
            "AC-F6: still one action per record_date"
        );
        assert_eq!(closed_actions[0].record_date, d(2026, 8, 13));
        assert_eq!(closed_actions[1].record_date, d(2026, 8, 14));
        assert!(closed_actions[0].sql.contains(&format!(
            "max_compacted_files => {TWCS_CLOSED_DAY_MAX_COMPACTED_FILES}"
        )));
    }

    /// AC-F3: samples partition key is `record_date` only (policy constant).
    #[test]
    fn twcs_partition_key_is_record_date_only() {
        let sql = partition_live_file_stats_sql("softprobe", "metric_samples");
        assert!(sql.contains("partition_value"));
        assert!(sql.contains("metric_samples"));
        assert!(!sql.contains("metric_name"));
    }

    #[test]
    fn plan_skips_quiet_partitions() {
        let today = d(2026, 8, 15);
        let parts = vec![PartitionFileStats {
            record_date: d(2026, 8, 14),
            live_file_count: 1,
            total_bytes: 100,
        }];
        let actions = plan_twcs_merges(&TwcsMergePlan {
            table: "metric_samples",
            catalog_alias: "softprobe",
            schema: "main",
            partitions: &parts,
            today,
            size_pressure: false,
            max_compacted_files: 32,
            policy: &policy(),
        });
        assert!(actions.is_empty());
    }

    /// AC-F8: 64 × 256 files/wave can compact ~10k leftover closed-day files.
    #[test]
    fn closed_day_wave_budget_covers_ten_thousand_files() {
        let p = policy();
        let cap = closed_day_file_capacity(&p);
        assert!(
            cap >= 10_000,
            "AC-F8: closed-day cap {cap} cannot finish 10k leftover files \
             ({} waves × {} files)",
            TWCS_CLOSED_DAY_MAX_WAVES,
            TWCS_CLOSED_DAY_MAX_COMPACTED_FILES
        );
        assert!(cap > open_day_file_capacity(&p));
        assert!(
            open_day_file_capacity(&p) >= 2000,
            "AC-F4: open-day cap {} cannot drain a ~2k-file ingest storm in one pass",
            open_day_file_capacity(&p)
        );
        assert_eq!(TWCS_MAX_WAVES_PER_TABLE, 32);
        assert_eq!(TWCS_MAX_COMPACTED_FILES_PER_WAVE, 32);
        assert_eq!(open_day_max_compacted_files(25, &p), 32);
        assert_eq!(open_day_max_compacted_files(1774, &p), 256);
    }

    #[test]
    fn open_day_empty_partition_stats_use_live_file_fallback() {
        let today = d(2026, 8, 15);
        assert_eq!(open_day_files_for_merge(&[], today, None), 0);
        assert_eq!(open_day_files_for_merge(&[], today, Some(1774)), 1774);
        let undercount = vec![PartitionFileStats {
            record_date: today,
            live_file_count: 10,
            total_bytes: 1_000,
        }];
        assert_eq!(
            open_day_files_for_merge(&undercount, today, Some(141)),
            141,
            "stale partition undercount must not hide live files"
        );
        let mixed = vec![
            PartitionFileStats {
                record_date: d(2026, 8, 14),
                live_file_count: 2,
                total_bytes: 1_000,
            },
            PartitionFileStats {
                record_date: today,
                live_file_count: 15,
                total_bytes: 1_000,
            },
        ];
        assert_eq!(open_day_files_for_merge(&mixed, today, Some(17)), 15);
    }

    #[test]
    fn live_file_count_sql_has_no_partition_join() {
        let sql = live_file_count_sql("softprobe", "metric_postings");
        assert!(sql.contains("ducklake_data_file"));
        assert!(!sql.contains("ducklake_file_partition_value"));
    }

    #[test]
    fn closed_day_needs_complete_merge_until_file_bar() {
        let today = d(2026, 8, 15);
        let closed = d(2026, 8, 14);
        let many = vec![PartitionFileStats {
            record_date: closed,
            live_file_count: 10_000,
            total_bytes: 20_000_000,
        }];
        assert!(closed_days_need_complete_merge(&many, today));
        assert_eq!(closed_day_live_file_count(&many, today), 10_000);
        let done = vec![PartitionFileStats {
            record_date: closed,
            live_file_count: 1,
            total_bytes: 20_000_000,
        }];
        assert!(!closed_days_need_complete_merge(&done, today));
        let two_over = vec![PartitionFileStats {
            record_date: closed,
            live_file_count: 2,
            total_bytes: 65 * 1024 * 1024,
        }];
        assert!(!closed_days_need_complete_merge(&two_over, today));
        let open_only = vec![PartitionFileStats {
            record_date: today,
            live_file_count: 50,
            total_bytes: 1_000,
        }];
        assert!(
            !closed_days_need_complete_merge(&open_only, today),
            "open-day files must not keep the closed-day complete-merge loop running"
        );
        assert_eq!(open_day_live_file_count(&open_only, today), 50);
        assert_eq!(closed_day_live_file_count(&open_only, today), 0);
    }
}
