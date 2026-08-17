//! Softprobe TWCS merge policy (§7.1).
//!
//! Time window = calendar day (`record_date`). Softprobe **plans** merges per day
//! (AC-F6) and never schedules a cross-day rewrite intent. DuckLake's
//! `ducklake_merge_adjacent_files` has no `partition_filter` in the versions we
//! ship; Softprobe therefore executes a bounded unscoped CALL and **relies on
//! DuckLake partition-local merge** when the table is `PARTITIONED BY (record_date)`.
//! Integration `T-F6` proves live files stay single-day after merge; if that fails,
//! do not claim AC-F6.

use chrono::NaiveDate;

/// Greptime-analog `trigger_file_num` for closed calendar-day partitions.
pub const TWCS_TRIGGER_FILE_NUM: usize = 4;
/// After a maintenance pass, today's open-day live sample files must be ≤ this (AC-F4).
pub const TWCS_OPEN_DAY_FILE_CAP: usize = 20;

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
pub fn should_merge_partition(
    stats: &PartitionFileStats,
    kind: DayKind,
    size_pressure: bool,
) -> bool {
    match kind {
        DayKind::Closed => stats.live_file_count >= TWCS_TRIGGER_FILE_NUM || size_pressure,
        DayKind::Open => stats.live_file_count > TWCS_OPEN_DAY_FILE_CAP || size_pressure,
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

/// Bounded merge CALL Softprobe actually executes (AC-Q9 wave size).
///
/// No `record_date =` filter string: that would be audit theater — DuckLake does
/// not accept `partition_filter` in our shipped extension. Partition locality is
/// a DuckLake invariant under `PARTITIONED BY (record_date)` (T-F6).
pub fn twcs_merge_sql(
    catalog_alias: &str,
    table: &str,
    schema: &str,
    max_compacted_files: u64,
) -> String {
    format!(
        "CALL ducklake_merge_adjacent_files('{catalog_alias}', '{table}', \
schema => '{schema}', max_compacted_files => {max_compacted_files});"
    )
}

/// Build per-day merge actions for one table. Each action covers exactly one
/// `record_date` — planning never combines two days into one intent (AC-F6).
pub fn plan_twcs_merges(
    table: &str,
    catalog_alias: &str,
    schema: &str,
    partitions: &[PartitionFileStats],
    today: NaiveDate,
    size_pressure: bool,
    max_compacted_files: u64,
) -> Vec<TwcsMergeAction> {
    let sql = twcs_merge_sql(catalog_alias, table, schema, max_compacted_files);
    let mut actions = Vec::new();
    for stats in partitions {
        let kind = day_kind(stats.record_date, today);
        if !should_merge_partition(stats, kind, size_pressure) {
            continue;
        }
        actions.push(TwcsMergeAction {
            table: table.to_string(),
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

/// Max compacted files per merge wave — keeps partition waves short (AC-Q9).
pub const TWCS_MAX_COMPACTED_FILES_PER_WAVE: u64 = 32;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn closed_day_triggers_at_four_files() {
        let today = d(2026, 8, 15);
        let day = d(2026, 8, 14);
        let low = PartitionFileStats {
            record_date: day,
            live_file_count: 3,
            total_bytes: 1_000,
        };
        let hit = PartitionFileStats {
            record_date: day,
            live_file_count: 4,
            total_bytes: 1_000,
        };
        assert!(!should_merge_partition(&low, day_kind(day, today), false));
        assert!(should_merge_partition(&hit, day_kind(day, today), false));
        assert!(should_merge_partition(&low, day_kind(day, today), true));
    }

    #[test]
    fn open_day_triggers_only_above_cap() {
        let today = d(2026, 8, 15);
        let under = PartitionFileStats {
            record_date: today,
            live_file_count: 20,
            total_bytes: 1_000,
        };
        let over = PartitionFileStats {
            record_date: today,
            live_file_count: 21,
            total_bytes: 1_000,
        };
        assert_eq!(day_kind(today, today), DayKind::Open);
        assert!(!should_merge_partition(&under, DayKind::Open, false));
        assert!(should_merge_partition(&over, DayKind::Open, false));
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
        let actions = plan_twcs_merges(
            "metric_samples",
            "softprobe",
            "main",
            &parts,
            today,
            false,
            TWCS_MAX_COMPACTED_FILES_PER_WAVE,
        );
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
            live_file_count: 2,
            total_bytes: 100,
        }];
        let actions = plan_twcs_merges(
            "metric_samples",
            "softprobe",
            "main",
            &parts,
            today,
            false,
            32,
        );
        assert!(actions.is_empty());
    }
}
