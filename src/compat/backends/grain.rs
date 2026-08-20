//! Sample grain selection for Prom queries (§9.1 steps 5–6).
//!
//! Picks `metric_samples` / `metric_samples_5m` / `metric_samples_1h` /
//! `metric_hist_samples` from the query window and Grafana step.
//!
//! Downsample tables are filled by maintenance (step 7), not ingest. When 5m/1h
//! numeric grains are empty the planner still selects them for **gauge/counter**
//! series (empty until ladder runs). Classic hist/summary use raw `metric_hist_samples`
//! for ≤2h, `metric_hist_samples_5m` for ≤48h, `metric_hist_samples_1h` beyond —
//! same ladder as gauges (Greptime/Thanos-style pre-aggregate, not Prom result cache).

use crate::storage::schema::metrics_layout::qualified_metrics_layout_table;

/// Raw grain window: end − start ≤ 12h.
/// Kept generous to avoid gaps from downsample watermark lag; step-bucketing
/// on raw keeps scan cost manageable for typical dashboard panels.
pub const RAW_RANGE_MS: i64 = 12 * 60 * 60 * 1000;
/// 5m grain window: end − start ≤ 48h (and > 2h) for gauges.
pub const FIVE_MIN_RANGE_MS: i64 = 48 * 60 * 60 * 1000;
/// Hist 5m grain: 12h < range ≤ 48h (beyond 48h uses hist_1h).
pub const HIST_FIVE_MIN_RANGE_MS: i64 = 48 * 60 * 60 * 1000;
/// Grafana step ≥ 1h → prefer `metric_samples_1h` even for shorter ranges.
pub const ONE_HOUR_STEP_MS: i64 = 60 * 60 * 1000;
/// Lag window (ms) for 5m downsample — raw data newer than this may not be in 5m.
/// Matches RAW_RANGE_MS so the raw tail always covers the gap.
pub const FIVE_MIN_LAG_MS: i64 = 12 * 60 * 60 * 1000;
/// Lag window (ms) for 1h downsample — raw data newer than this is not yet in 1h.
pub const ONE_HOUR_LAG_MS: i64 = 24 * 60 * 60 * 1000;

/// Physical sample table chosen after postings resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SampleGrain {
    /// `metric_samples` (`timestamp`, `value`).
    Raw,
    /// `metric_samples_5m` (`window_ts`, `last`).
    FiveMin,
    /// `metric_samples_1h` (`window_ts`, `last`).
    OneHour,
    /// `metric_hist_samples` for classic hist/summary (`_bucket` / `_sum` / `_count`).
    Hist,
    /// `metric_hist_samples_5m` (maintenance ladder; merged bucket arrays).
    HistFiveMin,
    /// `metric_hist_samples_1h`.
    HistOneHour,
}

impl SampleGrain {
    pub fn table_name(self) -> &'static str {
        match self {
            Self::Raw => "metric_samples",
            Self::FiveMin => "metric_samples_5m",
            Self::OneHour => "metric_samples_1h",
            Self::Hist => "metric_hist_samples",
            Self::HistFiveMin => "metric_hist_samples_5m",
            Self::HistOneHour => "metric_hist_samples_1h",
        }
    }

    /// Time column on the grain table.
    pub fn time_column(self) -> &'static str {
        match self {
            Self::Raw | Self::Hist => "timestamp",
            Self::FiveMin | Self::OneHour | Self::HistFiveMin | Self::HistOneHour => "window_ts",
        }
    }

    /// Value expression for Prom sample fetch (alias as `value` in SELECT).
    pub fn value_expr(self) -> &'static str {
        match self {
            Self::Raw => "sm.value",
            Self::FiveMin | Self::OneHour => "sm.last",
            Self::Hist | Self::HistFiveMin | Self::HistOneHour => "COALESCE(sm.sum, 0.0)",
        }
    }

    pub fn is_downsample(self) -> bool {
        matches!(
            self,
            Self::FiveMin | Self::OneHour | Self::HistFiveMin | Self::HistOneHour
        )
    }

    pub fn is_hist(self) -> bool {
        matches!(self, Self::Hist | Self::HistFiveMin | Self::HistOneHour)
    }
}

/// Inclusive query span in milliseconds, or `None` if unbounded.
pub fn query_range_ms(start_ms: Option<i64>, end_ms: Option<i64>) -> Option<i64> {
    match (start_ms, end_ms) {
        (Some(s), Some(e)) => Some((e - s).abs()),
        _ => None,
    }
}

/// §9.1 grain table after postings resolve.
///
/// - hist/summary → raw hist ≤2h, hist_5m ≤48h, hist_1h beyond (§7.2 ladder)
/// - end−start ≤ 2h → `metric_samples`
/// - ≤ 48h → `metric_samples_5m` (empty until maintenance; no ingest fake-fill)
/// - \> 48h → `metric_samples_1h`
/// - Grafana `step` ≥ 1h → prefer 1h even for shorter gauge ranges
pub fn select_sample_grain(
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    step_ms: Option<i64>,
    is_histogram: bool,
) -> SampleGrain {
    let range = query_range_ms(start_ms, end_ms).unwrap_or(0);

    if is_histogram {
        if step_ms.map(|s| s >= ONE_HOUR_STEP_MS).unwrap_or(false) {
            return SampleGrain::HistOneHour;
        }
        if range <= RAW_RANGE_MS {
            return SampleGrain::Hist;
        }
        if range <= HIST_FIVE_MIN_RANGE_MS {
            return SampleGrain::HistFiveMin;
        }
        return SampleGrain::HistOneHour;
    }

    if step_ms.map(|s| s >= ONE_HOUR_STEP_MS).unwrap_or(false) {
        return SampleGrain::OneHour;
    }

    if range <= RAW_RANGE_MS {
        SampleGrain::Raw
    } else if range <= FIVE_MIN_RANGE_MS {
        SampleGrain::FiveMin
    } else {
        SampleGrain::OneHour
    }
}

/// Qualified grain table name for SQL.
pub fn grain_table_sql(catalog: &str, grain: SampleGrain) -> String {
    qualified_metrics_layout_table(catalog, grain.table_name())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000;
    const DAY: i64 = 24 * HOUR;

    /// T-Q1 / AC-Q1: short-range query uses raw grain.
    #[test]
    fn planner_picks_raw_for_30m() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * 60 * 1000;
        assert_eq!(
            select_sample_grain(Some(start), Some(end), Some(15_000), false),
            SampleGrain::Raw
        );
        assert_eq!(
            select_sample_grain(Some(start), Some(end), Some(15_000), false).table_name(),
            "metric_samples"
        );
    }

    /// Boundary: exactly RAW_RANGE_MS → raw; just over → 5m.
    #[test]
    fn planner_raw_boundary_at_2h() {
        let end = 1_700_000_000_000i64;
        assert_eq!(
            select_sample_grain(Some(end - RAW_RANGE_MS), Some(end), None, false),
            SampleGrain::Raw
        );
        assert_eq!(
            select_sample_grain(Some(end - RAW_RANGE_MS - 1), Some(end), None, false),
            SampleGrain::FiveMin
        );
    }

    /// ≤ 48h (and > 2h) → 5m.
    #[test]
    fn planner_picks_5m_for_24h() {
        let end = 1_700_000_000_000i64;
        let start = end - DAY;
        assert_eq!(
            select_sample_grain(Some(start), Some(end), Some(60_000), false),
            SampleGrain::FiveMin
        );
    }

    /// T-Q2 / AC-Q2 / AC-W2: 30d → metric_samples_1h not raw.
    #[test]
    fn planner_picks_1h_for_30d() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * DAY;
        let g = select_sample_grain(Some(start), Some(end), Some(HOUR), false);
        assert_eq!(g, SampleGrain::OneHour);
        assert_eq!(g.table_name(), "metric_samples_1h");
        assert_ne!(g.table_name(), "metric_samples");
    }

    /// T-W5 / AC-W5: 90d → metric_samples_1h not raw.
    #[test]
    fn planner_picks_1h_for_90d() {
        let end = 1_700_000_000_000i64;
        let start = end - 90 * DAY;
        let g = select_sample_grain(Some(start), Some(end), Some(HOUR), false);
        assert_eq!(g, SampleGrain::OneHour);
        assert_ne!(g, SampleGrain::Raw);
    }

    /// T-W6 / AC-W6: 180d → metric_samples_1h not raw.
    #[test]
    fn planner_picks_1h_for_180d() {
        let end = 1_700_000_000_000i64;
        let start = end - 180 * DAY;
        let g = select_sample_grain(Some(start), Some(end), Some(HOUR), false);
        assert_eq!(g, SampleGrain::OneHour);
        assert_eq!(g.table_name(), "metric_samples_1h");
    }

    /// Grafana step ≥ 1h prefers 1h even for a short range.
    #[test]
    fn planner_prefers_1h_when_step_ge_1h() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * 60 * 1000; // 30m
        assert_eq!(
            select_sample_grain(Some(start), Some(end), Some(HOUR), false),
            SampleGrain::OneHour
        );
        assert_eq!(
            select_sample_grain(Some(start), Some(end), Some(HOUR - 1), false),
            SampleGrain::Raw
        );
    }

    /// Classic hist/summary selectors use the hist ladder (raw / 5m / 1h), not gauge grains.
    #[test]
    fn hist_selector_uses_hist_ladder_by_window() {
        let end = 1_700_000_000_000i64;
        let windows: &[(i64, SampleGrain)] = &[
            (30 * 60 * 1000, SampleGrain::Hist),
            (RAW_RANGE_MS, SampleGrain::Hist),
            (RAW_RANGE_MS + 1, SampleGrain::HistFiveMin),
            (DAY, SampleGrain::HistFiveMin),
            (2 * DAY, SampleGrain::HistFiveMin),
            (2 * DAY + 1, SampleGrain::HistOneHour),
            (30 * DAY, SampleGrain::HistOneHour),
            (90 * DAY, SampleGrain::HistOneHour),
        ];
        for &(range, want) in windows {
            let g = select_sample_grain(Some(end - range), Some(end), Some(15_000), true);
            assert_eq!(g, want, "hist grain for range={range}");
            assert!(g.is_hist());
        }
        assert_eq!(
            select_sample_grain(Some(end - DAY), Some(end), Some(HOUR), true),
            SampleGrain::HistOneHour
        );
    }

    /// AC-H6 / Q-window-matrix: gauge vs hist grain for common Grafana windows.
    #[test]
    fn window_series_type_grain_matrix() {
        let end = 1_700_000_000_000i64;
        let cases: &[(i64, Option<i64>, SampleGrain, SampleGrain)] = &[
            // range_ms, step_ms, gauge/counter, hist/summary
            (
                30 * 60 * 1000,
                Some(15_000),
                SampleGrain::Raw,
                SampleGrain::Hist,
            ),
            (6 * HOUR, Some(15_000), SampleGrain::Raw, SampleGrain::Hist),
            (
                RAW_RANGE_MS,
                Some(15_000),
                SampleGrain::Raw,
                SampleGrain::Hist,
            ),
            (
                RAW_RANGE_MS + 1,
                Some(20_000),
                SampleGrain::FiveMin,
                SampleGrain::HistFiveMin,
            ),
            (
                DAY,
                Some(60_000),
                SampleGrain::FiveMin,
                SampleGrain::HistFiveMin,
            ),
            (
                30 * DAY,
                Some(HOUR),
                SampleGrain::OneHour,
                SampleGrain::HistOneHour,
            ),
            (
                90 * DAY,
                Some(HOUR),
                SampleGrain::OneHour,
                SampleGrain::HistOneHour,
            ),
            // step ≥ 1h prefers 1h even for short gauge ranges; hist uses hist_1h
            (
                30 * 60 * 1000,
                Some(HOUR),
                SampleGrain::OneHour,
                SampleGrain::HistOneHour,
            ),
        ];
        for &(range, step, want_gauge, want_hist) in cases {
            let start = end - range;
            assert_eq!(
                select_sample_grain(Some(start), Some(end), step, false),
                want_gauge,
                "gauge range={range} step={step:?}"
            );
            assert_eq!(
                select_sample_grain(Some(start), Some(end), step, true),
                want_hist,
                "hist range={range} step={step:?}"
            );
        }
    }

    #[test]
    fn grain_table_sql_qualifies() {
        assert_eq!(
            grain_table_sql("softprobe", SampleGrain::OneHour),
            "softprobe.metric_samples_1h"
        );
    }
}
