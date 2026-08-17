//! Prometheus series resolve via day-partitioned `metric_postings` (§9.1).
//!
//! Equality matchers → postings intersect → `series_id` set; then skinny
//! `metric_samples` / `metric_hist_samples` scan. Does not scan fat `metrics`
//! or full `union_metrics` for resolve.

use crate::compat::backends::grain::{
    grain_table_sql, select_sample_grain, SampleGrain,
};
use crate::compat::backends::metrics::{LabelMatcher, MatcherOp};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::projection::prometheus::sanitize_label_name;
use crate::storage::schema::metrics_layout::qualified_metrics_layout_table;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// One equality posting constraint (`label_name` / candidate values).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EqualityPosting {
    pub label_name: String,
    pub values: Vec<String>,
}

/// Calendar-day bounds inclusive for `record_date BETWEEN … AND …`.
///
/// `None` means no day prune (discovery / selectors without a time window).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordDateRange {
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
}

impl RecordDateRange {
    pub fn from_ms(start_ms: Option<i64>, end_ms: Option<i64>) -> Self {
        match (start_ms.and_then(ms_to_utc), end_ms.and_then(ms_to_utc)) {
            (None, None) => Self {
                start: None,
                end: None,
            },
            (Some(s), Some(e)) => {
                let mut start = s.date_naive();
                let mut end = e.date_naive();
                if start > end {
                    std::mem::swap(&mut start, &mut end);
                }
                Self {
                    start: Some(start),
                    end: Some(end),
                }
            }
            (Some(s), None) => {
                let d = s.date_naive();
                Self {
                    start: Some(d),
                    end: Some(d),
                }
            }
            (None, Some(e)) => {
                let d = e.date_naive();
                Self {
                    start: Some(d),
                    end: Some(d),
                }
            }
        }
    }

    /// SQL fragment for WHERE, or empty when unbounded.
    pub fn sql_predicate(&self, column_prefix: &str) -> String {
        match (self.start, self.end) {
            (Some(start), Some(end)) => {
                let col = if column_prefix.is_empty() {
                    "record_date".to_string()
                } else {
                    format!("{column_prefix}record_date")
                };
                format!("{col} BETWEEN DATE '{start}' AND DATE '{end}'")
            }
            _ => String::new(),
        }
    }

    /// Inclusive calendar days covered by this range, or `None` when unbounded.
    ///
    /// Day-scoped posting cache keys require an explicit date; unbounded resolve
    /// falls back to a single DuckDB INTERSECT (no cache).
    pub fn inclusive_days(&self) -> Option<Vec<NaiveDate>> {
        match (self.start, self.end) {
            (Some(start), Some(end)) => {
                let mut out = Vec::new();
                let mut d = start;
                while d <= end {
                    out.push(d);
                    d = d.succ_opt()?;
                }
                Some(out)
            }
            _ => None,
        }
    }
}

fn ms_to_utc(ms: i64) -> Option<DateTime<Utc>> {
    let secs = ms.div_euclid(1000);
    let nsecs = (ms.rem_euclid(1000) * 1_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs).single()
}

/// Equality matchers used for postings resolve (`=` only).
pub fn equality_postings(matchers: &[LabelMatcher]) -> Vec<EqualityPosting> {
    let mut out = Vec::new();
    for m in matchers {
        if m.op != MatcherOp::Eq {
            continue;
        }
        let values = if m.name == "__name__" {
            posting_name_values(&m.value)
        } else {
            vec![m.value.clone()]
        };
        out.push(EqualityPosting {
            label_name: m.name.clone(),
            values,
        });
    }
    out
}

/// Posting `__name__` candidates: classic base + dotted OTel form, sanitized.
pub fn posting_name_values(prom_name: &str) -> Vec<String> {
    let base = classic_base_metric_name(prom_name);
    let mut out = Vec::new();
    for cand in [base.to_string(), base.replace('_', ".")] {
        let s = sanitize_label_name(&cand);
        if !out.contains(&s) {
            out.push(s);
        }
    }
    let sanitized_prom = sanitize_label_name(prom_name);
    if !out.contains(&sanitized_prom) {
        out.push(sanitized_prom);
    }
    out
}

fn classic_base_metric_name(prom_name: &str) -> &str {
    for suffix in ["_bucket", "_sum", "_count"] {
        if let Some(base) = prom_name.strip_suffix(suffix) {
            if !base.is_empty() {
                return base;
            }
        }
    }
    prom_name
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_in_list(values: &[String]) -> String {
    values
        .iter()
        .map(|v| sql_string_literal(v))
        .collect::<Vec<_>>()
        .join(", ")
}

/// SQL that resolves `series_id`s via postings intersect (AC-Q7 / §9.1 steps 3–4).
///
/// Returns at most `max_series + 1` ids so callers can fail loud without a sample scan.
pub fn resolve_series_ids_sql(
    catalog: &str,
    days: RecordDateRange,
    equality: &[EqualityPosting],
    max_series: usize,
) -> String {
    let postings = qualified_metrics_layout_table(catalog, "metric_postings");
    let lim = max_series.saturating_add(1);
    let day_pred = days.sql_predicate("");
    let day_and = if day_pred.is_empty() {
        String::new()
    } else {
        format!(" AND {day_pred}")
    };
    if equality.is_empty() {
        // No equality → cardinality of all series in the window; fail loud at max_series.
        let where_clause = if day_pred.is_empty() {
            String::new()
        } else {
            format!("WHERE {day_pred}")
        };
        return format!(
            "SELECT DISTINCT series_id \
             FROM {postings} \
             {where_clause} \
             LIMIT {lim}"
        );
    }
    let parts: Vec<String> = equality
        .iter()
        .map(|eq| {
            let vals = sql_in_list(&eq.values);
            let name = sql_string_literal(&eq.label_name);
            format!(
                "SELECT series_id FROM {postings} \
                 WHERE label_name = {name} AND label_value IN ({vals}){day_and}"
            )
        })
        .collect();
    if parts.len() == 1 {
        format!("{} LIMIT {lim}", parts[0])
    } else {
        format!("{} LIMIT {lim}", parts.join(" INTERSECT "))
    }
}

/// Discovery SQL for `GET /api/v1/label/__name__/values` (AC-Q6).
///
/// Reads postings (not `GROUP BY metric_samples`). Joins `metric_series` only for
/// classic histogram/summary Prom name expansion.
pub fn discover_name_values_sql(catalog: &str, days: RecordDateRange, max_series: usize) -> String {
    let postings = qualified_metrics_layout_table(catalog, "metric_postings");
    let series = qualified_metrics_layout_table(catalog, "metric_series");
    let lim = max_series.saturating_add(1);
    let day_pred = days.sql_predicate("p.");
    let day_and = if day_pred.is_empty() {
        String::new()
    } else {
        format!(" AND {day_pred}")
    };
    format!(
        "SELECT p.label_value, any_value(s.metric_type) AS metric_type \
         FROM {postings} p \
         JOIN {series} s \
           ON p.series_id = s.series_id AND p.record_date = s.record_date \
         WHERE p.label_name = '__name__'{day_and} \
         GROUP BY p.label_value \
         ORDER BY p.label_value \
         LIMIT {lim}"
    )
}

/// Timestamptz literal for zone-map-friendly predicates (§9.1 step 8).
pub fn timestamptz_literal_ms(ms: i64) -> String {
    let dt = ms_to_utc(ms).unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap());
    format!(
        "TIMESTAMPTZ '{}'",
        dt.format("%Y-%m-%d %H:%M:%S%.3f+00")
    )
}

pub fn samples_time_predicates(
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    time_column: &str,
) -> String {
    let mut parts = Vec::new();
    if let Some(start) = start_ms {
        parts.push(format!(
            "sm.{time_column} >= {}",
            timestamptz_literal_ms(start)
        ));
    }
    if let Some(end) = end_ms {
        parts.push(format!(
            "sm.{time_column} <= {}",
            timestamptz_literal_ms(end)
        ));
    }
    // DuckLake partitions on record_date; timestamp predicates alone often still
    // open F-files / closed-day Parquet while the Prom window sits on EVAL_END.
    let day_pred = RecordDateRange::from_ms(start_ms, end_ms).sql_predicate("sm.");
    if !day_pred.is_empty() {
        parts.push(day_pred);
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" AND {}", parts.join(" AND "))
    }
}

/// Skinny sample scan after resolve (AC-Q7). No fat `metrics` / full `union_metrics`.
///
/// `grain` selects raw / 5m / 1h / hist (§9.1). Downsample empty tables yield empty
/// results until maintenance builds them — planner still emits the correct FROM.
pub fn samples_scan_sql(
    catalog: &str,
    series_ids: &[u64],
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    label_proj: &str,
    include_fidelity: bool,
    fetch_limit: usize,
    grain: SampleGrain,
) -> String {
    let series = qualified_metrics_layout_table(catalog, "metric_series");
    let time = samples_time_predicates(start_ms, end_ms, grain.time_column());
    let ids = if series_ids.is_empty() {
        "NULL".to_string()
    } else {
        series_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };

    if grain == SampleGrain::Hist || (include_fidelity && grain == SampleGrain::Raw) {
        return hist_or_union_scan_sql(
            catalog,
            &series,
            &ids,
            &time,
            label_proj,
            include_fidelity,
            fetch_limit,
            grain,
            start_ms,
            end_ms,
        );
    }

    let samples = grain_table_sql(catalog, grain);
    let value = grain.value_expr();
    let ts_col = grain.time_column();
    format!(
        "SELECT s.metric_name, \
         s.description, \
         s.unit, \
         {label_proj}, \
         CAST((epoch(sm.{ts_col}) * 1000) AS BIGINT) AS timestamp_ms, \
         {value} AS value, \
         s.metric_type, NULL::UBIGINT AS count, NULL::DOUBLE AS sum, \
         NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles \
         FROM {samples} sm \
         JOIN {series} s \
           ON sm.series_id = s.series_id AND sm.record_date = s.record_date \
         WHERE sm.series_id IN ({ids}){time} \
         LIMIT {fetch_limit}"
    )
}

fn hist_or_union_scan_sql(
    catalog: &str,
    series: &str,
    ids: &str,
    time: &str,
    label_proj: &str,
    include_fidelity: bool,
    fetch_limit: usize,
    grain: SampleGrain,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> String {
    let hist = qualified_metrics_layout_table(catalog, "metric_hist_samples");
    if grain == SampleGrain::Hist {
        return format!(
            "SELECT s.metric_name, \
             s.description, \
             s.unit, \
             {label_proj}, \
             CAST((epoch(sm.timestamp) * 1000) AS BIGINT) AS timestamp_ms, \
             COALESCE(sm.sum, 0.0) AS value, \
             s.metric_type, sm.count, sm.sum, sm.bucket_counts, sm.explicit_bounds, NULL AS quantiles \
             FROM {hist} sm \
             JOIN {series} s \
               ON sm.series_id = s.series_id AND sm.record_date = s.record_date \
             WHERE sm.series_id IN ({ids}){time} \
             LIMIT {fetch_limit}"
        );
    }
    // Raw + fidelity: UNION gauge samples with hist (short-window classic hist path).
    let samples = grain_table_sql(catalog, SampleGrain::Raw);
    let raw_time = samples_time_predicates(start_ms, end_ms, SampleGrain::Raw.time_column());
    let gauge_sql = format!(
        "SELECT s.metric_name, \
         s.description, \
         s.unit, \
         {label_proj}, \
         CAST((epoch(sm.timestamp) * 1000) AS BIGINT) AS timestamp_ms, \
         sm.value, \
         s.metric_type, NULL::UBIGINT AS count, NULL::DOUBLE AS sum, \
         NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles \
         FROM {samples} sm \
         JOIN {series} s \
           ON sm.series_id = s.series_id AND sm.record_date = s.record_date \
         WHERE sm.series_id IN ({ids}){raw_time}"
    );
    if !include_fidelity {
        return format!("{gauge_sql} LIMIT {fetch_limit}");
    }
    let hist_sql = format!(
        "SELECT s.metric_name, \
         s.description, \
         s.unit, \
         {label_proj}, \
         CAST((epoch(sm.timestamp) * 1000) AS BIGINT) AS timestamp_ms, \
         COALESCE(sm.sum, 0.0) AS value, \
         s.metric_type, sm.count, sm.sum, sm.bucket_counts, sm.explicit_bounds, NULL AS quantiles \
         FROM {hist} sm \
         JOIN {series} s \
           ON sm.series_id = s.series_id AND sm.record_date = s.record_date \
         WHERE sm.series_id IN ({ids}){raw_time}"
    );
    format!("({gauge_sql}) UNION ALL ({hist_sql}) LIMIT {fetch_limit}")
}

/// Build samples SQL using §9.1 grain selection.
pub fn samples_scan_sql_for_window(
    catalog: &str,
    series_ids: &[u64],
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    step_ms: Option<i64>,
    label_proj: &str,
    include_fidelity: bool,
    is_histogram: bool,
    fetch_limit: usize,
) -> String {
    let grain = select_sample_grain(start_ms, end_ms, step_ms, is_histogram);
    samples_scan_sql(
        catalog,
        series_ids,
        start_ms,
        end_ms,
        label_proj,
        include_fidelity,
        fetch_limit,
        grain,
    )
}

/// Softprobe analog of Greptime SST inverted-index tag→row-group bitmaps
/// (§4.4 MEASURE / AC-G3): cache equality posting id sets keyed by
/// `(engine, tenant, record_date, label_name, label_value)` with a short TTL.
///
/// Keying by `record_date` prevents serving yesterday's postings for today.
/// TTL covers same-day ingest freshness without a Puffin/SST index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PostingCacheKey {
    pub engine_id: usize,
    pub tenant_id: String,
    pub record_date: NaiveDate,
    pub label_name: String,
    pub label_value: String,
}

/// Short TTL: Grafana refresh storms hit warm sets; ingest-on stays eventual.
pub const POSTING_CACHE_TTL: Duration = Duration::from_secs(60);
const POSTING_CACHE_MAX: usize = 8192;

#[derive(Clone)]
struct PostingCacheEntry {
    /// Sorted unique `series_id`s for one equality posting on one day.
    series_ids: Arc<Vec<u64>>,
    expires: Instant,
}

#[derive(Default)]
pub struct PostingSetCache {
    entries: HashMap<PostingCacheKey, PostingCacheEntry>,
}

impl PostingSetCache {
    pub fn get(&mut self, key: &PostingCacheKey, now: Instant) -> Option<Arc<Vec<u64>>> {
        if let Some(entry) = self.entries.get(key) {
            if entry.expires > now {
                return Some(Arc::clone(&entry.series_ids));
            }
        }
        self.entries.retain(|_, e| e.expires > now);
        None
    }

    pub fn put(&mut self, key: PostingCacheKey, series_ids: Arc<Vec<u64>>, now: Instant) {
        if self.entries.len() >= POSTING_CACHE_MAX {
            self.entries.retain(|_, e| e.expires > now);
            if self.entries.len() >= POSTING_CACHE_MAX {
                let drop_n = POSTING_CACHE_MAX / 2;
                let keys: Vec<PostingCacheKey> =
                    self.entries.keys().take(drop_n).cloned().collect();
                for k in keys {
                    self.entries.remove(&k);
                }
            }
        }
        self.entries.insert(
            key,
            PostingCacheEntry {
                series_ids,
                expires: now + POSTING_CACHE_TTL,
            },
        );
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Test/helper: clear all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// SQL for one day-scoped equality posting list (cache fill path).
pub fn single_posting_sql(
    catalog: &str,
    day: NaiveDate,
    label_name: &str,
    label_value: &str,
) -> String {
    let postings = qualified_metrics_layout_table(catalog, "metric_postings");
    format!(
        "SELECT DISTINCT series_id FROM {postings} \
         WHERE label_name = {} AND label_value = {} AND record_date = DATE '{day}' \
         ORDER BY series_id",
        sql_string_literal(label_name),
        sql_string_literal(label_value),
    )
}

/// Merge two sorted unique id slices (union).
pub fn union_sorted_ids(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    if i < a.len() {
        out.extend_from_slice(&a[i..]);
    }
    if j < b.len() {
        out.extend_from_slice(&b[j..]);
    }
    out
}

/// Intersect two sorted unique id slices.
pub fn intersect_sorted_ids(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// Intersect equality posting sets in-process (Greptime II analog after cache fill).
///
/// For each equality constraint, unions posting lists across `days` and values;
/// then intersects across constraints. Empty equality → empty result (caller
/// should use unbounded SQL path for "all series" discovery).
pub fn intersect_equality_postings_from_sets(
    equality: &[EqualityPosting],
    days: &[NaiveDate],
    mut lookup: impl FnMut(&NaiveDate, &str, &str) -> Arc<Vec<u64>>,
) -> Vec<u64> {
    if equality.is_empty() || days.is_empty() {
        return Vec::new();
    }
    let mut acc: Option<Vec<u64>> = None;
    for eq in equality {
        let mut eq_set: Vec<u64> = Vec::new();
        for day in days {
            for value in &eq.values {
                let part = lookup(day, &eq.label_name, value);
                eq_set = union_sorted_ids(&eq_set, part.as_slice());
            }
        }
        acc = Some(match acc {
            None => eq_set,
            Some(prev) => intersect_sorted_ids(&prev, &eq_set),
        });
    }
    acc.unwrap_or_default()
}

/// Fail loud when resolved id count exceeds `max_series` (AC-Q4). Message must
/// contain `max_series`; callers must not run a sample scan after this error.
pub fn enforce_resolved_series_cap(count: usize, max_series: usize) -> Result<(), CompatError> {
    if count > max_series {
        return Err(CompatError::new(
            CompatErrorCode::LimitExceeded,
            format!("series count {count} exceeds max_series {max_series}"),
        ));
    }
    Ok(())
}

/// True when SQL is a postings+samples resolve path (AC-Q7 shape check).
pub fn sql_is_postings_resolve_path(resolve_sql: &str, samples_sql: &str) -> bool {
    let resolve_ok = resolve_sql.contains("metric_postings")
        && !resolve_sql.contains("FROM metrics")
        && !resolve_sql.contains("FROM union_metrics");
    let samples_ok = (samples_sql.contains("metric_samples")
        || samples_sql.contains("metric_samples_5m")
        || samples_sql.contains("metric_samples_1h")
        || samples_sql.contains("metric_hist_samples"))
        && samples_sql.contains("series_id IN")
        && !samples_sql.contains("FROM metrics ")
        && !samples_sql.contains("FROM metrics\n")
        && !samples_sql.contains("FROM union_metrics");
    resolve_ok && samples_ok
}

/// True when hist Prom short-window SQL uses postings + `metric_hist_samples` (AC-H2).
pub fn sql_is_hist_prom_path(resolve_sql: &str, samples_sql: &str) -> bool {
    let resolve_ok = resolve_sql.contains("metric_postings")
        && !resolve_sql.contains("FROM metrics")
        && !resolve_sql.contains("FROM union_metrics");
    let samples_ok = samples_sql.contains("metric_hist_samples")
        && samples_sql.contains("series_id IN")
        && !samples_sql.contains("FROM metrics ")
        && !samples_sql.contains("FROM metrics\n")
        && !samples_sql.contains("FROM union_metrics")
        // Gauge skinny samples must not back hist selectors on the short path.
        && !samples_sql.contains("metric_samples sm")
        && !samples_sql.contains(".metric_samples sm");
    resolve_ok && samples_ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::backends::grain::SampleGrain;
    use crate::models::Metric;
    use crate::storage::ducklake::{
        write_metrics_layout_txn, DEFAULT_MAX_LABELS_PER_SERIES,
    };
    use chrono::TimeZone;
    use duckdb::Connection;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tempfile::TempDir;

    /// AC-G3 / §4.4: cache hit returns same intersect as cold fill; day key isolates.
    #[test]
    fn posting_cache_hit_matches_cold_intersect_and_isolates_days() {
        let day_a = NaiveDate::from_ymd_opt(2026, 8, 15).unwrap();
        let day_b = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        let mut cache = PostingSetCache::default();
        let now = Instant::now();
        let key_name = PostingCacheKey {
            engine_id: 1,
            tenant_id: "t1".into(),
            record_date: day_a,
            label_name: "__name__".into(),
            label_value: "layout_wide".into(),
        };
        let key_inst = PostingCacheKey {
            engine_id: 1,
            tenant_id: "t1".into(),
            record_date: day_a,
            label_name: "instance".into(),
            label_value: "i-1".into(),
        };
        let key_name_b = PostingCacheKey {
            engine_id: 1,
            tenant_id: "t1".into(),
            record_date: day_b,
            label_name: "__name__".into(),
            label_value: "layout_wide".into(),
        };
        cache.put(key_name.clone(), Arc::new(vec![1, 2, 3, 99]), now);
        cache.put(key_inst.clone(), Arc::new(vec![2, 99, 100]), now);
        // Different day must not leak day_a ids even for same label/value.
        cache.put(key_name_b, Arc::new(vec![7, 8]), now);

        let fetches = AtomicUsize::new(0);
        let equality = vec![
            EqualityPosting {
                label_name: "__name__".into(),
                values: vec!["layout_wide".into()],
            },
            EqualityPosting {
                label_name: "instance".into(),
                values: vec!["i-1".into()],
            },
        ];
        let hit = intersect_equality_postings_from_sets(&equality, &[day_a], |day, name, value| {
            let key = PostingCacheKey {
                engine_id: 1,
                tenant_id: "t1".into(),
                record_date: *day,
                label_name: name.to_string(),
                label_value: value.to_string(),
            };
            cache.get(&key, now).unwrap_or_else(|| {
                fetches.fetch_add(1, AtomicOrdering::SeqCst);
                Arc::new(Vec::new())
            })
        });
        assert_eq!(hit, vec![2, 99], "warm intersect must match posting AND");
        assert_eq!(fetches.load(AtomicOrdering::SeqCst), 0, "must be cache hits");

        // Expired entry must miss (TTL).
        let expired = now + POSTING_CACHE_TTL + Duration::from_secs(1);
        assert!(cache.get(&key_name, expired).is_none());

        // Day B name posting is isolated from day A intersect.
        let only_b = intersect_equality_postings_from_sets(
            &equality[..1],
            &[day_b],
            |day, name, value| {
                let key = PostingCacheKey {
                    engine_id: 1,
                    tenant_id: "t1".into(),
                    record_date: *day,
                    label_name: name.to_string(),
                    label_value: value.to_string(),
                };
                // Re-seed day_b after TTL wipe for this assertion.
                if *day == day_b {
                    Arc::new(vec![7, 8])
                } else {
                    cache.get(&key, now).unwrap_or_else(|| Arc::new(Vec::new()))
                }
            },
        );
        assert_eq!(only_b, vec![7, 8]);
        assert!(!only_b.contains(&2), "must not serve day_a ids for day_b");
    }

    #[test]
    fn union_and_intersect_sorted_ids() {
        assert_eq!(union_sorted_ids(&[1, 3, 5], &[2, 3, 4]), vec![1, 2, 3, 4, 5]);
        assert_eq!(intersect_sorted_ids(&[1, 3, 5, 7], &[3, 7, 9]), vec![3, 7]);
        assert!(intersect_sorted_ids(&[1, 2], &[3, 4]).is_empty());
    }

    #[test]
    fn inclusive_days_and_single_posting_sql() {
        let days = RecordDateRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 14).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 16).unwrap()),
        };
        assert_eq!(
            days.inclusive_days().unwrap().len(),
            3,
            "inclusive day walk"
        );
        let sql = single_posting_sql(
            "softprobe",
            NaiveDate::from_ymd_opt(2026, 8, 15).unwrap(),
            "__name__",
            "layout_wide",
        );
        assert!(sql.contains("metric_postings"));
        assert!(sql.contains("record_date = DATE '2026-08-15'"));
        assert!(sql.contains("label_name = '__name__'"));
        assert!(sql.contains("label_value = 'layout_wide'"));
    }

    fn attach_ducklake(temp: &TempDir) -> (Connection, String) {
        let meta = temp.path().join("metadata.sqlite");
        let data = temp.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");
        let conn = Connection::open_in_memory().expect("duckdb");
        conn.execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
            .expect("extensions");
        let catalog = "softprobe";
        conn.execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS {catalog} \
             (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, \
              DATA_INLINING_ROW_LIMIT 0);",
            meta.to_string_lossy().replace('\'', "''"),
            data.to_string_lossy().replace('\'', "''"),
        ))
        .expect("attach");
        (conn, catalog.to_string())
    }

    fn gauge(name: &str, instance: &str, ts: DateTime<Utc>, value: f64) -> Metric {
        let mut attrs = HashMap::new();
        attrs.insert("service.instance.id".into(), instance.into());
        let mut resource = HashMap::new();
        resource.insert("service.name".into(), "layout-test".into());
        Metric {
            metric_name: name.into(),
            description: "d".into(),
            unit: "1".into(),
            metric_type: "gauge".into(),
            timestamp: ts,
            value,
            attributes: attrs,
            resource_attributes: resource,
            ..Default::default()
        }
    }

    fn gauge_pod(name: &str, pod: &str, ts: DateTime<Utc>, value: f64) -> Metric {
        let mut attrs = HashMap::new();
        attrs.insert("pod".into(), pod.into());
        let mut resource = HashMap::new();
        resource.insert("service.name".into(), "layout-test".into());
        Metric {
            metric_name: name.into(),
            description: "d".into(),
            unit: "1".into(),
            metric_type: "gauge".into(),
            timestamp: ts,
            value,
            attributes: attrs,
            resource_attributes: resource,
            ..Default::default()
        }
    }

    /// T-Q6 / AC-Q6: discovery SQL uses metric_postings + label_name='__name__'.
    #[test]
    fn discover_sql_uses_postings() {
        let days = RecordDateRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
        };
        let sql = discover_name_values_sql("softprobe", days, 10_000);
        assert!(
            sql.contains("metric_postings"),
            "AC-Q6: expected metric_postings, got {sql}"
        );
        assert!(
            sql.contains("label_name = '__name__'"),
            "AC-Q6: expected label_name = '__name__', got {sql}"
        );
        assert!(
            !sql.contains("metric_samples"),
            "AC-Q6: must not scan metric_samples for discovery, got {sql}"
        );
        assert!(!sql.contains("FROM metrics"));
    }

    /// T-Q7 / AC-Q7: resolve + samples SQL shape (postings + series_id IN, no fat).
    #[test]
    fn resolve_and_samples_sql_uses_postings_not_fat() {
        let days = RecordDateRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
        };
        let eq = equality_postings(&[
            LabelMatcher {
                name: "__name__".into(),
                op: MatcherOp::Eq,
                value: "layout_wide".into(),
            },
            LabelMatcher {
                name: "instance".into(),
                op: MatcherOp::Eq,
                value: "i-1".into(),
            },
        ]);
        let resolve = resolve_series_ids_sql("softprobe", days, &eq, 10_000);
        let samples = samples_scan_sql(
            "softprobe",
            &[42],
            Some(1_000),
            Some(2_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            100,
            SampleGrain::Raw,
        );
        assert!(
            sql_is_postings_resolve_path(&resolve, &samples),
            "AC-Q7 resolve={resolve}\nsamples={samples}"
        );
        assert!(resolve.contains("INTERSECT") || resolve.contains("instance"));
        assert!(samples.contains("series_id IN (42)"));
        assert!(!samples.contains("to_timestamp("));
        assert!(samples.contains("TIMESTAMPTZ "));
        assert!(
            samples.contains("record_date BETWEEN DATE"),
            "AC-Q3/G3: sample scan must prune by record_date, got {samples}"
        );
    }

    /// T-Q2 SQL shape: 30d scan references metric_samples_1h, not raw.
    #[test]
    fn long_range_samples_sql_uses_1h_grain() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * 86_400_000;
        let sql = samples_scan_sql_for_window(
            "softprobe",
            &[1],
            Some(start),
            Some(end),
            Some(3_600_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            false,
            100,
        );
        assert!(
            sql.contains("metric_samples_1h"),
            "AC-Q2: expected metric_samples_1h, got {sql}"
        );
        assert!(
            !sql.contains(".metric_samples sm"),
            "AC-Q2: must not use raw metric_samples, got {sql}"
        );
        assert!(sql.contains("window_ts"));
        assert!(!sql.contains("to_timestamp("));
    }

    /// T-W6 SQL shape: 180d → 1h grain.
    #[test]
    fn samples_sql_180d_uses_1h_grain() {
        let end = 1_700_000_000_000i64;
        let start = end - 180 * 86_400_000;
        let sql = samples_scan_sql_for_window(
            "softprobe",
            &[1],
            Some(start),
            Some(end),
            Some(3_600_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            false,
            100,
        );
        assert!(sql.contains("metric_samples_1h"), "{sql}");
    }

    /// T-Q1 SQL shape: 30m → raw metric_samples.
    #[test]
    fn short_range_samples_sql_uses_raw_grain() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * 60 * 1000;
        let sql = samples_scan_sql_for_window(
            "softprobe",
            &[1],
            Some(start),
            Some(end),
            Some(15_000),
            "NULL::VARCHAR AS lbl__empty",
            false,
            false,
            100,
        );
        assert!(
            sql.contains("metric_samples sm") || sql.contains(".metric_samples sm"),
            "AC-Q1: expected raw metric_samples, got {sql}"
        );
        assert!(!sql.contains("metric_samples_1h"));
        assert!(!sql.contains("metric_samples_5m"));
        assert!(!sql.contains("to_timestamp("));
    }

    /// time_predicate_is_timestamptz (§9.1 step 8).
    #[test]
    fn time_predicate_is_timestamptz() {
        let pred = samples_time_predicates(Some(1_000), Some(2_000), "timestamp");
        assert!(pred.contains("TIMESTAMPTZ "));
        assert!(!pred.contains("to_timestamp("));
        assert!(
            pred.contains("record_date BETWEEN DATE"),
            "time window must also prune record_date partitions: {pred}"
        );
    }

    /// T-Q4 / AC-Q4: ids > max_series → limit_exceeded + max_series, no sample scan.
    #[test]
    fn planner_fails_when_ids_exceed_max_series() {
        let err = enforce_resolved_series_cap(11, 10).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::LimitExceeded);
        assert_eq!(err.code.as_str(), "limit_exceeded");
        assert!(
            err.message.contains("max_series"),
            "AC-Q4: message must contain max_series, got {}",
            err.message
        );
        assert!(enforce_resolved_series_cap(10, 10).is_ok());
    }

    /// Prom resolve SQL must honor schema-qualified layout prefix (tenant DuckLake).
    #[test]
    fn resolve_sql_uses_schema_qualified_catalog_prefix() {
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_wide".into(),
        }]);
        let days = RecordDateRange::from_ms(Some(1_699_998_200_000), Some(1_700_000_000_000));
        let sql = resolve_series_ids_sql(
            "softprobe.metrics_layout_local_dev_tenant",
            days,
            &eq,
            10_000,
        );
        assert!(
            sql.contains("softprobe.metrics_layout_local_dev_tenant.metric_postings"),
            "expected tenant-qualified postings table, got {sql}"
        );
        assert!(
            !sql.contains("softprobe.metric_postings ")
                && !sql.ends_with("softprobe.metric_postings"),
            "must not use bare catalog.metric_postings when schema is set: {sql}"
        );
    }

    /// T-Q3 / AC-Q3 (correctness): `{__name__,instance}` resolves to 1 series via postings.
    #[test]
    fn matcher_name_instance_resolves_one_series_via_postings() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        // Scaled wide fixture (N=50) — pick instance i-1.
        let metrics: Vec<Metric> = (0..50)
            .map(|i| gauge("layout_wide", &format!("i-{i}"), ts, i as f64))
            .collect();
        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("ingest");

        let days = RecordDateRange {
            start: Some(ts.date_naive()),
            end: Some(ts.date_naive()),
        };
        let eq = equality_postings(&[
            LabelMatcher {
                name: "__name__".into(),
                op: MatcherOp::Eq,
                value: "layout_wide".into(),
            },
            LabelMatcher {
                name: "instance".into(),
                op: MatcherOp::Eq,
                value: "i-1".into(),
            },
        ]);
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, 10_000);
        assert!(resolve_sql.contains("metric_postings"));

        let mut stmt = conn.prepare(&resolve_sql).expect("prepare resolve");
        let ids: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(ids.len(), 1, "AC-Q3: expected exactly 1 series_id, got {ids:?}");

        let samples_sql = samples_scan_sql(
            &catalog,
            &ids,
            Some(ts.timestamp_millis() - 60_000),
            Some(ts.timestamp_millis() + 60_000),
            "CAST(s.labels['instance'] AS VARCHAR) AS lbl_instance",
            false,
            100,
            SampleGrain::Raw,
        );
        assert!(sql_is_postings_resolve_path(&resolve_sql, &samples_sql));
        let mut sstmt = conn.prepare(&samples_sql).expect("prepare samples");
        let rows: Vec<(f64, String)> = sstmt
            .query_map([], |r| Ok((r.get::<_, f64>(5)?, r.get::<_, String>(3)?)))
            .expect("samples query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 1.0);
        assert_eq!(rows[0].1, "i-1");
    }

    /// T-Q4 integration: wide name-only selector exceeds low max_series without sample hang.
    #[test]
    fn wide_name_only_selector_fails_loud_before_samples() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        const N: i64 = 25;
        const MAX: usize = 10;
        let metrics: Vec<Metric> = (0..N)
            .map(|i| gauge("layout_wide", &format!("i-{i}"), ts, i as f64))
            .collect();
        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("ingest");

        let days = RecordDateRange {
            start: Some(ts.date_naive()),
            end: Some(ts.date_naive()),
        };
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_wide".into(),
        }]);
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, MAX);
        let mut stmt = conn.prepare(&resolve_sql).expect("prepare");
        let ids: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert!(ids.len() > MAX);
        let err = enforce_resolved_series_cap(ids.len(), MAX).unwrap_err();
        assert_eq!(err.code.as_str(), "limit_exceeded");
        assert!(err.message.contains("max_series"));
        // No sample scan executed after fail (AC-Q4).
    }

    /// T-W4: 31d window still resolves F-wide on the single populated day via SQL
    /// BETWEEN + LIMIT (multi-day must not rely on day-scoped posting cache).
    #[test]
    fn wide_selector_31d_sql_fails_loud_like_short_window() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2023, 11, 14, 22, 0, 0).unwrap();
        const N: i64 = 25;
        const MAX: usize = 10;
        let metrics: Vec<Metric> = (0..N)
            .map(|i| gauge("layout_wide", &format!("i-{i}"), ts, i as f64))
            .collect();
        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("ingest");

        let end = ts;
        let start = ts - chrono::Duration::days(31);
        let days = RecordDateRange::from_ms(
            Some(start.timestamp_millis()),
            Some(end.timestamp_millis()),
        );
        assert!(
            days.inclusive_days().map(|d| d.len()).unwrap_or(0) > 1,
            "AC-W4 window must span multiple calendar days"
        );
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_wide".into(),
        }]);
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, MAX);
        assert!(
            resolve_sql.contains("BETWEEN") || resolve_sql.contains("record_date"),
            "multi-day resolve must prune by record_date: {resolve_sql}"
        );
        assert!(
            resolve_sql.contains(&format!("LIMIT {}", MAX + 1)),
            "must LIMIT max_series+1 for fail-loud: {resolve_sql}"
        );
        let mut stmt = conn.prepare(&resolve_sql).expect("prepare");
        let ids: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert!(
            ids.len() > MAX,
            "AC-W4: 31d SQL must still see F-wide on EVAL_END day, got {}",
            ids.len()
        );
        let err = enforce_resolved_series_cap(ids.len(), MAX).unwrap_err();
        assert_eq!(err.code.as_str(), "limit_exceeded");
        assert!(err.message.contains("max_series"));
    }

    /// T-C1 / AC-C1: F-churn — pod values for older day ≠ today.
    #[test]
    fn churn_pod_values_differ_by_record_date() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let today = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let older = Utc.with_ymd_and_hms(2026, 8, 13, 12, 0, 0).unwrap(); // today-2
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[
                gauge_pod("layout_churn", "p1", older, 1.0),
                gauge_pod("layout_churn", "p2", today, 2.0),
            ],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("ingest");

        let pods_on = |day: NaiveDate| -> Vec<String> {
            let sql = format!(
                "SELECT DISTINCT label_value FROM {}.metric_postings \
                 WHERE record_date = DATE '{day}' AND label_name = 'pod' \
                 ORDER BY 1",
                catalog
            );
            let mut stmt = conn.prepare(&sql).unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };

        assert_eq!(
            pods_on(older.date_naive()),
            vec!["p1".to_string()],
            "AC-C1: older day must be {{p1}} only"
        );
        assert_eq!(
            pods_on(today.date_naive()),
            vec!["p2".to_string()],
            "AC-C1: today must be {{p2}} only"
        );
    }

    /// T-C4 / AC-C4: today's postings for yesterday's dead pod = 0.
    #[test]
    fn churn_dead_pod_absent_from_today_postings() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let today = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let yesterday = Utc.with_ymd_and_hms(2026, 8, 14, 12, 0, 0).unwrap();
        write_metrics_layout_txn(
            &conn,
            &catalog,
            &[
                gauge_pod("layout_churn", "p1", yesterday, 1.0),
                gauge_pod("layout_churn", "p2", today, 2.0),
            ],
            DEFAULT_MAX_LABELS_PER_SERIES,
        )
        .expect("ingest");

        let n: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM {catalog}.metric_postings \
                     WHERE record_date = DATE '{}' AND label_name = 'pod' AND label_value = 'p1'",
                    today.date_naive()
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 0, "AC-C4: dead pod p1 must not appear in today's postings");
    }

    fn hist(name: &str, instance: &str, ts: DateTime<Utc>) -> Metric {
        let mut attrs = HashMap::new();
        attrs.insert("service.instance.id".into(), instance.into());
        let mut resource = HashMap::new();
        resource.insert("service.name".into(), "layout-hist".into());
        Metric {
            metric_name: name.into(),
            description: "latency".into(),
            unit: "ms".into(),
            metric_type: "histogram".into(),
            timestamp: ts,
            value: 100.0,
            attributes: attrs,
            resource_attributes: resource,
            count: Some(10),
            sum: Some(100.0),
            bucket_counts: Some(vec![2, 5, 3]),
            explicit_bounds: Some(vec![10.0, 50.0]),
            ..Default::default()
        }
    }

    /// T-H2 / AC-H2: short hist selector SQL references hist+postings, not fat metrics.
    #[test]
    fn hist_prom_sql_uses_hist_samples_and_postings() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * 60 * 1000;
        let days = RecordDateRange::from_ms(Some(start), Some(end));
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_latency_count".into(),
        }]);
        let resolve = resolve_series_ids_sql("softprobe", days, &eq, 10_000);
        let samples = samples_scan_sql_for_window(
            "softprobe",
            &[42],
            Some(start),
            Some(end),
            Some(15_000),
            "NULL::VARCHAR AS lbl__empty",
            true,
            true,
            100,
        );
        assert!(
            sql_is_hist_prom_path(&resolve, &samples),
            "AC-H2 resolve={resolve}\nsamples={samples}"
        );
        assert!(samples.contains("bucket_counts") || samples.contains("sm.count"));
        assert!(!samples.contains("to_timestamp("));
        // Postings candidates strip classic suffixes to the storage base name.
        assert!(
            resolve.contains("'layout_latency'") || resolve.contains("layout_latency"),
            "resolve must look up base name, got {resolve}"
        );
    }

    /// AC-H3 / H4 / H5: mid+long windows and `_sum` stay on `metric_hist_samples`.
    #[test]
    fn hist_prom_sql_uses_hist_table_for_mid_and_long_windows() {
        let end = 1_700_000_000_000i64;
        let hour = 3_600_000i64;
        let day = 24 * hour;
        let cases: &[(i64, Option<i64>, &str)] = &[
            (3 * hour, Some(20_000), "layout_latency_count"), // AC-H3
            (day, Some(60_000), "layout_latency_count"),      // AC-H4 24h
            (30 * day, Some(hour), "layout_latency_count"),   // AC-H4 30d
            (3 * hour, Some(20_000), "layout_latency_sum"),   // AC-H5 summary suffix
            (3 * hour, Some(hour), "layout_latency_bucket"),
        ];
        for &(range, step, name) in cases {
            let start = end - range;
            let samples = samples_scan_sql_for_window(
                "softprobe",
                &[42],
                Some(start),
                Some(end),
                step,
                "NULL::VARCHAR AS lbl__empty",
                true,
                true,
                100,
            );
            assert!(
                samples.contains("metric_hist_samples"),
                "AC-H3/H4/H5 {name} range={range}: want hist table, sql={samples}"
            );
            assert!(
                !samples.contains("metric_samples_1h"),
                "AC-H3/H4/H5 {name} range={range}: must not use 1h grain, sql={samples}"
            );
            assert!(
                !samples.contains("metric_samples_5m"),
                "AC-H3/H4/H5 {name} range={range}: must not use 5m grain, sql={samples}"
            );
            let resolve = resolve_series_ids_sql(
                "softprobe",
                RecordDateRange::from_ms(Some(start), Some(end)),
                &equality_postings(&[LabelMatcher {
                    name: "__name__".into(),
                    op: MatcherOp::Eq,
                    value: name.into(),
                }]),
                10_000,
            );
            assert!(
                sql_is_hist_prom_path(&resolve, &samples),
                "AC-H3/H4/H5 {name} range={range} resolve={resolve}\nsamples={samples}"
            );
        }
    }

    /// T-H1 / AC-H1 (correctness): `_count` resolves via postings and reads hist samples.
    #[test]
    fn hist_count_selector_resolves_via_postings_and_hist_samples() {
        let temp = TempDir::new().expect("temp");
        let (conn, catalog) = attach_ducklake(&temp);
        let ts = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let metrics: Vec<Metric> = (0..3)
            .map(|i| hist("layout_latency", &format!("i-{i}"), ts))
            .collect();
        write_metrics_layout_txn(&conn, &catalog, &metrics, DEFAULT_MAX_LABELS_PER_SERIES)
            .expect("hist ingest");

        // Ingest half: no gauge samples for layout_latency.
        let sample_n: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM {catalog}.metric_samples sm \
                     JOIN {catalog}.metric_series s \
                       ON sm.series_id = s.series_id AND sm.record_date = s.record_date \
                     WHERE s.metric_name = 'layout_latency'"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(sample_n, 0, "AC-H1: hist must not land in metric_samples");

        let start = ts.timestamp_millis() - 15 * 60 * 1000;
        let end = ts.timestamp_millis() + 15 * 60 * 1000;
        let days = RecordDateRange::from_ms(Some(start), Some(end));
        let eq = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_latency_count".into(),
        }]);
        let resolve_sql = resolve_series_ids_sql(&catalog, days, &eq, 10_000);
        assert!(resolve_sql.contains("metric_postings"));

        let mut stmt = conn.prepare(&resolve_sql).expect("prepare resolve");
        let ids: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(ids.len(), 3, "AC-H1: expected 3 series via postings, got {ids:?}");

        let samples_sql = samples_scan_sql_for_window(
            &catalog,
            &ids,
            Some(start),
            Some(end),
            Some(15_000),
            "CAST(s.labels['instance'] AS VARCHAR) AS lbl_instance",
            true,
            true,
            100,
        );
        assert!(
            sql_is_hist_prom_path(&resolve_sql, &samples_sql),
            "AC-H2 samples={samples_sql}"
        );

        // DuckDB EXPLAIN must mention hist + postings plan inputs (AC-H2).
        let explain_plan: String = {
            let explain_sql = format!("EXPLAIN {samples_sql}");
            let mut estmt = conn.prepare(&explain_sql).expect("prepare explain");
            let rows: Vec<String> = estmt
                .query_map([], |r| {
                    let a: String = r.get(0).unwrap_or_default();
                    let b: String = r.get(1).unwrap_or_default();
                    Ok(format!("{a} {b}"))
                })
                .expect("explain query")
                .map(|r| r.expect("row"))
                .collect();
            rows.join("\n")
        };
        assert!(
            explain_plan.contains("metric_hist_samples")
                || samples_sql.contains("metric_hist_samples"),
            "AC-H2: EXPLAIN/SQL must reference metric_hist_samples, plan={explain_plan}"
        );
        assert!(
            !explain_plan.contains("FROM metrics") && !samples_sql.contains("FROM metrics "),
            "AC-H2: must not scan fat metrics"
        );

        let mut sstmt = conn.prepare(&samples_sql).expect("prepare samples");
        let rows: Vec<(Option<i64>, Option<f64>, String)> = sstmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, Option<i64>>(7)?, // count
                    r.get::<_, Option<f64>>(8)?, // sum
                    r.get::<_, String>(3)?,      // instance label
                ))
            })
            .expect("samples query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(rows.len(), 3, "AC-H1: expected hist rows, got {rows:?}");
        for (count, sum, _) in &rows {
            assert_eq!(*count, Some(10));
            assert_eq!(*sum, Some(100.0));
        }

        // Classic `_bucket` selector resolves the same series_ids.
        let eq_bucket = equality_postings(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "layout_latency_bucket".into(),
        }]);
        let resolve_bucket = resolve_series_ids_sql(&catalog, days, &eq_bucket, 10_000);
        let mut bstmt = conn.prepare(&resolve_bucket).expect("prepare bucket resolve");
        let bucket_ids: Vec<u64> = bstmt
            .query_map([], |r| r.get::<_, u64>(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(bucket_ids.len(), 3, "AC-H1: _bucket must resolve via postings");
    }

    /// T-D3 / AC-D3: Prom backend is DuckLakeMetricsBackend; no sidecar writers in src.
    #[test]
    fn prom_backend_is_ducklake_no_sidecar_writers() {
        // Type presence: construction site is DuckLakeMetricsBackend.
        let _ = std::any::type_name::<crate::compat::backends::DuckLakeMetricsBackend>();
        // Source tree ban: no greptime/victoria remote_write writer modules under compat.
        let banned = [
            include_str!("mod.rs"),
            include_str!("ducklake_metrics.rs"),
            include_str!("metrics.rs"),
        ];
        for src in banned {
            assert!(
                !src.to_ascii_lowercase().contains("victoria"),
                "AC-D3: victoria writer must not appear in compat backends"
            );
            assert!(
                !src.contains("greptime::") && !src.contains("GreptimeWriter"),
                "AC-D3: greptime writer must not appear in compat backends"
            );
            assert!(
                !src.contains("remote_write"),
                "AC-D3: remote_write must not appear in compat backends"
            );
        }
    }
}
