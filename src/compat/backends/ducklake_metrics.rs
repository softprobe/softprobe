//! DuckLake-backed Prometheus metrics discovery + sample fetch.

use crate::compat::backends::metrics::{
    labels_match, labels_match_any, LabelMatcher, MatcherOp, MetricMetadata, MetricSeries,
    MetricsDiscoveryRequest, MetricsQueryBackend, MetricsQueryRequest, Sample,
};
use crate::compat::backends::postings_resolve::{
    discover_name_values_sql, enforce_resolved_series_cap, equality_postings,
    intersect_equality_postings_from_sets, resolve_series_ids_sql, samples_scan_sql_for_window,
    single_posting_sql, timestamptz_literal_ms, EqualityPosting, PostingCacheKey, PostingSetCache,
    RecordDateRange,
};
use crate::compat::backends::prom_labels::{
    bindings_for_keys, metrics_promotion_by_source, parse_variant_stats_path,
    reserved_identity_keys, LabelBinding,
};
use crate::compaction::collapse::{collapse_fetch_limit, collapse_scan_sql};
use crate::storage::schema::variant::variant_varchar;
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::ordering::cmp_label_pairs;
use crate::compat::projection::prometheus::{
    project_prometheus_labels, project_prometheus_metric_type, sanitize_label_name,
};
use crate::compat::tenant::TenantContext;
use crate::promotion::telemetry_manifest_from_row;
use crate::query::duckdb::QueryResult;
use crate::query::QueryEngine;
use async_trait::async_trait;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Metrics backend that scans the tenant DuckLake `metrics` table.
pub struct DuckLakeMetricsBackend {
    query: Arc<QueryEngine>,
}

/// Short-TTL scan cache: Grafana refresh storms repeat the same selector window.
static SCAN_CACHE: Lazy<Mutex<ScanCache>> = Lazy::new(|| Mutex::new(ScanCache::default()));

/// Cached open attribute/resource keys from DuckLake variant stats.
static VARIANT_KEYS_CACHE: Lazy<Mutex<TimedCache<BTreeSet<String>>>> =
    Lazy::new(|| Mutex::new(TimedCache::default()));

/// Cached metrics promotion source→column map.
static PROMOTIONS_CACHE: Lazy<Mutex<TimedCache<BTreeMap<String, String>>>> =
    Lazy::new(|| Mutex::new(TimedCache::default()));

/// Grafana polls `/api/v1/label/__name__/values` on every refresh. One resolve
/// at a time; later callers wait for the cached result instead of stampeding
/// the query workers and starving panel `query_range`.
static NAME_VALUES_CACHE: Lazy<Mutex<TimedCache<Arc<Vec<String>>>>> =
    Lazy::new(|| Mutex::new(TimedCache::default()));

/// Day-scoped equality posting lists (§4.4 / AC-G3). Avoids re-INTERSECT in
/// DuckDB on Grafana refresh storms; keyed by record_date so cross-day stale
/// is impossible.
static POSTING_SET_CACHE: Lazy<Mutex<PostingSetCache>> =
    Lazy::new(|| Mutex::new(PostingSetCache::default()));

/// Full equality-resolve result (§4.4 / AC-G3): one INTERSECT answer per
/// `(engine, tenant, days, equality)` so cold multi-label resolves do not
/// re-hit DuckDB on every Grafana refresh / harness repeat.
static RESOLVE_RESULT_CACHE: Lazy<Mutex<TimedCache<Arc<Vec<u64>>>>> =
    Lazy::new(|| Mutex::new(TimedCache::default()));

#[derive(Default)]
struct ScanCache {
    entries: HashMap<u64, ScanCacheEntry>,
}

struct ScanCacheEntry {
    rows: Arc<Vec<RawMetricRow>>,
    expires: Instant,
}

#[derive(Default)]
struct TimedCache<T> {
    key: Option<u64>,
    value: Option<T>,
    expires: Option<Instant>,
}

const SCAN_CACHE_TTL: Duration = Duration::from_secs(30);
const SCAN_CACHE_MAX: usize = 256;
const META_CACHE_TTL: Duration = Duration::from_secs(60);

#[cfg(test)]
const DISCOVERY_LOOKBACK_MS: i64 = 5 * 60 * 1000;

impl DuckLakeMetricsBackend {
    pub fn new(query: Arc<QueryEngine>) -> Self {
        Self { query }
    }

    /// Catalog prefix for `metric_*` layout tables (includes metadata schema when not `main`).
    fn layout_catalog(&self) -> String {
        self.query.layout_catalog_prefix()
    }

    fn scan_cache_key(
        &self,
        tenant_id: &str,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        step_ms: Option<i64>,
        include_fidelity: bool,
        matcher_sql: &str,
        label_proj: &str,
    ) -> u64 {
        // Bucket time bounds so Grafana refresh storms (near-identical windows) hit.
        let bucket = |ms: Option<i64>| ms.map(|v| v.div_euclid(5_000) * 5_000);
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        // Engine identity: file-backed tests share one tenant id across AppStates.
        (Arc::as_ptr(&self.query) as usize).hash(&mut hasher);
        tenant_id.hash(&mut hasher);
        bucket(start_ms).hash(&mut hasher);
        bucket(end_ms).hash(&mut hasher);
        step_ms.hash(&mut hasher);
        include_fidelity.hash(&mut hasher);
        matcher_sql.hash(&mut hasher);
        label_proj.hash(&mut hasher);
        hasher.finish()
    }

    async fn scan_cache_get(&self, key: u64) -> Option<Arc<Vec<RawMetricRow>>> {
        let mut guard = SCAN_CACHE.lock().await;
        let now = Instant::now();
        if let Some(entry) = guard.entries.get(&key) {
            if entry.expires > now {
                return Some(Arc::clone(&entry.rows));
            }
        }
        guard.entries.retain(|_, e| e.expires > now);
        None
    }

    async fn scan_cache_put(&self, key: u64, rows: Arc<Vec<RawMetricRow>>) {
        let mut guard = SCAN_CACHE.lock().await;
        if guard.entries.len() >= SCAN_CACHE_MAX {
            let now = Instant::now();
            guard.entries.retain(|_, e| e.expires > now);
            if guard.entries.len() >= SCAN_CACHE_MAX {
                // Drop an arbitrary expired-or-oldest style: clear half.
                let keys: Vec<u64> = guard
                    .entries
                    .keys()
                    .copied()
                    .take(SCAN_CACHE_MAX / 2)
                    .collect();
                for k in keys {
                    guard.entries.remove(&k);
                }
            }
        }
        guard.entries.insert(
            key,
            ScanCacheEntry {
                rows,
                expires: Instant::now() + SCAN_CACHE_TTL,
            },
        );
    }

    async fn execute_soft(
        &self,
        ctx: &TenantContext,
        sql: &str,
    ) -> Result<QueryResult, CompatError> {
        Self::check_deadline(ctx)?;
        let remaining = ctx.remaining();
        let exec = self.query.execute_query(sql);
        let timed = tokio::time::timeout(remaining, exec).await;
        match timed {
            Err(_) => Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                "query deadline exceeded",
            )),
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => {
                let msg = err.to_string();
                // Fresh tenants may not have created metrics tables yet — treat as empty
                // result set (approved empty-tenant contract), not a query failure.
                if msg.contains("Table with name metrics does not exist")
                    || msg.contains("Table with name tm_all_metric does not exist")
                    || msg.contains("Table with name tm_cq_metric does not exist")
                    || msg.contains("Table with name metric_samples does not exist")
                    || msg.contains("Table with name metric_samples_5m does not exist")
                    || msg.contains("Table with name metric_samples_1h does not exist")
                    || msg.contains("Table with name metric_series does not exist")
                    || msg.contains("Table with name metric_hist_samples does not exist")
                    || msg.contains("Table with name metric_postings does not exist")
                    || msg.contains("Table with name metric_collapse_job_1h does not exist")
                {
                    return Ok(QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        row_count: 0,
                    });
                }
                Err(CompatError::new(
                    CompatErrorCode::BadRequest,
                    format!("metrics query failed: {msg}"),
                ))
            }
        }
    }

    fn check_deadline(ctx: &TenantContext) -> Result<(), CompatError> {
        if ctx.remaining().is_zero() {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                "query deadline exceeded",
            ));
        }
        Ok(())
    }

    fn time_predicates(start_ms: Option<i64>, end_ms: Option<i64>) -> String {
        let mut parts = Vec::new();
        if let Some(start) = start_ms {
            parts.push(format!("timestamp >= {}", timestamptz_literal_ms(start)));
        }
        if let Some(end) = end_ms {
            parts.push(format!("timestamp <= {}", timestamptz_literal_ms(end)));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(" AND {}", parts.join(" AND "))
        }
    }

    fn scan_cap(ctx: &TenantContext) -> usize {
        ctx.limits.max_series.saturating_mul(10).max(10_000)
    }

    /// Classic hist/summary Prom name (`_bucket` / `_sum` / `_count`) for grain.
    fn is_classic_hist_selector(matchers: &[LabelMatcher]) -> bool {
        matchers.iter().any(|m| {
            m.name == "__name__"
                && m.op == MatcherOp::Eq
                && (m.value.ends_with("_bucket")
                    || m.value.ends_with("_sum")
                    || m.value.ends_with("_count"))
        })
    }

    async fn scan_rows(
        &self,
        ctx: &TenantContext,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        step_ms: Option<i64>,
        include_fidelity: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<RawMetricRow>, CompatError> {
        Self::check_deadline(ctx)?;
        ctx.limits.validate_time_range_ms(start_ms, end_ms)?;
        let cap = Self::scan_cap(ctx);
        let fetch_limit = cap.saturating_add(1);
        let promotions = self.load_metrics_promotions(ctx).await;
        let bindings = self
            .resolve_label_bindings(ctx, matchers, &promotions)
            .await;
        let label_proj = Self::label_select_sql_from_series(&bindings);
        let equality = equality_postings(matchers);
        let resolve_key = format!("{equality:?}");
        let cache_key = self.scan_cache_key(
            ctx.tenant_id(),
            start_ms,
            end_ms,
            step_ms,
            include_fidelity,
            &resolve_key,
            &label_proj,
        );
        if let Some(cached) = self.scan_cache_get(cache_key).await {
            if cached.len() > cap {
                return Err(scan_cap_exceeded(cap));
            }
            return Ok((*cached).clone());
        }

        let series_ids = self
            .resolve_series_ids(ctx, start_ms, end_ms, matchers)
            .await?;
        if series_ids.is_empty() {
            return Ok(Vec::new());
        }

        let catalog = self.layout_catalog();
        let is_histogram = Self::is_classic_hist_selector(matchers);
        let sql = samples_scan_sql_for_window(
            &catalog,
            &series_ids,
            start_ms,
            end_ms,
            step_ms,
            &label_proj,
            include_fidelity,
            is_histogram,
            fetch_limit,
        );
        debug_assert!(
            (sql.contains("metric_samples")
                || sql.contains("metric_hist_samples"))
                && sql.contains("series_id IN")
                && !sql.contains("FROM union_metrics")
                && !sql.contains("FROM metrics "),
            "Prom sample scan must use skinny layout tables: {sql}"
        );
        debug_assert!(
            !sql.contains("to_timestamp("),
            "Prom scan must use timestamptz literals, not to_timestamp: {sql}"
        );
        debug_assert!(
            !sql.contains("CAST(attributes AS JSON)")
                && !sql.contains("CAST(resource_attributes AS JSON)"),
            "Prom scan must not JSON-cast attribute blobs: {sql}"
        );
        let result = self.execute_soft(ctx, &sql).await?;
        Self::check_deadline(ctx)?;
        if result.rows.len() > cap {
            return Err(scan_cap_exceeded(cap));
        }
        let rows = parse_raw_rows(&result, &bindings);
        self.scan_cache_put(cache_key, Arc::new(rows.clone())).await;
        Ok(rows)
    }

    /// §9.1 step 5 / AC-Q5 / AC-W3: read pre-aggregated job series from collapse.
    async fn query_collapse(
        &self,
        ctx: &TenantContext,
        metric_name: &str,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<MetricSeries>, CompatError> {
        Self::check_deadline(ctx)?;
        ctx.limits.validate_time_range_ms(start_ms, end_ms)?;
        // Hourly collapse rows are (job × hour), not raw series samples — budget by
        // window length so 90d × J stays under LIMIT (AC-W3). Enforce max_series on
        // parsed series count below; do not apply raw scan_cap to row count.
        let fetch_limit =
            collapse_fetch_limit(ctx.limits.max_series, start_ms, end_ms);
        let catalog = self.layout_catalog();
        let sql = collapse_scan_sql(&catalog, metric_name, start_ms, end_ms, fetch_limit);
        debug_assert!(
            sql.contains("metric_collapse_job_1h") && !sql.contains("to_timestamp("),
            "collapse Prom scan must use metric_collapse_job_1h: {sql}"
        );
        let result = self.execute_soft(ctx, &sql).await?;
        Self::check_deadline(ctx)?;
        let series = parse_collapse_series(&result, metric_name)?;
        let mut out = Vec::new();
        for s in series {
            if labels_match(&s.labels, matchers)? {
                out.push(s);
            }
        }
        if out.len() > ctx.limits.max_series {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                format!(
                    "series count {} exceeds max_series {}",
                    out.len(),
                    ctx.limits.max_series
                ),
            ));
        }
        Ok(out)
    }

    /// Postings intersect → series_id set. Fails loud at max_series before samples.
    ///
    /// Single calendar-day windows use the day-scoped posting cache (Greptime II
    /// analog / AC-G3). Multi-day and unbounded windows use one DuckDB SQL with
    /// `LIMIT max_series+1` so wide selectors fail loud (AC-Q4 / AC-W4).
    async fn resolve_series_ids(
        &self,
        ctx: &TenantContext,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<u64>, CompatError> {
        Self::check_deadline(ctx)?;
        let catalog = self.layout_catalog();
        let days = RecordDateRange::from_ms(start_ms, end_ms);
        let equality = equality_postings(matchers);

        // Day-scoped posting cache is for single-day Grafana refreshes (AC-G3 / Q3).
        // Multi-day windows (AC-W4 31d) use SQL + LIMIT so wide `__name__` selectors
        // fail loud at max_series. Filling N day caches with a 100k-id day is slow
        // and eviction of that entry before intersect yields empty 200 instead of
        // limit_exceeded.
        if let Some(day_list) = days.inclusive_days() {
            if !equality.is_empty() && day_list.len() == 1 {
                let engine_id = Arc::as_ptr(&self.query) as usize;
                let tenant_id = ctx.tenant_id().to_string();
                let ids = self
                    .resolve_series_ids_cached(
                        ctx,
                        &catalog,
                        engine_id,
                        &tenant_id,
                        &day_list,
                        &equality,
                    )
                    .await?;
                enforce_resolved_series_cap(ids.len(), ctx.limits.max_series)?;
                return Ok(ids);
            }
        }

        let sql = resolve_series_ids_sql(&catalog, days, &equality, ctx.limits.max_series);
        debug_assert!(
            sql.contains("metric_postings") && !sql.contains("FROM union_metrics"),
            "Prom resolve must use metric_postings: {sql}"
        );
        let result = self.execute_soft(ctx, &sql).await?;
        Self::check_deadline(ctx)?;
        let mut ids = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            if let Some(id) = cell_u64(row, 0) {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        ids.dedup();
        enforce_resolved_series_cap(ids.len(), ctx.limits.max_series)?;
        Ok(ids)
    }

    /// Cache-backed resolve: warm path INTERSECTs day-scoped posting sets in
    /// process. On any miss with ≥2 equalities, runs one DuckDB INTERSECT SQL
    /// (avoids materializing F-wide `__name__` lists of 15k+ ids for AC-G3).
    async fn resolve_series_ids_cached(
        &self,
        ctx: &TenantContext,
        catalog: &str,
        engine_id: usize,
        tenant_id: &str,
        days: &[chrono::NaiveDate],
        equality: &[EqualityPosting],
    ) -> Result<Vec<u64>, CompatError> {
        let result_key = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            engine_id.hash(&mut hasher);
            tenant_id.hash(&mut hasher);
            for d in days {
                d.hash(&mut hasher);
            }
            format!("{equality:?}").hash(&mut hasher);
            hasher.finish()
        };
        {
            let guard = RESOLVE_RESULT_CACHE.lock().await;
            if let (Some(v), Some(exp)) = (&guard.value, guard.expires) {
                if exp > Instant::now() && guard.key == Some(result_key) {
                    return Ok((**v).clone());
                }
            }
        }

        let mut needed: Vec<(chrono::NaiveDate, String, String)> = Vec::new();
        {
            let mut guard = POSTING_SET_CACHE.lock().await;
            let now = Instant::now();
            for day in days {
                for eq in equality {
                    for value in &eq.values {
                        let key = PostingCacheKey {
                            engine_id,
                            tenant_id: tenant_id.to_string(),
                            record_date: *day,
                            label_name: eq.label_name.clone(),
                            label_value: value.clone(),
                        };
                        if guard.get(&key, now).is_none() {
                            needed.push((*day, eq.label_name.clone(), value.clone()));
                        }
                    }
                }
            }
        }
        needed.sort();
        needed.dedup();

        // Multi-equality cold/partial miss: one INTERSECT lets DuckDB prune via
        // the selective label (instance) instead of shipping the full __name__ set.
        if !needed.is_empty() && equality.len() >= 2 {
            let days_range = RecordDateRange {
                start: days.first().copied(),
                end: days.last().copied(),
            };
            let sql =
                resolve_series_ids_sql(catalog, days_range, equality, ctx.limits.max_series);
            debug_assert!(
                sql.contains("metric_postings"),
                "cached miss INTERSECT must use postings: {sql}"
            );
            let result = self.execute_soft(ctx, &sql).await?;
            Self::check_deadline(ctx)?;
            let mut ids = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                if let Some(id) = cell_u64(row, 0) {
                    ids.push(id);
                }
            }
            ids.sort_unstable();
            ids.dedup();
            {
                let mut guard = RESOLVE_RESULT_CACHE.lock().await;
                guard.key = Some(result_key);
                guard.value = Some(Arc::new(ids.clone()));
                guard.expires = Some(Instant::now() + META_CACHE_TTL);
            }
            return Ok(ids);
        }

        for (day, label_name, label_value) in needed {
            Self::check_deadline(ctx)?;
            let sql = single_posting_sql(catalog, day, &label_name, &label_value);
            debug_assert!(
                sql.contains("metric_postings") && sql.contains("record_date = DATE"),
                "cached posting fill must be day-scoped: {sql}"
            );
            let result = self.execute_soft(ctx, &sql).await?;
            let mut ids = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                if let Some(id) = cell_u64(row, 0) {
                    ids.push(id);
                }
            }
            ids.sort_unstable();
            ids.dedup();
            let key = PostingCacheKey {
                engine_id,
                tenant_id: tenant_id.to_string(),
                record_date: day,
                label_name,
                label_value,
            };
            let mut guard = POSTING_SET_CACHE.lock().await;
            guard.put(key, Arc::new(ids), Instant::now());
        }

        let mut guard = POSTING_SET_CACHE.lock().await;
        let now = Instant::now();
        let ids = intersect_equality_postings_from_sets(equality, days, |day, name, value| {
            let key = PostingCacheKey {
                engine_id,
                tenant_id: tenant_id.to_string(),
                record_date: *day,
                label_name: name.to_string(),
                label_value: value.to_string(),
            };
            guard
                .get(&key, now)
                .unwrap_or_else(|| Arc::new(Vec::new()))
        });
        {
            let mut guard = RESOLVE_RESULT_CACHE.lock().await;
            guard.key = Some(result_key);
            guard.value = Some(Arc::new(ids.clone()));
            guard.expires = Some(Instant::now() + META_CACHE_TTL);
        }
        Ok(ids)
    }

    /// Project labels from `metric_series.labels` VARIANT (layout path).
    fn label_select_sql_from_series(bindings: &[LabelBinding]) -> String {
        if bindings.is_empty() {
            return "NULL::VARCHAR AS lbl__empty".to_string();
        }
        bindings
            .iter()
            .map(|b| {
                let mut parts = Vec::new();
                parts.push(variant_varchar("s.labels", &b.prom_label));
                for k in b.resource_keys.iter().chain(b.attribute_keys.iter()) {
                    let expr = variant_varchar("s.labels", k);
                    if !parts.contains(&expr) {
                        parts.push(expr);
                    }
                }
                let alias = b.sql_alias();
                if parts.len() == 1 {
                    format!("{} AS {alias}", parts[0])
                } else {
                    format!("COALESCE({}) AS {alias}", parts.join(", "))
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Legacy fat-view label projection (kept for matcher_predicates unit tests).
    #[cfg(test)]
    fn label_select_sql(bindings: &[LabelBinding]) -> String {
        if bindings.is_empty() {
            return "NULL::VARCHAR AS lbl__empty".to_string();
        }
        bindings
            .iter()
            .map(LabelBinding::sql_expr)
            .collect::<Vec<_>>()
            .join(", ")
    }

    async fn resolve_label_bindings(
        &self,
        ctx: &TenantContext,
        matchers: &[LabelMatcher],
        promotions: &BTreeMap<String, String>,
    ) -> Vec<LabelBinding> {
        let mut keys: BTreeSet<String> = BTreeSet::new();
        for k in reserved_identity_keys() {
            keys.insert((*k).to_string());
        }
        for m in matchers {
            if m.name == "__name__" {
                continue;
            }
            keys.insert(m.name.clone());
            let dotted = prom_label_to_otel_key(&m.name);
            if dotted != m.name {
                keys.insert(dotted);
            }
        }
        keys.extend(promotions.keys().cloned());
        // Docker/K8s identity used by smoke `topk … container_name`. Do not load
        // every variant-stats key: 40 VARIANT extracts per row prevent prune.
        keys.insert("container_name".into());
        keys.insert("container.name".into());
        // Cap open identity keys; reserved aliases always kept.
        let max = ctx
            .limits
            .max_labels_per_series
            .max(reserved_identity_keys().len());
        if keys.len() > max {
            let mut reserved: BTreeSet<String> = reserved_identity_keys()
                .iter()
                .map(|s| (*s).to_string())
                .collect();
            for m in matchers {
                if m.name != "__name__" {
                    reserved.insert(m.name.clone());
                }
            }
            for k in promotions.keys() {
                reserved.insert(k.clone());
            }
            let mut rest: Vec<String> = keys.difference(&reserved).cloned().collect();
            rest.sort();
            keys = reserved;
            let room = max.saturating_sub(keys.len());
            keys.extend(rest.into_iter().take(room));
        }
        bindings_for_keys(&keys, promotions)
    }

    async fn load_metrics_promotions(&self, ctx: &TenantContext) -> BTreeMap<String, String> {
        let cache_key = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            (Arc::as_ptr(&self.query) as usize).hash(&mut hasher);
            ctx.tenant_id().hash(&mut hasher);
            hasher.finish()
        };
        {
            let guard = PROMOTIONS_CACHE.lock().await;
            if let (Some(v), Some(exp), Some(k)) = (&guard.value, guard.expires, guard.key) {
                if exp > Instant::now() && k == cache_key {
                    return v.clone();
                }
            }
        }
        Self::check_deadline(ctx).ok();
        let alias = self.query.catalog_alias();
        let sql = format!(
            "SELECT spec_id, manifest_json FROM {alias}.promotion_specs \
             WHERE status = 'active' AND target_kind = 'telemetry_columns'"
        );
        let map = match self.execute_soft(ctx, &sql).await {
            Ok(result) => {
                let mut manifests = Vec::new();
                for row in &result.rows {
                    let spec_id = row.first().and_then(|v| v.as_str()).unwrap_or("");
                    let manifest = row.get(1).and_then(|v| v.as_str()).unwrap_or("");
                    if let Ok(Some(m)) = telemetry_manifest_from_row(spec_id, manifest) {
                        manifests.push(m);
                    }
                }
                metrics_promotion_by_source(&manifests)
            }
            Err(_) => BTreeMap::new(),
        };
        let mut guard = PROMOTIONS_CACHE.lock().await;
        guard.key = Some(cache_key);
        guard.value = Some(map.clone());
        guard.expires = Some(Instant::now() + META_CACHE_TTL);
        map
    }

    async fn load_variant_identity_keys(&self, ctx: &TenantContext) -> BTreeSet<String> {
        let cache_key = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            (Arc::as_ptr(&self.query) as usize).hash(&mut hasher);
            ctx.tenant_id().hash(&mut hasher);
            hasher.finish()
        };
        {
            let guard = VARIANT_KEYS_CACHE.lock().await;
            if let (Some(v), Some(exp), Some(k)) = (&guard.value, guard.expires, guard.key) {
                if exp > Instant::now() && k == cache_key {
                    return v.clone();
                }
            }
        }
        Self::check_deadline(ctx).ok();
        let alias = self.query.catalog_alias();
        // Prefer metrics-table paths when column/table metadata is available; fall back
        // to distinct variant_path across the catalog if the join is unsupported.
        let sql = format!(
            "SELECT DISTINCT vs.variant_path \
             FROM __ducklake_metadata_{alias}.ducklake_file_variant_stats vs \
             WHERE vs.variant_path IS NOT NULL \
             LIMIT 2048"
        );
        let keys = match self.execute_soft(ctx, &sql).await {
            Ok(result) => {
                let mut out = BTreeSet::new();
                for row in &result.rows {
                    if let Some(path) = row.first().and_then(|v| v.as_str()) {
                        if let Some(key) = parse_variant_stats_path(path) {
                            out.insert(key);
                        }
                    }
                }
                out
            }
            Err(_) => BTreeSet::new(),
        };
        let mut guard = VARIANT_KEYS_CACHE.lock().await;
        guard.key = Some(cache_key);
        guard.value = Some(keys.clone());
        guard.expires = Some(Instant::now() + META_CACHE_TTL);
        keys
    }

    /// Histogram fidelity columns are heavy; only pull them when the selector
    /// can expand classic `_bucket` / `_sum` / `_count` series.
    fn wants_histogram_fidelity(matchers: &[LabelMatcher]) -> bool {
        for m in matchers {
            if m.name != "__name__" {
                continue;
            }
            if m.value.ends_with("_bucket")
                || m.value.ends_with("_sum")
                || m.value.ends_with("_count")
            {
                return true;
            }
        }
        // No __name__ equality → may be scanning mixed types; keep fidelity.
        !matchers
            .iter()
            .any(|m| m.name == "__name__" && m.op == MatcherOp::Eq)
    }

    /// Legacy SQL matcher pushdown (unit-tested; Prom path uses postings resolve).
    #[cfg(test)]
    fn matcher_predicates(
        matchers: &[LabelMatcher],
        promotions: &BTreeMap<String, String>,
    ) -> String {
        let mut parts = Vec::new();
        for m in matchers {
            if m.op != MatcherOp::Eq {
                continue;
            }
            if m.name == "__name__" {
                let cands = storage_metric_name_candidates(&m.value);
                let lits: Vec<String> = cands.iter().map(|s| sql_string_literal(s)).collect();
                parts.push(format!("metric_name IN ({})", lits.join(", ")));
            } else if m.name == "job" || m.name == "instance" || is_safe_prom_label_name(&m.name) {
                let lit = sql_string_literal(&m.value);
                let binding = crate::compat::backends::prom_labels::binding_for_key(
                    &m.name,
                    promotions,
                );
                parts.push(format!("({} = {lit})", binding.sql_value_expr()));
            }
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(" AND {}", parts.join(" AND "))
        }
    }

    fn project_row(&self, ctx: &TenantContext, row: &RawMetricRow) -> BTreeMap<String, String> {
        project_prometheus_labels(
            &row.metric_name,
            &row.resource,
            &row.datapoint,
            ctx.limits.max_labels_per_series,
        )
    }

    /// Expand classic histogram / summary rows into Prometheus series identities + samples.
    fn expand_series(
        &self,
        ctx: &TenantContext,
        rows: &[RawMetricRow],
        matchers: &[LabelMatcher],
    ) -> Result<Vec<MetricSeries>, CompatError> {
        let mut by_labels: BTreeMap<BTreeMap<String, String>, Vec<Sample>> = BTreeMap::new();

        for row in rows {
            let base = self.project_row(ctx, row);
            let expansions = expand_classic_series(row, &base);
            for (labels, value) in expansions {
                if !labels_match(&labels, matchers)? {
                    continue;
                }
                by_labels.entry(labels).or_default().push(Sample {
                    timestamp_ms: row.timestamp_ms,
                    value,
                });
            }
        }

        if by_labels.len() > ctx.limits.max_series {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                format!(
                    "series count {} exceeds max_series {}",
                    by_labels.len(),
                    ctx.limits.max_series
                ),
            ));
        }

        let mut out: Vec<MetricSeries> = by_labels
            .into_iter()
            .map(|(labels, mut samples)| {
                samples.sort_by(|a, b| {
                    a.timestamp_ms
                        .cmp(&b.timestamp_ms)
                        .then_with(|| a.value.total_cmp(&b.value))
                });
                MetricSeries { labels, samples }
            })
            .collect();
        out.sort_by(|a, b| {
            let a_pairs: Vec<_> = a
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let b_pairs: Vec<_> = b
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            a_pairs
                .iter()
                .zip(b_pairs.iter())
                .fold(std::cmp::Ordering::Equal, |acc, (l, r)| {
                    if acc != std::cmp::Ordering::Equal {
                        acc
                    } else {
                        cmp_label_pairs(l, r)
                    }
                })
                .then_with(|| a.labels.len().cmp(&b.labels.len()))
                .then_with(|| format!("{:?}", a.labels).cmp(&format!("{:?}", b.labels)))
        });
        Ok(out)
    }

    /// Label names without match[] — variant stats + promotions, no full data scan.
    /// Empty lake (no layout rows yet) returns `[]` so discovery matches the empty-tenant contract.
    async fn label_names_from_catalog(
        &self,
        ctx: &TenantContext,
        _start_ms: Option<i64>,
        _end_ms: Option<i64>,
    ) -> Result<Vec<String>, CompatError> {
        Self::check_deadline(ctx)?;
        let probe = self
            .execute_soft(ctx, "SELECT 1 FROM union_metrics LIMIT 1")
            .await?;
        if probe.row_count == 0 {
            return Ok(Vec::new());
        }
        let promotions = self.load_metrics_promotions(ctx).await;
        let mut names = BTreeSet::new();
        for k in reserved_identity_keys() {
            names.insert(sanitize_label_name(k));
        }
        for k in promotions.keys() {
            names.insert(sanitize_label_name(k));
        }
        for k in self.load_variant_identity_keys(ctx).await {
            names.insert(sanitize_label_name(&k));
        }
        names.insert("__name__".into());
        Ok(names.into_iter().collect())
    }

    async fn label_values_prometheus_names(
        &self,
        ctx: &TenantContext,
        req: &MetricsDiscoveryRequest,
        sql_matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, CompatError> {
        // Postings discovery is cheap — do not clamp to wall-clock lookback (that
        // hid fixture/historical names). Honor client start/end when present.
        let start_ms = req.start_ms;
        let end_ms = req.end_ms;
        let cache_key = discovery_cache_key(
            Arc::as_ptr(&self.query) as usize,
            ctx.tenant_id(),
            start_ms,
            end_ms,
            sql_matchers,
        );
        {
            let guard = NAME_VALUES_CACHE.lock().await;
            if let (Some(v), Some(exp)) = (&guard.value, guard.expires) {
                if exp > Instant::now() && guard.key == Some(cache_key) {
                    return Ok((**v).clone());
                }
            }
        }
        // Do not hold the cache mutex across DuckDB execution (§9.1 step 9).
        let groups = self
            .distinct_metric_names_from_postings(ctx, start_ms, end_ms, sql_matchers)
            .await?;
        let mut values = BTreeSet::new();
        for (raw_name, metric_type) in groups {
            for prom_name in prometheus_names_for_storage_metric(&raw_name, &metric_type) {
                let mut labels = BTreeMap::new();
                labels.insert("__name__".into(), prom_name);
                if !labels_match_any(&labels, &req.matchers)? {
                    continue;
                }
                values.insert(labels.remove("__name__").unwrap_or_default());
            }
        }
        let out: Vec<_> = values.into_iter().collect();
        enforce_distinct_cap(out.len(), ctx.limits.max_series, "label values")?;
        {
            let mut guard = NAME_VALUES_CACHE.lock().await;
            guard.key = Some(cache_key);
            guard.value = Some(Arc::new(out.clone()));
            guard.expires = Some(Instant::now() + META_CACHE_TTL);
        }
        Ok(out)
    }

    /// `__name__` discovery via `metric_postings` (AC-Q6), not GROUP BY samples.
    async fn distinct_metric_names_from_postings(
        &self,
        ctx: &TenantContext,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        sql_matchers: &[LabelMatcher],
    ) -> Result<Vec<(String, String)>, CompatError> {
        Self::check_deadline(ctx)?;
        ctx.limits.validate_time_range_ms(start_ms, end_ms)?;
        let catalog = self.layout_catalog();
        let days = RecordDateRange::from_ms(start_ms, end_ms);
        let mut sql = discover_name_values_sql(&catalog, days, ctx.limits.max_series);
        // Optional equality pushdown on posting label values (e.g. exact __name__).
        if !sql_matchers.is_empty() {
            let mut extras = Vec::new();
            for m in sql_matchers {
                if m.name == "__name__" && m.op == MatcherOp::Eq {
                    let vals = crate::compat::backends::postings_resolve::posting_name_values(
                        &m.value,
                    );
                    let lits: Vec<_> = vals
                        .iter()
                        .map(|v| format!("'{}'", v.replace('\'', "''")))
                        .collect();
                    extras.push(format!("p.label_value IN ({})", lits.join(", ")));
                }
            }
            if !extras.is_empty() {
                sql = sql.replacen(
                    "WHERE p.label_name = '__name__'",
                    &format!(
                        "WHERE p.label_name = '__name__' AND {}",
                        extras.join(" AND ")
                    ),
                    1,
                );
            }
        }
        debug_assert!(
            sql.contains("metric_postings") && sql.contains("label_name = '__name__'"),
            "AC-Q6 discovery must use postings: {sql}"
        );
        let result = self.execute_soft(ctx, &sql).await?;
        Self::check_deadline(ctx)?;
        let mut out = Vec::new();
        for row in &result.rows {
            let raw_name = cell_str(row, 0).unwrap_or_default();
            if raw_name.is_empty() {
                continue;
            }
            let metric_type = cell_str(row, 1).unwrap_or_else(|| "unknown".into());
            out.push((raw_name, metric_type));
        }
        if out.len() > ctx.limits.max_series {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                format!(
                    "distinct metric groups {} exceed max_series {}",
                    out.len(),
                    ctx.limits.max_series
                ),
            ));
        }
        Ok(out)
    }
}

#[async_trait]
impl MetricsQueryBackend for DuckLakeMetricsBackend {
    async fn query_range(
        &self,
        ctx: &TenantContext,
        request: MetricsQueryRequest,
    ) -> Result<Vec<MetricSeries>, CompatError> {
        if let Some(ref metric) = request.collapse_metric {
            return self
                .query_collapse(
                    ctx,
                    metric,
                    request.start_ms,
                    request.end_ms,
                    &request.matchers,
                )
                .await;
        }
        let rows = self
            .scan_rows(
                ctx,
                request.start_ms,
                request.end_ms,
                request.step_ms,
                Self::wants_histogram_fidelity(&request.matchers),
                &request.matchers,
            )
            .await?;
        self.expand_series(ctx, &rows, &request.matchers)
    }

    async fn label_names(
        &self,
        ctx: &TenantContext,
        req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError> {
        if req.matchers.is_empty() {
            return self
                .label_names_from_catalog(ctx, req.start_ms, req.end_ms)
                .await;
        }
        // Discovery may OR across match[] groups — pull the time window without
        // over-filtering in SQL; matcher groups still apply in Rust.
        let flat: Vec<LabelMatcher> = if req.matchers.len() == 1 {
            req.matchers[0].clone()
        } else {
            Vec::new()
        };
        let rows = self
            .scan_rows(
                ctx,
                req.start_ms,
                req.end_ms,
                None,
                Self::wants_histogram_fidelity(&flat),
                &flat,
            )
            .await?;
        let mut names = BTreeSet::new();
        let mut any = false;
        for row in &rows {
            let expansions = expand_classic_series(row, &self.project_row(ctx, row));
            for (labels, _) in expansions {
                if !labels_match_any(&labels, &req.matchers)? {
                    continue;
                }
                any = true;
                for k in labels.keys() {
                    names.insert(k.clone());
                }
            }
        }
        if any {
            names.insert("__name__".into());
        }
        Ok(names.into_iter().collect())
    }

    async fn label_values(
        &self,
        ctx: &TenantContext,
        name: &str,
        req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError> {
        if name == "__name__" {
            if let Some(sql_matchers) = pushdown_distinct_metric_matchers(&req.matchers) {
                return self
                    .label_values_prometheus_names(ctx, req, sql_matchers)
                    .await;
            }
        }
        let flat: Vec<LabelMatcher> = if req.matchers.len() == 1 {
            req.matchers[0].clone()
        } else {
            Vec::new()
        };
        let rows = self
            .scan_rows(
                ctx,
                req.start_ms,
                req.end_ms,
                None,
                Self::wants_histogram_fidelity(&flat),
                &flat,
            )
            .await?;
        let mut values = BTreeSet::new();
        for row in &rows {
            let expansions = expand_classic_series(row, &self.project_row(ctx, row));
            for (labels, _) in expansions {
                if !labels_match_any(&labels, &req.matchers)? {
                    continue;
                }
                if let Some(v) = labels.get(name) {
                    values.insert(v.clone());
                }
            }
        }
        let out: Vec<_> = values.into_iter().collect();
        enforce_distinct_cap(out.len(), ctx.limits.max_series, "label values")?;
        Ok(out)
    }

    async fn series(
        &self,
        ctx: &TenantContext,
        req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<BTreeMap<String, String>>, CompatError> {
        let flat: Vec<LabelMatcher> = if req.matchers.len() == 1 {
            req.matchers[0].clone()
        } else {
            Vec::new()
        };
        let rows = self
            .scan_rows(
                ctx,
                req.start_ms,
                req.end_ms,
                None,
                Self::wants_histogram_fidelity(&flat),
                &flat,
            )
            .await?;
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for row in &rows {
            let expansions = expand_classic_series(row, &self.project_row(ctx, row));
            for (labels, _) in expansions {
                if !labels_match_any(&labels, &req.matchers)? {
                    continue;
                }
                if seen.insert(labels.clone()) {
                    out.push(labels);
                }
            }
        }
        if out.len() > ctx.limits.max_series {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                format!(
                    "series count {} exceeds max_series {}",
                    out.len(),
                    ctx.limits.max_series
                ),
            ));
        }
        out.sort_by(|a, b| {
            let a_s = serde_json::to_string(a).unwrap_or_default();
            let b_s = serde_json::to_string(b).unwrap_or_default();
            a_s.cmp(&b_s)
        });
        Ok(out)
    }

    async fn metadata(
        &self,
        ctx: &TenantContext,
        metric: Option<&str>,
        limit: Option<usize>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
    ) -> Result<Vec<MetricMetadata>, CompatError> {
        Self::check_deadline(ctx)?;
        ctx.limits.validate_time_range_ms(start_ms, end_ms)?;
        let time = Self::time_predicates(start_ms, end_ms);
        // Review leftover: still scans union_metrics (layout JOIN view). Prefer
        // metric_series / postings for metadata when converting; left as-is for
        // this grain-planner slice (not on the Prom sample hot path).
        // Do not filter by raw storage metric_name in SQL — clients query projected
        // Prometheus names (e.g. http_requests for OTel http.requests).
        let lim = match limit {
            Some(n) if n > ctx.limits.max_series => {
                return Err(CompatError::new(
                    CompatErrorCode::LimitExceeded,
                    format!(
                        "metadata limit {n} exceeds max_series {}",
                        ctx.limits.max_series
                    ),
                ));
            }
            Some(n) => n.max(1),
            None => ctx.limits.max_series.max(1),
        };
        let sql = format!(
            "SELECT metric_name, \
             any_value(description) AS description, \
             any_value(unit) AS unit, \
             any_value(metric_type) AS metric_type \
             FROM union_metrics \
             WHERE 1=1{time} \
             GROUP BY metric_name \
             ORDER BY metric_name"
        );
        let result = self.execute_soft(ctx, &sql).await?;
        Self::check_deadline(ctx)?;
        let want = metric
            .map(str::trim)
            .filter(|m| !m.is_empty())
            .map(str::to_string);
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut out = Vec::new();
        for row in &result.rows {
            let raw_name = cell_str(row, 0).unwrap_or_default();
            if raw_name.is_empty() {
                continue;
            }
            let metric_name = sanitize_label_name(&raw_name);
            if let Some(ref want) = want {
                if metric_name != *want {
                    continue;
                }
            }
            if !seen.insert(metric_name.clone()) {
                continue;
            }
            out.push(MetricMetadata {
                metric_name,
                help: cell_str(row, 1).unwrap_or_default(),
                unit: cell_str(row, 2).unwrap_or_default(),
                metric_type: project_prometheus_metric_type(
                    &cell_str(row, 3).unwrap_or_else(|| "unknown".into()),
                )
                .to_string(),
            });
            if out.len() >= lim {
                break;
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone)]
struct RawMetricRow {
    metric_name: String,
    #[allow(dead_code)]
    description: String,
    #[allow(dead_code)]
    unit: String,
    metric_type: String,
    resource: HashMap<String, String>,
    datapoint: HashMap<String, String>,
    timestamp_ms: i64,
    value: f64,
    count: Option<u64>,
    sum: Option<f64>,
    bucket_counts: Option<Vec<u64>>,
    explicit_bounds: Option<Vec<f64>>,
}

fn parse_collapse_series(
    result: &QueryResult,
    default_metric: &str,
) -> Result<Vec<MetricSeries>, CompatError> {
    let idx = |name: &str| result.columns.iter().position(|c| c == name);
    let i_name = idx("metric_name");
    let i_job = idx("job");
    let i_ts = idx("timestamp_ms");
    let i_val = idx("value");
    let mut by_job: BTreeMap<String, (String, Vec<Sample>)> = BTreeMap::new();
    for row in &result.rows {
        let metric = i_name
            .and_then(|i| cell_str(row, i))
            .unwrap_or_else(|| default_metric.to_string());
        let job = i_job
            .and_then(|i| cell_str(row, i))
            .unwrap_or_default();
        let ts = i_ts.and_then(|i| cell_i64(row, i)).unwrap_or(0);
        let value = i_val.and_then(|i| cell_f64(row, i)).unwrap_or(0.0);
        by_job
            .entry(job)
            .or_insert_with(|| (metric, Vec::new()))
            .1
            .push(Sample {
                timestamp_ms: ts,
                value,
            });
    }
    Ok(by_job
        .into_iter()
        .map(|(job, (metric_name, mut samples))| {
            samples.sort_by_key(|s| s.timestamp_ms);
            let mut labels = BTreeMap::new();
            labels.insert("__name__".into(), metric_name);
            labels.insert("job".into(), job);
            MetricSeries { labels, samples }
        })
        .collect())
}

fn parse_raw_rows(result: &QueryResult, bindings: &[LabelBinding]) -> Vec<RawMetricRow> {
    let idx = |name: &str| result.columns.iter().position(|c| c == name);
    let i_name = idx("metric_name");
    let i_desc = idx("description");
    let i_unit = idx("unit");
    let i_ts = idx("timestamp_ms");
    let i_val = idx("value");
    let i_type = idx("metric_type");
    let i_count = idx("count");
    let i_sum = idx("sum");
    let i_buckets = idx("bucket_counts");
    let i_bounds = idx("explicit_bounds");
    let label_idxs: Vec<(usize, &LabelBinding)> = bindings
        .iter()
        .filter_map(|b| idx(&b.sql_alias()).map(|i| (i, b)))
        .collect();

    result
        .rows
        .iter()
        .filter_map(|row| {
            let metric_name = i_name.and_then(|i| cell_str(row, i))?;
            let mut resource = HashMap::new();
            let mut datapoint = HashMap::new();
            for (i, binding) in &label_idxs {
                let Some(val) = cell_str(row, *i) else {
                    continue;
                };
                // Put into both maps so project_prometheus_labels job/instance
                // aliases resolve regardless of whether the COALESCE came from
                // resource or datapoint attributes.
                resource.insert(binding.otel_key.clone(), val.clone());
                datapoint.insert(binding.otel_key.clone(), val);
            }
            Some(RawMetricRow {
                metric_name,
                description: i_desc.and_then(|i| cell_str(row, i)).unwrap_or_default(),
                unit: i_unit.and_then(|i| cell_str(row, i)).unwrap_or_default(),
                metric_type: i_type.and_then(|i| cell_str(row, i)).unwrap_or_default(),
                resource,
                datapoint,
                timestamp_ms: i_ts.and_then(|i| cell_i64(row, i)).unwrap_or(0),
                value: i_val.and_then(|i| cell_f64(row, i)).unwrap_or(0.0),
                count: i_count.and_then(|i| cell_u64(row, i)),
                sum: i_sum.and_then(|i| cell_f64(row, i)),
                bucket_counts: i_buckets.and_then(|i| row.get(i)).and_then(parse_u64_list),
                explicit_bounds: i_bounds.and_then(|i| row.get(i)).and_then(parse_f64_list),
            })
        })
        .collect()
}

/// Expand one raw OTel row into Prometheus series (gauge/sum as-is; histogram classic naming).
fn expand_classic_series(
    row: &RawMetricRow,
    base_labels: &BTreeMap<String, String>,
) -> Vec<(BTreeMap<String, String>, f64)> {
    let base_name = sanitize_label_name(&row.metric_name);
    let mt = row.metric_type.to_ascii_lowercase();

    if mt == "histogram"
        && (row.bucket_counts.is_some() || row.count.is_some() || row.sum.is_some())
    {
        let mut out = Vec::new();
        if let (Some(counts), Some(bounds)) = (&row.bucket_counts, &row.explicit_bounds) {
            let mut cumulative = 0u64;
            for (i, bound) in bounds.iter().enumerate() {
                cumulative = cumulative.saturating_add(counts.get(i).copied().unwrap_or(0));
                let mut labels = base_labels.clone();
                labels.insert("__name__".into(), format!("{base_name}_bucket"));
                labels.insert("le".into(), format_le(*bound));
                out.push((labels, cumulative as f64));
            }
            // +Inf bucket: remaining count after last bound, or count field.
            let last = counts.get(bounds.len()).copied().unwrap_or(0);
            cumulative = cumulative.saturating_add(last);
            let inf_count = row.count.unwrap_or(cumulative);
            let mut labels = base_labels.clone();
            labels.insert("__name__".into(), format!("{base_name}_bucket"));
            labels.insert("le".into(), "+Inf".into());
            out.push((labels, inf_count as f64));
        }
        if let Some(sum) = row.sum {
            let mut labels = base_labels.clone();
            labels.insert("__name__".into(), format!("{base_name}_sum"));
            out.push((labels, sum));
        }
        if let Some(count) = row.count {
            let mut labels = base_labels.clone();
            labels.insert("__name__".into(), format!("{base_name}_count"));
            out.push((labels, count as f64));
        }
        if !out.is_empty() {
            return out;
        }
    }

    if mt == "summary" {
        // Phase 1: expose _sum/_count + base name only. Per-quantile `_quantile`
        // series are intentionally unsupported (see docs/compat/projections.md).
        let mut out = Vec::new();
        if let Some(sum) = row.sum {
            let mut labels = base_labels.clone();
            labels.insert("__name__".into(), format!("{base_name}_sum"));
            out.push((labels, sum));
        }
        if let Some(count) = row.count {
            let mut labels = base_labels.clone();
            labels.insert("__name__".into(), format!("{base_name}_count"));
            out.push((labels, count as f64));
        }
        if !out.is_empty() {
            // Base summary value is also exposed as the sanitized name.
            let mut labels = base_labels.clone();
            labels.insert("__name__".into(), base_name);
            out.push((labels, row.value));
            return out;
        }
    }

    vec![(base_labels.clone(), row.value)]
}

fn format_le(bound: f64) -> String {
    if bound.is_infinite() && bound.is_sign_positive() {
        "+Inf".into()
    } else if bound.fract() == 0.0 && bound.abs() < 1e15 {
        format!("{}", bound as i64)
    } else {
        // Prometheus-style shortest float.
        format!("{bound}")
    }
}

fn scan_cap_exceeded(cap: usize) -> CompatError {
    CompatError::new(
        CompatErrorCode::LimitExceeded,
        format!(
            "metrics scan exceeded scan_cap {cap} (max_series-derived); narrow the time window"
        ),
    )
}

#[cfg(test)]
fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
fn is_safe_prom_label_name(name: &str) -> bool {
    !name.is_empty()
        && name != "__name__"
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Best-effort reverse of sanitize_label_name for common OTel dotted keys.
fn prom_label_to_otel_key(name: &str) -> String {
    // Known multi-segment conventions: keep underscores that are not segment
    // separators only when they appear after the first segment — convert all
    // `_` → `.` for attribute lookup (http_method → http.method).
    name.replace('_', ".")
}

/// Cap discovery scans so Grafana's 15–30m dashboard window does not GROUP BY
/// every metric row. Names that only existed earlier than the lookback are
/// omitted — acceptable for the live metric picker.
#[cfg(test)]
fn clamp_discovery_window(
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> (Option<i64>, Option<i64>) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let end = end_ms.unwrap_or(now_ms);
    let min_start = end.saturating_sub(DISCOVERY_LOOKBACK_MS);
    let start = start_ms.map(|s| s.max(min_start)).unwrap_or(min_start);
    (Some(start), Some(end))
}

fn discovery_cache_key(
    engine_token: usize,
    tenant_id: &str,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    matchers: &[LabelMatcher],
) -> u64 {
    let bucket = |ms: Option<i64>| ms.map(|v| v.div_euclid(30_000) * 30_000);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    engine_token.hash(&mut hasher);
    tenant_id.hash(&mut hasher);
    if matchers.is_empty() {
        // Grafana polls with shifting start/end; engine + tenant + TTL is enough.
        return hasher.finish();
    }
    bucket(start_ms).hash(&mut hasher);
    bucket(end_ms).hash(&mut hasher);
    for m in matchers {
        m.name.hash(&mut hasher);
        m.value.hash(&mut hasher);
    }
    hasher.finish()
}

/// Map a Prometheus `__name__` matcher to the DuckLake storage `metric_name`
/// identity used for SQL pushdown (classic histogram/summary suffixes removed).
#[cfg(test)]
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

/// Equality candidates for DuckLake `metric_name` (underscored Prom + dotted OTel).
/// Avoid `regexp_replace(metric_name, …)` in WHERE — that forces a full scan.
#[cfg(test)]
fn storage_metric_name_candidates(prom_name: &str) -> Vec<String> {
    let base = classic_base_metric_name(prom_name);
    let mut out = vec![base.to_string()];
    let dotted = base.replace('_', ".");
    if dotted != base {
        out.push(dotted);
    }
    out
}

/// Matchers safe to push into `GROUP BY metric_name` (no OR groups, only `__name__` equality).
fn pushdown_distinct_metric_matchers(matchers: &[Vec<LabelMatcher>]) -> Option<&[LabelMatcher]> {
    if matchers.is_empty() {
        return Some(&[]);
    }
    if matchers.len() == 1 {
        let group = &matchers[0];
        if group
            .iter()
            .all(|m| m.name == "__name__" && m.op == MatcherOp::Eq)
        {
            return Some(group.as_slice());
        }
    }
    None
}

/// Projected Prometheus `__name__` values for one storage metric row group.
fn prometheus_names_for_storage_metric(raw_name: &str, metric_type: &str) -> Vec<String> {
    let base_name = sanitize_label_name(raw_name);
    let mt = metric_type.to_ascii_lowercase();
    match mt.as_str() {
        "histogram" => vec![
            format!("{base_name}_bucket"),
            format!("{base_name}_sum"),
            format!("{base_name}_count"),
        ],
        "summary" => vec![
            format!("{base_name}_sum"),
            format!("{base_name}_count"),
            base_name,
        ],
        _ => vec![base_name],
    }
}

fn enforce_distinct_cap(count: usize, max: usize, what: &str) -> Result<(), CompatError> {
    if count > max {
        return Err(CompatError::new(
            CompatErrorCode::LimitExceeded,
            format!("{what} count {count} exceeds max_series {max}"),
        ));
    }
    Ok(())
}

fn cell_str(row: &[Value], idx: usize) -> Option<String> {
    match row.get(idx)? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn cell_i64(row: &[Value], idx: usize) -> Option<i64> {
    match row.get(idx)? {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn cell_u64(row: &[Value], idx: usize) -> Option<u64> {
    match row.get(idx)? {
        Value::Number(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn cell_f64(row: &[Value], idx: usize) -> Option<f64> {
    match row.get(idx)? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => match s.as_str() {
            "NaN" | "nan" => Some(f64::NAN),
            "+Inf" | "Inf" | "+inf" | "inf" => Some(f64::INFINITY),
            "-Inf" | "-inf" => Some(f64::NEG_INFINITY),
            other => other.parse().ok(),
        },
        Value::Null => None,
        _ => None,
    }
}

fn parse_u64_list(value: &Value) -> Option<Vec<u64>> {
    match value {
        Value::Array(items) => Some(
            items
                .iter()
                .filter_map(|v| match v {
                    Value::Number(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)),
                    Value::String(s) => s.parse().ok(),
                    _ => None,
                })
                .collect(),
        ),
        Value::String(s) => {
            // DuckDB may stringify arrays as "[1, 2, 3]".
            let trimmed = s.trim().trim_start_matches('[').trim_end_matches(']');
            if trimmed.is_empty() {
                return Some(vec![]);
            }
            Some(
                trimmed
                    .split(',')
                    .filter_map(|p| p.trim().parse().ok())
                    .collect(),
            )
        }
        _ => None,
    }
}

fn parse_f64_list(value: &Value) -> Option<Vec<f64>> {
    match value {
        Value::Array(items) => Some(
            items
                .iter()
                .filter_map(|v| match v {
                    Value::Number(n) => n.as_f64(),
                    Value::String(s) => s.parse().ok(),
                    _ => None,
                })
                .collect(),
        ),
        Value::String(s) => {
            let trimmed = s.trim().trim_start_matches('[').trim_end_matches(']');
            if trimmed.is_empty() {
                return Some(vec![]);
            }
            Some(
                trimmed
                    .split(',')
                    .filter_map(|p| p.trim().parse().ok())
                    .collect(),
            )
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matcher_predicates_push_name_job_instance_via_variant() {
        let empty = BTreeMap::new();
        let sql = DuckLakeMetricsBackend::matcher_predicates(
            &[
                LabelMatcher {
                    name: "__name__".into(),
                    op: MatcherOp::Eq,
                    value: "http_requests".into(),
                },
                LabelMatcher {
                    name: "job".into(),
                    op: MatcherOp::Eq,
                    value: "checkout".into(),
                },
                LabelMatcher {
                    name: "instance".into(),
                    op: MatcherOp::Eq,
                    value: "host-1".into(),
                },
                LabelMatcher {
                    name: "extra".into(),
                    op: MatcherOp::Re,
                    value: ".*".into(),
                },
            ],
            &empty,
        );
        assert!(
            sql.contains("metric_name IN (") && sql.contains("'http_requests'"),
            "name equality must push metric_name IN (...), got {sql}"
        );
        assert!(
            !sql.contains("regexp_replace"),
            "row-wise regexp on metric_name prevents prune, got {sql}"
        );
        assert!(
            sql.contains("CAST(resource_attributes['service.name'] AS VARCHAR)"),
            "job must use VARIANT field access, got {sql}"
        );
        assert!(
            sql.contains("CAST(resource_attributes['host.name'] AS VARCHAR)")
                || sql.contains("CAST(resource_attributes['service.instance.id'] AS VARCHAR)"),
            "instance must use VARIANT field access, got {sql}"
        );
        assert!(
            !sql.contains("json_extract_string"),
            "must not defeat shredding with JSON extract: {sql}"
        );
        assert!(
            !sql.contains("CAST(attributes AS JSON)")
                && !sql.contains("CAST(resource_attributes AS JSON)"),
            "matcher predicates must not CAST entire VARIANT to JSON: {sql}"
        );
        assert!(sql.contains("'checkout'"));
        assert!(sql.contains("'host-1'"));
        assert!(!sql.contains("extra"));
    }

    #[test]
    fn matcher_predicates_prefer_promoted_column() {
        let promos = BTreeMap::from([("service.name".into(), "service_name".into())]);
        let sql = DuckLakeMetricsBackend::matcher_predicates(
            &[LabelMatcher {
                name: "job".into(),
                op: MatcherOp::Eq,
                value: "checkout".into(),
            }],
            &promos,
        );
        assert!(
            sql.contains("COALESCE(service_name,"),
            "job matcher should prefer promoted column, got {sql}"
        );
        assert!(!sql.contains("CAST(attributes AS JSON)"));
    }

    #[test]
    fn matcher_predicates_push_generic_equality_via_variant() {
        let empty = BTreeMap::new();
        let sql = DuckLakeMetricsBackend::matcher_predicates(
            &[LabelMatcher {
                name: "http_method".into(),
                op: MatcherOp::Eq,
                value: "GET".into(),
            }],
            &empty,
        );
        assert!(
            sql.contains("CAST(attributes['http_method'] AS VARCHAR)"),
            "got {sql}"
        );
        assert!(
            sql.contains("CAST(attributes['http.method'] AS VARCHAR)"),
            "dotted OTel key must be tried, got {sql}"
        );
        assert!(!sql.contains("CAST(attributes AS JSON)"));
    }

    #[test]
    fn label_select_sql_never_json_casts_blobs() {
        let promos = BTreeMap::from([("service.name".into(), "service_name".into())]);
        let mut keys = BTreeSet::new();
        keys.insert("service.name".into());
        keys.insert("http.method".into());
        let bindings = bindings_for_keys(&keys, &promos);
        let sql = DuckLakeMetricsBackend::label_select_sql(&bindings);
        assert!(!sql.contains("CAST(attributes AS JSON)"));
        assert!(!sql.contains("CAST(resource_attributes AS JSON)"));
        assert!(sql.contains("lbl_service_name") || sql.contains("AS lbl_service_name"));
        assert!(sql.contains("service_name") || sql.contains("service.name"));
    }

    #[test]
    fn wants_histogram_fidelity_only_for_classic_names() {
        assert!(DuckLakeMetricsBackend::wants_histogram_fidelity(&[
            LabelMatcher {
                name: "__name__".into(),
                op: MatcherOp::Eq,
                value: "http_duration_bucket".into(),
            }
        ]));
        assert!(!DuckLakeMetricsBackend::wants_histogram_fidelity(&[
            LabelMatcher {
                name: "__name__".into(),
                op: MatcherOp::Eq,
                value: "bench_http_requests".into(),
            }
        ]));
        assert!(DuckLakeMetricsBackend::wants_histogram_fidelity(&[]));
    }

    #[test]
    fn matcher_predicates_strip_classic_histogram_suffix() {
        let empty = BTreeMap::new();
        let sql = DuckLakeMetricsBackend::matcher_predicates(
            &[LabelMatcher {
                name: "__name__".into(),
                op: MatcherOp::Eq,
                value: "http_server_duration_bucket".into(),
            }],
            &empty,
        );
        assert!(
            sql.contains("'http_server_duration'"),
            "pushdown must use base storage name, got {sql}"
        );
        assert!(
            sql.contains("'http.server.duration'"),
            "pushdown must also try dotted OTel name, got {sql}"
        );
        assert!(
            !sql.contains("regexp_replace"),
            "row-wise regexp on metric_name prevents file prune, got {sql}"
        );
        assert!(
            !sql.contains("'http_server_duration_bucket'"),
            "must not filter storage metric_name by expanded Prom name"
        );
        assert_eq!(
            classic_base_metric_name("http_server_duration_sum"),
            "http_server_duration"
        );
        assert_eq!(classic_base_metric_name("http_requests"), "http_requests");
        assert_eq!(
            storage_metric_name_candidates("k6_vus"),
            vec!["k6_vus".to_string(), "k6.vus".to_string()]
        );
    }

    #[test]
    fn histogram_expansion_cumulative_and_inf() {
        let row = RawMetricRow {
            metric_name: "http.server.duration".into(),
            description: String::new(),
            unit: "ms".into(),
            metric_type: "histogram".into(),
            resource: HashMap::new(),
            datapoint: HashMap::new(),
            timestamp_ms: 1_000,
            value: 0.0,
            count: Some(10),
            sum: Some(100.0),
            bucket_counts: Some(vec![2, 5, 3]),
            explicit_bounds: Some(vec![10.0, 50.0]),
        };
        let base =
            project_prometheus_labels("http.server.duration", &HashMap::new(), &HashMap::new(), 40);
        let series = expand_classic_series(&row, &base);
        let buckets: Vec<_> = series
            .iter()
            .filter(|(l, _)| {
                l.get("__name__").map(String::as_str) == Some("http_server_duration_bucket")
            })
            .collect();
        assert_eq!(buckets.len(), 3);
        assert_eq!(
            buckets
                .iter()
                .find(|(l, _)| l.get("le").map(String::as_str) == Some("10"))
                .map(|(_, v)| *v),
            Some(2.0)
        );
        assert_eq!(
            buckets
                .iter()
                .find(|(l, _)| l.get("le").map(String::as_str) == Some("50"))
                .map(|(_, v)| *v),
            Some(7.0)
        );
        assert_eq!(
            buckets
                .iter()
                .find(|(l, _)| l.get("le").map(String::as_str) == Some("+Inf"))
                .map(|(_, v)| *v),
            Some(10.0)
        );
    }

    #[test]
    fn clamp_discovery_window_caps_wide_grafana_range() {
        let end = 1_700_000_000_000i64;
        let start = end - 30 * 60 * 1000;
        let (c_start, c_end) = clamp_discovery_window(Some(start), Some(end));
        assert_eq!(c_end, Some(end));
        assert_eq!(c_start, Some(end - DISCOVERY_LOOKBACK_MS));
        let (again_s, again_e) = clamp_discovery_window(c_start, c_end);
        assert_eq!((again_s, again_e), (c_start, c_end));
    }

    #[test]
    fn prometheus_names_for_storage_metric_expands_histogram_and_summary() {
        assert_eq!(
            prometheus_names_for_storage_metric("http.server.duration", "histogram"),
            vec![
                "http_server_duration_bucket".to_string(),
                "http_server_duration_sum".to_string(),
                "http_server_duration_count".to_string(),
            ]
        );
        assert_eq!(
            prometheus_names_for_storage_metric("rpc.latency", "summary"),
            vec![
                "rpc_latency_sum".to_string(),
                "rpc_latency_count".to_string(),
                "rpc_latency".to_string(),
            ]
        );
        assert_eq!(
            prometheus_names_for_storage_metric("http.requests", "gauge"),
            vec!["http_requests".to_string()]
        );
    }

    #[test]
    fn pushdown_distinct_metric_matchers_allows_empty_or_name_only() {
        assert_eq!(
            pushdown_distinct_metric_matchers(&[]),
            Some(&[] as &[LabelMatcher])
        );
        let group = vec![LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "http_requests".into(),
        }];
        assert_eq!(
            pushdown_distinct_metric_matchers(&[group.clone()]),
            Some(group.as_slice())
        );
        assert_eq!(
            pushdown_distinct_metric_matchers(&[
                group,
                vec![LabelMatcher {
                    name: "__name__".into(),
                    op: MatcherOp::Eq,
                    value: "other".into(),
                }]
            ]),
            None
        );
        assert_eq!(
            pushdown_distinct_metric_matchers(&[vec![LabelMatcher {
                name: "job".into(),
                op: MatcherOp::Eq,
                value: "checkout".into(),
            }]]),
            None
        );
    }

    #[test]
    fn scan_cap_and_distinct_cap_fail_loud() {
        let err = scan_cap_exceeded(10);
        assert_eq!(err.code, CompatErrorCode::LimitExceeded);
        assert!(err.message.contains("narrow the time window"));
        assert!(!err.message.contains("matcher"));
        assert!(enforce_distinct_cap(5, 10, "label values").is_ok());
        let err = enforce_distinct_cap(11, 10, "label values").unwrap_err();
        assert_eq!(err.code, CompatErrorCode::LimitExceeded);
        assert!(err.message.contains("label values count 11"));
    }
}
