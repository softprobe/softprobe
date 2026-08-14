//! DuckLake-backed Prometheus metrics discovery + sample fetch.

use crate::compat::backends::metrics::{
    labels_match, labels_match_any, LabelMatcher, MatcherOp, MetricMetadata, MetricSeries,
    MetricsDiscoveryRequest, MetricsQueryBackend, MetricsQueryRequest, Sample,
};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::ordering::cmp_label_pairs;
use crate::compat::projection::prometheus::{
    project_prometheus_labels, project_prometheus_metric_type, sanitize_label_name,
};
use crate::compat::tenant::TenantContext;
use crate::query::duckdb::QueryResult;
use crate::query::QueryEngine;
use crate::storage::schema::{variant_json_to_string_map, variant_varchar};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Metrics backend that scans the tenant DuckLake `metrics` table.
pub struct DuckLakeMetricsBackend {
    query: Arc<QueryEngine>,
}

impl DuckLakeMetricsBackend {
    pub fn new(query: Arc<QueryEngine>) -> Self {
        Self { query }
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
            parts.push(format!(
                "timestamp >= to_timestamp({} / 1000.0)",
                start as f64
            ));
        }
        if let Some(end) = end_ms {
            parts.push(format!(
                "timestamp <= to_timestamp({} / 1000.0)",
                end as f64
            ));
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

    async fn scan_rows(
        &self,
        ctx: &TenantContext,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        include_fidelity: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<RawMetricRow>, CompatError> {
        Self::check_deadline(ctx)?;
        ctx.limits.validate_time_range_ms(start_ms, end_ms)?;
        let cap = Self::scan_cap(ctx);
        // Fetch one past the cap so we can fail loud instead of silently truncating.
        let fetch_limit = cap.saturating_add(1);
        let time = Self::time_predicates(start_ms, end_ms);
        let matcher_sql = Self::matcher_predicates(matchers);
        let fidelity = if include_fidelity {
            ", metric_type, count, sum, bucket_counts, explicit_bounds, quantiles"
        } else {
            ", metric_type, NULL::UBIGINT AS count, NULL::DOUBLE AS sum, NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, NULL AS quantiles"
        };
        let sql = format!(
            "SELECT metric_name, description, unit, \
             CAST(attributes AS JSON) AS attributes, \
             CAST(resource_attributes AS JSON) AS resource_attributes, \
             CAST((epoch(timestamp) * 1000) AS BIGINT) AS timestamp_ms, \
             value{fidelity} \
             FROM union_metrics \
             WHERE 1=1{time}{matcher_sql} \
             ORDER BY timestamp DESC \
             LIMIT {fetch_limit}"
        );
        let result = self.execute_soft(ctx, &sql).await?;
        Self::check_deadline(ctx)?;
        if result.rows.len() > cap {
            return Err(scan_cap_exceeded(cap));
        }
        Ok(parse_raw_rows(&result))
    }

    /// Push common equality matchers into SQL to avoid full-window table scans.
    ///
    /// Pushed today: `__name__` → typed `metric_name`; `job` / `instance` via
    /// DuckLake VARIANT field access (not `CAST(... AS JSON)`). Other matchers
    /// still filter after Prometheus projection in Rust.
    ///
    /// Classic histogram/summary selectors use expanded Prometheus names
    /// (`{base}_bucket` / `_sum` / `_count`) while DuckLake stores the OTel
    /// base `metric_name`. Strip those suffixes before comparing so pushdown
    /// does not empty the scan.
    fn matcher_predicates(matchers: &[LabelMatcher]) -> String {
        let mut parts = Vec::new();
        for m in matchers {
            if m.op != MatcherOp::Eq {
                continue;
            }
            if m.name == "__name__" {
                let storage_name = classic_base_metric_name(&m.value);
                let lit = sql_string_literal(storage_name);
                // sanitize_label_name replaces non [A-Za-z0-9_] with '_'; digit-leading names get a '_'.
                parts.push(format!(
                    "(metric_name = {lit} OR regexp_replace(metric_name, '[^A-Za-z0-9_]', '_', 'g') = {lit} \
                     OR ('_' || regexp_replace(metric_name, '[^A-Za-z0-9_]', '_', 'g')) = {lit})"
                ));
            } else if m.name == "job" {
                let lit = sql_string_literal(&m.value);
                // Prefer VARIANT shredding paths over JSON extract so DuckLake can
                // prune shredded nested fields / file stats.
                parts.push(format!(
                    "({svc_res} = {lit} OR {svc_attr} = {lit} OR {job_attr} = {lit} OR {job_res} = {lit})",
                    svc_res = variant_varchar("resource_attributes", "service.name"),
                    svc_attr = variant_varchar("attributes", "service.name"),
                    job_attr = variant_varchar("attributes", "job"),
                    job_res = variant_varchar("resource_attributes", "job"),
                    lit = lit,
                ));
            } else if m.name == "instance" {
                let lit = sql_string_literal(&m.value);
                parts.push(format!(
                    "({inst_res} = {lit} OR {inst_attr} = {lit} OR {host_res} = {lit} OR {host_attr} = {lit} \
                     OR {inst_label_attr} = {lit} OR {inst_label_res} = {lit})",
                    inst_res = variant_varchar("resource_attributes", "service.instance.id"),
                    inst_attr = variant_varchar("attributes", "service.instance.id"),
                    host_res = variant_varchar("resource_attributes", "host.name"),
                    host_attr = variant_varchar("attributes", "host.name"),
                    inst_label_attr = variant_varchar("attributes", "instance"),
                    inst_label_res = variant_varchar("resource_attributes", "instance"),
                    lit = lit,
                ));
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
}

#[async_trait]
impl MetricsQueryBackend for DuckLakeMetricsBackend {
    async fn query_range(
        &self,
        ctx: &TenantContext,
        request: MetricsQueryRequest,
    ) -> Result<Vec<MetricSeries>, CompatError> {
        let rows = self
            .scan_rows(
                ctx,
                request.start_ms,
                request.end_ms,
                true,
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
        // Discovery may OR across match[] groups — pull the time window without
        // over-filtering in SQL; matcher groups still apply in Rust.
        let flat: Vec<LabelMatcher> = if req.matchers.len() == 1 {
            req.matchers[0].clone()
        } else {
            Vec::new()
        };
        let rows = self
            .scan_rows(ctx, req.start_ms, req.end_ms, true, &flat)
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
        let flat: Vec<LabelMatcher> = if req.matchers.len() == 1 {
            req.matchers[0].clone()
        } else {
            Vec::new()
        };
        let rows = self
            .scan_rows(ctx, req.start_ms, req.end_ms, true, &flat)
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
            .scan_rows(ctx, req.start_ms, req.end_ms, true, &flat)
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

fn parse_raw_rows(result: &QueryResult) -> Vec<RawMetricRow> {
    let idx = |name: &str| result.columns.iter().position(|c| c == name);
    let i_name = idx("metric_name");
    let i_desc = idx("description");
    let i_unit = idx("unit");
    let i_attrs = idx("attributes");
    let i_res = idx("resource_attributes");
    let i_ts = idx("timestamp_ms");
    let i_val = idx("value");
    let i_type = idx("metric_type");
    let i_count = idx("count");
    let i_sum = idx("sum");
    let i_buckets = idx("bucket_counts");
    let i_bounds = idx("explicit_bounds");

    result
        .rows
        .iter()
        .filter_map(|row| {
            let metric_name = i_name.and_then(|i| cell_str(row, i))?;
            Some(RawMetricRow {
                metric_name,
                description: i_desc.and_then(|i| cell_str(row, i)).unwrap_or_default(),
                unit: i_unit.and_then(|i| cell_str(row, i)).unwrap_or_default(),
                metric_type: i_type.and_then(|i| cell_str(row, i)).unwrap_or_default(),
                resource: i_res
                    .and_then(|i| row.get(i))
                    .map(json_to_string_map)
                    .unwrap_or_default(),
                datapoint: i_attrs
                    .and_then(|i| row.get(i))
                    .map(json_to_string_map)
                    .unwrap_or_default(),
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

fn json_to_string_map(value: &Value) -> HashMap<String, String> {
    variant_json_to_string_map(value)
}

fn scan_cap_exceeded(cap: usize) -> CompatError {
    CompatError::new(
        CompatErrorCode::LimitExceeded,
        format!(
            "metrics scan exceeded scan_cap {cap} (max_series-derived); narrow the time window"
        ),
    )
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Map a Prometheus `__name__` matcher to the DuckLake storage `metric_name`
/// identity used for SQL pushdown (classic histogram/summary suffixes removed).
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
        let sql = DuckLakeMetricsBackend::matcher_predicates(&[
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
        ]);
        assert!(sql.contains("regexp_replace(metric_name"));
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
    fn matcher_predicates_strip_classic_histogram_suffix() {
        let sql = DuckLakeMetricsBackend::matcher_predicates(&[LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: "http_server_duration_bucket".into(),
        }]);
        assert!(
            sql.contains("'http_server_duration'"),
            "pushdown must use base storage name, got {sql}"
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
