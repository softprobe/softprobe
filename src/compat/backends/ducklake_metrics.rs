//! DuckLake-backed Prometheus metrics discovery + sample fetch.

use crate::compat::backends::metrics::{
    labels_match, labels_match_any, LabelMatcher, MetricMetadata, MetricSeries,
    MetricsDiscoveryRequest, MetricsQueryBackend, MetricsQueryRequest, Sample,
};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::ordering::cmp_label_pairs;
use crate::compat::projection::prometheus::{project_prometheus_labels, sanitize_label_name};
use crate::compat::tenant::TenantContext;
use crate::query::duckdb::QueryResult;
use crate::query::QueryEngine;
use crate::storage::schema::variant_json_to_string_map;
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

    async fn execute_soft(&self, sql: &str) -> Result<QueryResult, CompatError> {
        match self.query.execute_query(sql).await {
            Ok(result) => Ok(result),
            Err(err) => {
                let msg = err.to_string();
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
    ) -> Result<Vec<RawMetricRow>, CompatError> {
        Self::check_deadline(ctx)?;
        ctx.limits.validate_time_range_ms(start_ms, end_ms)?;
        let cap = Self::scan_cap(ctx);
        // Fetch one past the cap so we can fail loud instead of silently truncating.
        let fetch_limit = cap.saturating_add(1);
        let time = Self::time_predicates(start_ms, end_ms);
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
             WHERE 1=1{time} \
             ORDER BY timestamp DESC \
             LIMIT {fetch_limit}"
        );
        let result = self.execute_soft(&sql).await?;
        Self::check_deadline(ctx)?;
        if result.rows.len() > cap {
            return Err(scan_cap_exceeded(cap));
        }
        Ok(parse_raw_rows(&result))
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
            .scan_rows(ctx, request.start_ms, request.end_ms, true)
            .await?;
        self.expand_series(ctx, &rows, &request.matchers)
    }

    async fn label_names(
        &self,
        ctx: &TenantContext,
        req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError> {
        let rows = self.scan_rows(ctx, req.start_ms, req.end_ms, true).await?;
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
        let rows = self.scan_rows(ctx, req.start_ms, req.end_ms, true).await?;
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
        let rows = self.scan_rows(ctx, req.start_ms, req.end_ms, true).await?;
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
        let metric_filter = match metric {
            Some(m) if !m.is_empty() => {
                format!(
                    " AND metric_name = '{}'",
                    crate::storage::ducklake::escape_sql_literal(m)
                )
            }
            _ => String::new(),
        };
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
             WHERE 1=1{time}{metric_filter} \
             GROUP BY metric_name \
             ORDER BY metric_name \
             LIMIT {lim}"
        );
        let result = self.execute_soft(&sql).await?;
        Self::check_deadline(ctx)?;
        let mut out = Vec::new();
        for row in &result.rows {
            let metric_name = cell_str(row, 0).unwrap_or_default();
            if metric_name.is_empty() {
                continue;
            }
            out.push(MetricMetadata {
                metric_name,
                help: cell_str(row, 1).unwrap_or_default(),
                unit: cell_str(row, 2).unwrap_or_default(),
                metric_type: cell_str(row, 3).unwrap_or_default(),
            });
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
        Value::String(s) => s.parse().ok(),
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
