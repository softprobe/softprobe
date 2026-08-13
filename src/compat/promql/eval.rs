//! PromQL subset evaluator over MetricSeries samples.

use super::parse::{extract_selector_matchers, matrix_range};
use crate::compat::backends::metrics::{
    MetricSeries, MetricsQueryBackend, MetricsQueryRequest, Sample,
};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::tenant::TenantContext;
use promql_parser::parser::token::{
    T_ADD, T_AVG, T_BOTTOMK, T_COUNT, T_DIV, T_EQLC, T_GTE, T_GTR, T_LAND, T_LOR, T_LSS, T_LTE,
    T_LUNLESS, T_MAX, T_MIN, T_MOD, T_MUL, T_NEQ, T_POW, T_SUB, T_SUM, T_TOPK,
};
use promql_parser::parser::{Expr, LabelModifier, Offset};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct InstantSample {
    pub labels: BTreeMap<String, String>,
    pub timestamp_ms: i64,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorResult {
    pub samples: Vec<InstantSample>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatrixResult {
    pub series: Vec<MetricSeries>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EvalResult {
    Vector(VectorResult),
    Matrix(MatrixResult),
    Scalar { timestamp_ms: i64, value: f64 },
}

/// Evaluate an instant query at `eval_ms`.
pub async fn eval_instant(
    backend: &dyn MetricsQueryBackend,
    ctx: &TenantContext,
    expr: &Expr,
    eval_ms: i64,
) -> Result<EvalResult, CompatError> {
    let value = eval_expr(backend, ctx, expr, eval_ms, None).await?;
    Ok(value)
}

/// Evaluate a range query from start..end at `step_ms`.
pub async fn eval_range(
    backend: &dyn MetricsQueryBackend,
    ctx: &TenantContext,
    expr: &Expr,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<EvalResult, CompatError> {
    if step_ms <= 0 {
        return Err(CompatError::new(
            CompatErrorCode::BadRequest,
            "step must be > 0",
        ));
    }
    if end_ms < start_ms {
        return Err(CompatError::new(
            CompatErrorCode::BadRequest,
            "end must be >= start",
        ));
    }

    // Matrix output: for each series identity, collect (ts, value) at each step.
    let mut by_labels: BTreeMap<BTreeMap<String, String>, Vec<Sample>> = BTreeMap::new();
    let mut t = start_ms;
    while t <= end_ms {
        let instant = eval_expr(backend, ctx, expr, t, None).await?;
        match instant {
            EvalResult::Vector(v) => {
                for s in v.samples {
                    by_labels.entry(s.labels).or_default().push(Sample {
                        timestamp_ms: s.timestamp_ms,
                        value: s.value,
                    });
                }
            }
            EvalResult::Scalar {
                timestamp_ms,
                value,
            } => {
                by_labels.entry(BTreeMap::new()).or_default().push(Sample {
                    timestamp_ms,
                    value,
                });
            }
            EvalResult::Matrix(_) => {
                return Err(CompatError::unsupported(
                    "promql: range query over matrix result",
                ));
            }
        }
        t = t.saturating_add(step_ms);
        if t == start_ms {
            // guard against zero step already handled; avoid infinite if overflow
            break;
        }
    }

    Ok(EvalResult::Matrix(MatrixResult {
        series: by_labels
            .into_iter()
            .map(|(labels, samples)| MetricSeries { labels, samples })
            .collect(),
    }))
}

async fn eval_expr(
    backend: &dyn MetricsQueryBackend,
    ctx: &TenantContext,
    expr: &Expr,
    eval_ms: i64,
    lookback_ms: Option<i64>,
) -> Result<EvalResult, CompatError> {
    let _ = lookback_ms;
    match expr {
        Expr::Paren(p) => Box::pin(eval_expr(backend, ctx, &p.expr, eval_ms, None)).await,
        Expr::Unary(u) => {
            let inner = Box::pin(eval_expr(backend, ctx, &u.expr, eval_ms, None)).await?;
            Ok(negate(inner, eval_ms)?)
        }
        Expr::NumberLiteral(n) => Ok(EvalResult::Scalar {
            timestamp_ms: eval_ms,
            value: n.val,
        }),
        Expr::StringLiteral(_) => Err(CompatError::unsupported("promql: string literal")),
        Expr::VectorSelector(vs) => {
            let matchers = extract_selector_matchers(vs)?;
            let off = offset_shift_ms(&vs.offset);
            let data_ms = eval_ms - off;
            let series =
                fetch_series(backend, ctx, &matchers, data_ms, default_lookback_ms()).await?;
            let mut vector = instant_vector_at(&series, data_ms);
            for s in &mut vector.samples {
                s.timestamp_ms = eval_ms;
            }
            Ok(EvalResult::Vector(vector))
        }
        Expr::MatrixSelector(ms) => {
            let matchers = extract_selector_matchers(&ms.vs)?;
            let off = offset_shift_ms(&ms.vs.offset);
            let data_ms = eval_ms - off;
            let range_ms = matrix_range(ms).as_millis() as i64;
            let series = fetch_series(backend, ctx, &matchers, data_ms, range_ms.max(1)).await?;
            Ok(EvalResult::Matrix(MatrixResult {
                series: truncate_to_window(series, data_ms - range_ms, data_ms),
            }))
        }
        Expr::Call(c) => eval_call(backend, ctx, c, eval_ms).await,
        Expr::Aggregate(a) => {
            let inner = Box::pin(eval_expr(backend, ctx, &a.expr, eval_ms, None)).await?;
            let vector = expect_vector(inner)?;
            let op = a.op.id();
            if op == T_TOPK || op == T_BOTTOMK {
                let param = a.param.as_ref().ok_or_else(|| {
                    CompatError::new(CompatErrorCode::BadRequest, "topk/bottomk missing param")
                })?;
                let k_val = Box::pin(eval_expr(backend, ctx, param, eval_ms, None)).await?;
                let k = expect_scalar_k(k_val)?;
                Ok(EvalResult::Vector(aggregate_topk(
                    k,
                    op == T_BOTTOMK,
                    &a.modifier,
                    vector,
                    eval_ms,
                )?))
            } else {
                Ok(EvalResult::Vector(aggregate(
                    op,
                    &a.modifier,
                    vector,
                    eval_ms,
                )?))
            }
        }
        Expr::Binary(b) => {
            let lhs = Box::pin(eval_expr(backend, ctx, &b.lhs, eval_ms, None)).await?;
            let rhs = Box::pin(eval_expr(backend, ctx, &b.rhs, eval_ms, None)).await?;
            let id = b.op.id();
            if id == T_LAND || id == T_LOR || id == T_LUNLESS {
                eval_set_op(id, lhs, rhs, eval_ms)
            } else {
                eval_binary(id, b.return_bool(), lhs, rhs, eval_ms)
            }
        }
        Expr::Subquery(_) => Err(CompatError::unsupported("promql: subquery")),
        Expr::Extension(_) => Err(CompatError::unsupported("promql: extension")),
    }
}

fn default_lookback_ms() -> i64 {
    5 * 60 * 1000
}

/// Prometheus `offset` shifts the data timestamp: `metric offset 5m` reads data from 5m earlier.
fn offset_shift_ms(offset: &Option<Offset>) -> i64 {
    match offset {
        None => 0,
        Some(Offset::Pos(d)) => d.as_millis() as i64,
        Some(Offset::Neg(d)) => -(d.as_millis() as i64),
    }
}

async fn fetch_series(
    backend: &dyn MetricsQueryBackend,
    ctx: &TenantContext,
    matchers: &[crate::compat::backends::metrics::LabelMatcher],
    eval_ms: i64,
    window_ms: i64,
) -> Result<Vec<MetricSeries>, CompatError> {
    let start_ms = eval_ms.saturating_sub(window_ms);
    backend
        .query_range(
            ctx,
            MetricsQueryRequest {
                start_ms: Some(start_ms),
                end_ms: Some(eval_ms),
                matchers: matchers.to_vec(),
            },
        )
        .await
}

fn truncate_to_window(series: Vec<MetricSeries>, start_ms: i64, end_ms: i64) -> Vec<MetricSeries> {
    series
        .into_iter()
        .map(|mut s| {
            s.samples
                .retain(|sm| sm.timestamp_ms >= start_ms && sm.timestamp_ms <= end_ms);
            s
        })
        .filter(|s| !s.samples.is_empty())
        .collect()
}

fn instant_vector_at(series: &[MetricSeries], eval_ms: i64) -> VectorResult {
    let lookback = default_lookback_ms();
    let mut samples = Vec::new();
    for s in series {
        if let Some(sample) = latest_sample_in_window(&s.samples, eval_ms - lookback, eval_ms) {
            samples.push(InstantSample {
                labels: s.labels.clone(),
                timestamp_ms: eval_ms,
                value: sample.value,
            });
        }
    }
    VectorResult { samples }
}

fn latest_sample_in_window(samples: &[Sample], start_ms: i64, end_ms: i64) -> Option<&Sample> {
    samples
        .iter()
        .rev()
        .find(|s| s.timestamp_ms >= start_ms && s.timestamp_ms <= end_ms)
}

async fn eval_call(
    backend: &dyn MetricsQueryBackend,
    ctx: &TenantContext,
    call: &promql_parser::parser::Call,
    eval_ms: i64,
) -> Result<EvalResult, CompatError> {
    let name = call.func.name.to_ascii_lowercase();
    if super::funcs::is_range_vector_fn(&name) {
        let arg = call.args.args.first().ok_or_else(|| {
            CompatError::new(CompatErrorCode::BadRequest, format!("{name}() missing arg"))
        })?;
        let matrix = match Box::pin(eval_expr(backend, ctx, arg, eval_ms, None)).await? {
            EvalResult::Matrix(m) => m,
            _ => {
                return Err(CompatError::new(
                    CompatErrorCode::BadRequest,
                    format!("{name}() requires range vector"),
                ))
            }
        };
        let keep_name = name == "last_over_time";
        let mut samples = Vec::new();
        for s in matrix.series {
            if let Some(v) = match name.as_str() {
                "rate" => counter_rate(&s.samples, false),
                "irate" => counter_rate(&s.samples, true),
                "increase" => counter_increase(&s.samples),
                "delta" => gauge_delta(&s.samples),
                "idelta" => gauge_idelta(&s.samples),
                "sum_over_time" => over_time_sum(&s.samples),
                "avg_over_time" => over_time_avg(&s.samples),
                "min_over_time" => over_time_min(&s.samples),
                "max_over_time" => over_time_max(&s.samples),
                "count_over_time" => Some(s.samples.len() as f64),
                "last_over_time" => s.samples.last().map(|x| x.value),
                _ => None,
            } {
                let mut labels = s.labels;
                if !keep_name {
                    labels.remove("__name__");
                }
                samples.push(InstantSample {
                    labels,
                    timestamp_ms: eval_ms,
                    value: v,
                });
            }
        }
        return Ok(EvalResult::Vector(VectorResult { samples }));
    }
    if super::funcs::is_math_fn(&name) {
        let arg = call.args.args.first().ok_or_else(|| {
            CompatError::new(CompatErrorCode::BadRequest, format!("{name}() missing arg"))
        })?;
        let to_nearest = if name == "round" && call.args.args.len() == 2 {
            let n = Box::pin(eval_expr(backend, ctx, &call.args.args[1], eval_ms, None)).await?;
            Some(expect_scalar_value(n)?)
        } else {
            None
        };
        let inner = Box::pin(eval_expr(backend, ctx, arg, eval_ms, None)).await?;
        return apply_math_fn(&name, inner, to_nearest, eval_ms);
    }
    Err(CompatError::unsupported(format!("promql: function {name}")))
}

fn gauge_delta(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    Some(samples.last()?.value - samples.first()?.value)
}

fn gauge_idelta(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let a = &samples[samples.len() - 2];
    let b = samples.last()?;
    Some(b.value - a.value)
}

fn over_time_sum(samples: &[Sample]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    Some(samples.iter().map(|s| s.value).sum())
}

fn over_time_avg(samples: &[Sample]) -> Option<f64> {
    // Prometheus avg_over_time includes NaNs (NaN poisons the mean). OTLP/OpenMetrics
    // ingest drops NaNs before storage, so lake fixtures usually never see them.
    if samples.is_empty() {
        return None;
    }
    Some(samples.iter().map(|s| s.value).sum::<f64>() / samples.len() as f64)
}

fn over_time_min(samples: &[Sample]) -> Option<f64> {
    let mut out: Option<f64> = None;
    for s in samples {
        if s.value.is_nan() {
            continue;
        }
        out = Some(match out {
            None => s.value,
            Some(m) => m.min(s.value),
        });
    }
    out.or_else(|| samples.last().map(|s| s.value))
}

fn over_time_max(samples: &[Sample]) -> Option<f64> {
    let mut out: Option<f64> = None;
    for s in samples {
        if s.value.is_nan() {
            continue;
        }
        out = Some(match out {
            None => s.value,
            Some(m) => m.max(s.value),
        });
    }
    out.or_else(|| samples.last().map(|s| s.value))
}

fn apply_math_fn(
    name: &str,
    value: EvalResult,
    to_nearest: Option<f64>,
    eval_ms: i64,
) -> Result<EvalResult, CompatError> {
    let map_one = |v: f64| -> f64 {
        match name {
            "abs" => v.abs(),
            "ceil" => v.ceil(),
            "floor" => v.floor(),
            "round" => {
                // Match Prometheus: math.Floor(v/nearest + 0.5) * nearest (ties toward +∞).
                let nearest = to_nearest.unwrap_or(1.0);
                if nearest == 0.0 {
                    v
                } else {
                    (v / nearest + 0.5).floor() * nearest
                }
            }
            _ => v,
        }
    };
    match value {
        EvalResult::Scalar { value, .. } => Ok(EvalResult::Scalar {
            timestamp_ms: eval_ms,
            value: map_one(value),
        }),
        EvalResult::Vector(v) => Ok(EvalResult::Vector(VectorResult {
            samples: v
                .samples
                .into_iter()
                .map(|mut s| {
                    s.value = map_one(s.value);
                    s.timestamp_ms = eval_ms;
                    // Math funcs drop __name__.
                    s.labels.remove("__name__");
                    s
                })
                .collect(),
        })),
        EvalResult::Matrix(_) => Err(CompatError::unsupported(format!(
            "promql: {name}() on matrix"
        ))),
    }
}

fn expect_scalar_value(value: EvalResult) -> Result<f64, CompatError> {
    match value {
        EvalResult::Scalar { value, .. } => Ok(value),
        EvalResult::Vector(v) if v.samples.len() == 1 => Ok(v.samples[0].value),
        _ => Err(CompatError::new(
            CompatErrorCode::BadRequest,
            "expected scalar",
        )),
    }
}

fn expect_scalar_k(value: EvalResult) -> Result<usize, CompatError> {
    let v = expect_scalar_value(value)?;
    if !v.is_finite() || v < 0.0 {
        return Err(CompatError::new(
            CompatErrorCode::BadRequest,
            "topk/bottomk k must be a non-negative finite number",
        ));
    }
    Ok(v.floor() as usize)
}

/// Counter reset: treat decreases as resets (non-decreasing then drop).
fn adjusted_delta(samples: &[Sample]) -> Option<(f64, i64)> {
    if samples.len() < 2 {
        return None;
    }
    let first_ts = samples.first()?.timestamp_ms;
    let last_ts = samples.last()?.timestamp_ms;
    if last_ts <= first_ts {
        return None;
    }
    let mut total = 0.0;
    let mut prev = samples[0].value;
    for s in samples.iter().skip(1) {
        if s.value < prev {
            // reset: contribute new value from 0
            total += s.value;
        } else {
            total += s.value - prev;
        }
        prev = s.value;
    }
    Some((total, last_ts - first_ts))
}

fn counter_increase(samples: &[Sample]) -> Option<f64> {
    adjusted_delta(samples).map(|(d, _)| d)
}

fn counter_rate(samples: &[Sample], irate: bool) -> Option<f64> {
    if irate {
        if samples.len() < 2 {
            return None;
        }
        let a = &samples[samples.len() - 2];
        let b = samples.last()?;
        let dt = (b.timestamp_ms - a.timestamp_ms) as f64 / 1000.0;
        if dt <= 0.0 {
            return None;
        }
        let delta = if b.value < a.value {
            b.value
        } else {
            b.value - a.value
        };
        return Some(delta / dt);
    }
    let (delta, range_ms) = adjusted_delta(samples)?;
    let secs = range_ms as f64 / 1000.0;
    if secs <= 0.0 {
        return None;
    }
    Some(delta / secs)
}

fn aggregate(
    op: u16,
    modifier: &Option<LabelModifier>,
    vector: VectorResult,
    eval_ms: i64,
) -> Result<VectorResult, CompatError> {
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<f64>> = BTreeMap::new();
    for s in vector.samples {
        let key = group_labels(&s.labels, modifier);
        groups.entry(key).or_default().push(s.value);
    }
    let mut samples = Vec::new();
    for (labels, values) in groups {
        let value = match op {
            x if x == T_SUM => values.iter().sum(),
            x if x == T_MIN => values.iter().cloned().fold(f64::INFINITY, f64::min),
            x if x == T_MAX => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            x if x == T_AVG => {
                if values.is_empty() {
                    continue;
                }
                values.iter().sum::<f64>() / values.len() as f64
            }
            x if x == T_COUNT => values.len() as f64,
            _ => {
                return Err(CompatError::unsupported(format!(
                    "promql: aggregation token {op}"
                )))
            }
        };
        samples.push(InstantSample {
            labels,
            timestamp_ms: eval_ms,
            value,
        });
    }
    Ok(VectorResult { samples })
}

fn aggregate_topk(
    k: usize,
    bottom: bool,
    modifier: &Option<LabelModifier>,
    vector: VectorResult,
    eval_ms: i64,
) -> Result<VectorResult, CompatError> {
    if k == 0 {
        return Ok(VectorResult { samples: vec![] });
    }
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<InstantSample>> = BTreeMap::new();
    for s in vector.samples {
        let key = group_labels(&s.labels, modifier);
        groups.entry(key).or_default().push(s);
    }
    let mut out = Vec::new();
    for (_g, mut samples) in groups {
        samples.sort_by(|a, b| {
            // NaNs sort after all finite values for both topk and bottomk.
            match (a.value.is_nan(), b.value.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => {
                    if bottom {
                        a.value.total_cmp(&b.value)
                    } else {
                        b.value.total_cmp(&a.value)
                    }
                }
            }
        });
        samples.truncate(k.min(samples.len()));
        for mut s in samples {
            s.timestamp_ms = eval_ms;
            out.push(s);
        }
    }
    Ok(VectorResult { samples: out })
}

fn eval_set_op(
    op: u16,
    lhs: EvalResult,
    rhs: EvalResult,
    eval_ms: i64,
) -> Result<EvalResult, CompatError> {
    // PromQL set ops require instant vectors — do not promote scalars.
    let lv = match lhs {
        EvalResult::Vector(v) => v,
        _ => {
            return Err(CompatError::new(
                CompatErrorCode::BadRequest,
                "promql: set operators require instant vectors",
            ))
        }
    };
    let rv = match rhs {
        EvalResult::Vector(v) => v,
        _ => {
            return Err(CompatError::new(
                CompatErrorCode::BadRequest,
                "promql: set operators require instant vectors",
            ))
        }
    };
    let rhs_keys: BTreeSet<_> = rv.samples.iter().map(|s| matching_key(&s.labels)).collect();
    let mut out = Vec::new();
    match op {
        x if x == T_LAND => {
            for s in lv.samples {
                if rhs_keys.contains(&matching_key(&s.labels)) {
                    let mut s = s;
                    s.timestamp_ms = eval_ms;
                    out.push(s);
                }
            }
        }
        x if x == T_LOR => {
            let mut seen: BTreeSet<_> = BTreeSet::new();
            for s in lv.samples {
                seen.insert(matching_key(&s.labels));
                let mut s = s;
                s.timestamp_ms = eval_ms;
                out.push(s);
            }
            for s in rv.samples {
                let key = matching_key(&s.labels);
                if !seen.contains(&key) {
                    let mut s = s;
                    s.timestamp_ms = eval_ms;
                    out.push(s);
                }
            }
        }
        x if x == T_LUNLESS => {
            for s in lv.samples {
                if !rhs_keys.contains(&matching_key(&s.labels)) {
                    let mut s = s;
                    s.timestamp_ms = eval_ms;
                    out.push(s);
                }
            }
        }
        _ => {
            return Err(CompatError::unsupported(format!(
                "promql: set operator token {op}"
            )))
        }
    }
    Ok(EvalResult::Vector(VectorResult { samples: out }))
}

fn group_labels(
    labels: &BTreeMap<String, String>,
    modifier: &Option<LabelModifier>,
) -> BTreeMap<String, String> {
    match modifier {
        None => BTreeMap::new(),
        Some(LabelModifier::Include(ls)) => {
            let mut out = BTreeMap::new();
            for name in &ls.labels {
                if let Some(v) = labels.get(name) {
                    out.insert(name.clone(), v.clone());
                }
            }
            out
        }
        Some(LabelModifier::Exclude(ls)) => {
            let exclude: std::collections::HashSet<_> = ls.labels.iter().cloned().collect();
            labels
                .iter()
                .filter(|(k, _)| *k != "__name__" && !exclude.contains(*k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
    }
}

fn eval_binary(
    op: u16,
    return_bool: bool,
    lhs: EvalResult,
    rhs: EvalResult,
    eval_ms: i64,
) -> Result<EvalResult, CompatError> {
    match (lhs, rhs) {
        (EvalResult::Scalar { value: lv, .. }, EvalResult::Scalar { value: rv, .. }) => {
            let value = apply_scalar_op(op, lv, rv, return_bool)?;
            if let Some(v) = value {
                Ok(EvalResult::Scalar {
                    timestamp_ms: eval_ms,
                    value: v,
                })
            } else {
                Ok(EvalResult::Vector(VectorResult { samples: vec![] }))
            }
        }
        (EvalResult::Vector(lv), EvalResult::Scalar { value: rv, .. }) => {
            Ok(EvalResult::Vector(VectorResult {
                samples: apply_vector_scalar(op, lv, rv, true, return_bool, eval_ms)?,
            }))
        }
        (EvalResult::Scalar { value: lv, .. }, EvalResult::Vector(rv)) => {
            Ok(EvalResult::Vector(VectorResult {
                samples: apply_vector_scalar(op, rv, lv, false, return_bool, eval_ms)?,
            }))
        }
        (EvalResult::Vector(lv), EvalResult::Vector(rv)) => Ok(EvalResult::Vector(VectorResult {
            samples: apply_vector_vector(op, lv, rv, return_bool, eval_ms)?,
        })),
        _ => Err(CompatError::unsupported(
            "promql: binary op on matrix values",
        )),
    }
}

fn apply_scalar_op(
    op: u16,
    lv: f64,
    rv: f64,
    return_bool: bool,
) -> Result<Option<f64>, CompatError> {
    if is_comparison(op) {
        let ok = cmp(op, lv, rv);
        if return_bool {
            Ok(Some(if ok { 1.0 } else { 0.0 }))
        } else if ok {
            Ok(Some(lv))
        } else {
            Ok(None)
        }
    } else {
        Ok(Some(arith(op, lv, rv)?))
    }
}

fn apply_vector_scalar(
    op: u16,
    vector: VectorResult,
    scalar: f64,
    vector_is_lhs: bool,
    return_bool: bool,
    eval_ms: i64,
) -> Result<Vec<InstantSample>, CompatError> {
    // Prometheus drops __name__ for arithmetic and for comparisons with `bool`.
    let drop_name = drops_metric_name(op, return_bool);
    let mut out = Vec::new();
    for s in vector.samples {
        let (lv, rv) = if vector_is_lhs {
            (s.value, scalar)
        } else {
            (scalar, s.value)
        };
        if let Some(v) = apply_scalar_op(op, lv, rv, return_bool)? {
            let mut labels = s.labels;
            if drop_name {
                labels.remove("__name__");
            }
            out.push(InstantSample {
                labels,
                timestamp_ms: eval_ms,
                value: v,
            });
        }
    }
    Ok(out)
}

fn apply_vector_vector(
    op: u16,
    lhs: VectorResult,
    rhs: VectorResult,
    return_bool: bool,
    eval_ms: i64,
) -> Result<Vec<InstantSample>, CompatError> {
    // One-to-one matching on ignoring(__name__) — match all non-name labels.
    // Fail loud on many-to-one / many-to-many (Prometheus requires group_*).
    let mut rhs_by_key: BTreeMap<BTreeMap<String, String>, InstantSample> = BTreeMap::new();
    for s in rhs.samples {
        let key = matching_key(&s.labels);
        if rhs_by_key.contains_key(&key) {
            return Err(CompatError::unsupported(
                "promql: many-to-one/many-to-many vector matching",
            ));
        }
        rhs_by_key.insert(key, s);
    }
    let mut seen_lhs: BTreeSet<BTreeMap<String, String>> = BTreeSet::new();
    let mut out = Vec::new();
    for s in lhs.samples {
        let key = matching_key(&s.labels);
        if !seen_lhs.insert(key.clone()) {
            return Err(CompatError::unsupported(
                "promql: many-to-one/many-to-many vector matching",
            ));
        }
        if let Some(r) = rhs_by_key.get(&key) {
            if let Some(v) = apply_scalar_op(op, s.value, r.value, return_bool)? {
                // Arithmetic and bool comparisons drop __name__; filtering comparisons keep LHS name.
                let drop_name = drops_metric_name(op, return_bool);
                let mut labels = s.labels;
                if drop_name {
                    labels.remove("__name__");
                }
                out.push(InstantSample {
                    labels,
                    timestamp_ms: eval_ms,
                    value: v,
                });
            }
        }
    }
    Ok(out)
}

fn matching_key(labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    labels
        .iter()
        .filter(|(k, _)| *k != "__name__")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn is_comparison(op: u16) -> bool {
    op == T_EQLC || op == T_NEQ || op == T_GTR || op == T_GTE || op == T_LSS || op == T_LTE
}

/// Prometheus drops `__name__` for arithmetic and for comparisons with `bool`.
fn drops_metric_name(op: u16, return_bool: bool) -> bool {
    !is_comparison(op) || return_bool
}

fn cmp(op: u16, lv: f64, rv: f64) -> bool {
    match op {
        x if x == T_EQLC => lv == rv,
        x if x == T_NEQ => lv != rv,
        x if x == T_GTR => lv > rv,
        x if x == T_GTE => lv >= rv,
        x if x == T_LSS => lv < rv,
        x if x == T_LTE => lv <= rv,
        _ => false,
    }
}

fn arith(op: u16, lv: f64, rv: f64) -> Result<f64, CompatError> {
    Ok(match op {
        x if x == T_ADD => lv + rv,
        x if x == T_SUB => lv - rv,
        x if x == T_MUL => lv * rv,
        x if x == T_DIV => lv / rv,
        x if x == T_MOD => lv % rv,
        x if x == T_POW => lv.powf(rv),
        _ => {
            return Err(CompatError::unsupported(format!(
                "promql: binary operator token {op}"
            )))
        }
    })
}

fn negate(value: EvalResult, eval_ms: i64) -> Result<EvalResult, CompatError> {
    match value {
        EvalResult::Scalar { value, .. } => Ok(EvalResult::Scalar {
            timestamp_ms: eval_ms,
            value: -value,
        }),
        EvalResult::Vector(v) => Ok(EvalResult::Vector(VectorResult {
            samples: v
                .samples
                .into_iter()
                .map(|mut s| {
                    // Prometheus drops __name__ on unary minus.
                    s.labels.remove("__name__");
                    s.value = -s.value;
                    s.timestamp_ms = eval_ms;
                    s
                })
                .collect(),
        })),
        EvalResult::Matrix(_) => Err(CompatError::unsupported("promql: unary on matrix")),
    }
}

fn expect_vector(value: EvalResult) -> Result<VectorResult, CompatError> {
    match value {
        EvalResult::Vector(v) => Ok(v),
        EvalResult::Scalar {
            timestamp_ms,
            value,
        } => Ok(VectorResult {
            samples: vec![InstantSample {
                labels: BTreeMap::new(),
                timestamp_ms,
                value,
            }],
        }),
        EvalResult::Matrix(_) => Err(CompatError::new(
            CompatErrorCode::BadRequest,
            "expected instant vector",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authn::TenantInfo;
    use crate::compat::backends::metrics::{LabelMatcher, MatcherOp, UnsupportedMetricsBackend};
    use crate::compat::promql::parse::parse_promql;
    use crate::compat::tenant::{ProtocolScope, QueryLimits};
    use async_trait::async_trait;
    use std::sync::Arc;

    struct MemBackend {
        series: Vec<MetricSeries>,
    }

    #[async_trait]
    impl MetricsQueryBackend for MemBackend {
        async fn query_range(
            &self,
            _ctx: &TenantContext,
            request: MetricsQueryRequest,
        ) -> Result<Vec<MetricSeries>, CompatError> {
            let mut out = Vec::new();
            for s in &self.series {
                if crate::compat::backends::metrics::labels_match(&s.labels, &request.matchers)? {
                    out.push(s.clone());
                }
            }
            Ok(out)
        }

        async fn label_names(
            &self,
            _: &TenantContext,
            _: &crate::compat::backends::metrics::MetricsDiscoveryRequest,
        ) -> Result<Vec<String>, CompatError> {
            Err(CompatError::unsupported("n/a"))
        }

        async fn label_values(
            &self,
            _: &TenantContext,
            _: &str,
            _: &crate::compat::backends::metrics::MetricsDiscoveryRequest,
        ) -> Result<Vec<String>, CompatError> {
            Err(CompatError::unsupported("n/a"))
        }

        async fn series(
            &self,
            _: &TenantContext,
            _: &crate::compat::backends::metrics::MetricsDiscoveryRequest,
        ) -> Result<Vec<BTreeMap<String, String>>, CompatError> {
            Err(CompatError::unsupported("n/a"))
        }

        async fn metadata(
            &self,
            _: &TenantContext,
            _: Option<&str>,
            _: Option<usize>,
            _: Option<i64>,
            _: Option<i64>,
        ) -> Result<Vec<crate::compat::backends::metrics::MetricMetadata>, CompatError> {
            Err(CompatError::unsupported("n/a"))
        }
    }

    fn ctx() -> TenantContext {
        TenantContext::from_authenticated(
            TenantInfo {
                tenant_id: "t".into(),
                bucket_name: "b".into(),
                dataset_id: "d".into(),
            },
            ProtocolScope::Prometheus,
            None,
            QueryLimits::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn selector_instant_returns_latest() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "up".into());
        labels.insert("job".into(), "api".into());
        let backend = MemBackend {
            series: vec![MetricSeries {
                labels,
                samples: vec![
                    Sample {
                        timestamp_ms: 1_000,
                        value: 0.0,
                    },
                    Sample {
                        timestamp_ms: 2_000,
                        value: 1.0,
                    },
                ],
            }],
        };
        let expr = parse_promql(r#"up{job="api"}"#).unwrap();
        let result = eval_instant(&backend, &ctx(), &expr, 2_500).await.unwrap();
        match result {
            EvalResult::Vector(v) => {
                assert_eq!(v.samples.len(), 1);
                assert_eq!(v.samples[0].value, 1.0);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn rate_handles_counter_reset() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "c".into());
        let backend = MemBackend {
            series: vec![MetricSeries {
                labels,
                samples: vec![
                    Sample {
                        timestamp_ms: 0,
                        value: 10.0,
                    },
                    Sample {
                        timestamp_ms: 1_000,
                        value: 20.0,
                    },
                    Sample {
                        timestamp_ms: 2_000,
                        value: 5.0, // reset
                    },
                    Sample {
                        timestamp_ms: 3_000,
                        value: 8.0,
                    },
                ],
            }],
        };
        let expr = parse_promql("rate(c[5m])").unwrap();
        let result = eval_instant(&backend, &ctx(), &expr, 3_000).await.unwrap();
        match result {
            EvalResult::Vector(v) => {
                assert_eq!(v.samples.len(), 1);
                // deltas: +10, +5 (reset), +3 = 18 over 3s → 6.0
                assert!((v.samples[0].value - 6.0).abs() < 1e-9);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn sum_by_job() {
        let mk = |job: &str, v: f64| {
            let mut labels = BTreeMap::new();
            labels.insert("__name__".into(), "http_requests".into());
            labels.insert("job".into(), job.into());
            MetricSeries {
                labels,
                samples: vec![Sample {
                    timestamp_ms: 1_000,
                    value: v,
                }],
            }
        };
        let backend = MemBackend {
            series: vec![mk("a", 1.0), mk("a", 2.0), mk("b", 4.0)],
        };
        // Two series with job=a collide on matching_key — MemBackend returns both;
        // aggregation groups by job.
        let expr = parse_promql(r#"sum by (job) (http_requests)"#).unwrap();
        let result = eval_instant(&backend, &ctx(), &expr, 1_000).await.unwrap();
        match result {
            EvalResult::Vector(v) => {
                assert_eq!(v.samples.len(), 2);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn comparison_filters() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "up".into());
        let backend = MemBackend {
            series: vec![MetricSeries {
                labels,
                samples: vec![Sample {
                    timestamp_ms: 1_000,
                    value: 1.0,
                }],
            }],
        };
        let expr = parse_promql("up > 0").unwrap();
        let result = eval_instant(&backend, &ctx(), &expr, 1_000).await.unwrap();
        match result {
            EvalResult::Vector(v) => {
                assert_eq!(v.samples.len(), 1);
                // Comparison without bool keeps __name__.
                assert_eq!(
                    v.samples[0].labels.get("__name__").map(String::as_str),
                    Some("up")
                );
            }
            other => panic!("unexpected {other:?}"),
        }
        let expr = parse_promql("up > 2").unwrap();
        let result = eval_instant(&backend, &ctx(), &expr, 1_000).await.unwrap();
        match result {
            EvalResult::Vector(v) => assert!(v.samples.is_empty()),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn vector_scalar_arith_drops_name() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "up".into());
        labels.insert("job".into(), "api".into());
        let backend = MemBackend {
            series: vec![MetricSeries {
                labels,
                samples: vec![Sample {
                    timestamp_ms: 1_000,
                    value: 10.0,
                }],
            }],
        };
        let expr = parse_promql("up / 2").unwrap();
        let result = eval_instant(&backend, &ctx(), &expr, 1_000).await.unwrap();
        match result {
            EvalResult::Vector(v) => {
                assert_eq!(v.samples.len(), 1);
                assert!((v.samples[0].value - 5.0).abs() < 1e-9);
                assert!(!v.samples[0].labels.contains_key("__name__"));
                assert_eq!(
                    v.samples[0].labels.get("job").map(String::as_str),
                    Some("api")
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn unary_minus_drops_name() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "up".into());
        labels.insert("job".into(), "api".into());
        let backend = MemBackend {
            series: vec![MetricSeries {
                labels,
                samples: vec![Sample {
                    timestamp_ms: 1_000,
                    value: 3.0,
                }],
            }],
        };
        let expr = parse_promql("-up").unwrap();
        let result = eval_instant(&backend, &ctx(), &expr, 1_000).await.unwrap();
        match result {
            EvalResult::Vector(v) => {
                assert_eq!(v.samples.len(), 1);
                assert!((v.samples[0].value + 3.0).abs() < 1e-9);
                assert!(!v.samples[0].labels.contains_key("__name__"));
                assert_eq!(
                    v.samples[0].labels.get("job").map(String::as_str),
                    Some("api")
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn comparison_bool_drops_name() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "up".into());
        labels.insert("job".into(), "api".into());
        let backend = MemBackend {
            series: vec![MetricSeries {
                labels,
                samples: vec![Sample {
                    timestamp_ms: 1_000,
                    value: 1.0,
                }],
            }],
        };
        let expr = parse_promql("up == bool 1").unwrap();
        let result = eval_instant(&backend, &ctx(), &expr, 1_000).await.unwrap();
        match result {
            EvalResult::Vector(v) => {
                assert_eq!(v.samples.len(), 1);
                assert!((v.samples[0].value - 1.0).abs() < 1e-9);
                assert!(!v.samples[0].labels.contains_key("__name__"));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn vector_vector_filter_keeps_lhs_name() {
        let mut a = BTreeMap::new();
        a.insert("__name__".into(), "test_total".into());
        a.insert("instance".into(), "localhost".into());
        let mut b = BTreeMap::new();
        b.insert("__name__".into(), "test_smaller".into());
        b.insert("instance".into(), "localhost".into());
        let backend = MemBackend {
            series: vec![
                MetricSeries {
                    labels: a,
                    samples: vec![Sample {
                        timestamp_ms: 1_000,
                        value: 50.0,
                    }],
                },
                MetricSeries {
                    labels: b,
                    samples: vec![Sample {
                        timestamp_ms: 1_000,
                        value: 10.0,
                    }],
                },
            ],
        };
        let expr = parse_promql("test_total > test_smaller").unwrap();
        let result = eval_instant(&backend, &ctx(), &expr, 1_000).await.unwrap();
        match result {
            EvalResult::Vector(v) => {
                assert_eq!(v.samples.len(), 1);
                assert_eq!(
                    v.samples[0].labels.get("__name__").map(String::as_str),
                    Some("test_total")
                );
                assert!((v.samples[0].value - 50.0).abs() < 1e-9);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn vector_vector_rejects_many_to_one() {
        let mut a1 = BTreeMap::new();
        a1.insert("__name__".into(), "a".into());
        a1.insert("job".into(), "api".into());
        // Two RHS series with identical matching keys (non-__name__ labels).
        let mut b1 = BTreeMap::new();
        b1.insert("__name__".into(), "b".into());
        b1.insert("job".into(), "api".into());
        let mut b2 = BTreeMap::new();
        b2.insert("__name__".into(), "b".into());
        b2.insert("job".into(), "api".into());
        let backend = MemBackend {
            series: vec![
                MetricSeries {
                    labels: a1,
                    samples: vec![Sample {
                        timestamp_ms: 1_000,
                        value: 1.0,
                    }],
                },
                MetricSeries {
                    labels: b1,
                    samples: vec![Sample {
                        timestamp_ms: 1_000,
                        value: 2.0,
                    }],
                },
                MetricSeries {
                    labels: b2,
                    samples: vec![Sample {
                        timestamp_ms: 1_000,
                        value: 3.0,
                    }],
                },
            ],
        };
        let expr = parse_promql("a + b").unwrap();
        let err = eval_instant(&backend, &ctx(), &expr, 1_000)
            .await
            .unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
        assert!(
            err.message.contains("many-to-one") || err.message.contains("many-to-many"),
            "{}",
            err.message
        );
    }

    #[tokio::test]
    async fn set_ops_reject_scalar_operands() {
        // Literal `1 and …` fails at the PromQL parser; guard the eval path directly.
        let lhs = EvalResult::Scalar {
            timestamp_ms: 1_000,
            value: 1.0,
        };
        let rhs = EvalResult::Vector(VectorResult { samples: vec![] });
        let err = eval_set_op(T_LAND, lhs, rhs, 1_000).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
        assert!(
            err.message.contains("vector"),
            "unexpected message: {}",
            err.message
        );

        let err = eval_set_op(
            T_LOR,
            EvalResult::Vector(VectorResult { samples: vec![] }),
            EvalResult::Scalar {
                timestamp_ms: 1_000,
                value: 2.0,
            },
            1_000,
        )
        .unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
        assert!(err.message.contains("vector"), "{}", err.message);
    }

    #[tokio::test]
    async fn sum_over_time_and_topk_and_offset() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "http_requests".into());
        labels.insert("job".into(), "api".into());
        let mut labels_b = BTreeMap::new();
        labels_b.insert("__name__".into(), "http_requests".into());
        labels_b.insert("job".into(), "app".into());
        let backend = MemBackend {
            series: vec![
                MetricSeries {
                    labels,
                    samples: vec![
                        Sample {
                            timestamp_ms: 0,
                            value: 1.0,
                        },
                        Sample {
                            timestamp_ms: 60_000,
                            value: 2.0,
                        },
                        Sample {
                            timestamp_ms: 120_000,
                            value: 3.0,
                        },
                    ],
                },
                MetricSeries {
                    labels: labels_b,
                    samples: vec![
                        Sample {
                            timestamp_ms: 0,
                            value: 10.0,
                        },
                        Sample {
                            timestamp_ms: 60_000,
                            value: 20.0,
                        },
                    ],
                },
            ],
        };

        let over = parse_promql("sum_over_time(http_requests[2m])").unwrap();
        let EvalResult::Vector(v) = eval_instant(&backend, &ctx(), &over, 120_000)
            .await
            .unwrap()
        else {
            panic!("vector");
        };
        assert_eq!(v.samples.len(), 2);
        let mut by_job: BTreeMap<String, f64> = BTreeMap::new();
        for s in &v.samples {
            by_job.insert(s.labels.get("job").unwrap().clone(), s.value);
        }
        assert_eq!(by_job.get("api"), Some(&6.0));
        assert_eq!(by_job.get("app"), Some(&30.0));

        let top = parse_promql("topk(1, http_requests)").unwrap();
        let EvalResult::Vector(tv) = eval_instant(&backend, &ctx(), &top, 120_000).await.unwrap()
        else {
            panic!("vector");
        };
        assert_eq!(tv.samples.len(), 1);
        assert_eq!(
            tv.samples[0].labels.get("job").map(String::as_str),
            Some("app")
        );
        assert!((tv.samples[0].value - 20.0).abs() < 1e-9);

        let off = parse_promql("http_requests offset 1m").unwrap();
        let EvalResult::Vector(ov) = eval_instant(&backend, &ctx(), &off, 120_000).await.unwrap()
        else {
            panic!("vector");
        };
        let api = ov
            .samples
            .iter()
            .find(|s| s.labels.get("job").map(String::as_str) == Some("api"))
            .unwrap();
        assert!((api.value - 2.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn avg_over_time_nan_poisons_mean() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "x".into());
        let backend = MemBackend {
            series: vec![MetricSeries {
                labels,
                samples: vec![
                    Sample {
                        timestamp_ms: 0,
                        value: 1.0,
                    },
                    Sample {
                        timestamp_ms: 60_000,
                        value: f64::NAN,
                    },
                    Sample {
                        timestamp_ms: 120_000,
                        value: 3.0,
                    },
                ],
            }],
        };
        let expr = parse_promql("avg_over_time(x[2m])").unwrap();
        let EvalResult::Vector(v) = eval_instant(&backend, &ctx(), &expr, 120_000)
            .await
            .unwrap()
        else {
            panic!("vector");
        };
        assert_eq!(v.samples.len(), 1);
        assert!(
            v.samples[0].value.is_nan(),
            "expected NaN mean, got {}",
            v.samples[0].value
        );
    }

    #[tokio::test]
    async fn abs_ceil_floor_round_unit() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "x".into());
        let backend = MemBackend {
            series: vec![MetricSeries {
                labels,
                samples: vec![Sample {
                    timestamp_ms: 1_000,
                    value: -1.5,
                }],
            }],
        };
        for (q, expect) in [
            ("abs(x)", 1.5),
            ("ceil(x)", -1.0),
            ("floor(x)", -2.0),
            // Prometheus round ties toward +∞: Floor(v + 0.5) → -1 for -1.5
            ("round(x)", -1.0),
        ] {
            let expr = parse_promql(q).unwrap();
            let EvalResult::Vector(v) = eval_instant(&backend, &ctx(), &expr, 1_000).await.unwrap()
            else {
                panic!("vector for {q}");
            };
            assert_eq!(v.samples.len(), 1);
            assert!(
                (v.samples[0].value - expect).abs() < 1e-9,
                "{q}: got {} want {expect}",
                v.samples[0].value
            );
        }
    }

    #[tokio::test]
    async fn unsupported_backend_surfaces() {
        let _ = UnsupportedMetricsBackend;
        let _ = Arc::new(());
        let _ = LabelMatcher {
            name: "x".into(),
            op: MatcherOp::Eq,
            value: "y".into(),
        };
    }
}
