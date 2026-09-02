use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::tenant::TenantContext;
use async_trait::async_trait;
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricSeries {
    pub labels: BTreeMap<String, String>,
    pub samples: Vec<Sample>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatcherOp {
    Eq,
    Ne,
    Re,
    Nre,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatcherOp,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsDiscoveryRequest {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    /// Each `match[]` selector → one matcher list (AND within, OR across).
    pub matchers: Vec<Vec<LabelMatcher>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsQueryRequest {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    /// Original client window used for request limits when `start_ms`/`end_ms`
    /// are internally expanded for PromQL lookback.
    pub client_start_ms: Option<i64>,
    pub client_end_ms: Option<i64>,
    /// Single selector matchers (AND).
    pub matchers: Vec<LabelMatcher>,
    /// Grafana/Prom `step` for grain selection (§9.1: step ≥ 1h → prefer 1h).
    pub step_ms: Option<i64>,
    /// When set, fetch from `metric_collapse_job_1h` for this metric (§9.1 step 5).
    /// Used for `sum by (job) (rate|irate|increase(…))` when window ≥ 2h.
    pub collapse_metric: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricMetadata {
    pub metric_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}

#[async_trait]
pub trait MetricsQueryBackend: Send + Sync {
    /// Fetch samples for a single selector (used by PromQL eval).
    async fn query_range(
        &self,
        ctx: &TenantContext,
        request: MetricsQueryRequest,
    ) -> Result<Vec<MetricSeries>, CompatError>;

    async fn label_names(
        &self,
        ctx: &TenantContext,
        req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError>;

    async fn label_values(
        &self,
        ctx: &TenantContext,
        name: &str,
        req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError>;

    async fn series(
        &self,
        ctx: &TenantContext,
        req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<BTreeMap<String, String>>, CompatError>;

    async fn metadata(
        &self,
        ctx: &TenantContext,
        metric: Option<&str>,
        limit: Option<usize>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
    ) -> Result<Vec<MetricMetadata>, CompatError>;
}

/// Phase 0 placeholder: every method returns `unsupported_feature`.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnsupportedMetricsBackend;

#[async_trait]
impl MetricsQueryBackend for UnsupportedMetricsBackend {
    async fn query_range(
        &self,
        _ctx: &TenantContext,
        _request: MetricsQueryRequest,
    ) -> Result<Vec<MetricSeries>, CompatError> {
        Err(CompatError::unsupported("metrics_query_backend"))
    }

    async fn label_names(
        &self,
        _ctx: &TenantContext,
        _req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("metrics_label_names"))
    }

    async fn label_values(
        &self,
        _ctx: &TenantContext,
        _name: &str,
        _req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("metrics_label_values"))
    }

    async fn series(
        &self,
        _ctx: &TenantContext,
        _req: &MetricsDiscoveryRequest,
    ) -> Result<Vec<BTreeMap<String, String>>, CompatError> {
        Err(CompatError::unsupported("metrics_series"))
    }

    async fn metadata(
        &self,
        _ctx: &TenantContext,
        _metric: Option<&str>,
        _limit: Option<usize>,
        _start_ms: Option<i64>,
        _end_ms: Option<i64>,
    ) -> Result<Vec<MetricMetadata>, CompatError> {
        Err(CompatError::unsupported("metrics_metadata"))
    }
}

/// Apply AND matchers against a projected label map.
pub fn labels_match(
    labels: &BTreeMap<String, String>,
    matchers: &[LabelMatcher],
) -> Result<bool, CompatError> {
    for m in matchers {
        let actual = labels.get(&m.name).map(String::as_str).unwrap_or("");
        let ok = match m.op {
            MatcherOp::Eq => actual == m.value,
            MatcherOp::Ne => actual != m.value,
            MatcherOp::Re => regex_full_match(&m.value, actual)?,
            MatcherOp::Nre => !regex_full_match(&m.value, actual)?,
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

/// OR across selector groups; empty groups means no filter (match all).
pub fn labels_match_any(
    labels: &BTreeMap<String, String>,
    selector_groups: &[Vec<LabelMatcher>],
) -> Result<bool, CompatError> {
    if selector_groups.is_empty() {
        return Ok(true);
    }
    for group in selector_groups {
        if labels_match(labels, group)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Compiled PromQL matcher regexes. `labels_match` runs once per sample row on
/// the expand path; recompiling `load.*` for every point was ~0.5ms × N and blew
/// the 100ms Grafana SLO for `job=~` selectors.
fn matcher_regex(pattern: &str) -> Result<Regex, CompatError> {
    static CACHE: OnceLock<Mutex<HashMap<String, Regex>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    {
        let guard = cache.lock().expect("matcher regex cache");
        if let Some(re) = guard.get(pattern) {
            return Ok(re.clone());
        }
    }
    let anchored = format!("^(?:{pattern})$");
    let re = Regex::new(&anchored).map_err(|e| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("invalid matcher regex '{pattern}': {e}"),
        )
    })?;
    let mut guard = cache.lock().expect("matcher regex cache");
    if guard.len() >= 1024 {
        guard.clear();
    }
    guard.insert(pattern.to_string(), re.clone());
    Ok(re)
}

fn regex_full_match(pattern: &str, value: &str) -> Result<bool, CompatError> {
    Ok(matcher_regex(pattern)?.is_match(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authn::TenantInfo;
    use crate::compat::errors::CompatErrorCode;
    use crate::compat::tenant::{ProtocolScope, QueryLimits};

    #[tokio::test]
    async fn unsupported_backend_returns_stable_error() {
        let ctx = TenantContext::from_authenticated(
            TenantInfo {
                tenant_id: "t".into(),
                bucket_name: "b".into(),
                dataset_id: "d".into(),
            },
            ProtocolScope::Prometheus,
            None,
            QueryLimits::default(),
        )
        .unwrap();
        let err = UnsupportedMetricsBackend
            .query_range(
                &ctx,
                MetricsQueryRequest {
                    start_ms: None,
                    end_ms: None,
                    client_start_ms: None,
                    client_end_ms: None,
                    matchers: vec![],
                    step_ms: None,
                    collapse_metric: None,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn matcher_eq_and_re() {
        let mut labels = BTreeMap::new();
        labels.insert("__name__".into(), "http_requests".into());
        labels.insert("job".into(), "api".into());
        assert!(labels_match(
            &labels,
            &[LabelMatcher {
                name: "job".into(),
                op: MatcherOp::Eq,
                value: "api".into(),
            }]
        )
        .unwrap());
        assert!(labels_match(
            &labels,
            &[LabelMatcher {
                name: "job".into(),
                op: MatcherOp::Re,
                value: "a.*".into(),
            }]
        )
        .unwrap());
        assert!(!labels_match(
            &labels,
            &[LabelMatcher {
                name: "job".into(),
                op: MatcherOp::Eq,
                value: "other".into(),
            }]
        )
        .unwrap());
        let err = labels_match(
            &labels,
            &[LabelMatcher {
                name: "job".into(),
                op: MatcherOp::Re,
                value: "(unclosed".into(),
            }],
        )
        .unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }
}
