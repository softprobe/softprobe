//! Prometheus query / discovery parameter parsing (GET query + POST form).

use crate::compat::backends::metrics::LabelMatcher;
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::promql::parse_match_selector;
use crate::compat::tenant::QueryLimits;
use chrono::{DateTime, NaiveDateTime};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct DiscoveryParams {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub matchers: Vec<Vec<LabelMatcher>>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetadataParams {
    pub metric: Option<String>,
    pub limit: Option<usize>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryParams {
    pub query: String,
    pub time_ms: Option<i64>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub step_ms: Option<i64>,
}

/// Collect raw key/value pairs from a query string or form body.
pub fn pairs_from_query(query: &str) -> Vec<(String, String)> {
    if query.is_empty() {
        return Vec::new();
    }
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = match pair.split_once('=') {
                Some((k, v)) => (k, v),
                None => (pair, ""),
            };
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

/// Parse `application/x-www-form-urlencoded` body.
pub fn pairs_from_form(body: &str) -> Vec<(String, String)> {
    pairs_from_query(body)
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let h = || {
                    let hi = from_hex(bytes[i + 1])?;
                    let lo = from_hex(bytes[i + 2])?;
                    Some((hi << 4) | lo)
                };
                if let Some(b) = h() {
                    out.push(b);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

pub fn parse_discovery_params(
    pairs: &[(String, String)],
    limits: &QueryLimits,
) -> Result<DiscoveryParams, CompatError> {
    let map = multi_map(pairs);
    // tenant_id is ignored (must not change scope).
    let _ = map.get("tenant_id");

    let start_ms = parse_optional_time(first(&map, "start"))?;
    let end_ms = parse_optional_time(first(&map, "end"))?;
    validate_range(start_ms, end_ms, limits)?;

    let mut matchers = Vec::new();
    if let Some(values) = map.get("match[]") {
        for v in values {
            if v.trim().is_empty() {
                continue;
            }
            matchers.push(parse_match_selector(v)?.matchers);
        }
    }
    Ok(DiscoveryParams {
        start_ms,
        end_ms,
        matchers,
    })
}

pub fn parse_metadata_params(
    pairs: &[(String, String)],
    limits: &QueryLimits,
) -> Result<MetadataParams, CompatError> {
    let map = multi_map(pairs);
    let _ = map.get("tenant_id");
    let start_ms = parse_optional_time(first(&map, "start"))?;
    let end_ms = parse_optional_time(first(&map, "end"))?;
    validate_range(start_ms, end_ms, limits)?;
    let metric = first(&map, "metric").filter(|s| !s.is_empty());
    let limit = match first(&map, "limit") {
        Some(s) if !s.is_empty() => Some(
            s.parse::<usize>()
                .map_err(|_| CompatError::new(CompatErrorCode::BadRequest, "invalid limit"))?,
        ),
        _ => None,
    };
    Ok(MetadataParams {
        metric,
        limit,
        start_ms,
        end_ms,
    })
}

pub fn parse_query_params(
    pairs: &[(String, String)],
    limits: &QueryLimits,
    range: bool,
) -> Result<QueryParams, CompatError> {
    let map = multi_map(pairs);
    let _ = map.get("tenant_id");
    // timeout is ignored (server uses capability timeout).
    let _ = map.get("timeout");

    let query = first(&map, "query")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CompatError::new(CompatErrorCode::BadRequest, "missing query parameter"))?;

    if range {
        let start_ms = parse_optional_time(first(&map, "start"))?.ok_or_else(|| {
            CompatError::new(CompatErrorCode::BadRequest, "missing start parameter")
        })?;
        let end_ms = parse_optional_time(first(&map, "end"))?.ok_or_else(|| {
            CompatError::new(CompatErrorCode::BadRequest, "missing end parameter")
        })?;
        validate_range(Some(start_ms), Some(end_ms), limits)?;
        let step_ms = parse_step(first(&map, "step"))?.ok_or_else(|| {
            CompatError::new(CompatErrorCode::BadRequest, "missing step parameter")
        })?;
        Ok(QueryParams {
            query,
            time_ms: None,
            start_ms: Some(start_ms),
            end_ms: Some(end_ms),
            step_ms: Some(step_ms),
        })
    } else {
        let time_ms = parse_optional_time(first(&map, "time"))?;
        Ok(QueryParams {
            query,
            time_ms,
            start_ms: None,
            end_ms: None,
            step_ms: None,
        })
    }
}

fn multi_map(pairs: &[(String, String)]) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for (k, v) in pairs {
        map.entry(k.clone()).or_default().push(v.clone());
    }
    map
}

fn first(map: &HashMap<String, Vec<String>>, key: &str) -> Option<String> {
    map.get(key).and_then(|v| v.first()).cloned()
}

pub fn parse_optional_time(raw: Option<String>) -> Result<Option<i64>, CompatError> {
    match raw {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Ok(None),
        Some(s) => Ok(Some(parse_time_ms(&s)?)),
    }
}

/// Prometheus float unix seconds or RFC3339 → unix ms.
pub fn parse_time_ms(raw: &str) -> Result<i64, CompatError> {
    let s = raw.trim();
    if let Ok(secs) = s.parse::<f64>() {
        if secs.is_finite() {
            return Ok((secs * 1000.0).round() as i64);
        }
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ") {
        return Ok(naive.and_utc().timestamp_millis());
    }
    Err(CompatError::new(
        CompatErrorCode::BadRequest,
        format!("invalid time '{s}'"),
    ))
}

fn parse_step(raw: Option<String>) -> Result<Option<i64>, CompatError> {
    let Some(s) = raw.filter(|s| !s.trim().is_empty()) else {
        return Ok(None);
    };
    let s = s.trim();
    if let Ok(secs) = s.parse::<f64>() {
        if secs > 0.0 && secs.is_finite() {
            return Ok(Some((secs * 1000.0).round() as i64));
        }
    }
    // Duration like 15s / 1m / 1h
    if let Ok(dur) = promql_parser::util::parse_duration(s) {
        let ms = dur.as_millis() as i64;
        if ms > 0 {
            return Ok(Some(ms));
        }
    }
    Err(CompatError::new(
        CompatErrorCode::BadRequest,
        format!("invalid step '{s}'"),
    ))
}

fn validate_range(
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    limits: &QueryLimits,
) -> Result<(), CompatError> {
    limits.validate_time_range_ms(start_ms, end_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::backends::metrics::MatcherOp;

    fn limits() -> QueryLimits {
        QueryLimits::default()
    }

    #[test]
    fn parses_float_and_rfc3339_times() {
        assert_eq!(parse_time_ms("1609459200").unwrap(), 1_609_459_200_000);
        assert_eq!(
            parse_time_ms("2021-01-01T00:00:00Z").unwrap(),
            1_609_459_200_000
        );
    }

    #[test]
    fn rejects_inverted_range() {
        let pairs = vec![("start".into(), "100".into()), ("end".into(), "50".into())];
        let err = parse_discovery_params(&pairs, &limits()).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }

    #[test]
    fn parses_match_selectors() {
        let pairs = vec![
            ("match[]".into(), r#"{job="api"}"#.into()),
            ("match[]".into(), r#"up{code=~"5.."}"#.into()),
            ("tenant_id".into(), "attacker".into()),
        ];
        let p = parse_discovery_params(&pairs, &limits()).unwrap();
        assert_eq!(p.matchers.len(), 2);
        assert!(p.matchers[0]
            .iter()
            .any(|m| m.name == "job" && m.op == MatcherOp::Eq && m.value == "api"));
    }

    #[test]
    fn rejects_garbage_match() {
        let pairs = vec![("match[]".into(), "{".into())];
        let err = parse_discovery_params(&pairs, &limits()).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }

    #[test]
    fn query_requires_query_param() {
        let err = parse_query_params(&[], &limits(), false).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }

    #[test]
    fn parses_step_duration() {
        let pairs = vec![
            ("query".into(), "up".into()),
            ("start".into(), "1".into()),
            ("end".into(), "100".into()),
            ("step".into(), "15s".into()),
        ];
        let p = parse_query_params(&pairs, &limits(), true).unwrap();
        assert_eq!(p.step_ms, Some(15_000));
    }
}
