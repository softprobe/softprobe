use super::traceql::{parse_traceql, TraceSelector};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::tenant::QueryLimits;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TempoSearchParams {
    pub tags: BTreeMap<String, String>,
    pub selector: Option<TraceSelector>,
    pub min_duration_ns: Option<i64>,
    pub max_duration_ns: Option<i64>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TempoTraceLookupParams {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
}

pub fn parse_tempo_trace_lookup_params(
    pairs: &[(String, String)],
    limits: &QueryLimits,
) -> Result<TempoTraceLookupParams, CompatError> {
    let mut start_ns = None;
    let mut end_ns = None;
    for (key, value) in pairs {
        match key.as_str() {
            "start" => start_ns = Some(parse_tempo_time_ns(value)?),
            "end" => end_ns = Some(parse_tempo_time_ns(value)?),
            "tenant_id" => {}
            _ => {
                return Err(unsupported(format!(
                    "Tempo trace lookup parameter '{key}' is unsupported"
                )))
            }
        }
    }
    validate_bounds(start_ns, end_ns, limits)?;
    Ok(TempoTraceLookupParams { start_ns, end_ns })
}

pub fn parse_tempo_tag_params(pairs: &[(String, String)]) -> Result<(), CompatError> {
    for (key, _) in pairs {
        match key.as_str() {
            "tenant_id" => {}
            _ => {
                return Err(unsupported(format!(
                    "Tempo tag query parameter '{key}' is unsupported"
                )))
            }
        }
    }
    Ok(())
}

pub fn parse_tempo_search_params(
    pairs: &[(String, String)],
    limits: &QueryLimits,
) -> Result<TempoSearchParams, CompatError> {
    let mut tags = BTreeMap::new();
    let mut selector = None;
    let mut min_duration_ns = None;
    let mut max_duration_ns = None;
    let mut start_ns = None;
    let mut end_ns = None;
    let mut limit = 20usize;

    for (key, value) in pairs {
        match key.as_str() {
            "tags" => tags = parse_tags(value)?,
            "q" => selector = Some(parse_traceql(value)?),
            "minDuration" => min_duration_ns = Some(parse_duration_ns(value)?),
            "maxDuration" => max_duration_ns = Some(parse_duration_ns(value)?),
            "start" => start_ns = Some(parse_tempo_time_ns(value)?),
            "end" => end_ns = Some(parse_tempo_time_ns(value)?),
            "limit" => limit = value.parse().map_err(|_| bad("invalid limit"))?,
            "tenant_id" => {}
            _ => {
                return Err(unsupported(format!(
                    "Tempo search parameter '{key}' is unsupported"
                )))
            }
        }
    }
    if limit == 0 {
        return Err(bad("limit must be greater than zero"));
    }
    if limit > limits.max_series {
        return Err(CompatError::new(
            CompatErrorCode::LimitExceeded,
            "limit exceeds Tempo compatibility limit",
        ));
    }
    validate_bounds(start_ns, end_ns, limits)?;
    if let (Some(min), Some(max)) = (min_duration_ns, max_duration_ns) {
        if max < min {
            return Err(bad(
                "maxDuration must be greater than or equal to minDuration",
            ));
        }
    }
    Ok(TempoSearchParams {
        tags,
        selector,
        min_duration_ns,
        max_duration_ns,
        start_ns,
        end_ns,
        limit,
    })
}

fn validate_bounds(
    start_ns: Option<i64>,
    end_ns: Option<i64>,
    limits: &QueryLimits,
) -> Result<(), CompatError> {
    if let (Some(start), Some(end)) = (start_ns, end_ns) {
        if end < start {
            return Err(bad("end must be greater than or equal to start"));
        }
        let range_ns = i128::from(end) - i128::from(start);
        if range_ns > i128::from(limits.max_query_range_seconds) * 1_000_000_000 {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                "query range exceeds max_query_range_seconds",
            ));
        }
    }
    Ok(())
}

fn parse_tags(raw: &str) -> Result<BTreeMap<String, String>, CompatError> {
    let mut tags = BTreeMap::new();
    let bytes = raw.as_bytes();
    let mut pos = 0;
    while pos < bytes.len() {
        while bytes.get(pos).is_some_and(u8::is_ascii_whitespace) {
            pos += 1;
        }
        if pos == bytes.len() {
            break;
        }
        let key_start = pos;
        while pos < bytes.len() && bytes[pos] != b'=' && !bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos == key_start || bytes.get(pos) != Some(&b'=') {
            return Err(bad("tags must use key=value pairs"));
        }
        let key = &raw[key_start..pos];
        pos += 1;
        let value = if bytes.get(pos) == Some(&b'"') {
            pos += 1;
            // Decode once at the end so multi-byte UTF-8 values survive.
            let mut raw: Vec<u8> = Vec::new();
            while pos < bytes.len() && bytes[pos] != b'"' {
                if bytes[pos] == b'\\' && pos + 1 < bytes.len() {
                    pos += 1;
                }
                raw.push(bytes[pos]);
                pos += 1;
            }
            if bytes.get(pos) != Some(&b'"') {
                return Err(bad("unterminated tag value"));
            }
            pos += 1;
            String::from_utf8_lossy(&raw).into_owned()
        } else {
            let start = pos;
            while pos < bytes.len() && !bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            raw[start..pos].to_string()
        };
        if key.is_empty() || value.is_empty() {
            return Err(bad("tags must not contain empty keys or values"));
        }
        tags.insert(key.to_string(), value);
    }
    Ok(tags)
}

fn parse_tempo_time_ns(raw: &str) -> Result<i64, CompatError> {
    let raw = raw.trim();
    if let Ok(seconds) = raw.parse::<i128>() {
        return i64::try_from(
            seconds
                .checked_mul(1_000_000_000)
                .ok_or_else(|| bad("timestamp is out of range"))?,
        )
        .map_err(|_| bad("timestamp is out of range"));
    }
    if let Some((whole, fraction)) = raw.split_once('.') {
        let sign = if whole.starts_with('-') { -1i128 } else { 1 };
        let whole = whole
            .parse::<i128>()
            .map_err(|_| bad("invalid timestamp"))?;
        if fraction.is_empty()
            || fraction.len() > 9
            || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(bad("invalid timestamp"));
        }
        let nanos = fraction
            .parse::<i128>()
            .map_err(|_| bad("invalid timestamp"))?
            * 10i128.pow((9 - fraction.len()) as u32);
        return i64::try_from(
            whole
                .checked_mul(1_000_000_000)
                .ok_or_else(|| bad("timestamp is out of range"))?
                + sign * nanos,
        )
        .map_err(|_| bad("timestamp is out of range"));
    }
    if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(raw) {
        return timestamp
            .timestamp_nanos_opt()
            .ok_or_else(|| bad("timestamp is out of range"));
    }
    Err(bad("invalid timestamp"))
}

fn parse_duration_ns(raw: &str) -> Result<i64, CompatError> {
    super::traceql::parse_duration_ns(raw).ok_or_else(|| bad("invalid duration"))
}

fn bad(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

fn unsupported(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::UnsupportedFeature, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::errors::CompatErrorCode;

    #[test]
    fn parses_tempo_search_bounds_durations_tags_and_limit() {
        let params = parse_tempo_search_params(
            &[
                (
                    "tags".into(),
                    r#"service.name=api http.method="GET""#.into(),
                ),
                ("minDuration".into(), "1.5ms".into()),
                ("maxDuration".into(), "2s".into()),
                ("start".into(), "1700000000.000000001".into()),
                ("end".into(), "1700000001.000000002".into()),
                ("limit".into(), "7".into()),
            ],
            &QueryLimits::default(),
        )
        .expect("params");
        assert_eq!(
            params.tags.get("service.name").map(String::as_str),
            Some("api")
        );
        assert_eq!(
            params.tags.get("http.method").map(String::as_str),
            Some("GET")
        );
        assert_eq!(params.min_duration_ns, Some(1_500_000));
        assert_eq!(params.start_ns, Some(1_700_000_000_000_000_001));
        assert_eq!(params.end_ns, Some(1_700_000_001_000_000_002));
        assert_eq!(params.limit, 7);
    }

    #[test]
    fn rejects_bad_bounds_and_unsupported_search_parameters_stably() {
        let err = parse_tempo_search_params(
            &[("start".into(), "2".into()), ("end".into(), "1".into())],
            &QueryLimits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);

        let err = parse_tempo_search_params(
            &[("q".into(), "{ duration > 1s } | count()".into())],
            &QueryLimits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn trace_lookup_requires_explicitly_supported_parameters() {
        let params = parse_tempo_trace_lookup_params(
            &[("start".into(), "1700000000.000000001".into())],
            &QueryLimits::default(),
        )
        .expect("lookup bounds");
        assert_eq!(params.start_ns, Some(1_700_000_000_000_000_001));

        let err = parse_tempo_trace_lookup_params(
            &[("mode".into(), "ignored".into())],
            &QueryLimits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn accepts_and_discards_tenant_id_on_lookup_and_tag_queries() {
        parse_tempo_trace_lookup_params(
            &[("tenant_id".into(), "spoofed".into())],
            &QueryLimits::default(),
        )
        .expect("tenant_id is a compatibility parameter");
        parse_tempo_tag_params(&[("tenant_id".into(), "spoofed".into())])
            .expect("tenant_id is a compatibility parameter");
    }

    #[test]
    fn rejects_unknown_tag_query_parameters() {
        let err = parse_tempo_tag_params(&[("mode".into(), "ignored".into())]).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }
}
