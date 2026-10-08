use crate::compat::backends::logs::LogDirection;
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::tenant::QueryLimits;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiParams {
    pub query: Option<String>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub time_ns: Option<i64>,
    pub limit: usize,
    pub direction: LogDirection,
    pub interval_ns: Option<i64>,
    pub step_ns: Option<i64>,
    pub since_ns: Option<i64>,
}

#[cfg(test)]
fn parse_loki_params(pairs: &[(String, String)], range: bool) -> Result<LokiParams, CompatError> {
    parse_loki_params_with_limits(pairs, range, &QueryLimits::default())
}

pub fn parse_loki_params_with_limits(
    pairs: &[(String, String)],
    range: bool,
    limits: &QueryLimits,
) -> Result<LokiParams, CompatError> {
    let map = pairs
        .iter()
        .fold(HashMap::<String, Vec<String>>::new(), |mut m, (k, v)| {
            m.entry(k.clone()).or_default().push(v.clone());
            m
        });
    let first = |key: &str| map.get(key).and_then(|v| v.first()).map(String::as_str);
    let query = first("query")
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string);
    if range && query.is_none() {
        return Err(bad("missing query parameter"));
    }
    let start_ns = parse_optional_ns(first("start"))?;
    let end_ns = parse_optional_ns(first("end"))?;
    let time_ns = parse_optional_ns(first("time"))?;
    let since_ns = first("since").map(parse_duration_ns).transpose()?;
    let interval_ns = first("interval").map(parse_duration_ns).transpose()?;
    let step_ns = first("step").map(parse_duration_ns).transpose()?;
    let limit = match first("limit") {
        None | Some("") => 1000,
        Some(raw) => raw.parse::<usize>().map_err(|_| bad("invalid limit"))?,
    };
    if limit == 0 {
        return Err(bad("limit must be greater than zero"));
    }
    if limit > limits.max_series.saturating_mul(100).max(1000) {
        return Err(CompatError::new(
            CompatErrorCode::LimitExceeded,
            "limit exceeds Loki compatibility limit",
        ));
    }
    let direction = match first("direction").unwrap_or("backward") {
        "forward" => LogDirection::Forward,
        "backward" => LogDirection::Backward,
        _ => return Err(bad("direction must be forward or backward")),
    };
    if let (Some(start), Some(end)) = (start_ns, end_ns) {
        if end < start {
            return Err(bad("end must be greater than or equal to start"));
        }
        let range_ns = (i128::from(end) - i128::from(start)) as u128;
        // 0 = unlimited (AC-W1 / §9.2) — same semantics as Prom validate_time_range_ms.
        if limits.max_query_range_seconds > 0
            && range_ns > u128::from(limits.max_query_range_seconds) * 1_000_000_000
        {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                "query range exceeds max_query_range_seconds",
            ));
        }
    }
    if let Some(since) = since_ns {
        if limits.max_query_range_seconds > 0
            && since as u128 > u128::from(limits.max_query_range_seconds) * 1_000_000_000
        {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                "since exceeds max_query_range_seconds",
            ));
        }
    }
    if range && start_ns.is_none() && since_ns.is_none() {
        return Err(bad("missing start or since parameter"));
    }
    if range && end_ns.is_none() && since_ns.is_none() {
        return Err(bad("missing end parameter"));
    }
    // interval/step are accepted for wire compatibility (Grafana always sends
    // a step for range queries). Stream results are currently returned
    // unsampled; true downsampling is tracked as Phase 2 follow-up work.
    Ok(LokiParams {
        query,
        start_ns,
        end_ns,
        time_ns,
        limit,
        direction,
        interval_ns,
        step_ns,
        since_ns,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiIndexStatsParams {
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiTailParams {
    pub query: String,
    pub start_ns: Option<i64>,
    pub limit: usize,
    pub delay_for_secs: u64,
}

pub fn parse_index_stats_params(
    pairs: &[(String, String)],
    limits: &QueryLimits,
) -> Result<LokiIndexStatsParams, CompatError> {
    let map = pairs
        .iter()
        .fold(HashMap::<String, Vec<String>>::new(), |mut m, (k, v)| {
            m.entry(k.clone()).or_default().push(v.clone());
            m
        });
    let first = |key: &str| map.get(key).and_then(|v| v.first()).map(String::as_str);
    let query = first("query")
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| bad("missing query parameter"))?
        .to_string();
    let start_ns = parse_optional_ns(first("start"))?.ok_or_else(|| bad("missing start"))?;
    let end_ns = parse_optional_ns(first("end"))?.ok_or_else(|| bad("missing end"))?;
    if end_ns < start_ns {
        return Err(bad("end must be greater than or equal to start"));
    }
    limits.validate_time_range_ms(Some(start_ns / 1_000_000), Some(end_ns / 1_000_000))?;
    Ok(LokiIndexStatsParams {
        query,
        start_ns,
        end_ns,
    })
}

pub fn parse_tail_params(
    pairs: &[(String, String)],
    limits: &QueryLimits,
) -> Result<LokiTailParams, CompatError> {
    let map = pairs
        .iter()
        .fold(HashMap::<String, Vec<String>>::new(), |mut m, (k, v)| {
            m.entry(k.clone()).or_default().push(v.clone());
            m
        });
    let first = |key: &str| map.get(key).and_then(|v| v.first()).map(String::as_str);
    let query = first("query")
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| bad("missing query parameter"))?
        .to_string();
    let start_ns = parse_optional_ns(first("start"))?;
    let limit = match first("limit") {
        None | Some("") => 100,
        Some(raw) => raw.parse::<usize>().map_err(|_| bad("invalid limit"))?,
    };
    if limit == 0 {
        return Err(bad("limit must be greater than zero"));
    }
    if limit > limits.max_series.saturating_mul(100).max(1000) {
        return Err(CompatError::new(
            CompatErrorCode::LimitExceeded,
            "limit exceeds Loki compatibility limit",
        ));
    }
    let delay_for_secs = match first("delay_for") {
        None | Some("") => 0,
        Some(raw) => raw
            .parse::<u64>()
            .map_err(|_| bad("invalid delay_for"))?
            .min(5),
    };
    Ok(LokiTailParams {
        query,
        start_ns,
        limit,
        delay_for_secs,
    })
}

fn parse_optional_ns(raw: Option<&str>) -> Result<Option<i64>, CompatError> {
    let Some(raw) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    if let Ok(value) = raw.parse::<i128>() {
        return i64::try_from(value)
            .map(Some)
            .map_err(|_| bad("timestamp is out of range"));
    }
    if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(raw) {
        return timestamp
            .timestamp_nanos_opt()
            .map(Some)
            .ok_or_else(|| bad("timestamp is out of range"));
    }
    if let Ok(seconds) = raw.parse::<f64>() {
        if !seconds.is_finite() {
            return Err(bad("timestamp is out of range"));
        }
        let nanos = seconds * 1_000_000_000.0;
        if !nanos.is_finite() || nanos < i64::MIN as f64 || nanos > i64::MAX as f64 {
            return Err(bad("timestamp is out of range"));
        }
        return Ok(Some(nanos.round() as i64));
    }
    Err(bad(format!("invalid time '{raw}'")))
}

pub(crate) fn parse_duration_ns(raw: &str) -> Result<i64, CompatError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(bad("duration must not be empty"));
    }
    let (number, multiplier) = [
        ("ns", 1i128),
        ("us", 1_000i128),
        ("ms", 1_000_000i128),
        ("s", 1_000_000_000i128),
        ("m", 60_000_000_000i128),
        ("h", 3_600_000_000_000i128),
        ("d", 86_400_000_000_000i128),
        ("w", 604_800_000_000_000i128),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| raw.strip_suffix(suffix).map(|number| (number, multiplier)))
    .ok_or_else(|| bad("invalid duration"))?;
    let value = number.parse::<f64>().map_err(|_| bad("invalid duration"))?;
    if !value.is_finite() || value <= 0.0 {
        return Err(bad("duration must be positive"));
    }
    let ns = (value * multiplier as f64).round();
    if ns > i64::MAX as f64 {
        return Err(bad("duration is out of range"));
    }
    Ok(ns as i64)
}

fn bad(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nanosecond_bounds_and_direction() {
        let parsed = parse_loki_params(
            &[
                ("query".into(), "{service_name=\"api\"}".into()),
                ("start".into(), "1000000001".into()),
                ("end".into(), "2000000002".into()),
                ("direction".into(), "forward".into()),
                ("limit".into(), "7".into()),
            ],
            false,
        )
        .expect("params");
        assert_eq!(parsed.start_ns, Some(1_000_000_001));
        assert_eq!(parsed.end_ns, Some(2_000_000_002));
        assert_eq!(parsed.limit, 7);
        assert_eq!(parsed.direction, LogDirection::Forward);
    }

    #[test]
    fn parses_rfc3339_timestamps_with_full_nanosecond_precision() {
        let parsed = parse_loki_params(
            &[
                ("query".into(), "{service_name=\"api\"}".into()),
                ("start".into(), "2021-01-01T00:00:00.123456789Z".into()),
                ("end".into(), "2021-01-01T00:00:01.000000001+00:00".into()),
            ],
            false,
        )
        .expect("params");
        assert_eq!(parsed.start_ns, Some(1_609_459_200_123_456_789));
        assert_eq!(parsed.end_ns, Some(1_609_459_201_000_000_001));
    }

    #[test]
    fn parses_all_loki_duration_suffixes_as_nanoseconds() {
        for (raw, expected) in [
            ("1ns", 1),
            ("1us", 1_000),
            ("1ms", 1_000_000),
            ("1s", 1_000_000_000),
            ("1m", 60_000_000_000),
            ("1h", 3_600_000_000_000),
            ("1d", 86_400_000_000_000),
            ("1w", 604_800_000_000_000),
        ] {
            assert_eq!(parse_duration_ns(raw).unwrap(), expected, "{raw}");
        }
    }

    #[test]
    fn accepts_stream_sampling_parameters_without_sampling() {
        let params = parse_loki_params(
            &[
                ("query".into(), "{service_name=\"api\"}".into()),
                ("start".into(), "1".into()),
                ("end".into(), "2".into()),
                ("step".into(), "1s".into()),
            ],
            true,
        )
        .expect("step is accepted");
        assert_eq!(params.step_ns, Some(1_000_000_000));
    }

    #[test]
    fn unlimited_max_query_range_accepts_explore_labels_window() {
        let limits = QueryLimits::default();
        assert_eq!(limits.max_query_range_seconds, 0);
        let parsed = parse_loki_params_with_limits(
            &[
                ("start".into(), "1788244951417000000".into()),
                ("end".into(), "1788248551417000000".into()),
            ],
            false,
            &limits,
        )
        .expect("1h Grafana Explore labels window");
        assert_eq!(parsed.start_ns, Some(1_788_244_951_417_000_000));
        assert_eq!(parsed.end_ns, Some(1_788_248_551_417_000_000));
    }
}
