use super::encode::{response_stream_labels_for_hit, MatrixSample, MatrixSeries};
use super::logql::parse_logql;
use super::params::parse_duration_ns;
use crate::compat::backends::logs::{LogDirection, LogHit, LogsQueryRequest};
use crate::compat::errors::{CompatError, CompatErrorCode};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsVolumeQuery {
    pub group_by: Vec<String>,
    pub request: LogsQueryRequest,
    pub range_ns: i64,
}

pub fn parse_logs_volume_query(query: &str) -> Result<Option<LogsVolumeQuery>, CompatError> {
    let query = query.trim();
    if query.is_empty() {
        return Ok(None);
    }
    let (group_by, rest) = parse_optional_sum_by(query)?;
    // Bare `count_over_time` on query_range stays phase-2 unsupported (501).
    if rest == query {
        return Ok(None);
    }
    let Some(inner) = rest.strip_prefix("count_over_time(") else {
        return Ok(None);
    };
    let (logql, range_raw) = split_count_over_time_inner(inner)?;
    let range_ns = parse_count_over_time_range(range_raw)?;
    let request = parse_logql(logql)?;
    Ok(Some(LogsVolumeQuery {
        group_by,
        request,
        range_ns,
    }))
}

pub fn eval_logs_volume(
    hits: &[LogHit],
    group_by: &[String],
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    range_ns: i64,
) -> Vec<MatrixSeries> {
    if step_ns <= 0 || end_ns <= start_ns {
        return Vec::new();
    }
    let step_count = ((i128::from(end_ns - start_ns) + i128::from(step_ns) - 1)
        / i128::from(step_ns))
    .max(0) as usize;
    if step_count == 0 {
        return Vec::new();
    }

    let mut counts: BTreeMap<BTreeMap<String, String>, Vec<u64>> = BTreeMap::new();
    for index in 0..step_count {
        let bucket_end = start_ns + i64::try_from(index + 1).unwrap_or(0) * step_ns;
        let window_start = bucket_end.saturating_sub(range_ns);
        for hit in hits {
            if hit.timestamp_ns <= window_start || hit.timestamp_ns > bucket_end {
                continue;
            }
            let labels = group_labels(hit, group_by);
            counts.entry(labels).or_insert_with(|| vec![0; step_count]);
        }
    }

    for index in 0..step_count {
        let bucket_end = start_ns + i64::try_from(index + 1).unwrap_or(0) * step_ns;
        let window_start = bucket_end.saturating_sub(range_ns);
        for hit in hits {
            if hit.timestamp_ns <= window_start || hit.timestamp_ns > bucket_end {
                continue;
            }
            let labels = group_labels(hit, group_by);
            if let Some(bucket_counts) = counts.get_mut(&labels) {
                bucket_counts[index] += 1;
            }
        }
    }

    counts
        .into_iter()
        .map(|(labels, bucket_counts)| MatrixSeries {
            labels,
            samples: bucket_counts
                .into_iter()
                .enumerate()
                .filter(|(_, count)| *count > 0)
                .map(|(index, count)| {
                    let ts_ns = start_ns + i64::try_from(index + 1).unwrap_or(0) * step_ns;
                    MatrixSample {
                        timestamp_ns: ts_ns,
                        value: count as f64,
                    }
                })
                .collect(),
        })
        .filter(|series| !series.samples.is_empty())
        .collect()
}

fn group_labels(hit: &LogHit, group_by: &[String]) -> BTreeMap<String, String> {
    if group_by.is_empty() {
        return BTreeMap::new();
    }
    let stream = response_stream_labels_for_hit(hit);
    group_by
        .iter()
        .map(|name| (name.clone(), group_label_value(&stream, name)))
        .collect()
}

fn group_label_value(labels: &BTreeMap<String, String>, name: &str) -> String {
    if name == "level" {
        for key in ["level", "lvl", "loglevel", "detected_level"] {
            if let Some(value) = labels.get(key) {
                return value.clone();
            }
        }
        return String::new();
    }
    labels.get(name).cloned().unwrap_or_default()
}

fn parse_optional_sum_by(query: &str) -> Result<(Vec<String>, &str), CompatError> {
    let trimmed = query.trim();
    let Some(rest) = trimmed.strip_prefix("sum by") else {
        return Ok((Vec::new(), trimmed));
    };
    let rest = rest.trim_start();
    let Some(label_section) = rest.strip_prefix('(') else {
        return Ok((Vec::new(), trimmed));
    };
    let close = label_section
        .find(')')
        .ok_or_else(|| bad("invalid sum by clause"))?;
    let labels = label_section[..close]
        .split(',')
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut rest = label_section[close + 1..].trim_start();
    if !rest.starts_with('(') || !rest.ends_with(')') {
        return Err(bad("invalid sum by clause"));
    }
    rest = rest[1..rest.len() - 1].trim();
    Ok((labels, rest))
}

fn split_count_over_time_inner(input: &str) -> Result<(&str, &str), CompatError> {
    let body = input
        .trim()
        .strip_suffix(')')
        .ok_or_else(|| bad("unterminated count_over_time"))?;
    split_log_range(body.trim())
}

fn split_log_range(input: &str) -> Result<(&str, &str), CompatError> {
    let open = input
        .rfind('[')
        .ok_or_else(|| bad("count_over_time requires a range"))?;
    if !input.ends_with(']') {
        return Err(bad("count_over_time requires a range"));
    }
    Ok((input[..open].trim(), input[open..].trim()))
}

fn parse_count_over_time_range(range_raw: &str) -> Result<i64, CompatError> {
    let inner = range_raw
        .trim()
        .strip_prefix('[')
        .and_then(|v| v.strip_suffix(']'))
        .ok_or_else(|| bad("count_over_time requires a range"))?;
    if inner == "$__auto" || inner == "$__interval" || inner == "$__range" {
        return Ok(1);
    }
    parse_duration_ns(inner)
}

pub fn volume_query_request(
    mut request: LogsQueryRequest,
    start_ns: i64,
    end_ns: i64,
    cap: usize,
) -> LogsQueryRequest {
    request.start_ns = Some(start_ns);
    request.end_ns = Some(end_ns);
    request.limit = cap;
    request.direction = LogDirection::Forward;
    request
}

pub fn default_step_ns(start_ns: i64, end_ns: i64) -> i64 {
    let range = (i128::from(end_ns) - i128::from(start_ns)).max(1);
    (range / 60).clamp(1, 300_000_000_000) as i64
}

fn bad(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_grafana_explore_logs_volume_with_drop_error() {
        let parsed = parse_logs_volume_query(
            r#"sum by (level) (count_over_time({service_name="load-generator"} |= `` | drop __error__[$__auto]))"#,
        )
        .expect("parse")
        .expect("volume");
        assert_eq!(parsed.group_by, vec!["level".to_string()]);
        assert_eq!(parsed.range_ns, 1);
        assert_eq!(parsed.request.matchers[0].name, "service_name");
        assert_eq!(
            parsed.request.line_filters,
            vec![crate::compat::backends::logs::LogLineFilter::Contains(
                String::new()
            )]
        );
    }

    #[test]
    fn parses_grafana_logs_volume_expression() {
        let parsed =
            parse_logs_volume_query(r#"sum by (level) (count_over_time({service_name="ad"}[1m]))"#)
                .expect("parse")
                .expect("volume");
        assert_eq!(parsed.group_by, vec!["level".to_string()]);
        assert_eq!(parsed.range_ns, 60_000_000_000);
        assert_eq!(parsed.request.matchers[0].name, "service_name");
    }

    #[test]
    fn rejects_non_volume_queries() {
        assert!(parse_logs_volume_query(r#"{job="api"}"#)
            .expect("parse")
            .is_none());
        assert!(
            parse_logs_volume_query(r#"count_over_time({service_name="checkout"}[1m])"#)
                .expect("parse")
                .is_none()
        );
    }

    #[test]
    fn buckets_counts_by_level_and_step() {
        let hits = vec![
            LogHit {
                timestamp_ns: 900_000_000,
                line: "a".into(),
                labels: [("level".into(), "info".into())].into_iter().collect(),
                structured_metadata: BTreeMap::new(),
            },
            LogHit {
                timestamp_ns: 1_500_000_000,
                line: "b".into(),
                labels: [("level".into(), "error".into())].into_iter().collect(),
                structured_metadata: BTreeMap::new(),
            },
        ];
        let series = eval_logs_volume(
            &hits,
            &["level".into()],
            0,
            3_000_000_000,
            1_000_000_000,
            1_000_000_000,
        );
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].samples[0].value, 1.0);
    }
}
