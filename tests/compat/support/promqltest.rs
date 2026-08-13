//! Minimal PromQL `promqltest` DSL parser + sample expansion (upstream format).

use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesSpec {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub values: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Load {
        interval: Duration,
        series: Vec<SeriesSpec>,
    },
    Clear,
    EvalInstant {
        at: Duration,
        query: String,
        /// Expected lines kept for documentation; differential uses live Prom.
        expected_lines: Vec<String>,
        line: usize,
    },
    EvalRange {
        from: Duration,
        to: Duration,
        step: Duration,
        query: String,
        expected_lines: Vec<String>,
        line: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    Message { line: usize, message: String },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Message { line, message } => write!(f, "line {line}: {message}"),
        }
    }
}

pub fn parse_promqltest(input: &str) -> Result<Vec<Command>, ParseError> {
    let lines: Vec<&str> = input.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let raw = lines[i];
        let line_no = i + 1;
        let trimmed = strip_comment(raw).trim();
        if trimmed.is_empty() {
            i += 1;
            continue;
        }
        if trimmed == "clear" {
            out.push(Command::Clear);
            i += 1;
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("load ") {
            let interval = parse_duration(rest.trim()).map_err(|m| ParseError::Message {
                line: line_no,
                message: m,
            })?;
            i += 1;
            let mut series = Vec::new();
            while i < lines.len() {
                let t = strip_comment(lines[i]).trim();
                if t.is_empty() {
                    i += 1;
                    continue;
                }
                if t.starts_with("load ")
                    || t.starts_with("eval ")
                    || t.starts_with("clear")
                    || t.starts_with("expect ")
                {
                    break;
                }
                series.push(parse_series_line(t).map_err(|m| ParseError::Message {
                    line: i + 1,
                    message: m,
                })?);
                i += 1;
            }
            out.push(Command::Load { interval, series });
            continue;
        }
        if trimmed.starts_with("eval instant") {
            let (at, query) = parse_eval_instant(trimmed).map_err(|m| ParseError::Message {
                line: line_no,
                message: m,
            })?;
            i += 1;
            let (expected, next) = collect_expected(&lines, i);
            i = next;
            out.push(Command::EvalInstant {
                at,
                query,
                expected_lines: expected,
                line: line_no,
            });
            continue;
        }
        if trimmed.starts_with("eval range") {
            let (from, to, step, query) =
                parse_eval_range(trimmed).map_err(|m| ParseError::Message {
                    line: line_no,
                    message: m,
                })?;
            i += 1;
            let (expected, next) = collect_expected(&lines, i);
            i = next;
            out.push(Command::EvalRange {
                from,
                to,
                step,
                query,
                expected_lines: expected,
                line: line_no,
            });
            continue;
        }
        // Ignore expect-only / deprecated directives at top level.
        if trimmed.starts_with("expect ") {
            i += 1;
            continue;
        }
        return Err(ParseError::Message {
            line: line_no,
            message: format!("unsupported command: {trimmed}"),
        });
    }
    Ok(out)
}

fn strip_comment(line: &str) -> &str {
    if let Some(idx) = line.find('#') {
        // Keep `#` inside quoted label values — rare in fixtures; simple split is OK for curated files.
        if !line[..idx].contains('"') {
            return &line[..idx];
        }
    }
    line
}

fn collect_expected(lines: &[&str], mut i: usize) -> (Vec<String>, usize) {
    let mut expected = Vec::new();
    while i < lines.len() {
        let t = strip_comment(lines[i]).trim();
        if t.is_empty() {
            i += 1;
            continue;
        }
        if t.starts_with("load ")
            || t.starts_with("eval ")
            || t.starts_with("clear")
            || t.starts_with("expect ")
        {
            break;
        }
        expected.push(t.to_string());
        i += 1;
    }
    (expected, i)
}

fn parse_eval_instant(line: &str) -> Result<(Duration, String), String> {
    // eval instant at <dur> <query>
    let rest = line
        .strip_prefix("eval instant at ")
        .ok_or_else(|| "expected 'eval instant at'".to_string())?;
    let mut parts = rest.splitn(2, char::is_whitespace);
    let at_s = parts.next().ok_or("missing eval timestamp")?;
    let query = parts.next().ok_or("missing eval query")?.trim();
    if query.is_empty() {
        return Err("missing eval query".into());
    }
    Ok((parse_duration(at_s)?, query.to_string()))
}

fn parse_eval_range(line: &str) -> Result<(Duration, Duration, Duration, String), String> {
    // eval range from <from> to <to> step <step> <query>
    let rest = line
        .strip_prefix("eval range from ")
        .ok_or_else(|| "expected 'eval range from'".to_string())?;
    let mut iter = rest.split_whitespace();
    let from_s = iter.next().ok_or("missing from")?;
    if iter.next() != Some("to") {
        return Err("expected 'to'".into());
    }
    let to_s = iter.next().ok_or("missing to")?;
    if iter.next() != Some("step") {
        return Err("expected 'step'".into());
    }
    let step_s = iter.next().ok_or("missing step")?;
    let query = iter.collect::<Vec<_>>().join(" ");
    if query.is_empty() {
        return Err("missing range query".into());
    }
    Ok((
        parse_duration(from_s)?,
        parse_duration(to_s)?,
        parse_duration(step_s)?,
        query,
    ))
}

pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".into());
    }
    // Upstream promqltest allows bare `0` (no unit).
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    let (num, unit) = s.split_at(
        s.find(|c: char| c.is_ascii_alphabetic())
            .ok_or_else(|| format!("duration missing unit: {s}"))?,
    );
    let n: f64 = num
        .parse()
        .map_err(|_| format!("invalid duration number: {s}"))?;
    let secs = match unit {
        "ms" => n / 1000.0,
        "s" => n,
        "m" => n * 60.0,
        "h" => n * 3600.0,
        "d" => n * 86400.0,
        "w" => n * 604800.0,
        "y" => n * 31536000.0,
        other => return Err(format!("unknown duration unit '{other}'")),
    };
    Ok(Duration::from_secs_f64(secs))
}

fn parse_series_line(line: &str) -> Result<SeriesSpec, String> {
    let line = line.trim();
    let (ident, values_str) = split_series_and_values(line)?;
    let (name, labels) = parse_metric_ident(ident)?;
    let values = expand_values(values_str)?;
    Ok(SeriesSpec {
        name,
        labels,
        values,
    })
}

fn split_series_and_values(line: &str) -> Result<(&str, &str), String> {
    // metric{...} values  OR metric values
    if let Some(brace) = line.find('{') {
        let end = line[brace..].find('}').ok_or("unclosed label set")? + brace;
        let ident = line[..=end].trim();
        let values = line[end + 1..].trim();
        if values.is_empty() {
            return Err("missing sample values".into());
        }
        Ok((ident, values))
    } else {
        let mut parts = line.splitn(2, char::is_whitespace);
        let ident = parts.next().ok_or("missing metric")?;
        let values = parts.next().ok_or("missing sample values")?.trim();
        Ok((ident, values))
    }
}

fn parse_metric_ident(ident: &str) -> Result<(String, BTreeMap<String, String>), String> {
    if let Some(brace) = ident.find('{') {
        let name = ident[..brace].trim().to_string();
        let inner = ident[brace + 1..ident.len() - 1].trim();
        let mut labels = BTreeMap::new();
        if !inner.is_empty() {
            for part in split_labels(inner)? {
                let (k, v) = part
                    .split_once('=')
                    .ok_or_else(|| format!("invalid label '{part}' (expected k=\"v\")"))?;
                let v = v.trim().trim_matches('"');
                labels.insert(k.trim().to_string(), v.to_string());
            }
        }
        Ok((name, labels))
    } else {
        Ok((ident.to_string(), BTreeMap::new()))
    }
}

fn split_labels(inner: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    for ch in inner.chars() {
        match ch {
            '"' => {
                in_quote = !in_quote;
                cur.push(ch);
            }
            ',' if !in_quote => {
                let t = cur.trim();
                if !t.is_empty() {
                    out.push(t.to_string());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    let t = cur.trim();
    if !t.is_empty() {
        out.push(t.to_string());
    }
    if in_quote {
        return Err("unclosed quote in labels".into());
    }
    Ok(out)
}

/// Expand PromQL test value tokens (`1`, `0+10x10`, spaced sequences).
pub fn expand_values(s: &str) -> Result<Vec<f64>, String> {
    let mut out = Vec::new();
    for tok in s.split_whitespace() {
        if tok == "_" || tok == "stale" {
            // Staleness markers: treat as NaN placeholder; curated files avoid these.
            out.push(f64::NAN);
            continue;
        }
        if let Some((start, rest)) = tok.split_once('+') {
            if let Some((inc, times)) = rest.split_once('x') {
                let start: f64 = start.parse().map_err(|_| format!("bad start in {tok}"))?;
                let inc: f64 = inc.parse().map_err(|_| format!("bad inc in {tok}"))?;
                let times: usize = times.parse().map_err(|_| format!("bad times in {tok}"))?;
                out.push(start);
                let mut v = start;
                for _ in 0..times {
                    v += inc;
                    out.push(v);
                }
                continue;
            }
        }
        let v: f64 = tok.parse().map_err(|_| format!("bad value token {tok}"))?;
        out.push(v);
    }
    if out.is_empty() {
        return Err("no values".into());
    }
    Ok(out)
}

/// Expand a loaded series into (unix_ms, value) samples starting at `base_ms`.
pub fn series_samples(interval: Duration, values: &[f64], base_ms: i64) -> Vec<(i64, f64)> {
    let step_ms = interval.as_millis() as i64;
    values
        .iter()
        .enumerate()
        .map(|(i, v)| (base_ms + i as i64 * step_ms, *v))
        .collect()
}

/// Render OpenMetrics text for promtool (timestamps in unix seconds).
pub fn to_openmetrics(series: &[(SeriesSpec, Duration)], as_counter: bool, base_ms: i64) -> String {
    let mut out = String::new();
    let mut seen = std::collections::BTreeSet::new();
    for (spec, interval) in series {
        if seen.insert(spec.name.clone()) {
            let ty = if as_counter { "counter" } else { "gauge" };
            out.push_str(&format!("# TYPE {} {ty}\n", spec.name));
        }
        let samples = series_samples(*interval, &spec.values, base_ms);
        for (ms, v) in samples {
            if v.is_nan() {
                continue;
            }
            let labels = format_labels(&spec.labels);
            let sec = ms as f64 / 1000.0;
            if labels.is_empty() {
                out.push_str(&format!("{} {} {}\n", spec.name, format_om_float(v), sec));
            } else {
                out.push_str(&format!(
                    "{}{{{}}} {} {}\n",
                    spec.name,
                    labels,
                    format_om_float(v),
                    sec
                ));
            }
        }
    }
    out.push_str("# EOF\n");
    out
}

fn format_labels(labels: &BTreeMap<String, String>) -> String {
    labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label(v)))
        .collect::<Vec<_>>()
        .join(",")
}

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"")
}

fn format_om_float(v: f64) -> String {
    if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".into()
        } else {
            "-Inf".into()
        }
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_increment_sequence() {
        assert_eq!(
            expand_values("0+10x10").unwrap(),
            vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
        );
    }

    #[test]
    fn parses_aggregator_snippet() {
        let cmds = parse_promqltest(
            r#"
load 5m
  http_requests{job="api-server", instance="0", group="production"} 0+10x10
eval instant at 50m sum by (group) (http_requests{job="api-server"})
  {group="production"} 100
"#,
        )
        .unwrap();
        assert_eq!(cmds.len(), 2);
        match &cmds[0] {
            Command::Load { interval, series } => {
                assert_eq!(*interval, Duration::from_secs(300));
                assert_eq!(series[0].name, "http_requests");
                assert_eq!(
                    series[0].labels.get("job").map(String::as_str),
                    Some("api-server")
                );
                assert_eq!(series[0].values.len(), 11);
            }
            other => panic!("{other:?}"),
        }
        match &cmds[1] {
            Command::EvalInstant { at, query, .. } => {
                assert_eq!(*at, Duration::from_secs(3000));
                assert!(query.contains("sum by (group)"));
            }
            other => panic!("{other:?}"),
        }
    }
}
