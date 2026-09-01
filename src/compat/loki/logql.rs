use crate::compat::backends::logs::{LogLineFilter, LogParser, LogsQueryRequest};
use crate::compat::backends::metrics::{LabelMatcher, MatcherOp};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::projection::prometheus::sanitize_label_name;

pub fn parse_logql(query: &str) -> Result<LogsQueryRequest, CompatError> {
    let query = query.trim();
    if let Some(function) = leading_function_name(query) {
        return Err(unsupported(format!(
            "LogQL function '{function}' is not supported for stream responses"
        )));
    }
    let (selector, pipeline) = split_selector(query)?;
    let mut request = LogsQueryRequest {
        start_ns: None,
        end_ns: None,
        matchers: parse_selector(selector)?,
        line_filters: Vec::new(),
        parser: None,
        parsed_filters: Vec::new(),
        unwrap: None,
        limit: 1000,
        direction: crate::compat::backends::logs::LogDirection::Backward,
    };
    for stage in split_pipeline(pipeline)? {
        let stage = stage.trim();
        if stage.is_empty() {
            continue;
        }
        match stage {
            "json" => request.parser = Some(LogParser::Json),
            "logfmt" => request.parser = Some(LogParser::Logfmt),
            s if s.starts_with("json ") || s.starts_with("logfmt ") => {
                return Err(unsupported("parser field expressions"));
            }
            s if s.starts_with("unwrap ") => {
                return Err(unsupported("LogQL unwrap"));
            }
            s if s.starts_with('=')
                || s.starts_with("!=")
                || s.starts_with('~')
                || s.starts_with("!~") =>
            {
                let (op, raw) = if let Some(v) = s.strip_prefix("!=") {
                    ("!=", v)
                } else if let Some(v) = s.strip_prefix("!~") {
                    ("!~", v)
                } else if let Some(v) = s.strip_prefix('=') {
                    ("=", v)
                } else {
                    ("~", &s[1..])
                };
                let value = quoted(raw.trim())?;
                match op {
                    "=" => request.line_filters.push(LogLineFilter::Contains(value)),
                    "!=" => request.line_filters.push(LogLineFilter::NotContains(value)),
                    "~" => {
                        regex(&value)?;
                        request.line_filters.push(LogLineFilter::Regex(value));
                    }
                    "!~" => {
                        regex(&value)?;
                        request.line_filters.push(LogLineFilter::NotRegex(value));
                    }
                    _ => unreachable!(),
                }
            }
            s if s.contains('=') || s.contains('!') => {
                let (name, op, value) = parse_matcher(s)?;
                if request.parser.is_none() {
                    return Err(unsupported(
                        "parsed-field matchers require json or logfmt parser",
                    ));
                }
                request.parsed_filters.push(LabelMatcher {
                    name: sanitize_label_name(name),
                    op,
                    value,
                });
            }
            s => {
                let (op, raw) = if let Some(v) = s.strip_prefix("=") {
                    ("=", v)
                } else if let Some(v) = s.strip_prefix("!=") {
                    ("!=", v)
                } else if let Some(v) = s.strip_prefix("~") {
                    ("~", v)
                } else if let Some(v) = s.strip_prefix("!~") {
                    ("!~", v)
                } else {
                    return Err(unsupported(format!("LogQL pipeline stage '{s}'")));
                };
                let value = quoted(raw.trim())?;
                match op {
                    "=" => request.line_filters.push(LogLineFilter::Contains(value)),
                    "!=" => request.line_filters.push(LogLineFilter::NotContains(value)),
                    "~" => {
                        regex(&value)?;
                        request.line_filters.push(LogLineFilter::Regex(value));
                    }
                    "!~" => {
                        regex(&value)?;
                        request.line_filters.push(LogLineFilter::NotRegex(value));
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
    Ok(request)
}

fn split_selector(input: &str) -> Result<(&str, &str), CompatError> {
    if !input.starts_with('{') {
        return Err(bad("LogQL must start with a stream selector"));
    }
    let mut quote = false;
    let mut escaped = false;
    for (i, ch) in input.char_indices() {
        if quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quote = false;
            }
        } else if ch == '"' {
            quote = true;
        } else if ch == '}' {
            return Ok((
                &input[1..i],
                input[i + 1..]
                    .trim_start()
                    .trim_start_matches('|')
                    .trim_start(),
            ));
        }
    }
    Err(bad("unterminated stream selector"))
}

fn leading_function_name(input: &str) -> Option<&str> {
    let open = input.find('(')?;
    let name = input[..open].trim();
    if !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Some(name)
    } else {
        None
    }
}

fn split_pipeline(input: &str) -> Result<Vec<String>, CompatError> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut quote = None::<char>;
    let mut escaped = false;
    for (i, ch) in input.char_indices() {
        match quote {
            Some('"') if escaped => escaped = false,
            Some('"') if ch == '\\' => escaped = true,
            Some('"') if ch == '"' => quote = None,
            Some('`') if ch == '`' => quote = None,
            Some(_) => {}
            None if ch == '"' => quote = Some('"'),
            None if ch == '`' => quote = Some('`'),
            None if ch == '|' => {
                out.push(input[start..i].trim().to_string());
                start = i + 1;
            }
            None => {}
        }
    }
    if quote.is_some() {
        return Err(bad("unterminated pipeline string"));
    }
    out.push(input[start..].trim().to_string());
    Ok(out)
}

pub fn parse_selector(input: &str) -> Result<Vec<LabelMatcher>, CompatError> {
    let input = if input.trim_start().starts_with('{') {
        let (selector, pipeline) = split_selector(input.trim_start())?;
        if !pipeline.trim().is_empty() {
            return Err(bad("selector must not include a pipeline"));
        }
        selector
    } else {
        input
    };
    let mut out = Vec::new();
    for item in split_commas(input)? {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let (name, op, value) = parse_matcher(item)?;
        out.push(LabelMatcher {
            name: sanitize_label_name(name),
            op,
            value,
        });
    }
    Ok(out)
}

fn parse_matcher(input: &str) -> Result<(&str, MatcherOp, String), CompatError> {
    for (token, op) in [
        ("=~", MatcherOp::Re),
        ("!~", MatcherOp::Nre),
        ("!=", MatcherOp::Ne),
        ("=", MatcherOp::Eq),
    ] {
        if let Some((name, raw)) = input.split_once(token) {
            let name = name.trim();
            if !valid_name(name) {
                return Err(bad("invalid label matcher name"));
            }
            let value = quoted(raw.trim())?;
            if matches!(op, MatcherOp::Re | MatcherOp::Nre) {
                regex(&value)?;
            }
            return Ok((name, op, value));
        }
    }
    Err(bad("invalid label matcher"))
}

fn split_commas(input: &str) -> Result<Vec<&str>, CompatError> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut quote = false;
    let mut escaped = false;
    for (i, ch) in input.char_indices() {
        if quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quote = false;
            }
        } else if ch == '"' {
            quote = true;
        } else if ch == ',' {
            out.push(&input[start..i]);
            start = i + 1;
        }
    }
    if quote {
        return Err(bad("unterminated matcher string"));
    }
    out.push(&input[start..]);
    Ok(out)
}

fn quoted(raw: &str) -> Result<String, CompatError> {
    if raw.starts_with('`') {
        let Some(inner) = raw.strip_prefix('`') else {
            return Err(bad("unterminated backtick string"));
        };
        let Some(value) = inner.strip_suffix('`') else {
            return Err(bad("unterminated backtick string"));
        };
        if value.contains('`') {
            return Err(bad("backtick strings may not contain backticks"));
        }
        return Ok(value.to_string());
    }
    serde_json::from_str(raw).map_err(|_| bad("LogQL values must be quoted strings"))
}

fn regex(pattern: &str) -> Result<(), CompatError> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map(|_| ())
        .map_err(|e| bad(format!("invalid LogQL regex: {e}")))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

fn bad(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

fn unsupported(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::UnsupportedFeature, message)
}

/// Minimal instant-metric expression support for Grafana datasource health
/// checks, which probe Loki datasources with `vector(1) + vector(1)`.
/// Literal vectors/scalars combined with `+ - * /`; anything else is not a
/// metric expression and falls through to the log-query path.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricExpr {
    Number(f64),
    Binary(Box<MetricExpr>, char, Box<MetricExpr>),
}

impl MetricExpr {
    pub fn eval(&self) -> f64 {
        match self {
            MetricExpr::Number(value) => *value,
            MetricExpr::Binary(left, op, right) => {
                let (l, r) = (left.eval(), right.eval());
                match op {
                    '+' => l + r,
                    '-' => l - r,
                    '*' => l * r,
                    '/' => l / r,
                    _ => f64::NAN,
                }
            }
        }
    }
}

/// Returns `Ok(Some(expr))` for a pure metric expression, `Ok(None)` when the
/// input is not metric-shaped (caller continues with log parsing), and `Err`
/// for metric-shaped input that uses unsupported syntax.
pub fn parse_metric_expression(query: &str) -> Result<Option<MetricExpr>, CompatError> {
    let query = query.trim();
    if query.is_empty() || query.starts_with('{') || query.contains('|') {
        return Ok(None);
    }
    let tokens = tokenize_metric(query)?;
    let mut parser = MetricParser {
        tokens: &tokens,
        pos: 0,
    };
    match parser.parse_sum() {
        Ok(expr) if parser.pos == parser.tokens.len() => Ok(Some(expr)),
        _ => Err(unsupported("LogQL metric expression")),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum MetricToken {
    Number(f64),
    Vector,
    Op(char),
    Paren(char),
}

fn tokenize_metric(query: &str) -> Result<Vec<MetricToken>, CompatError> {
    let mut tokens = Vec::new();
    let bytes: Vec<char> = query.chars().collect();
    let mut index = 0;
    while index < bytes.len() {
        let ch = bytes[index];
        match ch {
            ' ' | '\t' => index += 1,
            '+' | '-' | '*' | '/' => {
                // Unary minus binds to the following number literal.
                if ch == '-'
                    && matches!(
                        tokens.last(),
                        None | Some(MetricToken::Op(_)) | Some(MetricToken::Paren('('))
                    )
                {
                    index += 1;
                    let start = index;
                    while index < bytes.len()
                        && (bytes[index].is_ascii_digit() || bytes[index] == '.')
                    {
                        index += 1;
                    }
                    if start == index {
                        return Err(unsupported("LogQL metric expression"));
                    }
                    tokens.push(MetricToken::Number(-parse_metric_number(
                        &bytes[start..index],
                    )?));
                } else {
                    tokens.push(MetricToken::Op(ch));
                    index += 1;
                }
            }
            '(' => {
                tokens.push(MetricToken::Paren('('));
                index += 1;
            }
            ')' => {
                tokens.push(MetricToken::Paren(')'));
                index += 1;
            }
            'v' if bytes[index..].starts_with(&['v', 'e', 'c', 't', 'o', 'r']) => {
                index += "vector".len();
                tokens.push(MetricToken::Vector);
            }
            _ if ch.is_ascii_digit() => {
                let start = index;
                while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == '.')
                {
                    index += 1;
                }
                tokens.push(MetricToken::Number(parse_metric_number(
                    &bytes[start..index],
                )?));
            }
            _ => return Err(unsupported("LogQL metric expression")),
        }
    }
    Ok(tokens)
}

fn parse_metric_number(chars: &[char]) -> Result<f64, CompatError> {
    chars
        .iter()
        .collect::<String>()
        .parse::<f64>()
        .map_err(|_| unsupported("LogQL metric expression"))
}

struct MetricParser<'a> {
    tokens: &'a [MetricToken],
    pos: usize,
}

impl<'a> MetricParser<'a> {
    fn peek(&self) -> Option<&MetricToken> {
        self.tokens.get(self.pos)
    }

    fn parse_sum(&mut self) -> Result<MetricExpr, CompatError> {
        let mut left = self.parse_product()?;
        loop {
            match self.peek() {
                Some(MetricToken::Op(op @ '+')) | Some(MetricToken::Op(op @ '-')) => {
                    let op = *op;
                    self.pos += 1;
                    let right = self.parse_product()?;
                    left = MetricExpr::Binary(Box::new(left), op, Box::new(right));
                }
                _ => return Ok(left),
            }
        }
    }

    fn parse_product(&mut self) -> Result<MetricExpr, CompatError> {
        let mut left = self.parse_atom()?;
        loop {
            match self.peek() {
                Some(MetricToken::Op(op @ '*')) | Some(MetricToken::Op(op @ '/')) => {
                    let op = *op;
                    self.pos += 1;
                    let right = self.parse_atom()?;
                    left = MetricExpr::Binary(Box::new(left), op, Box::new(right));
                }
                _ => return Ok(left),
            }
        }
    }

    fn parse_atom(&mut self) -> Result<MetricExpr, CompatError> {
        match self.peek() {
            Some(MetricToken::Number(value)) => {
                let value = *value;
                self.pos += 1;
                Ok(MetricExpr::Number(value))
            }
            Some(MetricToken::Vector) => {
                self.pos += 1;
                match (
                    self.peek(),
                    self.tokens.get(self.pos + 1),
                    self.tokens.get(self.pos + 2),
                ) {
                    (
                        Some(MetricToken::Paren('(')),
                        Some(MetricToken::Number(value)),
                        Some(MetricToken::Paren(')')),
                    ) => {
                        let value = *value;
                        self.pos += 3;
                        Ok(MetricExpr::Number(value))
                    }
                    _ => Err(unsupported(
                        "vector() requires a numeric literal in LogQL metric expressions",
                    )),
                }
            }
            Some(MetricToken::Paren('(')) => {
                self.pos += 1;
                let expr = self.parse_sum()?;
                match self.peek() {
                    Some(MetricToken::Paren(')')) => {
                        self.pos += 1;
                        Ok(expr)
                    }
                    _ => Err(unsupported("LogQL metric expression")),
                }
            }
            _ => Err(unsupported("LogQL metric expression")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_literal_vector_metric_expressions() {
        let expr = parse_metric_expression("vector(1) + vector(1)")
            .expect("metric expression")
            .expect("vector expression");
        assert_eq!(expr.eval(), 2.0);

        let expr = parse_metric_expression("vector(2.5) * 4 - vector(1)")
            .expect("metric expression")
            .expect("vector expression");
        assert_eq!(expr.eval(), 9.0);

        // Scalar literals and parentheses are part of the same minimal grammar.
        assert_eq!(
            parse_metric_expression("(3 - 1) / 2")
                .expect("metric expression")
                .expect("scalar expression")
                .eval(),
            1.0
        );
    }

    #[test]
    fn log_selectors_are_not_metric_expressions() {
        assert!(parse_metric_expression(r#"{job="api"} |= "x""#)
            .expect("fall through")
            .is_none());
        assert!(parse_metric_expression("").expect("empty").is_none());
    }

    #[test]
    fn unsupported_vector_forms_are_explicit_errors() {
        let err = parse_metric_expression("vector({job=\"api\"})")
            .expect_err("selector inside vector must not fall through");
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
        let err = parse_metric_expression("vector(sum(rate({job=\"api\"}[5m])))")
            .expect_err("nested functions stay unsupported");
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn parses_selector_filters_and_json_parser() {
        let request = parse_logql(
            r#"{service.name="api", level=~"warn|error"} |= "failed" | json | status="500""#,
        )
        .unwrap();
        assert_eq!(request.matchers[0].name, "service_name");
        assert_eq!(
            request.line_filters,
            vec![LogLineFilter::Contains("failed".into())]
        );
        assert_eq!(request.parser, Some(LogParser::Json));
        assert_eq!(request.parsed_filters[0].name, "status");
    }

    #[test]
    fn rejects_parsed_field_matchers_without_a_parser() {
        let err = parse_logql(r#"{service.name="api"} | status="500""#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
        assert_eq!(
            err.message,
            "parsed-field matchers require json or logfmt parser"
        );
    }

    #[test]
    fn rejects_unbounded_or_unknown_functions() {
        let err = parse_logql(r#"rate({job="api"}[5m])"#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
        let err = parse_logql(r#"{job="api"} | regexp "x""#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn rejects_unwrap_and_metric_functions_before_backend_execution() {
        for query in [
            r#"{job="api"} | json | unwrap duration_ms"#,
            r#"sum_over_time({job="api"}[5m])"#,
        ] {
            let err = parse_logql(query).unwrap_err();
            assert_eq!(err.code, CompatErrorCode::UnsupportedFeature, "{query}");
        }
    }

    #[test]
    fn parses_negative_regex_line_filters_as_line_filters() {
        let request = parse_logql(r#"{job="api"} !~ "debug""#).unwrap();
        assert_eq!(
            request.line_filters,
            vec![LogLineFilter::NotRegex("debug".into())]
        );
    }

    #[test]
    fn parses_backtick_line_filters_including_grafana_builder_empty_filter() {
        let request = parse_logql(r#"{service_name="currency"} |= ``"#).unwrap();
        assert_eq!(
            request.line_filters,
            vec![LogLineFilter::Contains(String::new())]
        );

        let request = parse_logql(r#"{job="api"} |= `failed|timeout`"#).unwrap();
        assert_eq!(
            request.line_filters,
            vec![LogLineFilter::Contains("failed|timeout".into())]
        );
    }
}
