use crate::compat::errors::CompatError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceSelector {
    Predicate(TracePredicate),
    And(Box<TraceSelector>, Box<TraceSelector>),
    Or(Box<TraceSelector>, Box<TraceSelector>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceField {
    Span(String),
    Resource(String),
    Instrumentation(String),
    Intrinsic(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceValue {
    String(String),
    Number(String),
}

impl TraceValue {
    pub fn as_str(&self) -> &str {
        match self {
            Self::String(value) | Self::Number(value) => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TracePredicate {
    Eq(TraceField, TraceValue),
    NotEq(TraceField, TraceValue),
    Regex(TraceField, TraceValue),
    NotRegex(TraceField, TraceValue),
    Greater(TraceField, TraceValue),
    GreaterOrEqual(TraceField, TraceValue),
    Less(TraceField, TraceValue),
    LessOrEqual(TraceField, TraceValue),
}

pub fn parse_traceql(query: &str) -> Result<TraceSelector, CompatError> {
    let query = query.trim();
    if query.is_empty() {
        return Err(bad("TraceQL query must not be empty"));
    }
    if query.contains(">>") || query.contains(" | ") || query.contains("by(") {
        return Err(unsupported(
            "TraceQL pipelines and aggregations are unsupported",
        ));
    }
    let mut parser = Parser::new(query);
    let selector = parser.parse_selector()?;
    if !parser.is_eof() {
        return Err(unsupported(
            "TraceQL expression outside a selector is unsupported",
        ));
    }
    Ok(selector)
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn is_eof(&mut self) -> bool {
        self.skip_ws();
        self.pos >= self.input.len()
    }

    fn parse_selector(&mut self) -> Result<TraceSelector, CompatError> {
        self.skip_ws();
        if !self.take(b'{') {
            return Err(bad("TraceQL selector must start with '{'"));
        }
        let expression = self.parse_or()?;
        self.skip_ws();
        if !self.take(b'}') {
            return Err(bad("TraceQL selector is missing '}'"));
        }
        Ok(expression)
    }

    fn parse_or(&mut self) -> Result<TraceSelector, CompatError> {
        let mut left = self.parse_and()?;
        loop {
            self.skip_ws();
            if !self.take_bytes(b"||") {
                break;
            }
            left = TraceSelector::Or(Box::new(left), Box::new(self.parse_and()?));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<TraceSelector, CompatError> {
        let mut left = self.parse_primary()?;
        loop {
            self.skip_ws();
            if !self.take_bytes(b"&&") {
                break;
            }
            left = TraceSelector::And(Box::new(left), Box::new(self.parse_primary()?));
        }
        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<TraceSelector, CompatError> {
        self.skip_ws();
        if self.take(b'(') {
            let expression = self.parse_or()?;
            self.skip_ws();
            if !self.take(b')') {
                return Err(bad("TraceQL expression is missing ')'"));
            }
            return Ok(expression);
        }
        let field = self.parse_field()?;
        self.skip_ws();
        let op = self.parse_operator()?;
        self.skip_ws();
        let value = predicate_value(&field, self.parse_value()?)?;
        let predicate = match op.as_str() {
            "=" | "==" => TracePredicate::Eq(field, value),
            "!=" => TracePredicate::NotEq(field, value),
            "=~" => {
                regex::Regex::new(value.as_str()).map_err(|_| bad("invalid TraceQL regex"))?;
                TracePredicate::Regex(field, value)
            }
            "!~" => {
                regex::Regex::new(value.as_str()).map_err(|_| bad("invalid TraceQL regex"))?;
                TracePredicate::NotRegex(field, value)
            }
            ">" => TracePredicate::Greater(field, value),
            ">=" => TracePredicate::GreaterOrEqual(field, value),
            "<" => TracePredicate::Less(field, value),
            "<=" => TracePredicate::LessOrEqual(field, value),
            _ => {
                return Err(unsupported(format!(
                    "TraceQL operator '{op}' is unsupported"
                )))
            }
        };
        validate_predicate_types(&predicate)?;
        Ok(TraceSelector::Predicate(predicate))
    }

    fn parse_field(&mut self) -> Result<TraceField, CompatError> {
        let raw = self.parse_word()?;
        if raw.is_empty() {
            return Err(bad("TraceQL field is required"));
        }
        if let Some(field) = raw.strip_prefix("span.") {
            return Ok(TraceField::Span(field.to_string()));
        }
        if let Some(field) = raw.strip_prefix("resource.") {
            return Ok(TraceField::Resource(field.to_string()));
        }
        if let Some(field) = raw
            .strip_prefix("instrumentation.")
            .or_else(|| raw.strip_prefix("instrumentation:"))
        {
            if matches!(field, "name" | "version") {
                return Ok(TraceField::Instrumentation(field.to_string()));
            }
            return Err(unsupported(format!(
                "TraceQL instrumentation field '{raw}' is unsupported"
            )));
        }
        if raw.starts_with("event.") || raw.starts_with("link.") {
            return Err(unsupported(format!("TraceQL field '{raw}' is unsupported")));
        }
        if matches!(
            raw.as_str(),
            "name"
                | "span:name"
                | "span.name"
                | "kind"
                | "span:kind"
                | "span.kind"
                | "status"
                | "span:status"
                | "span.status"
                | "status_code"
                | "span:status_code"
                | "span.status_code"
                | "statusMessage"
                | "span:statusMessage"
                | "span.statusMessage"
                | "duration"
                | "span:duration"
                | "span.duration"
                | "traceDuration"
                | "trace:duration"
                | "trace.duration"
        ) {
            Ok(TraceField::Intrinsic(raw))
        } else {
            Err(unsupported(format!(
                "TraceQL intrinsic '{raw}' is unsupported"
            )))
        }
    }

    fn parse_operator(&mut self) -> Result<String, CompatError> {
        for op in [
            &b">="[..],
            &b"<="[..],
            &b"=~"[..],
            &b"!~"[..],
            &b"!="[..],
            &b"=="[..],
            &b"="[..],
            &b">"[..],
            &b"<"[..],
        ] {
            if self.take_bytes(op) {
                return Ok(String::from_utf8_lossy(op).into_owned());
            }
        }
        Err(bad("TraceQL comparison operator is required"))
    }

    fn parse_value(&mut self) -> Result<ParsedValue, CompatError> {
        if self.take(b'"') {
            // Accumulate bytes and decode once so multi-byte UTF-8 literals
            // (e.g. `{ span.customer = "José" }`) survive intact.
            let mut value: Vec<u8> = Vec::new();
            while self.pos < self.input.len() {
                let ch = self.input[self.pos];
                self.pos += 1;
                match ch {
                    b'"' => {
                        return Ok(ParsedValue {
                            value: String::from_utf8_lossy(&value).into_owned(),
                            quoted: true,
                        })
                    }
                    b'\\' if self.pos < self.input.len() => {
                        value.push(self.input[self.pos]);
                        self.pos += 1;
                    }
                    _ => value.push(ch),
                }
            }
            return Err(bad("unterminated TraceQL string"));
        }
        Ok(ParsedValue {
            value: self.parse_word()?,
            quoted: false,
        })
    }

    fn parse_word(&mut self) -> Result<String, CompatError> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.input.len()
            && !self.input[self.pos].is_ascii_whitespace()
            && !b"{}()&|=~!<>".contains(&self.input[self.pos])
        {
            self.pos += 1;
        }
        if self.pos == start {
            Err(bad("TraceQL value is required"))
        } else {
            Ok(String::from_utf8_lossy(&self.input[start..self.pos]).into_owned())
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn take(&mut self, byte: u8) -> bool {
        if self.input.get(self.pos) == Some(&byte) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn take_bytes(&mut self, bytes: &[u8]) -> bool {
        if self.input.get(self.pos..self.pos + bytes.len()) == Some(bytes) {
            self.pos += bytes.len();
            true
        } else {
            false
        }
    }
}

struct ParsedValue {
    value: String,
    quoted: bool,
}

fn predicate_value(field: &TraceField, parsed: ParsedValue) -> Result<TraceValue, CompatError> {
    if is_duration_field(field) {
        if parsed.quoted {
            return Err(bad(
                "duration predicates require an unquoted duration value",
            ));
        }
        if parse_duration_ns(&parsed.value).is_none() {
            return Err(bad(
                "duration predicates require a numeric duration with a supported unit",
            ));
        }
        Ok(TraceValue::Number(parsed.value))
    } else if is_numeric_field(field) {
        if parsed.quoted {
            return Err(bad(
                "numeric TraceQL predicates require an unquoted numeric value",
            ));
        }
        if parsed.value.parse::<i64>().is_err() {
            return Err(bad(
                "numeric TraceQL predicates require an unquoted integer value",
            ));
        }
        Ok(TraceValue::Number(parsed.value))
    } else if is_status_field(field) {
        if parsed.value.parse::<i64>().is_ok() {
            Ok(TraceValue::Number(parsed.value))
        } else {
            Ok(TraceValue::String(parsed.value))
        }
    } else {
        Ok(TraceValue::String(parsed.value))
    }
}

fn bad(message: impl Into<String>) -> CompatError {
    CompatError::new(crate::compat::errors::CompatErrorCode::BadRequest, message)
}

fn unsupported(message: impl Into<String>) -> CompatError {
    CompatError::new(
        crate::compat::errors::CompatErrorCode::UnsupportedFeature,
        message,
    )
}

fn validate_predicate_types(predicate: &TracePredicate) -> Result<(), CompatError> {
    let (field, expected, is_regex) = match predicate {
        TracePredicate::Eq(field, expected)
        | TracePredicate::NotEq(field, expected)
        | TracePredicate::Greater(field, expected)
        | TracePredicate::GreaterOrEqual(field, expected)
        | TracePredicate::Less(field, expected)
        | TracePredicate::LessOrEqual(field, expected) => (field, expected, false),
        TracePredicate::Regex(field, expected) | TracePredicate::NotRegex(field, expected) => {
            (field, expected, true)
        }
    };
    if is_duration_field(field) && !matches!(expected, TraceValue::Number(_)) {
        return Err(bad(
            "duration predicates require a numeric duration with a supported unit",
        ));
    }
    if is_numeric_field(field)
        && (is_regex
            || !matches!(expected, TraceValue::Number(_))
            || expected.as_str().parse::<i64>().is_err())
    {
        return Err(bad("numeric predicates require an unquoted integer value"));
    }
    Ok(())
}

pub fn is_status_field(field: &TraceField) -> bool {
    match field {
        TraceField::Span(key) => matches!(key.as_str(), "status" | "status_code"),
        TraceField::Intrinsic(key) => matches!(
            key.as_str(),
            "status"
                | "span:status"
                | "span.status"
                | "status_code"
                | "span:status_code"
                | "span.status_code"
        ),
        TraceField::Resource(_) | TraceField::Instrumentation(_) => false,
    }
}

pub fn canonical_status_code(value: &str) -> Option<i64> {
    let trimmed = value.trim();
    if let Ok(num) = trimmed.parse::<i64>() {
        return Some(num);
    }
    match trimmed.to_ascii_uppercase().as_str() {
        "UNSET" | "STATUS_CODE_UNSET" => Some(0),
        "OK" | "STATUS_CODE_OK" => Some(1),
        "ERROR" | "STATUS_CODE_ERROR" => Some(2),
        _ => None,
    }
}

pub fn is_numeric_field(field: &TraceField) -> bool {
    match field {
        TraceField::Span(key) => matches!(
            key.as_str(),
            "http.status_code" | "http.response.status_code"
        ),
        TraceField::Intrinsic(_) | TraceField::Resource(_) | TraceField::Instrumentation(_) => {
            false
        }
    }
}

pub fn is_duration_field(field: &TraceField) -> bool {
    matches!(
        field,
        TraceField::Intrinsic(key)
            if matches!(
                key.as_str(),
                "duration"
                    | "span:duration"
                    | "span.duration"
                    | "traceDuration"
                    | "trace:duration"
                    | "trace.duration"
            )
    )
}

pub fn parse_duration_ns(value: &str) -> Option<i64> {
    let value = value.trim();
    let (number, multiplier) = [
        ("ns", 1i128),
        ("us", 1_000),
        ("µs", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60_000_000_000),
        ("h", 3_600_000_000_000),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| value.strip_suffix(suffix).map(|n| (n, multiplier)))?;
    let float_val = number.parse::<f64>().ok()?;
    if !float_val.is_finite() || float_val <= 0.0 {
        return None;
    }
    let ns = (float_val * multiplier as f64).round();
    if ns > i64::MAX as f64 {
        return None;
    }
    Some(ns as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::errors::CompatErrorCode;

    #[test]
    fn parses_selector_boolean_equality_and_regex() {
        let selector = parse_traceql(
            r#"{ resource.service.name = "api" && (span.http.method = "GET" || name =~ "^/v1/") }"#,
        )
        .expect("selector");
        assert!(matches!(selector, TraceSelector::And(_, _)));
    }

    #[test]
    fn maps_malformed_traceql_to_bad_request() {
        let err = parse_traceql("{ span.foo = }").unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }

    #[test]
    fn maps_traceql_pipeline_to_unsupported() {
        let err = parse_traceql(r#"{ name = "x" } >> { name = "y" }"#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn parses_supported_instrumentation_scope_predicates() {
        let selector = parse_traceql(r#"{ instrumentation.name = "otel-rust" }"#)
            .expect("instrumentation scope selector");
        assert!(matches!(
            selector,
            TraceSelector::Predicate(TracePredicate::Eq(
                TraceField::Instrumentation(ref field),
                ref value
            )) if field == "name" && value.as_str() == "otel-rust"
        ));

        let selector = parse_traceql(r#"{ instrumentation:version = "1.0" }"#)
            .expect("Tempo instrumentation intrinsic");
        assert!(matches!(
            selector,
            TraceSelector::Predicate(TracePredicate::Eq(
                TraceField::Instrumentation(ref field),
                ref value
            )) if field == "version" && value.as_str() == "1.0"
        ));
    }

    #[test]
    fn rejects_unsupported_instrumentation_scope_attributes_explicitly() {
        let err = parse_traceql(r#"{ instrumentation.library = "otel-rust" }"#)
            .expect_err("unsupported scope attribute");
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn rejects_unknown_intrinsics_explicitly() {
        let err = parse_traceql(r#"{ rootName = "checkout" }"#).expect_err("unsupported intrinsic");
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn rejects_mixed_duration_types_before_sql_generation() {
        let err = parse_traceql(r#"{ duration > "slow" }"#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
        let err = parse_traceql(r#"{ duration >= 1500xyz }"#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
        parse_traceql(r#"{ duration >= 1500us }"#).expect("typed duration");
        parse_traceql(r#"{ duration >= 1.5ms }"#).expect("fractional duration");
        parse_traceql(r#"{ traceDuration < 2.5s }"#).expect("trace duration");
    }

    #[test]
    fn parses_status_predicates_unquoted_and_quoted() {
        parse_traceql(r#"{ status = error }"#).expect("unquoted error status");
        parse_traceql(r#"{ status = "error" }"#).expect("quoted error status");
        parse_traceql(r#"{ status = ok }"#).expect("unquoted ok status");
        parse_traceql(r#"{ status != unset }"#).expect("unquoted unset status");
        parse_traceql(r#"{ span:status = error }"#).expect("span:status");
        parse_traceql(r#"{ span.status = error }"#).expect("span.status");
        parse_traceql(r#"{ status_code >= 2 }"#).expect("numeric status code");
        parse_traceql(r#"{ span.status_code >= 500 }"#).expect("span status code");
    }

    #[test]
    fn rejects_non_numeric_values_for_numeric_span_fields() {
        let err = parse_traceql(r#"{ span.http.status_code >= "slow" }"#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
        parse_traceql(r#"{ span.http.status_code >= 500 }"#).expect("numeric status code");
    }

    #[test]
    fn rejects_fractional_numeric_predicates() {
        let err = parse_traceql(r#"{ span.http.status_code >= 500.5 }"#)
            .expect_err("fractional numeric predicate");
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }
}
