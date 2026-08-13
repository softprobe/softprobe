//! Parse PromQL with promql-parser and reject unsupported AST nodes.

use crate::compat::backends::metrics::{LabelMatcher, MatcherOp};
use crate::compat::errors::{CompatError, CompatErrorCode};
use promql_parser::label::{MatchOp, Matcher, METRIC_NAME};
use promql_parser::parser::token::{
    T_ADD, T_AVG, T_BOTTOMK, T_COUNT, T_DIV, T_EQLC, T_GTE, T_GTR, T_LAND, T_LOR, T_LSS, T_LTE,
    T_LUNLESS, T_MAX, T_MIN, T_MOD, T_MUL, T_NEQ, T_POW, T_SUB, T_SUM, T_TOPK,
};
use promql_parser::parser::{
    self, AggregateExpr, BinaryExpr, Call, Expr, MatrixSelector, VectorMatchCardinality,
    VectorSelector,
};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSelector {
    pub matchers: Vec<LabelMatcher>,
    pub range: Option<Duration>,
}

/// Parse a `match[]` vector selector string (e.g. `{job="api"}` or `up{job="a"}`).
pub fn parse_match_selector(input: &str) -> Result<ParsedSelector, CompatError> {
    let expr = parser::parse(input).map_err(|e| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("invalid match[] selector: {e}"),
        )
    })?;
    match expr {
        Expr::VectorSelector(vs) => {
            if vs.offset.is_some() {
                return Err(CompatError::unsupported(
                    "promql: offset is not allowed in match[]",
                ));
            }
            lower_vector_selector(&vs)
        }
        other => Err(CompatError::unsupported(format!(
            "match[] must be a vector selector, got {}",
            expr_kind(&other)
        ))),
    }
}

/// Parse a full PromQL expression and validate the supported subset.
pub fn parse_promql(input: &str) -> Result<Expr, CompatError> {
    let expr = parser::parse(input).map_err(|e| {
        CompatError::new(CompatErrorCode::BadRequest, format!("invalid PromQL: {e}"))
    })?;
    validate_supported(&expr)?;
    Ok(expr)
}

fn validate_supported(expr: &Expr) -> Result<(), CompatError> {
    match expr {
        Expr::VectorSelector(vs) => {
            if vs.at.is_some() {
                return Err(CompatError::unsupported("promql: @ modifier"));
            }
            // offset is supported in Phase 1 common-Grafana pack.
            if !vs.matchers.or_matchers.is_empty() {
                return Err(CompatError::unsupported("promql: OR matchers"));
            }
            Ok(())
        }
        Expr::MatrixSelector(ms) => {
            validate_supported(&Expr::VectorSelector(ms.vs.clone()))?;
            Ok(())
        }
        Expr::NumberLiteral(_) => Ok(()),
        Expr::StringLiteral(_) => Err(CompatError::unsupported("promql: string literal")),
        Expr::Unary(u) => validate_supported(&u.expr),
        Expr::Paren(p) => validate_supported(&p.expr),
        Expr::Binary(b) => {
            validate_supported(&b.lhs)?;
            validate_supported(&b.rhs)?;
            validate_binary_op(b)?;
            Ok(())
        }
        Expr::Aggregate(a) => {
            validate_aggregate(a)?;
            validate_supported(&a.expr)?;
            if let Some(param) = &a.param {
                validate_supported(param)?;
            }
            Ok(())
        }
        Expr::Call(c) => {
            validate_call(c)?;
            for arg in &c.args.args {
                validate_supported(arg)?;
            }
            Ok(())
        }
        Expr::Subquery(_) => Err(CompatError::unsupported("promql: subquery")),
        Expr::Extension(_) => Err(CompatError::unsupported("promql: extension")),
    }
}

fn validate_binary_op(b: &BinaryExpr) -> Result<(), CompatError> {
    let id = b.op.id();
    let arithmetic = matches!(
        id,
        x if x == T_ADD || x == T_SUB || x == T_MUL || x == T_DIV || x == T_MOD || x == T_POW
    );
    let comparison = matches!(
        id,
        x if x == T_EQLC || x == T_NEQ || x == T_GTR || x == T_GTE || x == T_LSS || x == T_LTE
    );
    let set_ops = matches!(id, x if x == T_LAND || x == T_LOR || x == T_LUNLESS);
    if !(arithmetic || comparison || set_ops) {
        return Err(CompatError::unsupported(format!(
            "promql: binary operator {}",
            b.op
        )));
    }
    if let Some(m) = &b.modifier {
        if set_ops {
            // Set ops use many-to-many matching; still reject on()/ignoring()/group_*.
            if m.matching.is_some() {
                return Err(CompatError::unsupported("promql: on()/ignoring() matching"));
            }
            if !matches!(
                m.card,
                VectorMatchCardinality::OneToOne | VectorMatchCardinality::ManyToMany
            ) {
                return Err(CompatError::unsupported("promql: group_left/group_right"));
            }
        } else {
            if !matches!(m.card, VectorMatchCardinality::OneToOne) {
                return Err(CompatError::unsupported("promql: group_left/group_right"));
            }
            if m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some() {
                return Err(CompatError::unsupported("promql: fill modifiers"));
            }
            if m.matching.is_some() {
                return Err(CompatError::unsupported("promql: on()/ignoring() matching"));
            }
        }
    }
    Ok(())
}

fn validate_aggregate(a: &AggregateExpr) -> Result<(), CompatError> {
    let id = a.op.id();
    if id == T_SUM || id == T_MIN || id == T_MAX || id == T_AVG || id == T_COUNT {
        return Ok(());
    }
    if id == T_TOPK || id == T_BOTTOMK {
        if a.param.is_none() {
            return Err(CompatError::new(
                CompatErrorCode::BadRequest,
                format!("{}() requires a parameter", a.op),
            ));
        }
        return Ok(());
    }
    Err(CompatError::unsupported(format!(
        "promql: aggregation {}",
        a.op
    )))
}

fn unwrap_parens(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(p) => unwrap_parens(&p.expr),
        other => other,
    }
}

fn validate_call(c: &Call) -> Result<(), CompatError> {
    let name = c.func.name.to_ascii_lowercase();
    if super::funcs::is_range_vector_fn(&name) {
        if c.args.args.len() != 1 {
            return Err(CompatError::new(
                CompatErrorCode::BadRequest,
                format!("{name}() expects 1 argument"),
            ));
        }
        return match unwrap_parens(c.args.args[0].as_ref()) {
            Expr::MatrixSelector(_) => Ok(()),
            _ => Err(CompatError::new(
                CompatErrorCode::BadRequest,
                format!("{name}() requires a range vector"),
            )),
        };
    }
    if super::funcs::is_math_fn(&name) {
        if name == "round" {
            if !(1..=2).contains(&c.args.args.len()) {
                return Err(CompatError::new(
                    CompatErrorCode::BadRequest,
                    "round() expects 1 or 2 arguments",
                ));
            }
        } else if c.args.args.len() != 1 {
            return Err(CompatError::new(
                CompatErrorCode::BadRequest,
                format!("{name}() expects 1 argument"),
            ));
        }
        return Ok(());
    }
    Err(CompatError::unsupported(format!("promql: function {name}")))
}

fn lower_vector_selector(vs: &VectorSelector) -> Result<ParsedSelector, CompatError> {
    if vs.at.is_some() {
        return Err(CompatError::unsupported("promql: @ modifier"));
    }
    // Discovery match[] must not carry offset (time-shifted series match is query-only).
    // Full PromQL allows offset via validate_supported + eval.
    let mut matchers = Vec::new();
    if let Some(name) = &vs.name {
        matchers.push(LabelMatcher {
            name: "__name__".into(),
            op: MatcherOp::Eq,
            value: name.clone(),
        });
    }
    for m in &vs.matchers.matchers {
        matchers.push(convert_matcher(m)?);
    }
    for group in &vs.matchers.or_matchers {
        if !group.is_empty() {
            return Err(CompatError::unsupported(
                "promql: or-matchers inside a single selector",
            ));
        }
    }
    Ok(ParsedSelector {
        matchers,
        range: None,
    })
}

pub(crate) fn convert_matcher(m: &Matcher) -> Result<LabelMatcher, CompatError> {
    let name = if m.name == METRIC_NAME {
        "__name__".to_string()
    } else {
        m.name.clone()
    };
    let op = match &m.op {
        MatchOp::Equal => MatcherOp::Eq,
        MatchOp::NotEqual => MatcherOp::Ne,
        MatchOp::Re(_) => MatcherOp::Re,
        MatchOp::NotRe(_) => MatcherOp::Nre,
    };
    Ok(LabelMatcher {
        name,
        op,
        value: m.value.clone(),
    })
}

pub(crate) fn expr_kind(expr: &Expr) -> &'static str {
    match expr {
        Expr::Aggregate(_) => "aggregate",
        Expr::Unary(_) => "unary",
        Expr::Binary(_) => "binary",
        Expr::Paren(_) => "paren",
        Expr::Subquery(_) => "subquery",
        Expr::NumberLiteral(_) => "number",
        Expr::StringLiteral(_) => "string",
        Expr::VectorSelector(_) => "vector_selector",
        Expr::MatrixSelector(_) => "matrix_selector",
        Expr::Call(_) => "call",
        Expr::Extension(_) => "extension",
    }
}

pub(crate) fn extract_selector_matchers(
    vs: &VectorSelector,
) -> Result<Vec<LabelMatcher>, CompatError> {
    Ok(lower_vector_selector(vs)?.matchers)
}

pub(crate) fn matrix_range(ms: &MatrixSelector) -> Duration {
    ms.range
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_match_selector_with_name_and_labels() {
        let sel = parse_match_selector(r#"http_requests{job="api",code=~"5.."}"#).unwrap();
        assert!(sel
            .matchers
            .iter()
            .any(|m| m.name == "__name__" && m.value == "http_requests" && m.op == MatcherOp::Eq));
        assert!(sel
            .matchers
            .iter()
            .any(|m| m.name == "job" && m.value == "api" && m.op == MatcherOp::Eq));
        assert!(sel
            .matchers
            .iter()
            .any(|m| m.name == "code" && m.op == MatcherOp::Re));
    }

    #[test]
    fn rejects_garbage_selector() {
        let err = parse_match_selector("{").unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }

    #[test]
    fn rejects_subquery() {
        let err = parse_promql("up[5m:1m]").unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn accepts_rate_and_sum() {
        parse_promql(r#"sum by (job) (rate(http_requests[5m]))"#).unwrap();
    }

    #[test]
    fn rejects_histogram_quantile() {
        let err = parse_promql(r#"histogram_quantile(0.9, x)"#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn accepts_offset_on_query() {
        parse_promql("up offset 5m").unwrap();
        parse_promql(r#"sum_over_time(up[5m])"#).unwrap();
        parse_promql(r#"http_requests and up"#).unwrap();
        parse_promql(r#"topk(3, up)"#).unwrap();
        parse_promql(r#"abs(up)"#).unwrap();
    }

    #[test]
    fn accepts_parenthesized_range_vector_arg() {
        parse_promql("sum_over_time((http_requests[5m]))").unwrap();
        parse_promql("rate((up[1m]))").unwrap();
    }

    #[test]
    fn rejects_offset_on_match_selector() {
        let err = parse_match_selector("up offset 5m").unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn rejects_on_matching_modifier() {
        let err = parse_promql(r#"a + on(job) b"#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }
}
