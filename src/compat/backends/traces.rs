use crate::compat::errors::CompatError;
use crate::compat::tempo::traceql::{is_duration_field, parse_duration_ns, TraceSelector};
use crate::compat::tenant::TenantContext;
use async_trait::async_trait;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceAttribute {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEvent {
    pub name: String,
    pub timestamp_unix_nano: i64,
    pub attributes: Vec<TraceAttribute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: Option<String>,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: Option<i64>,
    pub attributes: Vec<TraceAttribute>,
    pub status_code: Option<String>,
    pub status_message: Option<String>,
    pub events: Vec<TraceEvent>,
    pub service_name: Option<String>,
    pub resource_attributes: Vec<TraceAttribute>,
    pub instrumentation_scope: Option<serde_json::Value>,
    pub links: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TraceData {
    pub spans: Vec<TraceSpan>,
}

pub(crate) const PERSISTED_OTLP_STATUS_CODES: [(&str, i64); 12] = [
    ("STATUS_CODE_UNSET", 0),
    ("STATUS_CODE_OK", 1),
    ("STATUS_CODE_ERROR", 2),
    ("UNSET", 0),
    ("OK", 1),
    ("ERROR", 2),
    ("unset", 0),
    ("ok", 1),
    ("error", 2),
    ("status_code_unset", 0),
    ("status_code_ok", 1),
    ("status_code_error", 2),
];

pub(crate) fn persisted_status_code_numeric_value(value: &str) -> Option<i64> {
    crate::compat::tempo::traceql::canonical_status_code(value)
}

#[derive(Debug, Clone, PartialEq)]
pub struct TraceSearchHit {
    pub trace_id: String,
    pub root_service_name: Option<String>,
    pub root_trace_name: Option<String>,
    pub start_time_unix_nano: i64,
    pub duration_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSearchRequest {
    pub tags: BTreeMap<String, String>,
    pub selector: Option<TraceSelector>,
    pub min_duration_ns: Option<i64>,
    pub max_duration_ns: Option<i64>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TraceLookupBounds {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
}

/// Build the bounded raw trace scan. Protocol adapters never construct SQL.
pub fn trace_scan_sql(request: &TraceSearchRequest, trace_id: Option<&str>) -> String {
    let mut where_clauses = vec!["1=1".to_string()];
    if let Some(trace_id) = trace_id {
        where_clauses.push(format!("trace_id = '{}'", escape(trace_id)));
    }
    if let Some(start) = request.start_ns {
        where_clauses.push(format!("epoch_ns(timestamp) >= {start}"));
    }
    if let Some(end) = request.end_ns {
        where_clauses.push(format!("epoch_ns(timestamp) < {end}"));
    }
    let tag_predicates = request
        .tags
        .iter()
        .map(|(key, value)| tag_predicate_sql(key, value))
        .collect::<Vec<_>>();
    let selector_predicate = request
        .selector
        .as_ref()
        .map(selector_sql)
        .unwrap_or_else(|| "TRUE".into());
    let row_predicate = tag_predicates
        .into_iter()
        .chain([selector_predicate])
        .map(|predicate| format!("({predicate})"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut duration_predicates = Vec::new();
    if let Some(min) = request.min_duration_ns {
        duration_predicates.push(format!(
            "MAX(COALESCE(end_time_unix_nano, start_time_unix_nano)) - MIN(start_time_unix_nano) >= {min}"
        ));
    }
    if let Some(max) = request.max_duration_ns {
        duration_predicates.push(format!(
            "MAX(COALESCE(end_time_unix_nano, start_time_unix_nano)) - MIN(start_time_unix_nano) <= {max}"
        ));
    }
    let duration_predicate = if duration_predicates.is_empty() {
        "TRUE".to_string()
    } else {
        duration_predicates.join(" AND ")
    };
    format!(
        "WITH base AS (SELECT trace_id, span_id, parent_span_id, message_type, span_kind, app_id, \
         CAST(epoch_ns(timestamp) AS BIGINT) AS start_time_unix_nano, \
         CAST(epoch_ns(end_timestamp) AS BIGINT) AS end_time_unix_nano, \
         CAST(attributes AS JSON) AS attributes, CAST(resource_attributes AS JSON) AS resource_attributes, \
         CAST(instrumentation_scope AS JSON) AS instrumentation_scope, CAST(links AS JSON) AS links, \
         status_code, status_message, \
         CAST(events AS JSON) AS events FROM union_spans WHERE {}), \
         matching_traces AS (SELECT DISTINCT trace_id FROM base WHERE {}), \
         qualified_traces AS (SELECT trace_id FROM base GROUP BY trace_id HAVING {} ) \
         SELECT base.* FROM base \
         INNER JOIN matching_traces USING (trace_id) \
         INNER JOIN qualified_traces USING (trace_id) \
         ORDER BY base.trace_id ASC, base.start_time_unix_nano ASC, base.span_id ASC LIMIT {}",
        where_clauses.join(" AND "),
        row_predicate,
        duration_predicate,
        trace_scan_cap(request.limit)
    )
}

pub fn trace_scan_cap(limit: usize) -> usize {
    limit.saturating_mul(100).clamp(10_000, 100_000)
}

/// A scan reaching the cap is incomplete/ambiguous because the bounded query
/// cannot distinguish exactly-cap rows from rows that continue beyond it.
pub fn scan_reached_cap(row_count: usize, cap: usize) -> bool {
    row_count >= cap
}

fn escape(value: &str) -> String {
    value.replace('\'', "''")
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", escape(value))
}

fn json_path(key: &str) -> String {
    format!("$.\"{}\"", key.replace('\\', "\\\\").replace('"', "\\\""))
}

fn json_string(column: &str, key: &str) -> String {
    format!(
        "json_extract_string(CAST({column} AS JSON), {})",
        sql_literal(&json_path(key))
    )
}

fn span_attribute_value(key: &str) -> String {
    format!(
        "COALESCE({}, {})",
        json_string("attributes", key),
        json_string("resource_attributes", key)
    )
}

fn span_status_code_sql() -> String {
    format!(
        "COALESCE({}, status_code)",
        json_string("attributes", "status_code")
    )
}

fn persisted_status_code_numeric_sql(value: &str) -> String {
    let mappings = PERSISTED_OTLP_STATUS_CODES
        .iter()
        .map(|(name, code)| format!("WHEN {} THEN {code}", sql_literal(name)))
        .collect::<Vec<_>>()
        .join(" ");
    format!("CASE {value} {mappings} ELSE TRY_CAST({value} AS BIGINT) END")
}

fn is_persisted_status_code_field(field: &crate::compat::tempo::traceql::TraceField) -> bool {
    crate::compat::tempo::traceql::is_status_field(field)
}

fn tag_value_sql(key: &str) -> String {
    match key {
        "name" => "message_type".to_string(),
        "kind" => "span_kind".to_string(),
        "status" => "status_code".to_string(),
        "service.name" => format!(
            "COALESCE({}, {}, app_id)",
            json_string("attributes", key),
            json_string("resource_attributes", key)
        ),
        _ => span_attribute_value(key),
    }
}

fn tag_predicate_sql(key: &str, expected: &str) -> String {
    let actual = tag_value_sql(key);
    format!(
        "strpos(lower(COALESCE(CAST(({actual}) AS VARCHAR), '')), lower({})) > 0",
        sql_literal(expected)
    )
}

fn selector_sql(selector: &crate::compat::tempo::traceql::TraceSelector) -> String {
    use crate::compat::tempo::traceql::TraceSelector;
    match selector {
        TraceSelector::Predicate(predicate) => predicate_sql(predicate),
        TraceSelector::And(left, right) => {
            format!("({} AND {})", selector_sql(left), selector_sql(right))
        }
        TraceSelector::Or(left, right) => {
            format!("({} OR {})", selector_sql(left), selector_sql(right))
        }
    }
}

fn predicate_sql(predicate: &crate::compat::tempo::traceql::TracePredicate) -> String {
    use crate::compat::tempo::traceql::{is_numeric_field, TraceField, TracePredicate};
    let (field, operator, expected) = match predicate {
        TracePredicate::Eq(field, expected) => (field, "=", expected),
        TracePredicate::NotEq(field, expected) => (field, "!=", expected),
        TracePredicate::Regex(field, expected) => (field, "regex", expected),
        TracePredicate::NotRegex(field, expected) => (field, "not_regex", expected),
        TracePredicate::Greater(field, expected) => (field, ">", expected),
        TracePredicate::GreaterOrEqual(field, expected) => (field, ">=", expected),
        TracePredicate::Less(field, expected) => (field, "<", expected),
        TracePredicate::LessOrEqual(field, expected) => (field, "<=", expected),
    };
    let actual = match field {
        TraceField::Span(key) if key == "status_code" || key == "status" => span_status_code_sql(),
        TraceField::Span(key) => json_string("attributes", key),
        TraceField::Resource(key) => {
            if key == "service.name" {
                format!(
                    "COALESCE({}, app_id)",
                    json_string("resource_attributes", key)
                )
            } else {
                json_string("resource_attributes", key)
            }
        }
        TraceField::Instrumentation(key) => json_string("instrumentation_scope", key),
        TraceField::Intrinsic(key) => match key.as_str() {
            "name" | "span:name" | "span.name" => "message_type".into(),
            "kind" | "span:kind" | "span.kind" => "span_kind".into(),
            "status" | "span:status" | "span.status" => span_status_code_sql(),
            "status_code" | "span:status_code" | "span.status_code" => span_status_code_sql(),
            "statusMessage" | "span:statusMessage" | "span.statusMessage" => {
                "status_message".into()
            }
            "duration" | "span:duration" | "span.duration" | "traceDuration" | "trace:duration"
            | "trace.duration" => {
                "COALESCE(end_time_unix_nano, start_time_unix_nano) - start_time_unix_nano".into()
            }
            _ => "NULL".into(),
        },
    };
    let is_status = is_persisted_status_code_field(field);
    let numeric = is_numeric_field(field);
    let actual_numeric = if is_status || numeric {
        persisted_status_code_numeric_sql(&actual)
    } else {
        actual.clone()
    };
    let missing_guard = format!("{actual} IS NOT NULL");
    match operator {
        "regex" => {
            if is_status {
                format!(
                    "{missing_guard} AND (regexp_matches(CAST({actual} AS VARCHAR), {}) OR regexp_matches(CASE {actual_numeric} WHEN 0 THEN 'unset' WHEN 1 THEN 'ok' WHEN 2 THEN 'error' ELSE '' END, {}))",
                    sql_literal(expected.as_str()),
                    sql_literal(expected.as_str())
                )
            } else {
                format!(
                    "{missing_guard} AND regexp_matches(CAST({actual} AS VARCHAR), {})",
                    sql_literal(expected.as_str())
                )
            }
        }
        "not_regex" => {
            if is_status {
                format!(
                    "NOT ({missing_guard} AND (regexp_matches(CAST({actual} AS VARCHAR), {}) OR regexp_matches(CASE {actual_numeric} WHEN 0 THEN 'unset' WHEN 1 THEN 'ok' WHEN 2 THEN 'error' ELSE '' END, {})))",
                    sql_literal(expected.as_str()),
                    sql_literal(expected.as_str())
                )
            } else {
                format!(
                    "NOT regexp_matches(COALESCE(CAST({actual} AS VARCHAR), ''), {})",
                    sql_literal(expected.as_str())
                )
            }
        }
        comparison => {
            if is_duration_field(field) {
                let right = parse_duration_ns(expected.as_str())
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".into());
                format!("{missing_guard} AND CAST(({actual}) AS BIGINT) {comparison} {right}")
            } else if is_status {
                let right = crate::compat::tempo::traceql::canonical_status_code(expected.as_str())
                    .map(|c| c.to_string())
                    .or_else(|| expected.as_str().parse::<i64>().ok().map(|c| c.to_string()))
                    .unwrap_or_else(|| "NULL".into());
                format!("{missing_guard} AND {actual_numeric} {comparison} {right}")
            } else if numeric {
                let right = expected
                    .as_str()
                    .parse::<i64>()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|_| "NULL".into());
                format!("{missing_guard} AND {actual_numeric} {comparison} {right}")
            } else {
                let right = sql_literal(expected.as_str());
                format!("{missing_guard} AND CAST({actual} AS VARCHAR) {comparison} {right}")
            }
        }
    }
}

#[async_trait]
pub trait TraceQueryBackend: Send + Sync {
    async fn get_trace(
        &self,
        ctx: &TenantContext,
        trace_id: &str,
        bounds: TraceLookupBounds,
    ) -> Result<Option<TraceData>, CompatError>;

    async fn search(
        &self,
        ctx: &TenantContext,
        request: TraceSearchRequest,
    ) -> Result<Vec<TraceSearchHit>, CompatError>;

    async fn search_tags(&self, ctx: &TenantContext) -> Result<Vec<String>, CompatError>;

    async fn search_tag_values(
        &self,
        ctx: &TenantContext,
        tag: &str,
    ) -> Result<Vec<String>, CompatError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnsupportedTraceBackend;

#[async_trait]
impl TraceQueryBackend for UnsupportedTraceBackend {
    async fn get_trace(
        &self,
        _ctx: &TenantContext,
        _trace_id: &str,
        _bounds: TraceLookupBounds,
    ) -> Result<Option<TraceData>, CompatError> {
        Err(CompatError::unsupported("trace_get"))
    }

    async fn search(
        &self,
        _ctx: &TenantContext,
        _request: TraceSearchRequest,
    ) -> Result<Vec<TraceSearchHit>, CompatError> {
        Err(CompatError::unsupported("trace_search"))
    }

    async fn search_tags(&self, _ctx: &TenantContext) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("trace_search_tags"))
    }

    async fn search_tag_values(
        &self,
        _ctx: &TenantContext,
        _tag: &str,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("trace_search_tag_values"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::tempo::params::parse_tempo_search_params;

    #[test]
    fn trace_scan_is_tenant_neutral_and_bounded() {
        let params = parse_tempo_search_params(
            &[("limit".into(), "5".into())],
            &crate::compat::tenant::QueryLimits::default(),
        )
        .unwrap();
        let sql = trace_scan_sql(
            &TraceSearchRequest {
                tags: params.tags,
                selector: params.selector,
                min_duration_ns: params.min_duration_ns,
                max_duration_ns: params.max_duration_ns,
                start_ns: params.start_ns,
                end_ns: params.end_ns,
                limit: params.limit,
            },
            Some("trace-1"),
        );
        assert!(sql.contains("FROM union_spans"));
        assert!(sql.contains("trace_id = 'trace-1'"));
        assert!(sql.contains("LIMIT 10000"));
        assert!(!sql.contains("tenant_id ="));
    }

    #[test]
    fn trace_scan_qualifies_supported_filters_before_result_limit() {
        let selector = crate::compat::tempo::traceql::parse_traceql(
            r#"{ instrumentation.name = "otel-rust" && resource.service.name = "api" }"#,
        )
        .unwrap();
        let sql = trace_scan_sql(
            &TraceSearchRequest {
                tags: BTreeMap::from([(
                    String::from("deployment.environment"),
                    String::from("prod"),
                )]),
                selector: Some(selector),
                min_duration_ns: Some(1_000_000),
                max_duration_ns: Some(2_000_000),
                start_ns: None,
                end_ns: None,
                limit: 5,
            },
            None,
        );
        assert!(sql.contains("matching_traces AS"));
        assert!(sql.contains("qualified_traces AS"));
        assert!(sql.contains("instrumentation_scope"));
        assert!(sql.contains("deployment.environment"));
        assert!(sql.contains("HAVING"));
        assert!(sql.contains("LIMIT 10000"));
        assert!(sql.find("matching_traces AS").unwrap() < sql.find("LIMIT 10000").unwrap());
    }

    #[test]
    fn a_scan_reaching_the_cap_is_explicitly_incomplete() {
        assert!(scan_reached_cap(10_000, trace_scan_cap(5)));
        assert!(scan_reached_cap(10_001, trace_scan_cap(5)));
        assert!(!scan_reached_cap(9_999, trace_scan_cap(5)));
    }

    #[test]
    fn duration_predicates_compare_numbers_without_varchar_coercion() {
        let selector =
            crate::compat::tempo::traceql::parse_traceql(r#"{ duration >= 1ms }"#).unwrap();
        let sql = trace_scan_sql(
            &TraceSearchRequest {
                selector: Some(selector),
                tags: BTreeMap::new(),
                min_duration_ns: None,
                max_duration_ns: None,
                start_ns: None,
                end_ns: None,
                limit: 1,
            },
            None,
        );
        assert!(sql.contains("CAST((COALESCE(end_time_unix_nano, start_time_unix_nano) - start_time_unix_nano) AS BIGINT) >= 1000000"));
        assert!(!sql.contains("CAST(COALESCE(end_time_unix_nano, start_time_unix_nano) - start_time_unix_nano AS VARCHAR)"));
    }

    #[test]
    fn numeric_span_predicates_compare_numbers_without_varchar_coercion() {
        let selector =
            crate::compat::tempo::traceql::parse_traceql(r#"{ span.http.status_code >= 500 }"#)
                .unwrap();
        let sql = trace_scan_sql(
            &TraceSearchRequest {
                selector: Some(selector),
                tags: BTreeMap::new(),
                min_duration_ns: None,
                max_duration_ns: None,
                start_ns: None,
                end_ns: None,
                limit: 1,
            },
            None,
        );
        assert!(sql.contains("TRY_CAST(json_extract_string"));
        assert!(sql.contains(">= 500"));
        assert!(!sql.contains("AS VARCHAR) >= 500"));
    }

    #[test]
    fn persisted_span_status_code_predicates_use_the_post_filter_source() {
        let selector =
            crate::compat::tempo::traceql::parse_traceql(r#"{ span.status_code >= 2 }"#).unwrap();
        let sql = trace_scan_sql(
            &TraceSearchRequest {
                selector: Some(selector),
                tags: BTreeMap::new(),
                min_duration_ns: None,
                max_duration_ns: None,
                start_ns: None,
                end_ns: None,
                limit: 1,
            },
            None,
        );

        assert!(sql.contains("WHEN 'STATUS_CODE_ERROR' THEN 2"));
        assert!(sql.contains("WHEN 'error' THEN 2"));
        assert!(sql.contains(">= 2"));
    }
}
