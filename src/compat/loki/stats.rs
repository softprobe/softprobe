use super::logql::parse_logql;
use crate::compat::backends::logs::{LogDirection, LogsQueryRequest};
use crate::compat::errors::{CompatError, CompatErrorCode};

pub fn require_stream_selector(request: &LogsQueryRequest) -> Result<(), CompatError> {
    if !request.line_filters.is_empty()
        || request.parser.is_some()
        || !request.parsed_filters.is_empty()
        || request.unwrap.is_some()
    {
        return Err(CompatError::new(
            CompatErrorCode::BadRequest,
            "index stats requires a stream selector",
        ));
    }
    Ok(())
}

pub fn parse_stats_query(query: &str) -> Result<LogsQueryRequest, CompatError> {
    let request = parse_logql(query)?;
    require_stream_selector(&request)?;
    Ok(request)
}

pub fn stats_query_request(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_pipeline_queries_for_index_stats() {
        let err = parse_stats_query(r#"{job="api"} |= "x""#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::BadRequest);
    }

    #[test]
    fn accepts_stream_selector_for_index_stats() {
        parse_stats_query(r#"{service_name="ad"}"#).expect("selector");
    }
}
