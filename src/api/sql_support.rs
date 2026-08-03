//! Shared DuckDB SQL-compilation helpers for query modules (e.g. `api/llm/query.rs`).
//!
//! Extracted from `api/llm/query.rs` so literal-escaping, cursors, and time-bound clauses
//! are not copy-pasted across query recipes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Opaque keyset-pagination cursor: `(timestamp, tiebreaker id)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PageCursor {
    pub t: DateTime<Utc>,
    pub id: String,
}

pub(crate) fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Microseconds, not milliseconds: DuckDB TIMESTAMPTZ is microsecond-precision
/// and cursors carry a real column value round-trip. Rendering at millisecond
/// precision truncates the literal *below* the true value, so a keyset
/// predicate built from it (`start_time < cursor`) silently drops every row in
/// the same millisecond -- the last page comes back empty with a null
/// next_cursor and no error at all. Verified on DuckDB 1.5.5: paging 8 sessions
/// at limit=2 lost the final two.
pub(crate) fn timestamp_literal(value: &DateTime<Utc>) -> String {
    format!(
        "{}::TIMESTAMPTZ",
        sql_string_literal(&value.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
    )
}

pub(crate) fn encode_cursor(timestamp: DateTime<Utc>, id: &str) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let payload = PageCursor {
        t: timestamp,
        id: id.to_string(),
    };
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).expect("cursor json"))
}

pub(crate) fn decode_cursor(cursor: &str) -> Result<PageCursor, String> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor.as_bytes())
        .map_err(|_| "malformed cursor".to_string())?;
    serde_json::from_slice(&bytes).map_err(|_| "malformed cursor".to_string())
}

pub(crate) fn cursor_predicate(
    cursor: &str,
    timestamp_col: &str,
    id_col: &str,
) -> Result<String, String> {
    let decoded = decode_cursor(cursor)?;
    Ok(format!(
        "({timestamp_col} < {ts} OR ({timestamp_col} = {ts} AND {id_col} < {id}))",
        ts = timestamp_literal(&decoded.t),
        id = sql_string_literal(&decoded.id),
    ))
}

/// Push a bounded (or half-bounded) `timestamp` range condition.
pub(crate) fn push_optional_time_bounds(
    conditions: &mut Vec<String>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Result<(), String> {
    match (from, to) {
        (Some(from), Some(to)) => {
            if from > to {
                return Err("`from` must be <= `to`".to_string());
            }
            conditions.push(format!(
                "timestamp >= {} AND timestamp <= {}",
                timestamp_literal(&from),
                timestamp_literal(&to)
            ));
        }
        (Some(from), None) => {
            conditions.push(format!("timestamp >= {}", timestamp_literal(&from)));
        }
        (None, Some(to)) => {
            conditions.push(format!("timestamp <= {}", timestamp_literal(&to)));
        }
        (None, None) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_single_quotes_in_string_literal() {
        assert_eq!(sql_string_literal("it's"), "'it''s'");
    }

    #[test]
    fn cursor_round_trips() {
        let ts = DateTime::parse_from_rfc3339("2026-07-18T23:22:00.123Z")
            .unwrap()
            .with_timezone(&Utc);
        let encoded = encode_cursor(ts, "span-1");
        let decoded = decode_cursor(&encoded).expect("decode");
        assert_eq!(decoded.id, "span-1");
        assert_eq!(decoded.t, ts);
        assert!(decode_cursor("%%%not-base64%%%").is_err());
    }

    #[test]
    fn time_bounds_reject_inverted_range() {
        let from = DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut conditions = Vec::new();
        assert!(push_optional_time_bounds(&mut conditions, Some(from), Some(to)).is_err());
    }
}
