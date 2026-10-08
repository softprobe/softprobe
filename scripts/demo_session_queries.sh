#!/bin/bash
# Session-oriented demo queries against DuckLake via DuckDB (same combo init as interactive_query.sh).
# Prerequisites: CONFIG_FILE (or default host yaml), Postgres + MinIO + ingested traces.

set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

if ! command -v duckdb >/dev/null 2>&1; then
  echo "ERROR: duckdb CLI not found" >&2
  exit 1
fi

# shellcheck source=/dev/null
source "${ROOT}/scripts/duckdb_ducklake_combo.sh"

STATIC_INIT="${SOFTPROBE_DUCKDB_INIT:-}"
if [[ -n "$STATIC_INIT" ]]; then
  COMBO="$(softprobe_ducklake_build_combo_init "$ROOT" "$STATIC_INIT")"
else
  COMBO="$(softprobe_ducklake_build_combo_init "$ROOT")"
fi
trap 'rm -f "$COMBO"' EXIT

duckq() {
  duckdb -init "$COMBO" "$@"
}

echo "Session-Based Query Demonstrations (DuckLake)"
echo "============================================="
grep '^-- CONFIG_FILE=' "$COMBO" 2>/dev/null || true
echo ""

if ! grep -q "CREATE OR REPLACE VIEW traces" "$COMBO" 2>/dev/null; then
  echo "ERROR: No \`traces\` table in this DuckLake scope — ingest with the runtime using the same CONFIG_FILE, then re-run." >&2
  exit 1
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 1: Sessions with span counts"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
duckq -c "
SELECT
  LEFT(session_id, 40) AS session,
  COUNT(*) AS total_spans,
  MIN(timestamp)::VARCHAR AS first_seen,
  MAX(timestamp)::VARCHAR AS last_seen,
  COUNT(DISTINCT http_request_path) AS unique_endpoints
FROM traces
GROUP BY session_id
ORDER BY total_spans DESC
LIMIT 20;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 2: Timeline for one session"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
SAMPLE_SESSION=$(duckq -csv -noheader -c "SELECT session_id FROM traces WHERE session_id IS NOT NULL LIMIT 1;" | head -1 | tr -d '\r')
if [[ -z "$SAMPLE_SESSION" ]]; then
  echo "(no rows in traces; table exists but empty)"
else
  echo "Session: $SAMPLE_SESSION"
  ESC=${SAMPLE_SESSION//\'/\'\'}
  duckq -c "
  SELECT
    timestamp::VARCHAR AS time,
    COALESCE(http_request_method, '') || ' ' || COALESCE(http_request_path, '') AS request,
    http_response_status_code AS status
  FROM traces
  WHERE session_id = '$ESC'
  ORDER BY timestamp;
  "
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 3: Endpoints (HTTP paths)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
duckq -c "
SELECT
  http_request_path AS endpoint,
  COUNT(*) AS requests,
  COUNT(DISTINCT session_id) AS unique_sessions
FROM traces
WHERE http_request_path IS NOT NULL
GROUP BY http_request_path
ORDER BY requests DESC
LIMIT 20;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 4: Traces + logs for a session (correlation)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [[ -n "${SAMPLE_SESSION:-}" ]]; then
  ESC=${SAMPLE_SESSION//\'/\'\'}
  if grep -q "CREATE OR REPLACE VIEW logs" "$COMBO" 2>/dev/null; then
    duckq -c "
    SELECT type, session_id, trace_id, content, timestamp::VARCHAR AS ts FROM (
      SELECT 'trace' AS type, session_id, trace_id, CAST(message_type AS VARCHAR) AS content, timestamp
      FROM traces WHERE session_id = '$ESC'
      UNION ALL
      SELECT 'log', session_id, trace_id, body, timestamp FROM logs WHERE session_id = '$ESC'
    ) u
    ORDER BY timestamp
    LIMIT 50;
    "
  else
    echo "(skipped — no \`logs\` table in this scope yet)"
  fi
else
  echo "(skipped — no sample session)"
fi

echo ""
echo "Demo complete. For an interactive shell: make duckdb-shell"
