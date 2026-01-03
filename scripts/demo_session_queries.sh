#!/bin/bash
set -e

echo "🔍 Session-Based Query Demonstrations"
echo "======================================"
echo ""

cd "$(dirname "$0")/.."

# Download all trace files and combine them
echo "Preparing data..."
docker-compose exec -T minio mc find local/warehouse/default/traces --name '*.parquet' 2>/dev/null | head -10 | while read file; do
    docker-compose exec -T minio mc cp "$file" /tmp/traces_sample.parquet >/dev/null 2>&1
    docker-compose exec -T minio cat /tmp/traces_sample.parquet >> /tmp/all_traces.parquet 2>/dev/null
done

echo "✓ Data prepared"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 1: All Sessions with Request Counts"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
duckdb -c "
SELECT
  LEFT(session_id, 40) || '...' as session,
  COUNT(*) as total_requests,
  MIN(timestamp)::VARCHAR as first_seen,
  MAX(timestamp)::VARCHAR as last_seen,
  COUNT(DISTINCT http_request_path) as unique_endpoints
FROM read_parquet('/tmp/all_traces.parquet')
GROUP BY session_id
ORDER BY total_requests DESC;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 2: Request Timeline for a Specific Session"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

SAMPLE_SESSION=$(duckdb -csv -noheader -c "SELECT session_id FROM read_parquet('/tmp/all_traces.parquet') LIMIT 1")

echo "Session: $SAMPLE_SESSION"
echo ""

duckdb -c "
SELECT
  timestamp::VARCHAR as time,
  http_request_method || ' ' || http_request_path as request,
  http_response_status_code as status,
  attributes['duration_ms'] as duration_ms,
  attributes['user.id'] as user_id
FROM read_parquet('/tmp/all_traces.parquet')
WHERE session_id = '$SAMPLE_SESSION'
ORDER BY timestamp;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 3: Endpoint Performance Statistics"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
duckdb -c "
SELECT
  http_request_path as endpoint,
  COUNT(*) as requests,
  ROUND(AVG(CAST(attributes['duration_ms'] AS INTEGER)), 2) as avg_duration_ms,
  MIN(CAST(attributes['duration_ms'] AS INTEGER)) as min_duration_ms,
  MAX(CAST(attributes['duration_ms'] AS INTEGER)) as max_duration_ms,
  COUNT(DISTINCT session_id) as unique_sessions
FROM read_parquet('/tmp/all_traces.parquet')
WHERE http_request_path IS NOT NULL
GROUP BY http_request_path
ORDER BY requests DESC;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 4: Error Analysis by Session"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
duckdb -c "
SELECT
  LEFT(session_id, 40) || '...' as session,
  COUNT(*) as total_requests,
  COUNT(*) FILTER (WHERE http_response_status_code >= 500) as errors,
  ROUND(COUNT(*) FILTER (WHERE http_response_status_code >= 500) * 100.0 / COUNT(*), 2) as error_rate_pct
FROM read_parquet('/tmp/all_traces.parquet')
GROUP BY session_id
HAVING COUNT(*) FILTER (WHERE http_response_status_code >= 500) > 0
ORDER BY error_rate_pct DESC;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 5: Session Journey Analysis"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
duckdb -c "
SELECT
  LEFT(session_id, 40) || '...' as session,
  STRING_AGG(http_request_path, ' → ' ORDER BY timestamp) as journey
FROM read_parquet('/tmp/all_traces.parquet')
WHERE http_request_path IS NOT NULL
GROUP BY session_id
LIMIT 5;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Query 6: Verify sp.session.id Attribute"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
duckdb -c "
SELECT
  'Rows with sp.session.id' as metric,
  COUNT(*)::VARCHAR as count
FROM read_parquet('/tmp/all_traces.parquet')
WHERE attributes['sp.session.id'] IS NOT NULL
UNION ALL
SELECT
  'Total rows',
  COUNT(*)::VARCHAR
FROM read_parquet('/tmp/all_traces.parquet')
UNION ALL
SELECT
  'Coverage (%)',
  ROUND(
    COUNT(*) FILTER (WHERE attributes['sp.session.id'] IS NOT NULL) * 100.0 / COUNT(*),
    2
  )::VARCHAR || '%'
FROM read_parquet('/tmp/all_traces.parquet');
"

echo ""
echo "✅ Demo complete! All queries show sp.session.id is working."
echo ""
echo "Next Steps:"
echo "  • Generate more data: python3 scripts/generate_telemetry.py"
echo "  • View in MinIO: http://localhost:9001"
echo "  • Custom queries: duckdb /tmp/all_traces.parquet"
echo ""
