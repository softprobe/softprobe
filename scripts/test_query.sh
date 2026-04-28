#!/bin/bash
# Simple script to test a query and show the full error

SERVICE_URL="${1:-http://172.31.85.52:8090}"
QUERY="${2:-SELECT COUNT(*) AS errors FROM union_spans WHERE record_date >= DATE '2025-01-30' AND timestamp >= (CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL '5 minutes') AND (http_response_status_code >= 500 OR status_code = 'ERROR')}"

echo "Testing query against: $SERVICE_URL"
echo "Query: $QUERY"
echo ""

curl -v -X POST "$SERVICE_URL/v1/query/sql" \
  -H "Content-Type: application/json" \
  -d "{\"sql\": \"$QUERY\"}" \
  --max-time 30 \
  2>&1 | tee /tmp/query_test.log

echo ""
echo "--- Response body ---"
grep -A 100 "< HTTP" /tmp/query_test.log | tail -n +2 || cat /tmp/query_test.log
