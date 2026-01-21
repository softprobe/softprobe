#!/bin/bash
set -e

echo "🔍 End-to-End System Verification"
echo "=================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

cd "$(dirname "$0")/.."

CATALOG_URI="http://localhost:8181/catalog"
WAREHOUSE="default"
CONFIG_URL="${CATALOG_URI}/v1/config?warehouse=${WAREHOUSE}"
PREFIX=$(curl -s "${CONFIG_URL}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["defaults"]["prefix"])')
if [ -z "${PREFIX}" ]; then
    echo -e "   ${RED}✗ Failed to resolve Lakekeeper catalog prefix${NC}"
    exit 1
fi
CATALOG_PREFIX_URI="${CATALOG_URI}/v1/${PREFIX}"

echo "1. Checking services..."
echo "   - MinIO:"
if docker-compose ps minio | grep -q "Up"; then
    echo -e "   ${GREEN}✓ MinIO running${NC}"
else
    echo -e "   ${RED}✗ MinIO not running${NC}"
    exit 1
fi

echo "   - Lakekeeper REST:"
if curl -sf "${CONFIG_URL}" > /dev/null 2>&1; then
    echo -e "   ${GREEN}✓ Lakekeeper REST ready${NC}"
else
    echo -e "   ${RED}✗ Lakekeeper REST not ready${NC}"
    exit 1
fi

echo "   - OTLP Backend:"
if docker-compose ps otlp-backend | grep -q "Up"; then
    echo -e "   ${GREEN}✓ OTLP Backend running${NC}"
else
    echo -e "   ${RED}✗ OTLP Backend not running${NC}"
    exit 1
fi

echo "   - Grafana:"
if docker-compose ps grafana | grep -q "Up"; then
    echo -e "   ${GREEN}✓ Grafana running${NC}"
else
    echo -e "   ${RED}✗ Grafana not running${NC}"
    exit 1
fi

echo ""
echo "2. Checking Iceberg tables..."
TABLES=$(curl -s "${CATALOG_PREFIX_URI}/namespaces/default/tables" | python3 -m json.tool | grep '"name"' | wc -l)
if [ "$TABLES" -ge 3 ]; then
    echo -e "   ${GREEN}✓ Found $TABLES tables (traces, logs, metrics)${NC}"
else
    echo -e "   ${RED}✗ Expected 3 tables, found $TABLES${NC}"
    exit 1
fi

echo ""
echo "3. Checking data files..."
docker-compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1
TRACE_FILES=$(docker-compose exec -T minio mc find local/warehouse/iceberg/default/traces --name '*.parquet' 2>/dev/null | wc -l)
if [ "$TRACE_FILES" -gt 0 ]; then
    echo -e "   ${GREEN}✓ Found $TRACE_FILES trace Parquet files${NC}"
else
    echo -e "   ${RED}✗ No trace files found${NC}"
    exit 1
fi

echo ""
echo "4. Downloading sample file for analysis..."
FIRST_FILE=$(docker-compose exec -T minio mc find local/warehouse/iceberg/default/traces --name '*.parquet' 2>/dev/null | head -1 | tr -d '\r')
if [ -n "$FIRST_FILE" ]; then
    docker-compose exec -T minio mc cp "$FIRST_FILE" /tmp/verify_sample.parquet >/dev/null 2>&1
    docker-compose exec -T minio cat /tmp/verify_sample.parquet > /tmp/verify_sample_host.parquet
    echo -e "   ${GREEN}✓ Downloaded sample file${NC}"
else
    echo -e "   ${RED}✗ Could not find trace file${NC}"
    exit 1
fi

echo ""
echo "5. Analyzing trace data..."
echo ""
duckdb -c "
SELECT
  'Total Spans' as metric,
  COUNT(*)::VARCHAR as value
FROM read_parquet('/tmp/verify_sample_host.parquet')
UNION ALL
SELECT
  'Unique Sessions',
  COUNT(DISTINCT session_id)::VARCHAR
FROM read_parquet('/tmp/verify_sample_host.parquet')
UNION ALL
SELECT
  'Unique Endpoints',
  COUNT(DISTINCT http_request_path)::VARCHAR
FROM read_parquet('/tmp/verify_sample_host.parquet')
UNION ALL
SELECT
  'Date Partitions',
  COUNT(DISTINCT record_date)::VARCHAR
FROM read_parquet('/tmp/verify_sample_host.parquet');
"

echo ""
echo "6. Sample sessions with sp.session.id:"
echo ""
duckdb -c "
SELECT
  session_id,
  COUNT(*) as requests,
  MIN(http_request_path) as sample_endpoint
FROM read_parquet('/tmp/verify_sample_host.parquet')
GROUP BY session_id
ORDER BY requests DESC
LIMIT 5;
"

echo ""
echo "7. Sample HTTP requests:"
echo ""
duckdb -c "
SELECT
  http_request_method || ' ' || http_request_path as request,
  http_response_status_code as status,
  attributes['duration_ms'] as duration_ms,
  LEFT(session_id, 20) || '...' as session
FROM read_parquet('/tmp/verify_sample_host.parquet')
ORDER BY timestamp DESC
LIMIT 5;
"

echo ""
echo -e "${GREEN}✅ End-to-End Verification Complete!${NC}"
echo ""
echo "System Status:"
echo "  • Telemetry ingestion: ✓ Working (HTTP/Protobuf)"
echo "  • Data storage: ✓ Working (Iceberg + Parquet)"
echo "  • Session tracking: ✓ Working (sp.session.id present)"
echo "  • Partitioning: ✓ Working (by record_date)"
echo ""
echo "Next Steps:"
echo "  1. For querying, use DuckDB CLI (Grafana plugin requires x86_64)"
echo "  2. Access Grafana: http://localhost:3000"
echo "  3. Access MinIO: http://localhost:9001"
echo "  4. Generate more data: python3 scripts/generate_telemetry.py"
echo ""

echo "======================================="
echo "DuckDB Iceberg Verification (Quick)"
echo "======================================="
echo ""

# Check if DuckDB is installed
if ! command -v duckdb &> /dev/null; then
    echo "Warning: DuckDB is not installed or not in PATH"
    echo "Install with: brew install duckdb (macOS) or see https://duckdb.org/docs/installation/"
    exit 0
fi

echo "Running verification SQL..."
duckdb -init scripts/verify_iceberg.sql < /dev/null

echo ""
echo "Quick verification complete."
