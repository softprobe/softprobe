#!/bin/bash
# Drop all Iceberg tables from the REST catalog
# This script is useful for development when you need to reset the schema

set -e

echo "=== Dropping all Iceberg tables ==="
echo ""

# Configuration from docker-compose.yml
CATALOG_URI="http://localhost:8181/catalog"
WAREHOUSE="default"
NAMESPACE="default"

CONFIG_URL="${CATALOG_URI}/v1/config?warehouse=${WAREHOUSE}"
PREFIX=$(curl -s "${CONFIG_URL}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["defaults"]["prefix"])')
if [ -z "${PREFIX}" ]; then
    echo "✗ Failed to resolve Lakekeeper catalog prefix"
    exit 1
fi
CATALOG_PREFIX_URI="${CATALOG_URI}/v1/${PREFIX}"

# Tables to drop (including old test tables)
TABLES=("traces" "logs" "metrics" "otlp_traces" "otlp_logs" "otlp_metrics" "raw_sessions" "test_spans")

for table in "${TABLES[@]}"; do
    echo "Attempting to drop table: ${NAMESPACE}.${table}"

    # Try to drop the table using the Iceberg REST catalog API
    http_code=$(curl -s -o /tmp/drop_response.txt -w "%{http_code}" -X DELETE \
        "${CATALOG_PREFIX_URI}/namespaces/${NAMESPACE}/tables/${table}" \
        -H "Content-Type: application/json")

    if [ "$http_code" = "204" ] || [ "$http_code" = "200" ]; then
        echo "✓ Successfully dropped table: ${table}"
    elif [ "$http_code" = "404" ]; then
        echo "⚠ Table ${table} does not exist (already dropped or never created)"
    else
        echo "✗ Failed to drop table ${table} (HTTP ${http_code})"
        echo "Response:"
        cat /tmp/drop_response.txt
    fi
    echo ""
done

echo "=== Table drop complete ==="
echo ""
echo "Verification: Listing all tables in namespace '${NAMESPACE}'"
echo ""

# List remaining tables
list_response=$(curl -s "${CATALOG_PREFIX_URI}/namespaces/${NAMESPACE}/tables")
echo "$list_response" | jq '.' 2>/dev/null || echo "$list_response"
