#!/bin/bash
# Quick verification of Iceberg data using DuckDB
# This script provides a simple interactive way to query Iceberg tables

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "======================================="
echo "Iceberg Data Verification (DuckDB)"
echo "======================================="
echo ""

# Check if DuckDB is installed
if ! command -v duckdb &> /dev/null; then
    echo "Error: DuckDB is not installed or not in PATH"
    echo "Install with: brew install duckdb (macOS) or see https://duckdb.org/docs/installation/"
    exit 1
fi

# Check if MinIO is running
if ! nc -z localhost 9002 2>/dev/null; then
    echo "Warning: MinIO does not appear to be running on localhost:9002"
    echo "Start with: docker-compose up -d (if using dev-compose)"
    echo ""
fi

# Check if Iceberg REST catalog is running
if ! nc -z localhost 8181 2>/dev/null; then
    echo "Warning: Iceberg REST catalog does not appear to be running on localhost:8181"
    echo "Start with: docker-compose up -d (if using dev-compose)"
    echo ""
fi

echo "Running verification queries..."
echo ""

# Run the verification SQL
duckdb -init scripts/verify_iceberg.sql < /dev/null

echo ""
echo "======================================="
echo "Verification complete!"
echo ""
echo "Tips:"
echo "  - For interactive mode: duckdb"
echo "  - Then run: .read scripts/verify_iceberg.sql"
echo "  - Check specific session: .read scripts/verify_session.sql"
echo "======================================="
