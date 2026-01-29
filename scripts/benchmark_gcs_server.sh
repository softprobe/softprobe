#!/bin/bash
# Full stress performance benchmark script for GCS on a server
#
# Prerequisites:
#   1. GCS bucket created and accessible
#   2. Service account JSON key file with Storage Admin permissions
#   3. Iceberg REST catalog endpoint (if using REST catalog)
#   4. Rust/Cargo installed on server
#
# Usage:
#   ./scripts/benchmark_gcs_server.sh [OPTIONS]
#
# Environment Variables:
#   GOOGLE_APPLICATION_CREDENTIALS - Path to GCS service account JSON key
#   GCS_BUCKET - GCS bucket name (overrides config file)
#   CATALOG_URI - Iceberg REST catalog URI (overrides config file)
#   BENCHMARK_DURATION - Test duration in seconds (default: 300)
#   BENCHMARK_WARMUP - Warmup period in seconds (default: 60)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Default benchmark parameters
DURATION=${BENCHMARK_DURATION:-300}
WARMUP=${BENCHMARK_WARMUP:-60}
SPAN_QPS=${SPAN_QPS:-200}
LOG_QPS=${LOG_QPS:-200}
METRIC_QPS=${METRIC_QPS:-200}
QUERY_CONCURRENCY=${QUERY_CONCURRENCY:-16}
QUERY_INTERVAL_MS=${QUERY_INTERVAL_MS:-500}

# Configuration
CONFIG_FILE="${CONFIG_FILE:-config-gcs-benchmark.yaml}"
CACHE_DIR="${CACHE_DIR:-/var/tmp/datalake/cache}"
SPILL_DIR="${SPILL_DIR:-/var/tmp/datalake/duckdb_spill}"

echo "=========================================="
echo "GCS Server Benchmark Setup"
echo "=========================================="

# Check prerequisites
if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
    echo "ERROR: GOOGLE_APPLICATION_CREDENTIALS not set"
    echo "Set it to your GCS service account JSON key file path:"
    echo "  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"
    exit 1
fi

if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "ERROR: Service account key file not found: $GOOGLE_APPLICATION_CREDENTIALS"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Config file not found: $CONFIG_FILE"
    echo "Create it from config-gcs-benchmark.yaml template"
    exit 1
fi

# Create cache and spill directories
echo "Creating cache directories..."
mkdir -p "$CACHE_DIR"
mkdir -p "$SPILL_DIR"

# Verify GCS access
echo "Verifying GCS access..."
if ! gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS" 2>/dev/null; then
    echo "WARNING: gcloud CLI not available, skipping GCS access verification"
    echo "Ensure your service account has Storage Admin role"
fi

# Build in release mode for performance
echo "Building in release mode..."
cargo build --release --bin perf_stress

# Print benchmark configuration
echo ""
echo "=========================================="
echo "Benchmark Configuration"
echo "=========================================="
echo "Duration: ${DURATION}s"
echo "Warmup: ${WARMUP}s"
echo "Span QPS: ${SPAN_QPS}/s"
echo "Log QPS: ${LOG_QPS}/s"
echo "Metric QPS: ${METRIC_QPS}/s"
echo "Query Concurrency: ${QUERY_CONCURRENCY}"
echo "Query Interval: ${QUERY_INTERVAL_MS}ms"
echo "Cache Dir: ${CACHE_DIR}"
echo "Config: ${CONFIG_FILE}"
echo ""

# Run benchmark
echo "=========================================="
echo "Starting Benchmark"
echo "=========================================="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

RUST_LOG=${RUST_LOG:-info} \
CONFIG_FILE="$CONFIG_FILE" \
cargo run --release --bin perf_stress -- \
    --duration "$DURATION" \
    --warmup-secs "$WARMUP" \
    --span-qps "$SPAN_QPS" \
    --log-qps "$LOG_QPS" \
    --metric-qps "$METRIC_QPS" \
    --query-concurrency "$QUERY_CONCURRENCY" \
    --query-interval-ms "$QUERY_INTERVAL_MS"

echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""
echo "Results saved above. To analyze:"
echo "  - Check query latency breakdown by type"
echo "  - Monitor GCS egress costs in GCP console"
echo "  - Review cache hit rates (if cache_httpfs enabled)"
