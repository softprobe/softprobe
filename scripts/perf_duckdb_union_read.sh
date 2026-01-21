#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_TEMPLATE="$ROOT_DIR/tests/config/test-r2.yaml"

rm -f /tmp/datalake-r2-perf-*.yaml
CONFIG_TMP="$(mktemp /tmp/datalake-r2-perf-XXXXXX.yaml)"

cp "$CONFIG_TEMPLATE" "$CONFIG_TMP"

# Increase batching and avoid forced tiny files for perf validation.
perl -0pi -e "s/max_buffer_spans:\s*1\b/max_buffer_spans: 1000/" "$CONFIG_TMP"
perl -0pi -e "s/flush_interval_seconds:\s*1\b/flush_interval_seconds: 10/" "$CONFIG_TMP"
perl -0pi -e "s/force_close_after_append:\s*true\b/force_close_after_append: false/" "$CONFIG_TMP"

export PERF_CONFIG_FILE="$CONFIG_TMP"
export ICEBERG_TEST_TYPE=r2
export PERF_TARGET_MS=1000
export ICEBERG_NAMESPACE="perf_$(date +%s)_$RANDOM"

cargo test --test performance_test -- --ignored --nocapture

rm -f "$CONFIG_TMP"
