#!/usr/bin/env bash
# Unified DuckLake stress driver (local MinIO | R2 | GCS).
# Invoked by Make: BACKEND=local|r2|gcs ./scripts/stress-test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BACKEND="${BACKEND:-local}"
DURATION="${DURATION:-60}"
SPAN_QPS="${SPAN_QPS:-50}"
LOG_QPS="${LOG_QPS:-70}"
METRIC_QPS="${METRIC_QPS:-70}"
QUERY_CONCURRENCY="${QUERY_CONCURRENCY:-4}"
QUERY_INTERVAL_MS="${QUERY_INTERVAL_MS:-500}"

PERF_ARGS=(
  --duration "${DURATION}"
  --span-qps "${SPAN_QPS}"
  --log-qps "${LOG_QPS}"
  --metric-qps "${METRIC_QPS}"
  --query-concurrency "${QUERY_CONCURRENCY}"
  --query-interval-ms "${QUERY_INTERVAL_MS}"
)

wait_health() {
  local port="$1" log="$2" attempts="${3:-15}"
  local i
  for i in $(seq 1 "${attempts}"); do
    if curl -sf "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "❌ softprobe-runtime failed to start" >&2
  cat "${log}" >&2 || true
  return 1
}

run_perf() {
  local config="$1" port="$2"
  shift 2
  CONFIG_FILE="${config}" cargo run --bin perf_stress -- \
    --service-url "http://127.0.0.1:${port}" \
    "$@"
}

smoke_ok() {
  local log="$1"
  if rg -n "errors:\\s*[1-9]|Total query errors:\\s*[1-9]|Steady-state query errors:\\s*[1-9]" "${log}" >/dev/null; then
    return 1
  fi
  return 0
}

case "${BACKEND}" in
  local)
    make setup
    PORT="${PORT:-38090}"
    TMP_CONFIG="/tmp/splake-stress.yaml"
    LOG="/tmp/splake-stress.log"
    sed "s/port: 8090/port: ${PORT}/" config.yaml > "${TMP_CONFIG}"
    echo "🚀 Starting softprobe-runtime on port ${PORT} (local MinIO)..."
    SPLAKE_RESET_DUCKLAKE=1 CONFIG_FILE="${TMP_CONFIG}" cargo run --bin softprobe-runtime >"${LOG}" 2>&1 &
    PID=$!
    trap 'kill ${PID} >/dev/null 2>&1 || true; make --no-print-directory _teardown-minio >/dev/null 2>&1 || true; rm -f "${TMP_CONFIG}"' EXIT
    wait_health "${PORT}" "${LOG}" 10
    run_perf "${TMP_CONFIG}" "${PORT}" "${PERF_ARGS[@]}"
    ;;
  r2)
    R2_CONFIG="${R2_CONFIG:-tests/config/test-r2.yaml}"
    PORT="${PORT:-38091}"
    TMP_CONFIG="/tmp/splake-r2-ducklake-stress.yaml"
    LOG="/tmp/splake-r2-ducklake-stress.log"
    SMOKE_LOG="/tmp/perf-r2-ducklake-smoke.log"
    test -f "${R2_CONFIG}" || { echo "❌ R2 config not found: ${R2_CONFIG}" >&2; exit 1; }
    rg -n '^ducklake:\s*$' "${R2_CONFIG}" >/dev/null || { echo "❌ ${R2_CONFIG} missing ducklake: block" >&2; exit 1; }
    R2_BUCKET="${R2_BUCKET:-$(rg '^\s*data_path:\s*' "${R2_CONFIG}" -m 1 | sed -E 's|.*s3://([^/]+)/.*|\1|' | xargs)}"
    if [[ -z "${R2_BUCKET}" || "${R2_BUCKET}" == "YOUR-R2-BUCKET" || "${R2_BUCKET}" == "your-bucket-name" ]]; then
      echo "❌ Could not resolve R2 bucket from ${R2_CONFIG} (or pass R2_BUCKET=...)" >&2
      exit 1
    fi
    cp "${R2_CONFIG}" "${TMP_CONFIG}"
    sed -i.bak "s/port: 8090/port: ${PORT}/" "${TMP_CONFIG}" && rm -f "${TMP_CONFIG}.bak"
    sed -i.bak "s|data_path: .*|data_path: \"s3://${R2_BUCKET}/ducklake/\"|" "${TMP_CONFIG}" && rm -f "${TMP_CONFIG}.bak"
    echo "🚀 Starting softprobe-runtime on port ${PORT} (R2 bucket ${R2_BUCKET})..."
    SPLAKE_RESET_DUCKLAKE=1 CONFIG_FILE="${TMP_CONFIG}" cargo run --bin softprobe-runtime >"${LOG}" 2>&1 &
    PID=$!
    trap 'kill ${PID} >/dev/null 2>&1 || true; rm -f "${TMP_CONFIG}"' EXIT
    wait_health "${PORT}" "${LOG}" 15
    echo "🧪 10s smoke..."
    run_perf "${TMP_CONFIG}" "${PORT}" \
      --duration 10 --span-qps 10 --log-qps 10 --metric-qps 10 --query-concurrency 1 --query-interval-ms 1000 \
      >"${SMOKE_LOG}" 2>&1
    if ! smoke_ok "${SMOKE_LOG}"; then
      echo "❌ R2 smoke failed"; cat "${SMOKE_LOG}"; rg -n "ERROR|Error|failed|Failed" "${LOG}" || true; exit 1
    fi
    echo "✅ Smoke ok; full stress..."
    run_perf "${TMP_CONFIG}" "${PORT}" "${PERF_ARGS[@]}"
    ;;
  gcs)
    GCP_CONFIG="${GCP_CONFIG:-tests/config/test-gcp.yaml}"
    PORT="${PORT:-38092}"
    CACHE_ROOT="${CACHE_ROOT:-/tmp/splake-gcs-ducklake}"
    TMP_CONFIG="/tmp/splake-gcs-ducklake-stress.yaml"
    LOG="/tmp/splake-gcs-ducklake-stress.log"
    WARMUP_LOG="/tmp/perf-gcs-ducklake-warmup.log"
    SMOKE_LOG="/tmp/perf-gcs-ducklake-smoke.log"
    test -f "${GCP_CONFIG}" || { echo "❌ GCP config not found: ${GCP_CONFIG}" >&2; exit 1; }
    rg -n '^ducklake:\s*$' "${GCP_CONFIG}" >/dev/null || { echo "❌ ${GCP_CONFIG} missing ducklake: block" >&2; exit 1; }
    GCS_BUCKET="${GCS_BUCKET:-$(rg '^\s*data_path:\s*' "${GCP_CONFIG}" -m 1 | sed -E 's|.*(gs|s3)://([^/]+)/.*|\2|' | xargs)}"
    if [[ -z "${GCS_BUCKET}" || "${GCS_BUCKET}" == YOUR-GCS-BUCKET* || "${GCS_BUCKET}" == "your-bucket-name" ]]; then
      echo "❌ Could not resolve GCS bucket (or pass GCS_BUCKET=...)" >&2
      exit 1
    fi
    if [[ -z "${GCS_HMAC_ACCESS_KEY_ID:-}" || -z "${GCS_HMAC_SECRET:-}" ]]; then
      echo "❌ GCS_HMAC_ACCESS_KEY_ID and GCS_HMAC_SECRET are required" >&2
      exit 1
    fi
    cp "${GCP_CONFIG}" "${TMP_CONFIG}"
    sed -i.bak "s/port: 8090/port: ${PORT}/" "${TMP_CONFIG}" && rm -f "${TMP_CONFIG}.bak"
    rm -rf "${CACHE_ROOT}"
    mkdir -p "${CACHE_ROOT}/cache"
    sed -i.bak "s|cache_dir: .*|cache_dir: \"${CACHE_ROOT}/cache\"|" "${TMP_CONFIG}" && rm -f "${TMP_CONFIG}.bak"
    sed -i.bak "s|data_path: .*|data_path: \"gs://${GCS_BUCKET}/ducklake/\"|" "${TMP_CONFIG}" && rm -f "${TMP_CONFIG}.bak"
    echo "🚀 Starting softprobe-runtime on port ${PORT} (GCS bucket ${GCS_BUCKET})..."
    SPLAKE_RESET_DUCKLAKE=1 CONFIG_FILE="${TMP_CONFIG}" cargo run --bin softprobe-runtime >"${LOG}" 2>&1 &
    PID=$!
    trap 'kill ${PID} >/dev/null 2>&1 || true; rm -f "${TMP_CONFIG}"' EXIT
    wait_health "${PORT}" "${LOG}" 15
    echo "♨️  ingest-only warmup..."
    run_perf "${TMP_CONFIG}" "${PORT}" \
      --duration 12 --span-qps 10 --log-qps 10 --metric-qps 10 --query-concurrency 0 --query-interval-ms 1000 \
      >"${WARMUP_LOG}" 2>&1
    echo "🧪 10s smoke..."
    run_perf "${TMP_CONFIG}" "${PORT}" \
      --duration 10 --span-qps 10 --log-qps 10 --metric-qps 10 --query-concurrency 1 --query-interval-ms 1000 \
      >"${SMOKE_LOG}" 2>&1
    if ! smoke_ok "${SMOKE_LOG}"; then
      echo "❌ GCS smoke failed"; cat "${SMOKE_LOG}"; rg -n "ERROR|Error|failed|Failed" "${LOG}" || true; exit 1
    fi
    echo "✅ Smoke ok; full stress..."
    run_perf "${TMP_CONFIG}" "${PORT}" "${PERF_ARGS[@]}"
    ;;
  *)
    echo "❌ Unknown BACKEND=${BACKEND} (use local|r2|gcs)" >&2
    exit 1
    ;;
esac

echo "✅ stress-test BACKEND=${BACKEND} completed"
