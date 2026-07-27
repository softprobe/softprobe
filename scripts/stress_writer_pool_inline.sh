#!/usr/bin/env bash
# Postgres (Docker) + GCS stress matrix for writer_pool_size × data_inlining_row_limit.
# Requires: release binaries, ducklake-postgres, auth-mock, GCS HMAC env, ADC key.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

: "${GCS_HMAC_ACCESS_KEY_ID:?}"
: "${GCS_HMAC_SECRET:?}"
: "${GOOGLE_APPLICATION_CREDENTIALS:?}"

PORT="${PORT:-38095}"
export DUCKDB_DOWNLOAD_LIB=1
export LD_LIBRARY_PATH="${ROOT}/target/duckdb-download/x86_64-unknown-linux-gnu/1.5.2:${LD_LIBRARY_PATH:-}"
export SOFTPROBE_AUTH_URL=http://127.0.0.1:8080/validate
export SOFTPROBE_ADMIN_API_KEY=admin-stress-key
export RUST_LOG=info

RESULTS_DIR=/tmp/perf-pool-inline-matrix
mkdir -p "$RESULTS_DIR"
SUMMARY="${RESULTS_DIR}/summary.tsv"
echo -e "cell\tpool\tinline\tbatch\tingest_eps\tingest_p95_span\tquery_only_p95\tmixed_p95\tparquet_files\thttp_errors" > "$SUMMARY"

stop_runtime() {
  if [[ -f /tmp/splake-pool-inline.pid ]]; then
    kill "$(cat /tmp/splake-pool-inline.pid)" 2>/dev/null || true
    sleep 1
    kill -9 "$(cat /tmp/splake-pool-inline.pid)" 2>/dev/null || true
    rm -f /tmp/splake-pool-inline.pid
  fi
}

run_cell() {
  local cell="$1" pool="$2" inline="$3" batch="$4"
  local ts schema data_path cfg cache log_rt log_perf
  ts="$(date +%Y%m%d-%H%M%S)"
  schema="perf_pool_${cell}_${ts}"
  # schema must be valid PG ident
  schema="${schema//-/_}"
  data_path="gs://softprobe-datalake-ducklake/perf-pool-inline-${cell}-${ts}/"
  cfg="${RESULTS_DIR}/${cell}.yaml"
  cache="/tmp/splake-pool-inline-${cell}"
  log_rt="${RESULTS_DIR}/${cell}-runtime.log"
  log_perf="${RESULTS_DIR}/${cell}-perf.log"

  echo ""
  echo "======== CELL ${cell}: pool=${pool} inline=${inline} batch=${batch} ========"
  stop_runtime
  rm -rf "$cache"
  mkdir -p "$cache/cache" "$cache/spill"
  docker exec ducklake-postgres psql -U ducklake -d ducklake -c "DROP SCHEMA IF EXISTS ${schema} CASCADE;" >/dev/null
  docker exec ducklake-postgres psql -U ducklake -d ducklake -c "DROP SCHEMA IF EXISTS ${schema}_tenant CASCADE;" >/dev/null || true

  cat > "$cfg" <<EOF
server:
  port: ${PORT}
  host: "127.0.0.1"
  max_body_size: 104857600
  worker_threads: null
object_store:
  region: "us-central1"
query:
  max_connections: 10
  cache_dir: "${cache}/cache"
maintenance:
  enabled: true
  target_file_size_bytes: 67108864
  interval_seconds: 3600
ducklake:
  catalog_type: "postgres"
  metadata_path: "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake"
  data_path: "${data_path}"
  catalog_alias: "softprobe"
  metadata_schema: "${schema}"
  data_inlining_row_limit: ${inline}
  writer_pool_size: ${pool}
dropdown_catalog:
  enabled: false
EOF

  export CONFIG_FILE="$cfg"
  export SPLAKE_RESET_DUCKLAKE=1
  ./target/release/softprobe-runtime > "$log_rt" 2>&1 &
  echo $! > /tmp/splake-pool-inline.pid
  local i
  for i in $(seq 1 45); do
    if curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null; then
      break
    fi
    if ! kill -0 "$(cat /tmp/splake-pool-inline.pid)" 2>/dev/null; then
      echo "runtime died"; tail -40 "$log_rt"; exit 1
    fi
    sleep 1
  done
  curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null || { echo fail; tail -40 "$log_rt"; exit 1; }

  curl -sf -X POST "http://127.0.0.1:${PORT}/v1/tenants" \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer admin-stress-key' \
    -d "$(python3 - <<PY
import json
print(json.dumps({
  "tenantId": "local-dev-tenant",
  "storageHints": {
    "ducklakeMetadataSchema": "${schema}_tenant",
    "ducklakeDataPath": "${data_path}",
    "gcsBucket": "softprobe-datalake-ducklake"
  }
}))
PY
)" >/dev/null

  unset SPLAKE_RESET_DUCKLAKE
  ./target/release/perf_stress \
    --service-url "http://127.0.0.1:${PORT}" \
    --api-token test-token \
    --phases ingest,query,mixed \
    --duration 60 \
    --warmup-secs 20 \
    --span-qps 500 \
    --log-qps 1000 \
    --metric-qps 1000 \
    --batch-size "$batch" \
    --ingest-concurrency 16 \
    --query-concurrency 1 \
    --query-interval-ms 2000 \
    > "$log_perf" 2>&1

  local parquet_n
  parquet_n=$(gsutil ls -r "${data_path}" 2>/dev/null | grep -c '\.parquet$' || true)

  CELL="$cell" POOL="$pool" INLINE="$inline" BATCH="$batch" PARQUET_N="$parquet_n" LOG_PERF="$log_perf" \
  python3 <<'PY' >> "$SUMMARY"
import os, re
cell=os.environ["CELL"]; pool=os.environ["POOL"]; inline=os.environ["INLINE"]
batch=os.environ["BATCH"]; parquet_n=os.environ["PARQUET_N"]
text=open(os.environ["LOG_PERF"]).read()
def section(name):
    marker = f"Phase Report: {name}"
    i = text.find(marker)
    if i < 0:
        return ""
    j = text.find("Phase Report:", i + len(marker))
    k = text.find("Interference Summary", i)
    end = len(text)
    for cand in (j, k):
        if cand > i:
            end = min(end, cand)
    return text[i:end]
ingest, query, mixed = section("ingest_only"), section("query_only"), section("mixed")
def m1(sec, pat, default="NA"):
    m=re.search(pat, sec, re.S)
    return m.group(1) if m else default
eps=m1(ingest, r"Total ingest: offered=[\d.]+/s achieved=([\d.]+)/s")
sp95=m1(ingest, r"span:.*?p95=(\d+)ms")
qp95=m1(query, r"Steady-state query latency:.*?p95=(\d+)ms")
mp95=m1(mixed, r"Steady-state query latency:.*?p95=(\d+)ms")
errs=0
for sig in ("span","log","metric"):
    m=re.search(rf"{sig}:.*?http_errors=(\d+)", ingest, re.S)
    if m: errs += int(m.group(1))
print(f"{cell}\t{pool}\t{inline}\t{batch}\t{eps}\t{sp95}\t{qp95}\t{mp95}\t{parquet_n}\t{errs}")
PY

  echo "cell ${cell} done; parquet=${parquet_n}"
  stop_runtime
}

# cells: A0 A1 B1 B2 C1 D1 D2
run_cell A0 1 0 200
run_cell A1 1 10000 200
run_cell B1 4 10000 200
run_cell B2 8 10000 200
run_cell C1 4 50000 200
run_cell D1 4 10000 5000
run_cell D2 4 50000 5000

echo ""
echo "======== SUMMARY ========"
column -t -s $'\t' "$SUMMARY" || cat "$SUMMARY"
echo "Full logs under ${RESULTS_DIR}"
