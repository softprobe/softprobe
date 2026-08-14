#!/usr/bin/env bash
# Softprobe Prom micro-benchmark (Option A): host Softprobe + hostmetrics OTLP + curated PromQL.
#
# Usage (repo root):
#   ./scripts/bench-prom-baseline.sh
#   ./scripts/bench-prom-baseline.sh --self-check
#   BENCH_LABEL=variant-pushdown ./scripts/bench-prom-baseline.sh
#   BENCH_FORCE_PARQUET=1 BENCH_LABEL=small-files make bench-prom-baseline
#   LEAVE_UP=1 ./scripts/bench-prom-baseline.sh
#
# Make: make bench-prom-baseline

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose}"
STATE_DIR="${THELAKE_BENCH_STATE_DIR:-/tmp/thelake-prom-bench}"
BENCH_DIR="$ROOT/tests/compat/prometheus/benchmark"
COMPOSE_FILE="$BENCH_DIR/docker-compose.yml"
COMPOSE_CHURN="$BENCH_DIR/docker-compose.churn.yml"
COMPOSE_PROJECT="${THELAKE_BENCH_COMPOSE_PROJECT:-thelake-prom-bench}"
QUERIES_FILE="${BENCH_QUERIES_FILE:-$BENCH_DIR/queries.promql}"
RESULTS_DIR="${BENCH_RESULTS_DIR:-$ROOT/docs/perf/results}"
LOG="$STATE_DIR/softprobe.log"
PID_FILE="$STATE_DIR/softprobe.pid"
CONFIG="$STATE_DIR/config.yaml"
AUTH_URL="${SOFTPROBE_AUTH_URL:-http://127.0.0.1:18080/validate}"
API_KEY="${SOFTPROBE_API_KEY:-local-dev-key}"
SOFTPROBE_URL_HOST="${SOFTPROBE_LISTEN:-http://127.0.0.1:8090}"
WARMUP_SECS="${BENCH_WARMUP_SECS:-20}"
MEASURE_SECS="${BENCH_MEASURE_SECS:-60}"
REPEAT="${BENCH_REPEAT:-3}"
LABEL="${BENCH_LABEL:-baseline}"
LEAVE_UP="${LEAVE_UP:-0}"
FORCE_PARQUET="${BENCH_FORCE_PARQUET:-0}"
CARDINALITY="${BENCH_CARDINALITY:-0}"
INSTANCES_PER_JOB="${BENCH_INSTANCES:-3}"
INLINE_LIMIT=10000
if [[ "$FORCE_PARQUET" == "1" || "$CARDINALITY" -gt 0 ]]; then
  INLINE_LIMIT=0
fi
if [[ "$CARDINALITY" -gt 0 ]]; then
  QUERIES_FILE="${BENCH_QUERIES_FILE:-$BENCH_DIR/queries.card.promql}"
  if [[ "$LABEL" == "baseline" ]]; then
    LABEL="killcase"
  fi
fi

compose_up() {
  if [[ "$CARDINALITY" -gt 0 ]]; then
    # Auth only — loadgen supplies high-cardinality metrics (no hostmetrics noise).
    # shellcheck disable=SC2086
    $COMPOSE -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d auth-mock
  elif [[ "$FORCE_PARQUET" == "1" ]]; then
    # shellcheck disable=SC2086
    $COMPOSE -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" -f "$COMPOSE_CHURN" up -d
  else
    # shellcheck disable=SC2086
    $COMPOSE -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d
  fi
}

port_busy() {
  local port="$1"
  ss -ltn 2>/dev/null | grep -qE ":${port}\\s" || return 1
}

our_softprobe_running() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  [[ -n "${pid:-}" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  local cmd
  cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  [[ "$cmd" == *softprobe-runtime* ]] || return 1
  return 0
}

self_check() {
  echo "==> self-check"
  [[ -f "$QUERIES_FILE" ]] || { echo "missing $QUERIES_FILE" >&2; exit 1; }
  [[ -f "$COMPOSE_FILE" ]] || { echo "missing $COMPOSE_FILE" >&2; exit 1; }
  [[ -f "$BENCH_DIR/otelcol-config.yaml" ]] || { echo "missing otelcol config" >&2; exit 1; }
  [[ -f "$BENCH_DIR/otelcol-config.churn.yaml" ]] || { echo "missing otelcol churn config" >&2; exit 1; }
  [[ -f "$COMPOSE_CHURN" ]] || { echo "missing $COMPOSE_CHURN" >&2; exit 1; }
  local n=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    n=$((n + 1))
  done <"$QUERIES_FILE"
  [[ "$n" -ge 1 ]] || { echo "no queries in $QUERIES_FILE" >&2; exit 1; }
  command -v curl >/dev/null || { echo "curl required" >&2; exit 1; }
  command -v python3 >/dev/null || { echo "python3 required" >&2; exit 1; }
  docker info >/dev/null 2>&1 || { echo "Docker required for full run (self-check of files OK)" >&2; }
  echo "self-check ok ($n queries)"
}

# Prefer hostmetrics-style names so a leftover Astronomy Shop collector cannot
# silently become the series under test.
pick_metric() {
  python3 -c '
import json,sys
d=json.load(sys.stdin)
names=d.get("data") or []
prefs=("system_","process_","system.","process.")
for n in names:
    if any(n.startswith(p) for p in prefs):
        print(n); raise SystemExit
print(names[0] if names else "")
'
}

reject_foreign_otlp() {
  if [[ "${BENCH_ALLOW_FOREIGN_OTLP:-0}" == "1" ]]; then
    return 0
  fi
  # Official demo container is named `otel-collector`; ours is `thelake-prom-bench-otelcol`.
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx 'otel-collector'; then
    echo "ERROR: container 'otel-collector' is running (likely OpenTelemetry Demo)." >&2
    echo "  It exports to host :8090 and will contaminate this micro-benchmark." >&2
    echo "  Stop it: make grafana-down" >&2
    echo "  Or set BENCH_ALLOW_FOREIGN_OTLP=1 to proceed anyway." >&2
    exit 1
  fi
}

if [[ "${1:-}" == "--self-check" ]]; then
  self_check
  exit 0
fi

self_check

reject_foreign_otlp

mkdir -p "$STATE_DIR/data" "$STATE_DIR/cache" "$RESULTS_DIR"

if port_busy 8090 && ! our_softprobe_running; then
  echo "ERROR: :8090 is in use by another process. Stop it (make grafana-down / bench-prom-down)." >&2
  exit 1
fi
if port_busy 18080 && ! curl -sf -X POST "$AUTH_URL" -H 'Content-Type: application/json' -d '{}' >/dev/null 2>&1; then
  echo "ERROR: :18080 busy but auth-mock not healthy. Free the port or make bench-prom-down." >&2
  exit 1
fi

# Fresh Softprobe process + data for a clean baseline.
if our_softprobe_running; then
  echo "==> stopping previous bench Softprobe"
  ./scripts/bench-prom-down.sh
fi

rm -rf "$STATE_DIR/data" "$STATE_DIR/cache"
rm -f "$STATE_DIR/metadata.sqlite"
mkdir -p "$STATE_DIR/data" "$STATE_DIR/cache"

cat >"$CONFIG" <<EOF
server:
  port: 8090
  host: "0.0.0.0"
  max_body_size: 104857600
  worker_threads: null

object_store:
  region: "us-east-1"
  endpoint: null

query:
  max_connections: 4
  cache_dir: "$STATE_DIR/cache"

maintenance:
  enabled: true
  target_file_size_bytes: 67108864
  interval_seconds: 300
  metadata_enabled: true
  metadata_interval_seconds: 300
  remove_orphan_files_enabled: true
  remove_orphan_older_than_seconds: 0

ducklake:
  catalog_type: "sqlite"
  metadata_path: "$STATE_DIR/metadata.sqlite"
  data_path: "$STATE_DIR/data/"
  catalog_alias: "softprobe"
  metadata_schema: "main"
  data_inlining_row_limit: $INLINE_LIMIT
  writer_pool_size: 2

dropdown_catalog:
  enabled: false
EOF

if [[ "$FORCE_PARQUET" == "1" ]]; then
  echo "==> BENCH_FORCE_PARQUET=1 (data_inlining_row_limit=0, 1s hostmetrics + tiny batches)"
fi

echo "==> building softprobe-runtime (+ loadgen + maintenance)"
cargo build -q --bin softprobe-runtime --bin ingest_sample --bin bench_prom_loadgen

RUNTIME_BIN="${CARGO_TARGET_DIR:-target}/debug/softprobe-runtime"
INGEST_SAMPLE_BIN="${CARGO_TARGET_DIR:-target}/debug/ingest_sample"
LOADGEN_BIN="${CARGO_TARGET_DIR:-target}/debug/bench_prom_loadgen"
if [[ ! -x "$RUNTIME_BIN" ]]; then
  echo "ERROR: missing $RUNTIME_BIN" >&2
  exit 1
fi
if [[ ! -x "$INGEST_SAMPLE_BIN" ]]; then
  echo "ERROR: missing $INGEST_SAMPLE_BIN" >&2
  exit 1
fi
if [[ "$CARDINALITY" -gt 0 && ! -x "$LOADGEN_BIN" ]]; then
  echo "ERROR: missing $LOADGEN_BIN" >&2
  exit 1
fi

echo "==> starting auth-mock + otel-collector"
compose_up

echo "==> waiting for auth-mock"
auth_ok=0
for _ in $(seq 1 40); do
  if curl -sf -X POST "$AUTH_URL" -H 'Content-Type: application/json' -d '{}' >/dev/null 2>&1; then
    auth_ok=1
    break
  fi
  sleep 0.5
done
if [[ "$auth_ok" != 1 ]]; then
  echo "ERROR: auth-mock did not become ready at $AUTH_URL" >&2
  exit 1
fi

TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
DUCKDB_LIB_DIR="$(find "${TARGET_DIR}/duckdb-download" -type f \( -name 'libduckdb.so*' -o -name 'libduckdb.dylib*' \) -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
if [[ -z "${DUCKDB_LIB_DIR}" ]]; then
  echo "ERROR: libduckdb not found under ${TARGET_DIR}/duckdb-download" >&2
  exit 1
fi
case "$(uname -s)" in
  Darwin) export DYLD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}" ;;
  *) export LD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" ;;
esac

echo "==> starting Softprobe on :8090"
export CONFIG_FILE="$CONFIG"
export SOFTPROBE_AUTH_URL="$AUTH_URL"
export SOFTPROBE_GRPC_DISABLE=1
export RUST_LOG="${RUST_LOG:-warn}"
: >"$LOG"
nohup "$RUNTIME_BIN" >>"$LOG" 2>&1 &
echo $! >"$PID_FILE"
disown || true
SOFTPROBE_PID="$(cat "$PID_FILE")"

echo "==> waiting for Softprobe /ready"
ok=0
for _ in $(seq 1 60); do
  if curl -sf "$SOFTPROBE_URL_HOST/ready" >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 0.5
done
if [[ "$ok" != 1 ]]; then
  echo "ERROR: Softprobe did not become ready; log: $LOG" >&2
  tail -40 "$LOG" >&2 || true
  ./scripts/bench-prom-down.sh || true
  exit 1
fi

job="svc-000"
instance="svc-000-i0"
if [[ "$CARDINALITY" -gt 0 ]]; then
  echo "==> high-cardinality loadgen jobs=${CARDINALITY} instances=${INSTANCES_PER_JOB} for ${WARMUP_SECS}s"
  "$LOADGEN_BIN" \
    --url "$SOFTPROBE_URL_HOST" \
    --token "$API_KEY" \
    --metric "bench.http.requests" \
    --jobs "$CARDINALITY" \
    --instances "$INSTANCES_PER_JOB" \
    --seconds "$WARMUP_SECS" \
    --interval 0.4
  metric="bench_http_requests"
  # Prefer sanitized Prom name; fall back to dotted storage name if needed.
  body="$(curl -sf -H "Authorization: Bearer $API_KEY" \
    "$SOFTPROBE_URL_HOST/api/v1/label/__name__/values" 2>/dev/null || true)"
  if echo "$body" | grep -q 'bench_http_requests'; then
    metric="bench_http_requests"
  elif echo "$body" | grep -q 'bench.http.requests'; then
    metric="bench.http.requests"
  fi
  mid=$((CARDINALITY / 2))
  job="$(printf 'svc-%03d' "$mid")"
  instance="${job}-i0"
else
  echo "==> warm-up ${WARMUP_SECS}s (hostmetrics ingest)"
  metric=""
  deadline=$((SECONDS + WARMUP_SECS))
  while (( SECONDS < deadline )); do
    body="$(curl -sf -H "Authorization: Bearer $API_KEY" \
      "$SOFTPROBE_URL_HOST/api/v1/label/__name__/values" 2>/dev/null || true)"
    if [[ -n "$body" && "$body" == *'"status":"success"'* && "$body" == *'"data":['* ]]; then
      metric="$(pick_metric <<<"$body")"
      if [[ -n "$metric" ]]; then
        echo "==> discovered metric: $metric"
        break
      fi
    fi
    sleep 2
  done
  remain=$((deadline - SECONDS))
  if (( remain > 0 )); then
    sleep "$remain"
  fi
  body="$(curl -sf -H "Authorization: Bearer $API_KEY" \
    "$SOFTPROBE_URL_HOST/api/v1/label/__name__/values" 2>/dev/null || true)"
  metric="$(pick_metric <<<"$body")"
fi

if [[ -z "$metric" ]]; then
  echo "ERROR: no metrics after warm-up. last names: ${body:-<empty>}" >&2
  echo "  softprobe: tail -40 $LOG" >&2
  ./scripts/bench-prom-down.sh || true
  exit 1
fi
echo "==> measuring against metric=$metric job=$job (${MEASURE_SECS}s, repeat=$REPEAT)"

RAW_LAT="$STATE_DIR/latencies.jsonl"
: >"$RAW_LAT"

now_s() { date +%s; }

measure_deadline=$(( $(now_s) + MEASURE_SECS ))
rounds=0
while (( $(now_s) < measure_deadline )); do
  rounds=$((rounds + 1))
  while IFS= read -r qline || [[ -n "$qline" ]]; do
    [[ -z "$qline" || "$qline" =~ ^[[:space:]]*# ]] && continue
    query="${qline//\{\{metric\}\}/$metric}"
    query="${query//\{\{job\}\}/$job}"
    query="${query//\{\{instance\}\}/$instance}"
    for _ in $(seq 1 "$REPEAT"); do
      end="$(now_s)"
      start=$((end - 120))
      # Prefer query_range (Grafana-like). Prometheus float unix seconds.
      t0="$(python3 -c 'import time; print(time.perf_counter())')"
      http_code="$(curl -sS -o "$STATE_DIR/last_body.json" -w '%{http_code}' \
        -H "Authorization: Bearer $API_KEY" \
        -G "$SOFTPROBE_URL_HOST/api/v1/query_range" \
        --data-urlencode "query=$query" \
        --data-urlencode "start=${start}" \
        --data-urlencode "end=${end}" \
        --data-urlencode "step=15" \
        || echo "000")"
      t1="$(python3 -c 'import time; print(time.perf_counter())')"
      ms="$(python3 -c "print(int(($t1 - $t0) * 1000))")"
      ok=0
      if [[ "$http_code" == "200" ]]; then
        if python3 -c '
import json,sys
p=sys.argv[1]
try:
    d=json.load(open(p))
    sys.exit(0 if d.get("status")=="success" else 1)
except Exception:
    sys.exit(1)
' "$STATE_DIR/last_body.json"; then
          ok=1
        fi
      fi
      python3 -c '
import json,sys
print(json.dumps({
  "query": sys.argv[1],
  "ms": int(sys.argv[2]),
  "ok": sys.argv[3] == "1",
  "http": sys.argv[4],
}))
' "$query" "$ms" "$ok" "$http_code" >>"$RAW_LAT"
    done
  done <"$QUERIES_FILE"
done

rss_kb="0"
if [[ -r "/proc/$SOFTPROBE_PID/status" ]]; then
  rss_kb="$(awk '/VmRSS:/ {print $2}' "/proc/$SOFTPROBE_PID/status")"
fi
data_bytes="$(python3 -c '
import os,sys
root=sys.argv[1]
total=0
for dirpath,_,files in os.walk(root):
    for f in files:
        try:
            total += os.path.getsize(os.path.join(dirpath,f))
        except OSError:
            pass
print(total)
' "$STATE_DIR/data")"
parquet_before="$(find "$STATE_DIR/data" -type f \( -name '*.parquet' -o -name '*.parq' \) 2>/dev/null | wc -l | tr -d ' ')"
file_count="$(find "$STATE_DIR/data" -type f 2>/dev/null | wc -l | tr -d ' ')"

echo "==> running DuckLake maintenance once (merge adjacent files)"
# Pause Softprobe writes briefly by stopping otelcol so merge is less contested.
# shellcheck disable=SC2086
$COMPOSE -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" stop otel-collector >/dev/null 2>&1 || true
sleep 1
export CONFIG_FILE="$CONFIG"
export LD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
MAINTENANCE_RUN_ONCE=1 "$INGEST_SAMPLE_BIN" || echo "WARN: maintenance run reported failure" >&2
parquet_after="$(find "$STATE_DIR/data" -type f \( -name '*.parquet' -o -name '*.parq' \) 2>/dev/null | wc -l | tr -d ' ')"
echo "==> parquet files before compact=${parquet_before} after=${parquet_after}"
parquet_files="$parquet_after"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_JSON="$RESULTS_DIR/${STAMP}-${LABEL}.json"
OUT_MD="$RESULTS_DIR/${STAMP}-${LABEL}.md"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

STAMP="$STAMP" LABEL="$LABEL" GIT_SHA="$GIT_SHA" METRIC="$metric" \
WARMUP_SECS="$WARMUP_SECS" MEASURE_SECS="$MEASURE_SECS" REPEAT="$REPEAT" ROUNDS="$rounds" \
RSS_KB="$rss_kb" DATA_BYTES="$data_bytes" FILE_COUNT="$file_count" PARQUET_FILES="$parquet_files" \
PARQUET_BEFORE="$parquet_before" PARQUET_AFTER="$parquet_after" FORCE_PARQUET="$FORCE_PARQUET" \
python3 - "$RAW_LAT" "$OUT_JSON" "$OUT_MD" <<'PY'
import json, os, sys

raw_path, out_json, out_md = sys.argv[1], sys.argv[2], sys.argv[3]
rows = []
with open(raw_path) as f:
    for line in f:
        line = line.strip()
        if line:
            rows.append(json.loads(line))

by_q = {}
for r in rows:
    by_q.setdefault(r["query"], []).append(r)

def pct(xs, p):
    if not xs:
        return None
    ys = sorted(xs)
    i = min(len(ys) - 1, max(0, int(round((p / 100.0) * (len(ys) - 1)))))
    return ys[i]

per_query = []
all_ms = []
ok_n = 0
for q, rs in sorted(by_q.items(), key=lambda kv: kv[0]):
    ms = [r["ms"] for r in rs]
    oks = sum(1 for r in rs if r["ok"])
    ok_n += oks
    all_ms.extend(ms)
    per_query.append({
        "query": q,
        "n": len(rs),
        "ok": oks,
        "p50_ms": pct(ms, 50),
        "p95_ms": pct(ms, 95),
        "max_ms": max(ms) if ms else None,
    })

env = {
    "stamp": os.environ["STAMP"],
    "label": os.environ["LABEL"],
    "git_sha": os.environ["GIT_SHA"],
    "metric": os.environ["METRIC"],
    "warmup_secs": int(os.environ["WARMUP_SECS"]),
    "measure_secs": int(os.environ["MEASURE_SECS"]),
    "repeat": int(os.environ["REPEAT"]),
    "rounds": int(os.environ["ROUNDS"]),
    "softprobe_rss_kb": int(os.environ["RSS_KB"]),
    "data_bytes": int(os.environ["DATA_BYTES"]),
    "data_file_count": int(os.environ["FILE_COUNT"]),
    "parquet_file_count": int(os.environ["PARQUET_FILES"]),
    "parquet_before_compact": int(os.environ["PARQUET_BEFORE"]),
    "parquet_after_compact": int(os.environ["PARQUET_AFTER"]),
    "force_parquet": os.environ.get("FORCE_PARQUET") == "1",
    "total_requests": len(rows),
    "ok_requests": ok_n,
    "overall_p50_ms": pct(all_ms, 50),
    "overall_p95_ms": pct(all_ms, 95),
    "overall_max_ms": max(all_ms) if all_ms else None,
    "per_query": per_query,
}
with open(out_json, "w") as f:
    json.dump(env, f, indent=2)
    f.write("\n")

lines = [
    f"# Prom micro-benchmark — `{env['label']}`",
    "",
    f"- UTC: `{env['stamp']}`",
    f"- Git: `{env['git_sha']}`",
    f"- Metric under test: `{env['metric']}`",
    f"- Warm-up / measure: {env['warmup_secs']}s / {env['measure_secs']}s (repeat={env['repeat']}, rounds={env['rounds']})",
    f"- Requests: {env['ok_requests']}/{env['total_requests']} ok",
    f"- Latency overall: p50={env['overall_p50_ms']}ms p95={env['overall_p95_ms']}ms max={env['overall_max_ms']}ms",
    f"- Softprobe RSS: {env['softprobe_rss_kb']} KiB",
    f"- Data dir: {env['data_bytes']} bytes, {env['data_file_count']} files",
    f"- Parquet: before compact={env['parquet_before_compact']} after={env['parquet_after_compact']} (force_parquet={env['force_parquet']})",
    "",
    "| Query | n | ok | p50 ms | p95 ms | max ms |",
    "|-------|---|----|--------|--------|--------|",
]
for pq in per_query:
    q = pq["query"].replace("|", "\\|")
    lines.append(
        f"| `{q}` | {pq['n']} | {pq['ok']} | {pq['p50_ms']} | {pq['p95_ms']} | {pq['max_ms']} |"
    )
lines += ["", "Harness: `tests/compat/prometheus/benchmark/` (Option A).", ""]
with open(out_md, "w") as f:
    f.write("\n".join(lines))
print(out_json)
print(out_md)
if env["total_requests"] and env["ok_requests"] == 0:
    raise SystemExit(
        "ERROR: 0 successful Prom responses — latency numbers are not meaningful "
        "(likely SQL/cast regression). See /tmp/thelake-prom-bench/last_body.json"
    )
PY

echo ""
echo "Results:"
echo "  $OUT_JSON"
echo "  $OUT_MD"
cat "$OUT_MD"

if [[ "$LEAVE_UP" == "1" ]]; then
  echo ""
  echo "LEAVE_UP=1 — Softprobe + otelcol still running. Teardown: make bench-prom-down"
else
  ./scripts/bench-prom-down.sh
fi
