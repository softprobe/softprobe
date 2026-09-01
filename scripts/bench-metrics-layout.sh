#!/usr/bin/env bash
# Metrics-layout PERF_SUITE harness (§10.3).
# Invoked only via: PERF_SUITE=metrics-layout make test-perf
#
# Env:
#   METRICS_LAYOUT_PROFILE=pr_floor|release_full  (default pr_floor)
#   COMPARE_GREPTIME=0|1
#   GREPTIME_BIN — path to greptime binary (optional; auto-detected when COMPARE_GREPTIME=1)
#   GREPTIME_URL — already-running Greptime HTTP base (optional; skips local start)
#   GREPTIME_SRC — sibling clone root (default: $ROOT/../greptime)
#   CARGO_PROFILE_FLAG=|--release
#   PERF_LAYOUT_GOAL_SECS (enforced by Makefile)
#   LEAVE_UP=1 to keep Softprobe/infra after run
#
# Greptime binary discovery (COMPARE_GREPTIME=1), first hit wins:
#   1) GREPTIME_BIN if set and executable
#   2) $GREPTIME_SRC/target/release/greptime
#   3) $ROOT/../greptime/target/release/greptime
# Build sibling: (cd ../greptime && make build RELEASE=true)
# Pin: workspace greptime @ a8924bb… (or GREPTIME_GIT_SHA from that clone).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose}"
STATE_DIR="${THELAKE_LAYOUT_STATE_DIR:-/tmp/thelake-metrics-layout}"
# Guard against stale smoke/debug state dirs from prior experiments.
if [[ "$STATE_DIR" == *smoke* ]]; then
  STATE_DIR="/tmp/thelake-metrics-layout"
fi
COMPOSE_FILE="$ROOT/tests/compat/prometheus/layout/docker-compose.yml"
COMPOSE_PROJECT="${THELAKE_LAYOUT_COMPOSE_PROJECT:-thelake-metrics-layout}"
RESULTS_DIR="$ROOT/docs/perf/results"
LOG="$STATE_DIR/softprobe.log"
PID_FILE="$STATE_DIR/softprobe.pid"
AUTH_PID_FILE="$STATE_DIR/auth.pid"
SENDER_PID_FILE="$STATE_DIR/heartbeat.pid"
CONFIG="$STATE_DIR/config.yaml"

PROFILE="${METRICS_LAYOUT_PROFILE:-pr_floor}"
COMPARE_GREPTIME="${COMPARE_GREPTIME:-0}"
CARGO_PROFILE_FLAG="${CARGO_PROFILE_FLAG:-}"
LEAVE_UP="${LEAVE_UP:-0}"

SP_PORT="${LAYOUT_SP_PORT:-18091}"
AUTH_PORT="${LAYOUT_AUTH_HOST_PORT:-18081}"
PG_PORT="${LAYOUT_PG_HOST_PORT:-5435}"
# Always bind auth URL to AUTH_PORT (ignore stale SOFTPROBE_AUTH_URL from parent env).
AUTH_URL="http://127.0.0.1:${AUTH_PORT}/validate"
API_KEY="${SOFTPROBE_API_KEY:-local-dev-key}"
SOFTPROBE_URL="http://127.0.0.1:${SP_PORT}"
PG_SCHEMA="${LAYOUT_PG_SCHEMA:-metrics_layout}"
TENANT_ID="${LAYOUT_TENANT_ID:-local-dev-tenant}"

export LAYOUT_AUTH_HOST_PORT="$AUTH_PORT"
export LAYOUT_PG_HOST_PORT="$PG_PORT"
export THELAKE_LAYOUT_STATE_DIR="$STATE_DIR"
export SOFTPROBE_AUTH_URL="$AUTH_URL"

mkdir -p "$STATE_DIR/data/$TENANT_ID" "$STATE_DIR/cache" "$STATE_DIR/postgres" "$RESULTS_DIR"

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

stop_owned() {
  if [[ -f "$SENDER_PID_FILE" ]]; then
    sp="$(cat "$SENDER_PID_FILE" 2>/dev/null || true)"
    if [[ -n "${sp:-}" ]]; then
      kill "$sp" 2>/dev/null || true
      wait "$sp" 2>/dev/null || true
    fi
    rm -f "$SENDER_PID_FILE"
  fi
  if [[ -f "$AUTH_PID_FILE" ]]; then
    ap="$(cat "$AUTH_PID_FILE" 2>/dev/null || true)"
    if [[ -n "${ap:-}" ]]; then
      kill "$ap" 2>/dev/null || true
      wait "$ap" 2>/dev/null || true
    fi
    rm -f "$AUTH_PID_FILE"
  fi
  if [[ -f "$PID_FILE" ]]; then
    old="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "${old:-}" ]] && kill -0 "$old" 2>/dev/null; then
      kill "$old" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$old" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$old" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  # shellcheck disable=SC2086
  $COMPOSE -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true
}

cleanup() {
  if [[ "$LEAVE_UP" == "1" ]]; then
    echo "==> LEAVE_UP=1 — Softprobe/infra left running ($STATE_DIR)"
    return 0
  fi
  stop_owned
}
trap cleanup EXIT

echo "==> metrics-layout profile=$PROFILE compare_greptime=$COMPARE_GREPTIME cargo_profile=${CARGO_PROFILE_FLAG:-dev}"

if [[ "$PROFILE" != "pr_floor" && "$PROFILE" != "release_full" ]]; then
  echo "ERROR: METRICS_LAYOUT_PROFILE must be pr_floor|release_full" >&2
  exit 1
fi

if port_busy "$SP_PORT" && ! our_softprobe_running; then
  echo "ERROR: :$SP_PORT in use. Free it or set LAYOUT_SP_PORT." >&2
  exit 1
fi

stop_owned
rm -rf "$STATE_DIR/data" "$STATE_DIR/cache"
# Postgres files are root-owned from the container user — wipe via docker with a hard timeout.
if [[ -d "$STATE_DIR/postgres" ]]; then
  timeout 90 docker run --rm -v "$STATE_DIR/postgres:/data" alpine:3.20 \
    sh -c 'rm -rf /data/* /data/.[!.]* /data/..?*' >/dev/null 2>&1 \
    || rm -rf "$STATE_DIR/postgres" 2>/dev/null \
    || true
  rm -rf "$STATE_DIR/postgres" 2>/dev/null || true
fi
mkdir -p "$STATE_DIR/data/$TENANT_ID" "$STATE_DIR/cache" "$STATE_DIR/postgres"

echo "==> building softprobe-runtime + layout_otlp_fixture ${CARGO_PROFILE_FLAG:-}"
# shellcheck disable=SC2086
cargo build -q $CARGO_PROFILE_FLAG --bin softprobe-runtime --bin layout_otlp_fixture

TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
if [[ "$CARGO_PROFILE_FLAG" == "--release" ]]; then
  RUNTIME_BIN="$TARGET_DIR/release/softprobe-runtime"
  FIXTURE_BIN="$TARGET_DIR/release/layout_otlp_fixture"
else
  RUNTIME_BIN="$TARGET_DIR/debug/softprobe-runtime"
  FIXTURE_BIN="$TARGET_DIR/debug/layout_otlp_fixture"
fi
if [[ ! -x "$RUNTIME_BIN" ]]; then
  echo "ERROR: missing $RUNTIME_BIN" >&2
  exit 1
fi
if [[ ! -x "$FIXTURE_BIN" ]]; then
  echo "ERROR: missing $FIXTURE_BIN" >&2
  exit 1
fi
export LAYOUT_OTLP_FIXTURE_BIN="$FIXTURE_BIN"

echo "==> starting Postgres catalog"
# shellcheck disable=SC2086
$COMPOSE -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d

echo "==> starting host auth-mock on :$AUTH_PORT"
python3 - "$AUTH_PORT" <<'PY' &
import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(sys.argv[1])
body = json.dumps({
    "success": True,
    "data": {
        "tenantId": "local-dev-tenant",
        "resources": [{
            "resourceType": "BIGQUERY_STORAGE",
            "configJson": "{\"dataset_id\":\"local\",\"bucket_name\":\"warehouse\"}",
        }],
    },
}).encode()

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")
    def log_message(self, *args):
        pass

HTTPServer(("127.0.0.1", port), H).serve_forever()
PY
echo $! >"$AUTH_PID_FILE"

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
  echo "ERROR: auth-mock not ready at $AUTH_URL" >&2
  exit 1
fi

echo "==> waiting for Postgres :$PG_PORT"
pg_ok=0
for _ in $(seq 1 60); do
  if docker inspect -f '{{.State.Health.Status}}' thelake-metrics-layout-postgres 2>/dev/null | grep -q healthy; then
    pg_ok=1
    break
  fi
  sleep 0.5
done
if [[ "$pg_ok" != 1 ]]; then
  echo "ERROR: Postgres not healthy" >&2
  exit 1
fi

cat >"$CONFIG" <<EOF
server:
  port: $SP_PORT
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
  enabled: false
  target_file_size_bytes: 67108864
  interval_seconds: 300
  metadata_enabled: false
  metadata_interval_seconds: 300
  remove_orphan_files_enabled: false
  remove_orphan_older_than_seconds: 0
  max_snapshot_age_seconds: 3600

ducklake:
  catalog_type: "postgres"
  metadata_path: "host=127.0.0.1 port=$PG_PORT dbname=ducklake user=ducklake password=ducklake"
  data_path: "$STATE_DIR/data/"
  catalog_alias: "softprobe"
  metadata_schema: "$PG_SCHEMA"
  data_inlining_row_limit: 0
  writer_pool_size: 1

dropdown_catalog:
  enabled: false
EOF

DUCKDB_LIB_DIR="$(find "${TARGET_DIR}/duckdb-download" -type f \( -name 'libduckdb.so*' -o -name 'libduckdb.dylib*' \) -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
if [[ -z "${DUCKDB_LIB_DIR}" ]]; then
  echo "ERROR: libduckdb not found under ${TARGET_DIR}/duckdb-download" >&2
  exit 1
fi
case "$(uname -s)" in
  Darwin) export DYLD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}" ;;
  *) export LD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" ;;
esac

echo "==> starting Softprobe on :$SP_PORT ($RUNTIME_BIN)"
export CONFIG_FILE="$CONFIG"
export SOFTPROBE_AUTH_URL="$AUTH_URL"
export SOFTPROBE_ADMIN_API_KEY="${SOFTPROBE_ADMIN_API_KEY:-local-dev-admin-key}"
export SOFTPROBE_GRPC_DISABLE=1
export RUST_LOG="${RUST_LOG:-warn}"
: >"$LOG"
nohup "$RUNTIME_BIN" >>"$LOG" 2>&1 &
echo $! >"$PID_FILE"
disown || true

echo "==> waiting for Softprobe /ready"
ok=0
for _ in $(seq 1 90); do
  if curl -sf "$SOFTPROBE_URL/ready" >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 0.5
done
if [[ "$ok" != 1 ]]; then
  echo "ERROR: Softprobe not ready; tail $LOG" >&2
  tail -60 "$LOG" >&2 || true
  exit 1
fi

ADMIN_API_KEY="${SOFTPROBE_ADMIN_API_KEY:-local-dev-admin-key}"
TENANT_SCHEMA="${PG_SCHEMA}_${TENANT_ID//-/_}"
echo "==> provisioning tenant $TENANT_ID (Postgres catalog schema $TENANT_SCHEMA)"
export SOFTPROBE_ADMIN_API_KEY="$ADMIN_API_KEY"
tenant_payload="$(TENANT_ID="$TENANT_ID" TENANT_SCHEMA="$TENANT_SCHEMA" TENANT_DATA_PATH="$STATE_DIR/data/$TENANT_ID/" python3 - <<'PY'
import json, os
print(json.dumps({
    "tenantId": os.environ["TENANT_ID"],
    "storageHints": {
        "ducklakeMetadataSchema": os.environ["TENANT_SCHEMA"],
        "ducklakeDataPath": os.environ["TENANT_DATA_PATH"],
        "gcsBucket": "warehouse",
    },
}))
PY
)"
tenant_http="$(curl -sS -o "$STATE_DIR/tenant-provision.json" -w '%{http_code}' \
  -X POST "$SOFTPROBE_URL/v1/tenants" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d "$tenant_payload" || true)"
if [[ "$tenant_http" != "200" && "$tenant_http" != "201" ]]; then
  echo "ERROR: tenant provision HTTP $tenant_http — $(cat "$STATE_DIR/tenant-provision.json" 2>/dev/null || true)" >&2
  exit 1
fi

# Create layout tables once before concurrent heartbeat + fixture load.
echo "==> priming metrics layout tables (single OTLP write)"
now_ns="$(date +%s%N)"
printf '%s\n' "{\"name\":\"layout_prime\",\"kind\":\"gauge\",\"labels\":{\"job\":\"prime\",\"instance\":\"p0\"},\"points\":[[${now_ns},1.0]]}" \
  | "$FIXTURE_BIN" --url "$SOFTPROBE_URL" --token "$API_KEY"

# OTLP heartbeat sender (AC-Q0) — single long-running process
echo "==> starting OTLP heartbeat sender"
"$FIXTURE_BIN" --url "$SOFTPROBE_URL" --token "$API_KEY" --heartbeat-secs 1 >/dev/null 2>&1 &
echo $! >"$SENDER_PID_FILE"
export LAYOUT_SENDER_ALIVE=1
export LAYOUT_SENDER_PID_FILE="$SENDER_PID_FILE"
# Never skip unit ACs by default. Explicit LAYOUT_SKIP_UNIT=1 is allowed for
# scoped timing runs (e.g. AC-G3 release re-measure) so cargo --release unit
# batches do not blow the wall-clock budget before fixtures load.
if [[ "${LAYOUT_SKIP_UNIT:-0}" != "1" ]]; then
  unset LAYOUT_SKIP_UNIT
fi

# Pin Greptime SHA + auto-detect GREPTIME_BIN from sibling clone when present
GREPTIME_SRC="${GREPTIME_SRC:-$ROOT/../greptime}"
if [[ -d "$GREPTIME_SRC/.git" ]]; then
  export GREPTIME_GIT_SHA
  GREPTIME_GIT_SHA="$(git -C "$GREPTIME_SRC" rev-parse HEAD 2>/dev/null || echo missing)"
fi
if [[ "$COMPARE_GREPTIME" == "1" ]]; then
  if [[ -z "${GREPTIME_BIN:-}" || ! -x "${GREPTIME_BIN}" ]]; then
    for candidate in \
      "${GREPTIME_BIN:-}" \
      "$GREPTIME_SRC/target/release/greptime" \
      "$ROOT/../greptime/target/release/greptime"; do
      if [[ -n "$candidate" && -x "$candidate" ]]; then
        GREPTIME_BIN="$candidate"
        break
      fi
    done
  fi
  if [[ -n "${GREPTIME_BIN:-}" && -x "${GREPTIME_BIN}" ]]; then
    export GREPTIME_BIN
    echo "==> GREPTIME_BIN=$GREPTIME_BIN (sha=${GREPTIME_GIT_SHA:-unknown})"
  elif [[ -n "${GREPTIME_URL:-}" ]]; then
    export GREPTIME_URL
    echo "==> GREPTIME_URL=$GREPTIME_URL (external; sha=${GREPTIME_GIT_SHA:-unknown})"
  else
    echo "==> WARN: COMPARE_GREPTIME=1 but no GREPTIME_BIN/GREPTIME_URL; build with: (cd \"$GREPTIME_SRC\" && make build RELEASE=true)" >&2
  fi
fi
export GREPTIME_SRC
export GREPTIME_DATA_HOME="${GREPTIME_DATA_HOME:-$STATE_DIR/greptime-data}"
# Do NOT default GREPTIME_URL — empty means harness starts GREPTIME_BIN locally.
# Only export when the caller already set an external base.
if [[ -n "${GREPTIME_URL:-}" ]]; then
  export GREPTIME_URL
fi

export SOFTPROBE_URL API_KEY SOFTPROBE_API_KEY="$API_KEY"
export METRICS_LAYOUT_PROFILE="$PROFILE"
export COMPARE_GREPTIME CARGO_PROFILE_FLAG
export LAYOUT_SENDER_ALIVE LAYOUT_SENDER_PID_FILE
export LAYOUT_OTLP_FIXTURE_BIN="$FIXTURE_BIN"
export LAYOUT_SQL_SCHEMA="softprobe.${TENANT_SCHEMA}"

echo "==> running metrics_layout_harness.py"
HARNESS_START_EPOCH="$(date +%s)"
export HARNESS_START_EPOCH
set +e
PYTHONUNBUFFERED=1 python3 "$ROOT/scripts/metrics_layout_harness.py"
rc=$?
set -e

# Always validate latest JSON schema
latest="$(ls -1t "$RESULTS_DIR"/*-metrics-layout.json 2>/dev/null | head -1 || true)"
if [[ -z "$latest" ]]; then
  echo "ERROR: no metrics-layout JSON written" >&2
  exit 1
fi
# Reject stale JSON from a prior run (harness must write this invocation).
if [[ -n "${HARNESS_START_EPOCH:-}" ]]; then
  latest_mtime="$(stat -c %Y "$latest" 2>/dev/null || echo 0)"
  if (( latest_mtime < HARNESS_START_EPOCH )); then
    echo "ERROR: latest JSON $latest is stale (mtime=$latest_mtime < start=$HARNESS_START_EPOCH); harness did not write results" >&2
    exit 1
  fi
fi
echo "==> validate schema: $latest"
python3 "$ROOT/scripts/validate-metrics-layout-results.py" "$latest" || rc=1

# Ready-gate validation only when profiles match
if [[ "$CARGO_PROFILE_FLAG" == "--release" && "$PROFILE" == "release_full" && "$COMPARE_GREPTIME" == "1" ]]; then
  echo "==> validate ready gate"
  python3 "$ROOT/scripts/validate-metrics-layout-results.py" --ready "$latest" || rc=1
fi

if [[ "$rc" -ne 0 ]]; then
  echo "metrics-layout FAIL (exit $rc) — see $latest" >&2
  exit "$rc"
fi
echo "metrics-layout OK — $latest"
exit 0
