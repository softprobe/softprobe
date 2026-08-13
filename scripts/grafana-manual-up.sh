#!/usr/bin/env bash
# Start host Softprobe + pinned Grafana for manual Prom inspection.
# Usage (from repo root): ./scripts/grafana-manual-up.sh
# Teardown: ./scripts/grafana-manual-down.sh  (or: make grafana-down)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose}"
STATE_DIR="${THELAKE_GRAFANA_STATE_DIR:-/tmp/thelake-grafana-manual}"
COMPOSE_FILE="$ROOT/tests/compat/grafana/docker-compose.manual.yml"
LOG="$STATE_DIR/softprobe.log"
PID_FILE="$STATE_DIR/softprobe.pid"
CONFIG="$STATE_DIR/config.yaml"
SEED_BIN="$STATE_DIR/seed-otlp.bin"
AUTH_URL="${SOFTPROBE_AUTH_URL:-http://127.0.0.1:18080/validate}"
API_KEY="${SOFTPROBE_API_KEY:-local-dev-key}"
SOFTPROBE_URL_HOST="${SOFTPROBE_LISTEN:-http://127.0.0.1:8090}"

mkdir -p "$STATE_DIR/data" "$STATE_DIR/cache"

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
  # Ensure cmdline looks like our binary (avoid killing unrelated pid reuse).
  local cmd
  cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  [[ "$cmd" == *softprobe-runtime* ]] || return 1
  return 0
}

seed_and_print() {
  echo "==> seeding demo metrics (checkout + payments)"
  "$SEED_TOOL" "$SEED_BIN"
  curl -sf -X POST "$SOFTPROBE_URL_HOST/v1/metrics" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/x-protobuf" \
    --data-binary @"$SEED_BIN" >/dev/null
  curl -sf -H "Authorization: Bearer $API_KEY" "$SOFTPROBE_URL_HOST/api/v1/labels" >/dev/null

  cat <<EOF

Grafana is ready for manual inspection.

  URL:        http://127.0.0.1:3000
  Login:      admin / admin
  Dashboard:  Softprobe → Softprobe Prometheus smoke
  Softprobe:  $SOFTPROBE_URL_HOST  (Bearer $API_KEY)
  PID file:   $PID_FILE
  Log:        $LOG

  Expected panel shapes (single clean seed):
    http_requests / sum / topk / offset / compare  → ramps or filtered ramps
    rate(http_requests[5m])                       → nearly FLAT ≈ 0.0167/s
    avg_over_time(...)                            → rising (smoothed ramp)
    payments                                      → sine wave (not a ramp)

Teardown: make grafana-down
EOF
}

# Reuse only if *we* own Softprobe + Grafana is healthy.
# Do NOT re-seed: overlapping ramps corrupt lookback and inflate rate().
if our_softprobe_running \
  && curl -sf "$SOFTPROBE_URL_HOST/ready" >/dev/null 2>&1 \
  && curl -sf -o /dev/null -u admin:admin http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
  echo "already up (owned Softprobe pid=$(cat "$PID_FILE")); not re-seeding (avoids overlapping series)."
  echo "  For a fresh hour of demo data: make grafana-down && make grafana-up"
  cat <<EOF

  URL:        http://127.0.0.1:3000  (admin / admin)
  Dashboard:  Softprobe → Softprobe Prometheus smoke
  Softprobe:  $SOFTPROBE_URL_HOST

Teardown: make grafana-down
EOF
  exit 0
fi

# Refuse to clobber a foreign Softprobe on :8090.
if port_busy 8090 && ! our_softprobe_running; then
  echo "ERROR: :8090 is in use by another process. Stop it or make grafana-down first." >&2
  exit 1
fi
if port_busy 3000 && ! curl -sf -o /dev/null -u admin:admin http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
  echo "ERROR: :3000 is in use but not our Grafana. Free the port or set a different mapping." >&2
  exit 1
fi

# Tear any previous host process we started.
if [[ -f "$PID_FILE" ]]; then
  old="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${old:-}" ]] && kill -0 "$old" 2>/dev/null; then
    cmd="$(ps -p "$old" -o args= 2>/dev/null || true)"
    if [[ "$cmd" == *softprobe-runtime* ]]; then
      kill "$old" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$old" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$old" 2>/dev/null || true
    fi
  fi
  rm -f "$PID_FILE"
fi

# Fresh DuckLake so a single seed is authoritative (no overlapping ramps).
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
  enabled: false
  target_file_size_bytes: 67108864
  interval_seconds: 3600

ducklake:
  catalog_type: "sqlite"
  metadata_path: "$STATE_DIR/metadata.sqlite"
  data_path: "$STATE_DIR/data/"
  catalog_alias: "softprobe"
  metadata_schema: "main"
  data_inlining_row_limit: 10000
  writer_pool_size: 2

dropdown_catalog:
  enabled: false
EOF

echo "==> building softprobe-runtime + grafana_seed_otlp"
cargo build -q --bin softprobe-runtime --bin grafana_seed_otlp

RUNTIME_BIN="${CARGO_TARGET_DIR:-target}/debug/softprobe-runtime"
SEED_TOOL="${CARGO_TARGET_DIR:-target}/debug/grafana_seed_otlp"
if [[ ! -x "$RUNTIME_BIN" ]]; then
  echo "ERROR: missing $RUNTIME_BIN" >&2
  exit 1
fi

echo "==> starting Grafana + auth-mock"
$COMPOSE -f "$COMPOSE_FILE" up -d

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

# libduckdb is dynamically linked (same pattern as scripts/run-isolated-cargo-tests.sh).
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
DUCKDB_LIB_DIR="$(find "${TARGET_DIR}/duckdb-download" -type f \( -name 'libduckdb.so*' -o -name 'libduckdb.dylib*' \) -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
if [[ -z "${DUCKDB_LIB_DIR}" ]]; then
  echo "ERROR: libduckdb not found under ${TARGET_DIR}/duckdb-download (build with DUCKDB_DOWNLOAD_LIB=1?)" >&2
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
export RUST_LOG="${RUST_LOG:-info}"
: >"$LOG"
nohup "$RUNTIME_BIN" >>"$LOG" 2>&1 &
echo $! >"$PID_FILE"
disown || true

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
  exit 1
fi

echo "==> waiting for Grafana"
graf_ok=0
for _ in $(seq 1 40); do
  if curl -sf -o /dev/null -u admin:admin http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
    graf_ok=1
    break
  fi
  sleep 0.5
done
if [[ "$graf_ok" != 1 ]]; then
  echo "ERROR: Grafana did not become ready on :3000" >&2
  exit 1
fi

seed_and_print
