#!/usr/bin/env bash
# Start host Softprobe + pinned Grafana + OpenTelemetry Demo (Astronomy Shop)
# as the live OTLP traffic source.
# Usage (from repo root): ./scripts/grafana-manual-up.sh
# Teardown: ./scripts/grafana-manual-down.sh  (or: make grafana-down)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose}"
STATE_DIR="${THELAKE_GRAFANA_STATE_DIR:-/tmp/thelake-grafana-manual}"
COMPOSE_FILE="$ROOT/tests/compat/grafana/docker-compose.manual.yml"
GRAFANA_COMPOSE_IMAGE="${GRAFANA_COMPOSE_IMAGE:-$(python3 - "$ROOT/docs/compat/references.v0.yaml" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
match = re.search(
    r"(?ms)^\s+grafana:\s*\n\s+image:\s*([^\s#]+)\s*\n\s+tag:\s*[\"']?([^\s\"']+).*?\n\s+digest:\s*[\"']?(sha256:[0-9a-fA-F]{64})",
    text,
)
if not match:
    raise SystemExit("canonical Grafana manifest entry is missing an immutable digest")
print(f"{match.group(1)}@{match.group(3)}")
PY
)}"
export GRAFANA_COMPOSE_IMAGE
OVERLAY_DIR="$ROOT/tests/compat/grafana/otel-demo"
COLLECTOR_EXTRAS="$OVERLAY_DIR/otelcol-config-extras.yml"
COMPOSE_SOFTPROBE="$OVERLAY_DIR/compose.softprobe.yaml"
LOG="$STATE_DIR/softprobe.log"
PID_FILE="$STATE_DIR/softprobe.pid"
CONFIG="$STATE_DIR/config.yaml"
AUTH_URL="${SOFTPROBE_AUTH_URL:-http://127.0.0.1:18080/validate}"
API_KEY="${SOFTPROBE_API_KEY:-local-dev-key}"
SOFTPROBE_URL_HOST="${SOFTPROBE_LISTEN:-http://127.0.0.1:8090}"

# Official Astronomy Shop pin (https://github.com/open-telemetry/opentelemetry-demo).
OTEL_DEMO_TAG="${OTEL_DEMO_TAG:-3.0.0}"
CACHE_ROOT="${THELAKE_CACHE_ROOT:-$HOME/.cache/thelake}"
DEMO_DIR="${OTEL_DEMO_DIR:-$CACHE_ROOT/otel-demo/$OTEL_DEMO_TAG}"
DEMO_PROJECT="${OTEL_DEMO_COMPOSE_PROJECT:-thelake-otel-demo}"
STORE_URL="${OTEL_DEMO_STORE_URL:-http://127.0.0.1:8080}"

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
  local cmd
  cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  [[ "$cmd" == *softprobe-runtime* ]] || return 1
  return 0
}

demo_compose() {
  # shellcheck disable=SC2086
  (cd "$DEMO_DIR" && \
    DEMO_VERSION="$OTEL_DEMO_TAG" \
    IMAGE_VERSION="$OTEL_DEMO_TAG" \
    OTEL_COLLECTOR_CONFIG_EXTRAS="$COLLECTOR_EXTRAS" \
    $COMPOSE -p "$DEMO_PROJECT" \
      --env-file .env \
      -f compose.yaml \
      -f "$COMPOSE_SOFTPROBE" \
      "$@")
}

ensure_otel_demo_checkout() {
  if [[ -f "$DEMO_DIR/compose.yaml" ]]; then
    echo "==> OpenTelemetry Demo $OTEL_DEMO_TAG already at $DEMO_DIR"
    return 0
  fi
  echo "==> cloning OpenTelemetry Demo $OTEL_DEMO_TAG → $DEMO_DIR"
  mkdir -p "$(dirname "$DEMO_DIR")"
  rm -rf "$DEMO_DIR"
  git clone --depth 1 --branch "$OTEL_DEMO_TAG" \
    https://github.com/open-telemetry/opentelemetry-demo.git "$DEMO_DIR"
}

print_ready() {
  cat <<EOF

Grafana is ready for manual inspection (live Astronomy Shop traffic).

  Grafana:     http://127.0.0.1:3000  (admin / admin)
  Dashboards:  Softprobe → Overview / Selectors / rate / Aggregations /
               Operators / over_time / Histograms (+ smoke)
  Softprobe:   $SOFTPROBE_URL_HOST  (Bearer $API_KEY)
  Store UI:    $STORE_URL
  Demo pin:    $OTEL_DEMO_TAG  ($DEMO_DIR)
  Softprobe log: $LOG

Panels use live Astronomy Shop metrics covering the declared PromQL subset.
Explore may show additional metric names as services warm up.

Teardown: make grafana-down
EOF
}

wait_for_demo_metrics() {
  echo "==> waiting for Softprobe to see demo metrics"
  local ok=0
  local body=""
  for _ in $(seq 1 90); do
    body="$(curl -sf -H "Authorization: Bearer $API_KEY" \
      "$SOFTPROBE_URL_HOST/api/v1/label/__name__/values" 2>/dev/null || true)"
    if [[ -n "$body" ]] && [[ "$body" != *'"data":[]'* ]] && [[ "$body" == *'"status":"success"'* ]]; then
      # Prefer evidence of multi-service / spanmetrics / http server metrics.
      if echo "$body" | grep -Eqi 'http_|traces_span|rpc_|process_|otelcol_|calls|duration'; then
        ok=1
        break
      fi
      # Any non-empty name list after collector is up is enough to proceed.
      if echo "$body" | grep -q '"data":\[.'; then
        ok=1
        break
      fi
    fi
    sleep 2
  done
  if [[ "$ok" != 1 ]]; then
    echo "ERROR: no metrics appeared in Softprobe after starting OTel Demo." >&2
    echo "  last /api/v1/label/__name__/values: ${body:-<empty>}" >&2
    echo "  collector: docker logs otel-collector 2>&1 | tail -40" >&2
    exit 1
  fi
  echo "==> Softprobe metric names: $(echo "$body" | head -c 400)…"
}

# Reuse if Softprobe + Grafana + demo collector already healthy.
if our_softprobe_running \
  && curl -sf "$SOFTPROBE_URL_HOST/ready" >/dev/null 2>&1 \
  && curl -sf -o /dev/null -u admin:admin http://127.0.0.1:3000/api/health >/dev/null 2>&1 \
  && docker inspect -f '{{.State.Running}}' otel-collector 2>/dev/null | grep -q true; then
  echo "already up (owned Softprobe pid=$(cat "$PID_FILE") + otel-collector)."
  print_ready
  exit 0
fi

if port_busy 8090 && ! our_softprobe_running; then
  echo "ERROR: :8090 is in use by another process. Stop it or make grafana-down first." >&2
  exit 1
fi
if port_busy 3000 && ! curl -sf -o /dev/null -u admin:admin http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
  echo "ERROR: :3000 is in use but not our Grafana. Free the port or set a different mapping." >&2
  exit 1
fi
if port_busy 8080; then
  echo "WARN: :8080 busy — Astronomy Shop UI may fail to bind (ENVOY_PORT). Softprobe ingest can still work." >&2
fi

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

echo "==> building softprobe-runtime"
cargo build -q --bin softprobe-runtime

RUNTIME_BIN="${CARGO_TARGET_DIR:-target}/debug/softprobe-runtime"
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

ensure_otel_demo_checkout

echo "==> starting OpenTelemetry Demo $OTEL_DEMO_TAG (minimal, Softprobe backend)"
demo_compose up --pull missing --remove-orphans --detach

wait_for_demo_metrics
print_ready
