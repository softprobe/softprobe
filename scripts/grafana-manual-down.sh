#!/usr/bin/env bash
# Stop Softprobe + Grafana + OpenTelemetry Demo started by grafana-manual-up.sh.
# Usage (from repo root): ./scripts/grafana-manual-down.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose}"
STATE_DIR="${THELAKE_GRAFANA_STATE_DIR:-/tmp/thelake-grafana-manual}"
COMPOSE_FILE="$ROOT/tests/compat/grafana/docker-compose.manual.yml"
OVERLAY_DIR="$ROOT/tests/compat/grafana/otel-demo"
COMPOSE_SOFTPROBE="$OVERLAY_DIR/compose.softprobe.yaml"
COLLECTOR_EXTRAS="$OVERLAY_DIR/otelcol-config-extras.yml"
PID_FILE="$STATE_DIR/softprobe.pid"

OTEL_DEMO_TAG="${OTEL_DEMO_TAG:-3.0.0}"
CACHE_ROOT="${THELAKE_CACHE_ROOT:-$HOME/.cache/thelake}"
DEMO_DIR="${OTEL_DEMO_DIR:-$CACHE_ROOT/otel-demo/$OTEL_DEMO_TAG}"
DEMO_PROJECT="${OTEL_DEMO_COMPOSE_PROJECT:-thelake-otel-demo}"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    if [[ "$cmd" == *softprobe-runtime* ]]; then
      echo "==> stopping Softprobe pid=$pid"
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$pid" 2>/dev/null || true
    else
      echo "==> pid $pid is not softprobe-runtime; leaving it alone"
    fi
  fi
  rm -f "$PID_FILE"
else
  echo "==> no Softprobe pid file at $PID_FILE (skip host kill)"
fi

if [[ -f "$DEMO_DIR/compose.yaml" ]]; then
  echo "==> stopping OpenTelemetry Demo ($DEMO_PROJECT)"
  (cd "$DEMO_DIR" && \
    DEMO_VERSION="$OTEL_DEMO_TAG" \
    IMAGE_VERSION="$OTEL_DEMO_TAG" \
    OTEL_COLLECTOR_CONFIG_EXTRAS="$COLLECTOR_EXTRAS" \
    $COMPOSE -p "$DEMO_PROJECT" \
      --env-file .env \
      -f compose.yaml \
      -f "$COMPOSE_SOFTPROBE" \
      down --remove-orphans >/dev/null 2>&1) || true
else
  echo "==> no OTel Demo checkout at $DEMO_DIR (skip)"
fi

echo "==> stopping Grafana compose"
THELAKE_GRAFANA_STATE_DIR="$STATE_DIR" \
  $COMPOSE -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true

echo "grafana manual stack down"
