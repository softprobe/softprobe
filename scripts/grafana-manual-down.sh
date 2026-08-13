#!/usr/bin/env bash
# Stop host Softprobe + Grafana manual stack started by grafana-manual-up.sh.
# Usage (from repo root): ./scripts/grafana-manual-down.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose}"
STATE_DIR="${THELAKE_GRAFANA_STATE_DIR:-/tmp/thelake-grafana-manual}"
COMPOSE_FILE="$ROOT/tests/compat/grafana/docker-compose.manual.yml"
PID_FILE="$STATE_DIR/softprobe.pid"

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

echo "==> stopping Grafana compose"
$COMPOSE -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true

echo "grafana manual stack down"
