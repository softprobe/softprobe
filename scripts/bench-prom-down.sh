#!/usr/bin/env bash
# Tear down Softprobe + Prom micro-benchmark compose (auth-mock + otelcol).
# Usage (repo root): ./scripts/bench-prom-down.sh  |  make bench-prom-down

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose}"
STATE_DIR="${THELAKE_BENCH_STATE_DIR:-/tmp/thelake-prom-bench}"
COMPOSE_FILE="$ROOT/tests/compat/prometheus/benchmark/docker-compose.yml"
COMPOSE_PROJECT="${THELAKE_BENCH_COMPOSE_PROJECT:-thelake-prom-bench}"
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

echo "==> stopping Prom bench compose ($COMPOSE_PROJECT)"
# shellcheck disable=SC2086
$COMPOSE -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true

echo "prom bench stack down"
