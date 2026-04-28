#!/usr/bin/env bash
#
# Run `perf_stress` against a remote `splake` HTTP API (intended for the catalog EC2 host).
#
# Usage:
#   bash scripts/ec2_run_perf_stress.sh [--background] [perf_stress args...]
#
# Examples:
#   bash scripts/ec2_run_perf_stress.sh --duration 120 --warmup-secs 10
#   bash scripts/ec2_run_perf_stress.sh --background --duration 86400 --span-qps 100 --log-qps 100 --metric-qps 100
#
# Env overrides:
#   PERF_STRESS_BIN=~/perf_stress
#   SERVICE_URL=http://172.31.85.52:8090
#   PERF_LOG=~/perf_stress.log
#   CONFIG_FILE=~/config-aws-ec2-perf.yaml   (optional; only used if perf_stress reads it)
#
set -euo pipefail

BACKGROUND=0
if [[ "${1:-}" == "--background" ]]; then
  BACKGROUND=1
  shift
fi

PERF_STRESS_BIN="${PERF_STRESS_BIN:-$HOME/perf_stress}"
SERVICE_URL="${SERVICE_URL:-http://172.31.85.52:8090}"
PERF_LOG="${PERF_LOG:-$HOME/perf_stress.log}"
CONFIG_FILE="${CONFIG_FILE:-$HOME/config-aws-ec2-perf.yaml}"

die() { echo "error: $*" >&2; exit 1; }

[[ -x "$PERF_STRESS_BIN" ]] || die "binary not found/executable: $PERF_STRESS_BIN"

ARGS=()
ARGS+=(--service-url "$SERVICE_URL")
ARGS+=("$@")

if [[ "$BACKGROUND" -eq 1 ]]; then
  echo "Starting perf_stress in background..."
  echo "log: $PERF_LOG"
  quoted_args="$(printf '%q ' "${ARGS[@]}")"
  nohup bash -lc "set -euo pipefail
    export CONFIG_FILE='$CONFIG_FILE'
    exec '$PERF_STRESS_BIN' $quoted_args
  " >"$PERF_LOG" 2>&1 &
  echo "pid=$!"
  exit 0
fi

export CONFIG_FILE="$CONFIG_FILE"
exec "$PERF_STRESS_BIN" "${ARGS[@]}"
