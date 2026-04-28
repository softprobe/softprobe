#!/usr/bin/env bash
#
# Start/stop/status for `splake` on the "app" EC2 instance.
#
# Usage (run on the app EC2 host, inside ~/datalake):
#   bash scripts/ec2_start_splake.sh start
#   bash scripts/ec2_start_splake.sh stop
#   bash scripts/ec2_start_splake.sh status
#
# Env overrides:
#   CONFIG_FILE=~/config-aws-ec2-perf.yaml
#   SPLAKE_BIN=~/datalake/target/release/splake
#   SPLAKE_LOG=~/splake.log
#   SPLAKE_PID_FILE=~/splake.pid
#   RUST_LOG=info
#
set -euo pipefail

CMD="${1:-}"
if [[ -z "$CMD" ]]; then
  echo "usage: $0 <start|stop|status>" >&2
  exit 2
fi

CONFIG_FILE="${CONFIG_FILE:-$HOME/config-aws-ec2-perf.yaml}"
SPLAKE_BIN="${SPLAKE_BIN:-$HOME/datalake/target/release/splake}"
SPLAKE_LOG="${SPLAKE_LOG:-$HOME/splake.log}"
SPLAKE_PID_FILE="${SPLAKE_PID_FILE:-$HOME/splake.pid}"
export RUST_LOG="${RUST_LOG:-info}"

die() { echo "error: $*" >&2; exit 1; }

is_running() {
  local pid="$1"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1
}

read_pid() {
  [[ -f "$SPLAKE_PID_FILE" ]] || return 1
  tr -d '[:space:]' < "$SPLAKE_PID_FILE"
}

case "$CMD" in
  start)
    [[ -x "$SPLAKE_BIN" ]] || die "binary not found/executable: $SPLAKE_BIN"
    [[ -f "$CONFIG_FILE" ]] || die "config not found: $CONFIG_FILE"

    if pid="$(read_pid)" && is_running "$pid"; then
      echo "splake already running (pid=$pid)"
      exit 0
    fi

    echo "Starting splake..."
    mkdir -p "$(dirname "$SPLAKE_LOG")"

    nohup env CONFIG_FILE="$CONFIG_FILE" "$SPLAKE_BIN" >"$SPLAKE_LOG" 2>&1 &
    pid="$!"
    echo "$pid" > "$SPLAKE_PID_FILE"

    echo "splake pid=$pid"
    echo "log: $SPLAKE_LOG"
    echo "health: curl -fsS http://127.0.0.1:8090/health"
    ;;

  stop)
    pid="$(read_pid || true)"
    if [[ -z "${pid:-}" ]]; then
      echo "splake not running (no pid file at $SPLAKE_PID_FILE)"
      exit 0
    fi
    if ! is_running "$pid"; then
      echo "splake not running (stale pid=$pid)"
      rm -f "$SPLAKE_PID_FILE"
      exit 0
    fi

    echo "Stopping splake (pid=$pid)..."
    kill "$pid" || true
    for _ in {1..30}; do
      if ! is_running "$pid"; then
        rm -f "$SPLAKE_PID_FILE"
        echo "stopped"
        exit 0
      fi
      sleep 1
    done
    echo "splake did not exit; sending SIGKILL..."
    kill -9 "$pid" || true
    rm -f "$SPLAKE_PID_FILE"
    ;;

  status)
    pid="$(read_pid || true)"
    if [[ -z "${pid:-}" ]]; then
      echo "splake: not running (no pid file)"
      exit 1
    fi
    if is_running "$pid"; then
      echo "splake: running (pid=$pid)"
      exit 0
    fi
    echo "splake: not running (stale pid=$pid)"
    exit 1
    ;;

  *)
    die "unknown command: $CMD"
    ;;
esac

