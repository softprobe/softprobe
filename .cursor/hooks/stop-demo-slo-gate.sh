#!/usr/bin/env bash
# Cursor stop hook: do not let the agent finish until the demo SLO gate is green.
# stdout must be JSON only. Diagnostics go to stderr and the state log.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

STATE_DIR="$ROOT/.cursor/hooks/.state"
LOG="$STATE_DIR/last-stop-gate.log"
FAILS="$STATE_DIR/failures.txt"
PY="$ROOT/.cursor/hooks/grafana_dashboard_slo.py"
GRAFANA_STATE="${THELAKE_GRAFANA_STATE_DIR:-/tmp/thelake-grafana-manual}"
PID_FILE="$GRAFANA_STATE/softprobe.pid"
OTEL_PROJECT="${OTEL_DEMO_COMPOSE_PROJECT:-thelake-otel-demo}"
# Never honor skip env vars. The gate is the product stop condition.

mkdir -p "$STATE_DIR"
: >"$FAILS"

log() { printf '%s\n' "$*" | tee -a "$LOG" >&2; }
log "===== $(date -u +%Y-%m-%dT%H:%M:%SZ) stop-demo-slo-gate ====="

read_status() {
  python3 -c 'import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    d={}
print(d.get("status") or "")
print(d.get("loop_count") if d.get("loop_count") is not None else 0)
'
}

input="$(cat || true)"
status="$(printf '%s' "$input" | read_status | sed -n '1p')"
loop_count="$(printf '%s' "$input" | read_status | sed -n '2p')"
log "status=${status:-?} loop_count=${loop_count:-0}"

if [[ "${status}" == "aborted" ]]; then
  echo '{}'
  exit 0
fi

fail() {
  printf '%s\n' "$1" >>"$FAILS"
  log "FAIL: $1"
}

# --- 1. working tree committed ---
dirty="$(git status --porcelain | grep -vE '^\?\? \.codegraph/|^\?\? \.agent/|^\?\? \.cursor/hooks/\.state/' || true)"
if [[ -n "$dirty" ]]; then
  preview="$(printf '%s\n' "$dirty" | head -n 40)"
  fail "Code is not committed. Dirty paths:
$preview"
else
  log "git: clean at $(git rev-parse --short HEAD)"
fi

# --- 2. OTEL demo running ---
grafana_up=0
if docker inspect -f '{{.State.Running}}' thelake-grafana-manual 2>/dev/null | grep -qx true; then
  grafana_up=1
fi
otel_n="$(docker ps --filter "label=com.docker.compose.project=${OTEL_PROJECT}" --filter status=running --format '{{.Names}}' 2>/dev/null | wc -l | tr -d ' ' || echo 0)"
otel_collector="$(docker ps --filter "label=com.docker.compose.project=${OTEL_PROJECT}" --filter status=running --format '{{.Names}}' 2>/dev/null | grep -Ei 'otel-collector|collector' || true)"

softprobe_ok=0
if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    if [[ "$cmd" == *softprobe-runtime* ]]; then
      softprobe_ok=1
    fi
  fi
fi
if curl -sf -m 2 "http://127.0.0.1:8090/api/v1/status/buildinfo" >/dev/null 2>&1 \
  || curl -sf -m 2 -H "Authorization: Bearer ${SOFTPROBE_API_KEY:-local-dev-key}" \
       "http://127.0.0.1:8090/api/v1/label/__name__/values" >/dev/null 2>&1; then
  softprobe_ok=1
fi

if [[ "$grafana_up" != 1 && "$softprobe_ok" != 1 ]]; then
  log "demo not running; skip stop gate (workspace is not on a live Grafana session)"
  echo '{}'
  exit 0
fi

if [[ "$grafana_up" != 1 ]]; then
  fail "Grafana demo is not running (container thelake-grafana-manual). Start with: make grafana-up"
fi
if [[ "${otel_n:-0}" -lt 1 || -z "$otel_collector" ]]; then
  fail "OpenTelemetry Demo is not running (compose project ${OTEL_PROJECT}, need otel-collector). Start with: make grafana-up"
fi
if [[ "$softprobe_ok" != 1 ]]; then
  fail "Softprobe is not serving on :8090. Start with: make grafana-up"
fi

# --- 3. live ingest + Grafana 100ms SLO ---
slo_rc=0
slo_out="$(python3 "$PY" --slo-ms 100 --repeats 3 --workers 1 2>&1)" || slo_rc=$?
printf '%s\n' "$slo_out" | tee -a "$LOG" >&2
if [[ "$slo_rc" -ne 0 ]]; then
  fail "OTEL ingest and/or Grafana SLO (every dashboard at 5m, 15m, 30m, 1h, 3h, 24h, 30d, 180d consistently ≤100ms) failed:
$slo_out"
fi

# --- 4. tests green (committed tree only; cache by HEAD) ---
if [[ -n "$dirty" ]]; then
  fail "Tests not verified: commit the working tree first, then this hook will run make test."
else
  head_sha="$(git rev-parse HEAD)"
  cache="$STATE_DIR/last-green-test"
  cached="$(cat "$cache" 2>/dev/null || true)"
  if [[ "$cached" == "$head_sha" ]]; then
    log "tests: cached green for $head_sha"
  else
    log "tests: running make test"
    if make test >>"$LOG" 2>&1; then
      printf '%s\n' "$head_sha" >"$cache"
      log "tests: green"
    else
      tail_txt="$(tail -n 80 "$LOG")"
      fail "make test is not green:
$tail_txt"
    fi
  fi
fi

if [[ ! -s "$FAILS" ]]; then
  log "stop gate passed"
  echo '{}'
  exit 0
fi

python3 - "$FAILS" <<'PY'
import json, sys
from pathlib import Path
fails = Path(sys.argv[1]).read_text(encoding="utf-8")
msg = f"""STOP GATE FAILED. Do not stop.

All of these must be true before you may finish:
1. All tests green (`make test`).
2. Code committed (clean git status, ignoring .codegraph/ and hook state).
3. OTEL Astronomy Shop demo running and observability data ingesting (live, non-flat scrapes).
4. Every Grafana dashboard PromQL at 5m, 15m, 30m, 1h, 3h, 24h, 30d, 180d consistently ≤100ms (3 repeats after warmup).

Failures this turn:
{fails}

If Grafana latency or ingest failed: go read and learn from ../greptime (workspace sibling; also ./greptime if present). Study how Greptime serves PromQL range queries (storage layout, indexing, aggregation, compaction). Apply those lessons to Softprobe. Do not vendor or fork Greptime. Then re-measure until this gate is quiet.

Keep working. Do not stop until this hook returns success.
"""
print(json.dumps({"followup_message": msg}))
PY
exit 0
