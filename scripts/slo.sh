#!/usr/bin/env bash
# Shared phase timing + optional SLO enforcement for Make gates.
# Usage:
#   ./scripts/slo.sh phase NAME -- command...
#   ./scripts/slo.sh total LABEL SECONDS GOAL_SECS
set -euo pipefail

cmd="${1:?phase|total}"
shift

case "${cmd}" in
  phase)
    name="${1:?phase name}"
    shift
    if [[ "${1:-}" == "--" ]]; then shift; fi
    echo "PHASE=${name} start"
    start="$(date +%s)"
    "$@"
    end="$(date +%s)"
    echo "PHASE=${name} elapsed=$((end - start))s"
    ;;
  total)
    label="${1:?label}"
    total="${2:?seconds}"
    goal="${3:?goal seconds}"
    echo "TOTAL=${total}s goal=${goal}s (${label})"
    if [[ "${CI:-}" == "true" || "${ENFORCE_SLO:-0}" == "1" ]]; then
      if [[ "${total}" -gt "${goal}" ]]; then
        echo "❌ ${label} wall clock ${total}s exceeds ${goal}s goal" >&2
        exit 1
      fi
    fi
    ;;
  *)
    echo "usage: $0 phase NAME -- cmd... | $0 total LABEL SECONDS GOAL" >&2
    exit 2
    ;;
esac
