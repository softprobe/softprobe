#!/bin/bash
# Interactive DuckDB against the same DuckLake scope as softprobe-runtime (from CONFIG_FILE).
#
# Usage:
#   make duckdb-shell
#   CONFIG_FILE=/path/to/runtime.yaml ./scripts/interactive_query.sh
#
# Optional:
#   SOFTPROBE_DUCKDB_INIT  If set, path to a static attach-only .sql (skips YAML renderer; legacy).

set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

if ! command -v duckdb >/dev/null 2>&1; then
  echo "ERROR: duckdb CLI not found (brew install duckdb)" >&2
  exit 1
fi

# shellcheck source=/dev/null
source "${ROOT}/scripts/duckdb_ducklake_combo.sh"

STATIC_INIT="${SOFTPROBE_DUCKDB_INIT:-}"
if [[ -n "$STATIC_INIT" ]]; then
  COMBO="$(softprobe_ducklake_build_combo_init "$ROOT" "$STATIC_INIT")"
else
  COMBO="$(softprobe_ducklake_build_combo_init "$ROOT")"
fi
trap 'rm -f "$COMBO"' EXIT

echo "Starting DuckDB (DuckLake attach + views for existing tables in this config scope)"
grep '^-- CONFIG_FILE=' "$COMBO" 2>/dev/null || true
grep '^-- DuckLake scope:' "$COMBO" 2>/dev/null || true
if [[ -n "$STATIC_INIT" ]]; then
  echo "  (static attach SQL: $STATIC_INIT)"
fi
echo "  Point CONFIG_FILE at the same YAML as the running runtime so metadata_schema / data_path match."
if grep -q "CREATE OR REPLACE VIEW" "$COMBO" 2>/dev/null; then
  echo "  Convenience views: $(grep 'CREATE OR REPLACE VIEW' "$COMBO" | sed -n 's/.*VIEW \([^ ]*\) AS.*/\1/p' | tr '\n' ' ')"
else
  echo "  No telemetry tables in this scope yet — ATTACH only. Ingest, then re-run or .read scripts/duckdb_ducklake_local_views.sql"
fi
echo ""

duckdb -init "$COMBO" -c "SELECT 1 AS attach_ok;" >/dev/null || {
  echo "ERROR: DuckDB could not load DuckLake init. Check Postgres, MinIO, and CONFIG_FILE (default: tests/config/duckdb-shell-host.yaml)." >&2
  exit 1
}

exec duckdb -init "$COMBO"
