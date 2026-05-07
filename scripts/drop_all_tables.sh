#!/usr/bin/env bash
# Drop DuckLake telemetry tables for the scope in CONFIG_FILE (same as duckdb-shell).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

if ! command -v duckdb >/dev/null 2>&1; then
  echo "ERROR: duckdb CLI not found" >&2
  exit 1
fi

R=$(mktemp)
M=$(mktemp)
trap 'rm -f "$R" "$M"' EXIT

python3 "${ROOT}/scripts/duckdb_ducklake_render_init.py" --root "$ROOT" --meta "$M" >"$R"
# shellcheck disable=SC1090
source "$M"
P="$SOFTPROBE_DL_QUALIFIED_PREFIX"

echo "=== Dropping DuckLake tables: ${P}.(traces|logs|metrics) ==="
duckdb -init "$R" -c "
DROP TABLE IF EXISTS ${P}.traces;
DROP TABLE IF EXISTS ${P}.logs;
DROP TABLE IF EXISTS ${P}.metrics;
SELECT 'drop complete' AS status;
"
echo "=== Done ==="
