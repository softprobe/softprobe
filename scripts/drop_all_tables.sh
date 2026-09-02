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

echo "=== Dropping DuckLake tables: ${P}.(traces|logs|metric_samples|metric_hist_samples|metric_summary_samples|metric_postings) ==="
duckdb -init "$R" -c "
DROP TABLE IF EXISTS ${P}.traces;
DROP TABLE IF EXISTS ${P}.logs;
DROP TABLE IF EXISTS ${P}.metric_samples;
DROP TABLE IF EXISTS ${P}.metric_hist_samples;
DROP TABLE IF EXISTS ${P}.metric_summary_samples;
DROP TABLE IF EXISTS ${P}.metric_samples_5m;
DROP TABLE IF EXISTS ${P}.metric_samples_1h;
DROP TABLE IF EXISTS ${P}.metric_samples_1d;
DROP TABLE IF EXISTS ${P}.metric_hist_samples_5m;
DROP TABLE IF EXISTS ${P}.metric_hist_samples_1h;
DROP TABLE IF EXISTS ${P}.metric_hist_samples_1d;
DROP TABLE IF EXISTS ${P}.metric_postings;
SELECT 'drop complete' AS status;
"
echo "=== Done ==="
