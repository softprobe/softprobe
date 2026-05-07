#!/usr/bin/env bash
# Build a single DuckDB -init file: ATTACH (from runtime YAML or static SQL) + CREATE VIEW only
# for telemetry tables that exist (catalog_alias + metadata_schema from config).
#
# Usage (from softprobe-runtime repo root):
#   source scripts/duckdb_ducklake_combo.sh
#   COMBO=$(softprobe_ducklake_build_combo_init "$(pwd)")
#   trap 'rm -f "$COMBO" "$RENDERED" "$META"' EXIT
#   duckdb -init "$COMBO"

softprobe_ducklake_build_combo_init() {
  local root="$1"
  local attach_override="${2:-}"

  local attach=""
  local rendered=""
  local meta=""
  local catalog="softprobe"
  local schema="main"
  local qual_prefix="softprobe"

  if [[ -n "$attach_override" ]]; then
    attach="$attach_override"
    if [[ "$attach" != /* ]]; then
      attach="${root}/${attach}"
    fi
    if [[ ! -f "$attach" ]]; then
      echo "ERROR: attach SQL not found: $attach" >&2
      return 1
    fi
    # Static file: keep legacy hardcoded names (docker defaults).
    catalog="softprobe"
    schema="softprobe"
    qual_prefix="softprobe.softprobe"
  else
    rendered="$(mktemp -t softprobe_duckdb_render.XXXXXX)"
    meta="$(mktemp -t softprobe_duckdb_meta.XXXXXX)"
    if python3 "${root}/scripts/duckdb_ducklake_render_init.py" --root "$root" --meta "$meta" >"$rendered" 2>/dev/null; then
      # shellcheck disable=SC1090
      source "$meta"
      catalog="${SOFTPROBE_DL_CATALOG_ALIAS:-softprobe}"
      schema="${SOFTPROBE_DL_METADATA_SCHEMA:-main}"
      qual_prefix="${SOFTPROBE_DL_QUALIFIED_PREFIX:-$catalog}"
      attach="$rendered"
      rm -f "$meta"
      meta=""
    else
      rm -f "$rendered" "$meta"
      rendered=""
      echo "ERROR: Could not render DuckDB init from CONFIG_FILE / default YAML." >&2
      echo "  Set CONFIG_FILE to a runtime config that contains ducklake: (same as the running server)." >&2
      echo "  Or pass a static init path as second arg to softprobe_ducklake_build_combo_init, or set SOFTPROBE_DUCKDB_INIT." >&2
      return 1
    fi
  fi

  local combo
  combo="$(mktemp -t softprobe_duckdb_init.XXXXXX)"
  {
    cat "$attach"
    local out
    out="$(
      duckdb -init "$attach" -csv -noheader -c "
        SELECT table_name
        FROM duckdb_tables()
        WHERE database_name = '${catalog//\'/\'\'}'
          AND schema_name = '${schema//\'/\'\'}'
          AND table_name IN ('traces', 'logs', 'metrics')
        ORDER BY table_name;
      " 2>/dev/null
    )" || true
    local qpref="${qual_prefix//\'/\'\'}"
    while IFS= read -r t; do
      t="$(echo "$t" | tr -d '\r')"
      [[ -z "$t" ]] && continue
      echo "CREATE OR REPLACE VIEW ${t} AS SELECT * FROM ${qpref}.${t};"
    done <<< "$out"
  } >"$combo"

  # Caller removes temps; keep rendered attach file path via global is fragile — store in combo only.
  if [[ -n "$rendered" ]]; then
    rm -f "$rendered"
  fi
  echo "$combo"
}
