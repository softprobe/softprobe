#!/usr/bin/env bash
# Interactive DuckDB session attached to production-style DuckLake:
#   - Metadata: Postgres (e.g. Supabase pooler + sslmode=require)
#   - Data: GCS (gs://...) via DuckDB httpfs + HMAC keys (same as softprobe-runtime)
#
# Requires: duckdb CLI on PATH (version compatible with server-side DuckLake).
#
# Environment (required):
#   DUCKLAKE_METADATA_PATH   Postgres connection options for DuckLake metadata, same shape as
#                            runtime config: either full "postgres:host=... user=... password=..."
#                            or "host=... port=... dbname=postgres user=... password=... sslmode=require"
#                            (password single-quotes in SQL are not used here; use env / URL-safe tokens).
#   GCS_HMAC_ACCESS_KEY_ID    GCS interoperability HMAC access key id (or GCP_HMAC_ACCESS_KEY_ID)
#   GCS_HMAC_SECRET           GCS interoperability HMAC secret (or GCP_HMAC_SECRET)
#
# Environment (optional):
#   DUCKLAKE_GCS_DATA_PATH    Warehouse prefix, e.g. gs://your-bucket/ducklake-data/main/
#   DUCKLAKE_CATALOG_ALIAS    ATTACH alias (default: softprobe)
#   DUCKLAKE_METADATA_SCHEMA  Postgres schema for DuckLake tables (default: ducklake_schema)
#   DUCKLAKE_SKIP_GCS_SECRET   If 1, do not create the GCS HMAC secret (only useful when not using gs://)
#   DUCKLAKE_SHORTHAND_VIEWS     If 1 (default), CREATE VIEW traces/logs/metrics -> catalog.* (fails if tables missing)
#
# This script optionally sources repo-root ../../.env if present (same pattern as telemetrygen_hosted.sh).
#
# Usage:
#   export DUCKLAKE_METADATA_PATH='host=... port=5432 dbname=postgres user=ducklake.xxx password=*** sslmode=require'
#   export GCS_HMAC_ACCESS_KEY_ID=...
#   export GCS_HMAC_SECRET=...
#   export DUCKLAKE_GCS_DATA_PATH='gs://your-bucket/ducklake-data/main/'
#   ./scripts/interactive_query_ducklake_production.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

GCS_HMAC_ACCESS_KEY_ID="${GCS_HMAC_ACCESS_KEY_ID:-${GCP_HMAC_ACCESS_KEY_ID:-}}"
GCS_HMAC_SECRET="${GCS_HMAC_SECRET:-${GCP_HMAC_SECRET:-}}"

DUCKLAKE_GCS_DATA_PATH="${DUCKLAKE_GCS_DATA_PATH:-}"
DUCKLAKE_CATALOG_ALIAS="${DUCKLAKE_CATALOG_ALIAS:-softprobe}"
DUCKLAKE_METADATA_SCHEMA="${DUCKLAKE_METADATA_SCHEMA:-ducklake_schema}"
DUCKLAKE_SKIP_GCS_SECRET="${DUCKLAKE_SKIP_GCS_SECRET:-0}"
DUCKLAKE_SHORTHAND_VIEWS="${DUCKLAKE_SHORTHAND_VIEWS:-1}"

usage() {
  cat <<'EOF'
Interactive DuckDB against DuckLake (Postgres metadata + GCS data).

Required env:
  DUCKLAKE_METADATA_PATH     Postgres options for DuckLake (with or without postgres: prefix)
  GCS_HMAC_ACCESS_KEY_ID     + GCS_HMAC_SECRET when DUCKLAKE_GCS_DATA_PATH is gs://...
  DUCKLAKE_GCS_DATA_PATH     e.g. gs://bucket/ducklake-data/main/

Optional env:
  DUCKLAKE_CATALOG_ALIAS, DUCKLAKE_METADATA_SCHEMA, DUCKLAKE_SKIP_GCS_SECRET, DUCKLAKE_SHORTHAND_VIEWS

Run:  ./interactive_query_ducklake_production.sh
Help: ./interactive_query_ducklake_production.sh --help
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

# SQL escape for content inside single-quoted SQL string literals.
sql_escape_literal() {
  # shellcheck disable=SC2001
  printf '%s' "$1" | sed "s/'/''/g"
}

if [[ -z "${DUCKLAKE_METADATA_PATH:-}" ]]; then
  echo "error: DUCKLAKE_METADATA_PATH is not set (Postgres metadata connection for DuckLake)." >&2
  echo "See: $0 --help" >&2
  exit 1
fi

if [[ ! "$DUCKLAKE_CATALOG_ALIAS" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
  echo "error: DUCKLAKE_CATALOG_ALIAS must be a simple identifier (letters, digits, underscore)." >&2
  exit 1
fi

if [[ -z "$DUCKLAKE_GCS_DATA_PATH" ]]; then
  echo "error: DUCKLAKE_GCS_DATA_PATH is not set (e.g. gs://.../ducklake-data/main/)." >&2
  exit 1
fi

if [[ "$DUCKLAKE_GCS_DATA_PATH" == gs://* ]]; then
  if [[ "$DUCKLAKE_SKIP_GCS_SECRET" != "1" ]]; then
    if [[ -z "$GCS_HMAC_ACCESS_KEY_ID" || -z "$GCS_HMAC_SECRET" ]]; then
      echo "error: GCS data path requires GCS HMAC keys for DuckDB httpfs (set GCS_HMAC_ACCESS_KEY_ID and GCS_HMAC_SECRET)." >&2
      echo "See: https://duckdb.org/docs/current/guides/network_cloud_storage/gcs_import.html" >&2
      exit 1
    fi
  fi
fi

if [[ "$DUCKLAKE_METADATA_PATH" == postgres:* ]]; then
  ATTACH_TARGET="$DUCKLAKE_METADATA_PATH"
else
  ATTACH_TARGET="postgres:${DUCKLAKE_METADATA_PATH}"
fi

TARGET_ESC=$(sql_escape_literal "$ATTACH_TARGET")
DATA_ESC=$(sql_escape_literal "$DUCKLAKE_GCS_DATA_PATH")
SCHEMA_ESC=$(sql_escape_literal "$DUCKLAKE_METADATA_SCHEMA")
KID_ESC=$(sql_escape_literal "$GCS_HMAC_ACCESS_KEY_ID")
SEC_ESC=$(sql_escape_literal "$GCS_HMAC_SECRET")

if [[ "$DUCKLAKE_METADATA_SCHEMA" == "main" ]]; then
  TABLE_PREFIX="${DUCKLAKE_CATALOG_ALIAS}"
else
  TABLE_PREFIX="${DUCKLAKE_CATALOG_ALIAS}.${DUCKLAKE_METADATA_SCHEMA}"
fi

INIT_SQL="$(mktemp -t duckdb_ducklake_prod_init.XXXXXX.sql)"
cleanup() { rm -f "$INIT_SQL"; }
trap cleanup EXIT

{
  cat <<'EOSQL'
INSTALL httpfs;
LOAD httpfs;
SET unsafe_enable_version_guessing = true;
EOSQL

  if [[ "$DUCKLAKE_GCS_DATA_PATH" == gs://* && "$DUCKLAKE_SKIP_GCS_SECRET" != "1" ]]; then
    # Match softprobe-runtime/src/storage/ducklake/mod.rs configure_httpfs_gcs_for_data_path
    printf "CREATE OR REPLACE SECRET gcs_hmac (TYPE GCS, KEY_ID '%s', SECRET '%s');\n" "$KID_ESC" "$SEC_ESC"
  fi

  cat <<'EOSQL'
INSTALL ducklake;
LOAD ducklake;
INSTALL postgres;
LOAD postgres;
EOSQL

  if [[ "$DUCKLAKE_METADATA_SCHEMA" != "main" ]]; then
    printf "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s', META_SCHEMA '%s');\n" \
      "$TARGET_ESC" "$DUCKLAKE_CATALOG_ALIAS" "$DATA_ESC" "$SCHEMA_ESC" "$SCHEMA_ESC"
  else
    printf "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s');\n" \
      "$TARGET_ESC" "$DUCKLAKE_CATALOG_ALIAS" "$DATA_ESC"
  fi

  cat <<EOSQL
.print ''
.print '================================================'
.print 'DuckDB — DuckLake (Postgres metadata + GCS data)'
.print '================================================'
.print ''
.print 'Catalog: ${DUCKLAKE_CATALOG_ALIAS}'
EOSQL

  if [[ "$DUCKLAKE_SHORTHAND_VIEWS" == "1" ]]; then
    cat <<EOSQL
.print 'Convenience views: traces, logs, metrics'
EOSQL
  else
    cat <<EOSQL
.print 'Shorthand views skipped (DUCKLAKE_SHORTHAND_VIEWS=0). Use ${DUCKLAKE_CATALOG_ALIAS}.traces etc.'
EOSQL
  fi

  cat <<EOSQL
.print ''
.print 'Examples:'
.print '  SELECT COUNT(*) FROM ${TABLE_PREFIX}.traces;'
.print '  SELECT * FROM ${TABLE_PREFIX}.logs ORDER BY timestamp DESC LIMIT 10;'
.print '  DESCRIBE ${TABLE_PREFIX}.traces;'
.print ''
EOSQL

  if [[ "$DUCKLAKE_SHORTHAND_VIEWS" == "1" ]]; then
    printf "CREATE OR REPLACE VIEW traces AS SELECT * FROM %s.traces;\n" "$TABLE_PREFIX"
    printf "CREATE OR REPLACE VIEW logs AS SELECT * FROM %s.logs;\n" "$TABLE_PREFIX"
    printf "CREATE OR REPLACE VIEW metrics AS SELECT * FROM %s.metrics;\n" "$TABLE_PREFIX"
  fi

} >"$INIT_SQL"

DUCKDB_BIN="${DUCKDB_BIN:-}"
if [[ -z "$DUCKDB_BIN" ]]; then
  if [[ -x "/opt/homebrew/bin/duckdb" ]]; then
    DUCKDB_BIN="/opt/homebrew/bin/duckdb"
  else
    DUCKDB_BIN="duckdb"
  fi
fi

"$DUCKDB_BIN" -init "$INIT_SQL"
