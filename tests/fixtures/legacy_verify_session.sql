-- DuckLake: session drill-down (legacy filename). Prefer scripts/duckdb_ducklake_local_init.sql + views.
-- Strict correlation test: tests/integration/storage_contract_validation.rs

INSTALL httpfs;
LOAD httpfs;
INSTALL ducklake;
LOAD ducklake;
INSTALL postgres;
LOAD postgres;

SET s3_endpoint = 'localhost:9000';
SET s3_url_style = 'path';
SET s3_use_ssl = false;
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';
SET s3_region = 'us-east-1';

ATTACH 'ducklake:postgres:host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake' AS softprobe (
  DATA_PATH 's3://warehouse/ducklake/data/',
  METADATA_SCHEMA 'softprobe',
  META_SCHEMA 'softprobe',
  DATA_INLINING_ROW_LIMIT 10000
);

-- Replace SESSION_ID, then run the SELECT.
-- SELECT 'trace' AS type, session_id, trace_id, span_id, message_type AS content, timestamp
--   FROM softprobe.softprobe.traces WHERE session_id = 'SESSION_ID'
-- UNION ALL
-- SELECT 'log', session_id, trace_id, span_id, body, timestamp
--   FROM softprobe.softprobe.logs WHERE session_id = 'SESSION_ID'
-- ORDER BY timestamp;
