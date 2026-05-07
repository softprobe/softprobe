-- Renamed purpose: DuckLake reference queries (legacy filename kept for grep/discussion).
-- Strict checks: tests/integration/storage_contract_validation.rs
-- Working CLI: softprobe-runtime/scripts/duckdb_ducklake_local_init.sql then query views `traces` / `logs`.

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

SELECT COUNT(*) AS total_spans FROM softprobe.softprobe.traces;
SELECT session_id, COUNT(*) AS span_count FROM softprobe.softprobe.traces GROUP BY session_id ORDER BY span_count DESC LIMIT 10;
SELECT record_date, COUNT(*) AS span_count FROM softprobe.softprobe.traces GROUP BY record_date ORDER BY record_date DESC;

SELECT COUNT(*) AS total_logs FROM softprobe.softprobe.logs;
SELECT session_id, COUNT(*) AS log_count FROM softprobe.softprobe.logs GROUP BY session_id ORDER BY log_count DESC LIMIT 10;

SELECT COUNT(*) AS correlated_logs, COUNT(DISTINCT trace_id) AS unique_traces FROM softprobe.softprobe.logs WHERE trace_id IS NOT NULL;
