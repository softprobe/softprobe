-- Legacy static ATTACH (Docker service hostnames). Prefer CONFIG_FILE + duckdb_ducklake_render_init.py
-- so catalog_alias / metadata_schema / data_path match the running runtime.
-- Use: SOFTPROBE_DUCKDB_INIT=scripts/duckdb_ducklake_local_init.sql ./scripts/interactive_query.sh

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
