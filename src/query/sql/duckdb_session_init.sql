-- DuckDB per-connection session initialization.
--
-- Keep this script free of dynamic values (paths, credentials). Those should be
-- configured via parameterized SET statements in Rust.
LOAD httpfs;
LOAD iceberg;
SET unsafe_enable_version_guessing = true;

