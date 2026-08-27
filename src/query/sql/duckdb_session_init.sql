-- DuckDB per-connection session initialization.
--
-- Keep this script free of dynamic values (paths, credentials). Those should be
-- configured via parameterized SET statements in Rust.
LOAD httpfs;
LOAD ducklake;
-- Version guessing lets a worker serve a cached DuckLake snapshot instead of
-- reading the latest committed version from the catalog, which made freshly
-- ingested data invisible to interactive queries for minutes at a time.
-- Correctness/compatibility (#25) requires read-after-write visibility; the
-- catalog roundtrip per query is cheap next to serving stale evidence.
SET unsafe_enable_version_guessing = false;

