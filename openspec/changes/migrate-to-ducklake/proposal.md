## Why

The current Iceberg catalog path is accumulating optimization work around frequent commits, metadata pinning, manifest reads, and `iceberg_scan` performance. DuckLake moves lakehouse metadata into a SQL catalog that DuckDB can read and write directly, which better matches the project's DuckDB-centered query engine and append-only telemetry workload.

## What Changes

- **BREAKING** Replace the committed Apache Iceberg storage layer with DuckLake-backed tables for traces, logs, and metrics.
- Keep the existing in-memory buffer, local WAL, staged Parquet, and union-read freshness model.
- Replace Iceberg REST catalog/Lakekeeper configuration with DuckLake catalog configuration.
- Use PostgreSQL as the production DuckLake catalog backend and MinIO/S3/R2 as the Parquet data path.
- Replace `iceberg_scan` and pinned Iceberg metadata views with direct DuckLake table views in DuckDB.
- Replace Iceberg metadata maintenance with DuckLake compaction, snapshot expiration, and cleanup functions.

## Impact

- Affected specs: `storage-engine`, `query-engine`, `ingest-engine`
- Affected code: `src/storage/`, `src/ingest_engine/`, `src/query/duckdb.rs`, `src/config.rs`, `docker-compose.yml`, tests, and deployment docs
- External dependencies: DuckDB/DuckLake extension support, PostgreSQL catalog database, S3-compatible object storage
- Open question: the current Rust `duckdb` crate is pinned to DuckDB 1.4.3, while DuckLake v1.0 requires DuckDB 1.5.2+
