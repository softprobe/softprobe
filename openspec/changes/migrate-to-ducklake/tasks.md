## 1. Discovery and Spike

- [ ] 1.1 Verify the Rust DuckDB binding can load `ducklake` with DuckDB 1.5.2+ or choose a system/sidecar integration path.
- [ ] 1.2 Run a minimal DuckLake spike: attach PostgreSQL catalog, create table, insert Parquet-backed data, query from another connection, compact, expire snapshots.
- [ ] 1.3 Confirm MinIO/S3 endpoint, credentials, and `httpfs` settings work for DuckLake data paths.

## 2. Storage Layer

- [ ] 2.1 Add `ducklake` configuration and defaults.
- [ ] 2.2 Add `src/storage/ducklake` with attach/setup, table DDL, options, and schema creation.
- [ ] 2.3 Introduce a committed-storage trait used by ingest and query code.
- [ ] 2.4 Implement DuckLake commits from staged record batches with `INSERT INTO ... SELECT ... FROM read_parquet(...)`.
- [ ] 2.5 Preserve schema promotion for traces, logs, and metrics.

## 3. Query Path

- [ ] 3.1 Install/load `ducklake` and catalog extensions in every DuckDB query worker.
- [ ] 3.2 Replace committed Iceberg views with DuckLake table views.
- [ ] 3.3 Preserve buffer and staged views for near-real-time freshness.
- [ ] 3.4 Remove pinned Iceberg metadata and `iceberg_scan` fallback paths after parity tests pass.

## 4. Maintenance and Deployment

- [ ] 4.1 Replace Iceberg metadata maintenance with DuckLake merge, expire, cleanup, and optional inlined-data flush jobs.
- [ ] 4.2 Update docker-compose to remove Lakekeeper and use PostgreSQL as the DuckLake catalog.
- [ ] 4.3 Update config files, scripts, and docs to describe DuckLake deployment.
- [ ] 4.4 Remove Iceberg dependencies and feature flags after the DuckLake backend is the only supported backend.

## 5. Tests

- [ ] 5.1 Add table creation and append/query integration tests.
- [ ] 5.2 Add union-read freshness tests for buffer + staged + DuckLake committed data.
- [ ] 5.3 Add WAL replay tests that recover staged data before DuckLake commit.
- [ ] 5.4 Add schema promotion tests against DuckLake tables.
- [ ] 5.5 Add maintenance tests for compaction and snapshot expiration.
- [ ] 5.6 Add migration/parity tests comparing DuckLake query results to existing Iceberg fixtures where practical.
