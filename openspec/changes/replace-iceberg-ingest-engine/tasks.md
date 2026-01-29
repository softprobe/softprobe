## 1. Implementation
- [x] 1.1 Define WAL format and object-store layout for buffered data (Arrow/Parquet, no JSON)
- [x] 1.2 Unify WAL/flush schema with Iceberg table schemas (DRY)
- [x] 1.3 Implement buffer manager (in-memory Arrow + NVMe cache) with size/time flush policy
- [x] 1.4 Preserve session-based row-grouping when flushing traces/logs to Parquet
- [x] 1.5 Implement DuckDB union-read query path (read_parquet/read_ipc + Iceberg snapshot)
- [x] 1.6 Implement background optimizer to flush/compact buffer into Iceberg
- [x] 1.7 Update configuration and operational tooling
- [x] 1.8 Add end-to-end tests for ingestion, freshness, and recovery (DuckDB union-read + WAL replay)

## 2. WAL View + Manifest Updates
- [x] 2.1 Define local `wal_manifest.json` schema and update cadence (time/file-count) for WAL visibility
- [x] 2.2 Implement WAL manifest writer (append/update outside ingest hot path)
- [x] 2.3 Update DuckDB union-read to snapshot in-memory buffer, staged files, and Iceberg, refreshing only on schema change or staged updates
- [x] 2.4 Add integration tests that validate real-time queries include Buffer + Staged + Committed via DuckDB SQL
- [x] 2.5 Add performance checks to confirm sub-second warm queries for "last N days" paths
- [x] 2.6 Implement buffer snapshot as Arrow RecordBatch for DuckDB query registration (enables buffer queries without disk writes)

## 3. End-to-End Performance Validation
- [x] 3.1 Define performance targets and acceptance criteria for sub-second DuckDB queries (cold vs warm)
- [x] 3.2 Add end-to-end tests that stream events, then query via DuckDB SQL for WAL + staged + Iceberg
- [x] 3.3 Add parallel query load test to validate <1s latency under concurrency
- [x] 3.4 Capture and report metrics (p50/p95 latency, throughput) for near real-time queries
