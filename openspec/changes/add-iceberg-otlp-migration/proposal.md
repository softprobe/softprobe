## Why
MongoDB-based storage for recording/replay data does not scale efficiently for high-volume workloads. It results in high costs, degraded query performance, and limited analytics. We need a scalable, OpenTelemetry-compatible pipeline that stores raw data in Apache Iceberg while serving fast queries from metadata indexes.

## What Changes
- Introduce OTLP ingestion via a Rust service exposing `/v1/traces` and session-aware buffering
- Write raw spans to an Iceberg `raw_sessions` table (Parquet, multi-app row groups)
- Add real-time ETL to populate ClickHouse metadata tables and an Iceberg `session_metadata` table
- Enable Java applications to query ClickHouse metadata and Iceberg payloads directly (shared client library) instead of a centralized Rust query router
- Maintain compatibility for existing MongoDB query parameters (greenfield deployment; no dual-write)

## Impact
- Affected specs: `ingestion`, `storage`, `metadata`, `query`, `compatibility`
- Affected code: Rust OTLP ingestion (`softprobe-otlp-backend`), Java services (`sp-storage`, direct query module), ETL jobs (Flink/Spark), ClickHouse schemas, Iceberg tables

## Notes
- Greenfield deployment: no dual-write or phased traffic migration in scope
- Targets: <100ms P95 metadata queries, <500ms P95 session queries, >85% storage cost reduction
