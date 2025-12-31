## Why
Build a standalone OpenTelemetry-compatible collector with Apache Iceberg storage backend. This provides a scalable, cost-effective alternative to traditional time-series databases while maintaining full OTLP protocol compliance for traces, logs, and metrics.

## What Changes
- Implement complete OTLP v1 ingestion API: `/v1/traces`, `/v1/logs`, `/v1/metrics`
- Add session-aware buffering: unified for traces/logs, separate for metrics (aggregations)
- Write telemetry data to Iceberg tables (separate tables for traces, logs, metrics)
- Provide query APIs using direct Iceberg scans with predicate pushdown
- Support multi-app row groups in Parquet for efficient cross-application queries
- Leverage Iceberg's built-in metadata (manifests, partition stats) for query optimization

## Impact
- New specs: `ingestion` (OTLP traces/logs/metrics), `storage` (Iceberg tables), `metadata` (Iceberg built-in metadata), `query` (retrieval APIs)
- Affected code: Rust OTLP collector ([src/api/](../../../src/api/)), storage layer ([src/storage/](../../../src/storage/)), query engine ([src/query/](../../../src/query/)), Iceberg table definitions

## Notes
- Full OTLP protocol compliance for interoperability with standard OTel SDKs
- Target: 10k+ spans/sec throughput per collector instance
- Support multiple object storage backends: S3, MinIO, Cloudflare R2
- Metrics storage in Iceberg is experimental (time-series aggregations may require different storage)
