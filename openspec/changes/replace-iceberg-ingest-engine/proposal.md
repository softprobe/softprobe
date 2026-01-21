## Why
The current Iceberg commit-per-flush design causes metadata bloat, small files, and slow real-time queries. We need a write-aware ingestion engine that preserves Iceberg openness while delivering low-latency writes and reads.

## What Changes
- **BREAKING** Replace direct Iceberg commits with a Moonlink-style ingest engine: object-store WAL + buffer + background optimizer.
- Add union-read query path that merges uncommitted buffered data with Iceberg snapshots for freshness.
- Introduce NVMe-backed cache for hot data with object-store as source of truth.
- Update configuration and operational workflows for the new ingestion engine.
- Defer update/delete indexing (Puffin + deletion vectors) as optional future work for non-OTLP use cases.

## Impact
- Affected specs: ingest-engine, storage-engine, query-engine
- Affected code: ingestion pipeline, compaction/maintenance, query path, configuration, tests
