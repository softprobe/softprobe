## Context
We need a long-term architecture that avoids Iceberg metadata bloat and small-file churn while enabling sub-second read freshness. The Mooncake whitepaper proposes a write-aware ingestion engine with buffering, object-store WAL, and union-read semantics.

## Goals / Non-Goals
- Goals:
  - Sub-second read freshness without frequent Iceberg commits
  - Controlled metadata growth and larger Parquet files
  - Object-store as source of truth for committed data with fast local WAL/cache
  - DuckDB SQL as the primary query interface with Iceberg catalog compatibility
  - Preserve session-based row-grouping semantics for traces/logs
- Non-Goals:
  - Perfect code reuse with the current commit-per-flush path
  - Building a REST query API as the primary interface (DuckDB is the contract)

## Decisions
- Decision: Replace direct Iceberg commits with a buffer + WAL ingest engine.
  - Why: decouples freshness from metadata commits and reduces write amplification.
- Decision: Use Arrow in-memory buffers and Parquet/Arrow WAL segments that reuse the same schema as Iceberg tables.
  - Why: removes JSON schema duplication (DRY) and allows union-read with DuckDB `read_parquet`/`read_ipc`.
- Decision: Use a local-only WAL as the ingestion source of truth until commit, with object-store uploads only during Iceberg commits.
  - Why: keeps the hot path fast for small row groups and avoids per-write object-store latency.
- Decision: Use DuckDB as the query engine and keep all data paths Iceberg/Catalog compatible.
  - Why: DuckDB is the supported SQL interface and must interoperate with Iceberg catalogs (REST/Glue/etc.) without bespoke APIs.
- Decision: Preserve session-based row-grouping when flushing Parquet for traces/logs.
  - Why: query performance depends on row groups clustered by session and must not regress.
- Decision: Use stable DuckDB views for union-read that reference partitioned WAL/staged directories (glob-based) or a local WAL manifest, and only refresh on schema change.
  - Why: avoids rebuilding views on every WAL write while keeping new data visible for real-time queries.
- Decision: Update a small local `wal_manifest.json` on a cadence (time or file-count) instead of per-ingest.
  - Why: keeps WAL ingestion hot path fast while bounding metadata churn and view refresh frequency.
## Deferred
- Iceberg-native index files (Puffin) and deletion vectors are deferred until we have update/delete workloads (e.g., CDC, GDPR deletions, dedup).

## Risks / Trade-offs
- Increased system complexity and more moving parts.
- New operational requirements for local disk/NVMe sizing and health.
- Uncommitted data is not durable across node loss until the commit completes.
- Requires careful recovery semantics to reconcile WAL + buffer + Iceberg snapshots.
- Union-read needs a DuckDB-accessible representation (e.g., temp tables/views) without breaking Iceberg catalog compatibility.
- Session-based row group clustering constrains compaction and file merge policies.

## Migration Plan
1. Implement new ingest engine behind a feature flag.
2. Dual-write for a limited period to validate correctness.
3. Switch query path to union-read for the new engine.
4. Remove legacy commit-per-flush path once validated.

## Open Questions
- Recovery SLA and how much WAL history to retain.
- Default WAL manifest update cadence (seconds vs. file count) and retention horizon for “last N days” queries.
