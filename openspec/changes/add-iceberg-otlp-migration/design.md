## Context
Migrate recording/replay data from MongoDB to an OpenTelemetry-compatible pipeline with Iceberg-backed raw storage and ClickHouse metadata for fast queries. See `docs/design.md` for detailed architecture, schemas, and phases.

## Goals / Non-Goals
- Goals: OTLP ingestion, Iceberg raw storage, ClickHouse metadata ETL, Iceberg scan API for data retrieval, compatibility
- Non-Goals: Full agent rewrite to OTel SDK (long-term), replay pipeline redesign, storing Iceberg internal paths in ClickHouse

## Decisions
- Rust OTLP ingestion service with session-aware buffering and multi-app row-groups
- Iceberg `raw_sessions` Parquet with day-only partitions; row group per session; internal sort by session_id, trace_id, timestamp
- Real-time ETL to ClickHouse ONLY (`session_metadata`, `recording_events`, `recording_ops_minute`, `recording_tag_counts`) for metadata queries
- ALL metadata in ClickHouse; NO Iceberg internal paths (file URIs, row group indices) stored
- Query pattern: ClickHouse for metadata filtering → Iceberg scan API with predicates for full data retrieval
- Loose coupling: ClickHouse and Iceberg operate independently; Iceberg manages its own file locations

## Risks / Trade-offs
- Collector bottlenecks → horizontal scale, consistent hashing
- ETL lag → streaming jobs, SLA <5 minutes
- Query fan-out → file grouping and row-group pruning, caching

## Migration Plan
- No migration step is needed. Brand new deployment. Keep it simple.

## Open Questions
- Session timeout defaults (recommend 30 minutes inactivity)
- Handling >10MB payloads (external object reference)

## References and Canonical Sources
- Canonical system architecture: `docs/design.md` §2 Architecture Overview
- Canonical raw storage schema and partitioning: `docs/design.md` §4 Iceberg Raw Data Storage
- Canonical metadata schemas (ClickHouse and session metadata): `docs/design.md` §5 Real-Time ETL for Metadata Extraction (5.2, 5.3)

Note: This change design intentionally summarizes deltas and decisions only. For the full architecture diagrams and complete DDLs, refer to the canonical sections above to avoid duplication and drift.

## Key Design Decisions (copied from docs/design.md)

**Decision 1**: Build a custom Rust OTLP Collector instead of using the standard Collector
- **Rationale**: Enables session-aware buffering, multi-app row-group batching, and direct Iceberg writes with tuned flush triggers
- **Alternatives Considered**: Standard OTel Collector (lacks session-aware buffering and row-group control), direct agent-to-backend (larger change for agents initially)

**Decision 2**: Store payloads inline in Iceberg events array
- **Rationale**: Simplifies query patterns, reduces S3 object count, enables efficient batch reads
- **Alternatives Considered**: Separate payload storage (more complex, more S3 objects)

**Decision 3**: Multi-app row groups in single Parquet file
- **Rationale**: Reduces S3 PUT/GET operations, maintains query efficiency via row group pruning
- **Alternatives Considered**: One file per session (too many files), one file per app (inefficient for multi-app queries)

**Decision 4**: ClickHouse for metadata queries
- **Rationale**: Fast OLAP queries, optimized for time-series, excellent performance for high-frequency lookups
- **Alternatives Considered**: MongoDB time-series (scalability concerns), Elasticsearch (higher cost)

**Decision 5**: Real-time ETL with <5 minute lag
- **Rationale**: Balance between freshness and performance, acceptable for most use cases
- **Alternatives Considered**: Batch ETL (higher lag, lower cost), streaming ETL (lower lag, higher complexity)

