# Current Architecture Decisions

This file contains decisions that define the current runtime architecture.
Superseded Iceberg-era decisions are preserved in
[`legacy/decision-log-iceberg-era.md`](legacy/decision-log-iceberg-era.md).

## ADR-014: DuckLake is the only runtime table format

**Date:** 2026-07-16
**Status:** Accepted

### Context

The former Apache Iceberg design required a REST catalog, Lakekeeper,
snapshot/manifest maintenance, and a parallel query path. The application also
carried an in-memory buffer, staged Parquet tier, and application WAL to reduce
small-file pressure. Those layers increased operational and code complexity.

The runtime already uses DuckDB for writes and queries. DuckLake provides the
catalog, snapshots, data-file management, and DuckDB integration directly.

### Decision

Use DuckLake as the sole durable store for spans, logs, and metrics.

- PostgreSQL is the production/multi-tenant catalog.
- SQLite is the local multi-client catalog.
- Parquet data lives under the configured local or object-store `data_path`
  when rows are not catalog-inlined.
- Apache Iceberg, Lakekeeper, the application ingest buffer, staged tier, and
  application WAL are not supported runtime paths.

### Consequences

- Each OTLP request writes through in one DuckLake transaction; the upstream
  OpenTelemetry collector owns batching.
- DuckLake data inlining is used to avoid tiny object-store files for normal
  collector batches.
- Query workers ATTACH the same tenant DuckLake scope as ingest.
- Maintenance uses DuckLake merge, snapshot expiry, and old-file cleanup
  procedures.
- Historical `union_*`, `committed_*`, `buffer_*`, `staged_*`, and
  `iceberg_*` query names may resolve to the same committed DuckLake tables for
  compatibility; they do not imply multiple physical tiers.

## Current invariant: catalog backend policy

Use PostgreSQL for production and tenant-scoped operation, and SQLite for local
multi-client development. Reject DuckDB as a DuckLake catalog backend because
it is single-client only.

SQLite's `META_JOURNAL_MODE 'WAL'` is a database journal setting and must not
be described as an application ingest WAL.

## Current invariant: flush-through ingest

Do not batch telemetry inside the runtime. Decode one OTLP request and commit
its records immediately through `DuckLakeWriter`.

The writer may create a temporary local Parquet file to bridge Arrow into
DuckLake. That file is deleted after commit or failure and is not durable,
queryable, or recoverable storage.

Conflict retries belong to DuckLake. If retries are exhausted, surface the
failure so the exporter can retry.

## Current invariant: tenant-bound runtime engines

Resolve tenant identity at authentication/instantiation boundaries and create a
tenant-bound runtime engine containing storage, ingest, query, and optional
session/catalog services. Operational APIs must not accept an arbitrary tenant
or DuckLake scope after binding.

For PostgreSQL catalogs, store each tenant's metadata schema and data path in
the durable scope registry.
