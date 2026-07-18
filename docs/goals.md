# Softprobe Runtime Goals

**Status:** Current
**Last updated:** 2026-07-18

## Product goal

Store complete application telemetry, including HTTP request and response
context, so engineers and AI agents can investigate real business sessions
with SQL.

## Current technical goals

1. **Complete capture**
   - Accept OTLP traces, logs, and metrics.
   - Preserve HTTP bodies and business attributes needed for investigation.
   - Avoid sampling in this storage service.

2. **Simple durable storage**
   - Use one DuckLake write and query path.
   - Use PostgreSQL metadata for production and SQLite metadata for local
     development.
   - Keep non-inlined data in Parquet under a configurable local or
     object-store data path.

3. **Tenant isolation**
   - Bind tenant identity before ingest, query, session, or promotion work.
   - Give each provisioned tenant a DuckLake metadata schema and data path.

4. **SQL accessibility**
   - Query with DuckDB through the attached DuckLake catalog.
   - Support telemetry APIs for common evidence searches.
   - Provide tenant-scoped connection material for local DuckDB clients.

5. **Operational simplicity**
   - Let the OpenTelemetry collector batch upstream.
   - Commit each request directly to DuckLake.
   - Rely on DuckLake for conflict retries, snapshots, data inlining, and file
     management.
   - Run DuckLake-native compaction and retention maintenance.

6. **Schema evolution without parallel storage paths**
   - Keep canonical trace, log, and metric schemas in one shared module.
   - Add tenant-scoped nullable columns through promotion manifests.

## Non-goals

- Reintroducing Apache Iceberg or a second durable table format.
- Maintaining an application-level ingest buffer, staged tier, or WAL.
- Hiding failed commits behind an application retry/fallback path.
- Accepting arbitrary tenant identifiers in already tenant-bound operational
  APIs.

## References

- [Current architecture](design.md)
- [Current architecture decisions](decision_log.md)
- [Instrumentation guide](instrumentation_guide.md)
- [Ad hoc DuckDB/DuckLake queries](adhoc-duckdb-ducklake.md)
- [Legacy documentation](legacy/README.md)
