# Softprobe Runtime Goals

**Status:** Current
**Last updated:** 2026-08-12

## Product goal

Preserve production AI traces and application recordings as durable,
customer-controlled evidence that can be reused across investigation,
evaluation, regression, governance, and continuous-improvement workflows.
Keep the original business context directly queryable with SQL so engineers
and AI agents can build those workflows on open data rather than short-lived
operational telemetry.

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
   - Add tenant-scoped nullable columns through promotion manifests
     (`POST /v1/promotions/apply`).
   - Keep `sp.*` as an explicit instrumentation convention; promote only the
     fields a tenant declares.

7. **Query-only observability compatibility (in progress)**
   - Keep OTLP as the canonical write path.
   - Expose Prometheus-, Loki-, and Tempo-compatible **query** APIs so
     existing Grafana datasources can read lake evidence without a second
     write pipeline. See [compat/matrix.md](compat/matrix.md).

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
- [Schema promotion](promotion.md)
- [Ad hoc DuckDB/DuckLake queries](adhoc-duckdb-ducklake.md)
- [Compatibility matrix (Prom/Loki/Tempo)](compat/matrix.md)
- [Legacy documentation](legacy/README.md)
