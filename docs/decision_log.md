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

## Current invariant: explicit business attributes and tenant promotion

Business identifiers use an application-owned `sp.*` attribute convention.
Softprobe does not invent or auto-promote those keys.

Schema promotion is tenant-scoped:

- apply manifests with authenticated `POST /v1/promotions/apply`;
- store active specs in the tenant metadata schema (`promotion_specs`);
- add only nullable telemetry columns, extracted on **future** ingest;
- do not configure promotion through process-global `config.yaml`.

Canonical contract: [`promotion.md`](promotion.md).

## Proposed: metrics time-series layout on DuckLake

**Date:** 2026-08-15 (redesign after GreptimeDB study; original goals 2026-08-14)
**Status:** Proposed — not accepted until the verification report maps every
AC-\* id in [`metrics-timeseries-layout.md`](metrics-timeseries-layout.md).

### Context

The fat `metrics` event table cannot serve Grafana under Astronomy Shop ingest:
mixed-name Parquet files, day-floored snapshot expiry (thousands of live
snapshots), and `max_query_range_seconds = 86400`. Product constraints still
forbid a second TSDB, an application WAL, and deleting tenant data to make
queries fast.

### Decision (proposed)

Keep DuckLake as the only store. **Learn from GreptimeDB** (TWCS, inverted-index *ideas*, metric-engine multiplexing, Flow-style rollups) without forking or embedding it. Split metrics into per-day `metric_series` + `metric_postings` + skinny samples/hist, with **TWCS-shaped** maintenance, 5m/1h downsamples, and `metric_collapse_job_1h`. **Remove** Softprobe-imposed Prom `max_query_range` (retention/TTL bounds data, like Greptime). Expire snapshots at **second** granularity.

**Programmable Softprobe∶Greptime gate (G9):** shared OTLP fixtures; Softprobe_p95 ≤ **10 ×** Greptime_p95 on a pinned query set under `make test-perf` (`COMPARE_GREPTIME=1`). Beating Greptime remains a non-goal. Expected healthy gap ~2–10× (§4.4). Matching Greptime p50 requires reopening G1 or flush-through — not a silent sidecar.

**Query range:** no Softprobe-imposed Prom max (not 90d / not 180d). Like Greptime, retention/TTL decides availability; planner uses 1h/collapse for all windows > 48h. Tested SLO windows remain 30d / 90d / 180d.

### Consequences (if accepted)

- Flush-through ingest still commits once per OTLP request; snapshot **count** is `ceil(age / commit_interval) + headroom`.
- Prom resolves series from day postings (not SST row-group prune) and fails loud at `max_series`.
- SQL names `union_metrics` / `committed_metrics` remain; Prom must not scan the fat/view path.
- Layout + G9 tests live under `make test-perf` (no new public Make target).
- Research clone `./greptime` is reference + **external** bench target only.
- Ready gate: 49 AC ids, `release_full` JSON schema, Greptime ratio rows required.
