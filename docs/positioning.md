# Softprobe Runtime Competitive Positioning

**Status:** Product and technical positioning
**Last researched:** 2026-07-28

## Primary positioning

> **Softprobe Runtime is an open, tenant-scoped observability evidence lake. It
> preserves complete application and business context in DuckLake, uses
> Parquet VARIANT shredding and tenant-controlled typed-column promotion to
> create workload-specific query paths for the fields that matter, and keeps
> the evidence directly queryable with SQL by engineers and AI agents.**

The product should lead with complete evidence, data ownership, and adaptive
physical design. It should not lead with an unsupported claim that a data lake
is universally faster than ClickHouse or that Parquet shredding is unique.

## What exists today

The current durable path is:

```text
OTLP HTTP/gRPC
  -> authenticated tenant-bound runtime
  -> Arrow and temporary Parquet
  -> DuckLake transaction
  -> PostgreSQL/SQLite metadata
  -> inlined rows or object-store Parquet
  -> DuckDB SQL
```

The implementation provides:

- tenant-bound DuckLake catalogs, data paths, writers, and query engines;
- OTLP trace, log, and metric ingestion without runtime sampling, with HTTP
  payload fields preserved when instrumentation supplies them;
- DuckLake `VARIANT` columns for hot telemetry attribute maps;
- Parquet VARIANT shredding and file-level shredded-path statistics when rows
  are stored in Parquet;
- PostgreSQL-backed, tenant-scoped promotion manifests that add typed nullable
  columns and extract their values on subsequent ingest;
- open SQL access through DuckDB and DuckLake.

These are real implementation properties. They are not, by themselves,
evidence of lower total cost or faster queries than another platform.

## Why the architecture can be valuable

Observability has an awkward schema problem. Common fields should be typed and
columnar, but each customer also has high-cardinality business fields that are
important only to that customer. A single global physical schema either
becomes extremely wide and sparse or leaves those fields inside JSON-like
containers that are more expensive to search.

Softprobe uses two complementary optimization layers:

1. **VARIANT shredding** retains flexible semi-structured attributes while
   allowing stable subfields to become physical Parquet columns with
   projection and file-pruning benefits.
2. **Tenant-controlled promotion** gives a field a stable name and SQL type
   when that tenant wants an explicit, governed fast path.

This combination can avoid a global union of every tenant's business schema.
It is particularly promising for selective investigations over long
retention, where a query first finds a small set of sessions or traces and
only then reads large request or response bodies.

## What is and is not differentiated

### Meaningful differentiation

- **Customer-controlled, directly queryable evidence:** customers can retain
  data in open Parquet-backed storage and access snapshot-correct tables
  through DuckDB with the DuckLake extension.
- **Complete application and business context:** the focus extends beyond
  infrastructure telemetry to HTTP payloads, business identifiers, and whole
  sessions useful to engineers and AI agents.
- **Per-tenant physical design:** one tenant can optimize `customer.id` while
  another optimizes `device.serial` without creating a global sparse schema.
- **Explicit governance:** promotion manifests make important fields and
  their types reviewable rather than relying only on automatic inference.
- **Embedded and local analysis:** DuckDB is well suited to ad hoc, local, and
  agent-driven investigation.

### Enabling technology, not a moat

- Object storage is used by ClickHouse Cloud, Observe, Grafana Loki, and
  multiple newer observability systems.
- Columnar compression is standard across modern analytical stores.
- ClickHouse supports native JSON, Dynamic, and Variant data, dedicated
  dynamic paths, type hints, materialized columns, projections, and skip
  indexes.
- Observe markets an Iceberg-based open observability lake with accelerated
  datasets, token indexes, and a semantic context graph.
- DuckLake VARIANT shredding is an upstream capability available to every
  DuckLake adopter.
- Adding a typed column from an attribute is reproducible in other databases.

The defensible system must therefore be the policy and feedback loop around
these primitives: deciding which tenant fields to accelerate, proving the
benefit, managing their lifecycle, and applying the same semantics across SQL,
APIs, dashboards, and AI investigations.

## Competitive assessment

| Alternative | Its structural advantage | Softprobe's credible opening |
|---|---|---|
| ClickHouse / ClickStack | Mature high-rate ingestion, distributed execution, native JSON paths, indexes, projections, replication, and a growing complete observability product | Customer-controlled open files, tenant-governed schemas, complete business evidence, and simpler embedded/local SQL access |
| Observe | Managed data-lake observability, semantic context graph, accelerated datasets, integrations, explorers, alerting, and AI investigation | Simpler standard SQL, self-hosted or bring-your-own-cloud operation, direct storage control, and less proprietary transformation |
| SigNoz and other ClickHouse products | Complete open-source observability experience backed by a proven analytical engine | Evidence-lake workflows and per-tenant business schema control |
| Grafana Loki | Low-cost object-store log retention with a deliberately small metadata index | Cross-signal relational SQL and efficient typed access to selected business fields |
| Elastic and search-oriented platforms | Mature full-text search, indexing, security analytics, and ecosystem | Lower-cost long retention and open analytical access where full-text search is not primary |
| Datadog, New Relic, and Dynatrace | Product completeness, integrations, operational workflows, and managed reliability | Data ownership, transparent storage, business evidence, and potentially lower long-retention cost |
| Direct S3 plus Iceberg/DuckLake | Maximum openness and low raw storage cost | Packaged OTLP ingestion, tenant isolation, promotion governance, query APIs, and evidence correlation |

### ClickHouse

ClickHouse is the strongest database-level competitor. Its JSON type already
stores common paths as subcolumns and uses a shared structure for paths beyond
the configured dynamic-path limit. Stable schemas with explicit types remain
faster than dynamic inference, which is the same underlying reason Softprobe
promotion can help. See ClickHouse's descriptions of its
[JSON storage internals](https://clickhouse.com/blog/json-data-type-gets-even-better)
and [JSON guidance](https://clickhouse.com/blog/10-best-practice-tips).

Softprobe should not claim that ClickHouse cannot handle dynamic tenant
schemas or that Parquet is intrinsically faster. ClickHouse is likely to win
broad comparisons involving sustained ingest, high query concurrency,
real-time aggregation, replication, or full-text indexing.

The credible comparison is narrower: for selective, intermittent queries over
long-lived customer-owned evidence, can DuckLake file pruning and promoted
columns deliver acceptable latency at materially lower total cost?

### Observe

Observe is the closest product-thesis competitor. It combines an
[open observability lake](https://www.observeinc.com/), transformations,
accelerated datasets, a context graph, and a managed investigation product.
Softprobe's use of a data lake is therefore not a differentiator from Observe.

The opening is control and simplicity: standard SQL, a smaller self-hostable
stack, customer control of storage, and ingest-time typed promotion without an
additional proprietary transformation or acceleration layer. That trade-off
must be evaluated against Observe's far more complete user experience and
operational maturity.

## Best-fit market

Softprobe is best positioned initially for teams that:

- require long retention of unsampled telemetry or application evidence;
- need HTTP and business payload context, not only infrastructure signals;
- want direct SQL access to customer-controlled storage;
- run selective investigations rather than constant high-concurrency
  dashboards over all data;
- want evidence that both engineers and AI agents can inspect;
- accept a less mature visualization, alerting, and integration surface in
  exchange for control and lower potential retention cost.

It should not initially compete for workloads dominated by:

- ultra-high-rate real-time log search;
- arbitrary full-text search;
- subsecond metrics alerting;
- high dashboard concurrency;
- turnkey enterprise integrations;
- globally distributed petabyte-scale clusters.

## Claims policy

### Claims the project can make now

- DuckLake is the sole durable telemetry backend.
- Non-inlined data is retained as Parquet in configurable local or object
  storage.
- Selected telemetry attribute containers are stored as DuckLake `VARIANT`
  and can be shredded in Parquet.
- With a PostgreSQL catalog, promotion is tenant-scoped and creates typed
  columns for future ingest; SQLite catalogs skip promotion.
- Customers can query tenant-bound evidence through DuckDB SQL.
- The architecture is designed for open access and economical retention.

### Claims that require benchmark evidence

- Lower storage cost than ClickHouse, Observe, or another named platform.
- Faster queries than ClickHouse or another named analytical engine.
- A specific compression ratio.
- A specific ingest rate, concurrency level, or cold-query latency.
- A specific reduction in bytes scanned due to shredding or promotion.
- Lower total cost of ownership after compute and operational labor.

Until comparative results exist, use **designed to**, **can**, or
**architecture intended to** rather than **is faster**, **is cheaper**, or
**best-in-class**.

## Current limitations that affect positioning

- Promotion affects future ingest; historical rows are not automatically
  backfilled.
- Promotion apply and ingest extraction require a PostgreSQL catalog; SQLite
  local catalogs skip promotion.
- VARIANT file statistics require data to land in Parquet rather than remain
  catalog-inlined.
- Existing legacy MAP tables require an operator-owned migration to VARIANT.
- Flush-through ingestion makes one DuckLake commit per collector request, so
  collector batch sizing and catalog contention matter.
- Per-tenant promotion still needs quotas and lifecycle policy to prevent
  excessively wide schemas.
- Business-table promotion provisions schemas, but automatic OTLP row
  materialization is not yet complete.
- Current integration tests prove correctness and the presence of shredded
  statistics; they do not establish competitive performance or cost.

## Required proof

A reproducible benchmark should compare:

1. Softprobe VARIANT lookup without explicit promotion.
2. Softprobe promoted typed columns.
3. ClickHouse JSON with default dynamic paths.
4. ClickHouse JSON with explicit type hints or materialized columns.
5. An object-store-oriented system such as OpenObserve where practical.

Use identical OTLP data and publish:

- raw and compressed bytes;
- catalog/index metadata size;
- ingest events per second, CPU, and object-store request counts;
- cold and warm query p50, p95, and p99;
- bytes read per query;
- high-cardinality equality lookup;
- time-range aggregation;
- rare unpromoted-field lookup;
- session lookup followed by large-body retrieval;
- mixed ingest, query, and compaction behavior;
- promotion and historical rewrite cost;
- behavior across many tenants and divergent schemas.

The decisive workload is not a generic analytical benchmark. It is an
incident investigation:

```text
find one user or business transaction in 30 days of telemetry
  -> locate its sessions and traces
  -> retrieve the complete payload evidence
```

If Softprobe can demonstrate that workflow with competitive interactive
latency, substantially fewer bytes read, and lower total retention cost, the
architecture becomes a measurable advantage rather than a positioning
hypothesis.

## Strategic direction

The most defensible roadmap is a workload-driven promotion control loop:

1. observe tenant query patterns;
2. recommend candidate fields and types;
3. estimate latency, bytes-read, and storage effects before applying;
4. enforce cardinality and schema-width limits;
5. promote without interrupting ingestion;
6. optionally rewrite historical files;
7. demote cold fields without losing raw evidence;
8. report the realized cost and latency change.

That control loop, combined with complete business evidence and open SQL
access, is more difficult to copy than the storage format alone.

## Primary sources

- [Apache Parquet VARIANT shredding specification](https://parquet.apache.org/docs/file-format/types/variantshredding/)
- [Apache Parquet format versions](https://parquet.apache.org/docs/file-format/versions/)
- [DuckLake data types and VARIANT statistics](https://ducklake.select/docs/stable/specification/data_types)
- [DuckLake schema evolution](https://ducklake.select/docs/stable/duckdb/usage/schema_evolution)
- [ClickHouse JSON storage internals](https://clickhouse.com/blog/json-data-type-gets-even-better)
- [ClickHouse JSON guidance](https://clickhouse.com/blog/10-best-practice-tips)
- [Managed ClickStack architecture](https://clickhouse.com/cloud/clickstack)
- [Observe platform architecture](https://www.observeinc.com/)
- [Observe pricing](https://www.observeinc.com/pricing)
- [Observe acceleration behavior](https://docs.observeinc.com/docs/acceleration-manager)
- [Grafana Loki storage architecture](https://grafana.com/docs/loki/latest/configure/storage/)
- [SigNoz architecture](https://signoz.io/docs/architecture/)
