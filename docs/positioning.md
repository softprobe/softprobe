# Softprobe Runtime: AI Evidence Lake Positioning

**Status:** Product and technical positioning
**Last researched:** 2026-07-28

## Primary positioning

> **Softprobe Runtime is the durable evidence foundation for production AI. It
> preserves AI traces and recordings as customer-controlled data assets,
> directly queryable with standard SQL and reusable across investigation,
> evaluation, regression, governance, and continuous-improvement workflows.**

The product should lead with the durable value of AI evidence. Open storage,
DuckLake, Parquet VARIANT shredding, and tenant-controlled column promotion are
the mechanisms that preserve and keep that evidence useful; they are not the
category definition.

## Category thesis

Traditional software observability treats most telemetry as operational
exhaust:

```text
software telemetry
  -> detect and diagnose an incident
  -> retain for a short operational window
  -> aggregate or discard
```

Production AI changes the value of a trace:

```text
AI trace or recording
  -> investigate behavior and failures
  -> evaluate quality, safety, cost, and outcomes
  -> become a regression or replay case
  -> support audit, governance, and lineage
  -> contribute to future system improvement
```

An AI trace can contain prompts, model responses, retrieval context, tool
calls, business inputs and outcomes, token usage, cost, feedback, and
evaluations. Those connected facts are not merely developer diagnostics. They
are a record of how an intelligent system behaved and why.

The same recording can acquire new value long after its original incident
window. A failure becomes a regression test. A high-quality interaction
becomes an evaluation example. A disputed outcome becomes audit evidence. A
population of traces becomes the basis for product analytics or a curated
improvement dataset.

Softprobe therefore treats production AI evidence as a durable data asset, not
a disposable by-product of monitoring.

## Product principles

### Preserve before predicting future value

Teams cannot know at ingest time every question they will ask later. Preserve
the original context without runtime sampling so future investigations and
evaluations are not constrained by yesterday's dashboards or indexes.

### Keep the evidence customer-controlled and open

AI recordings may outlive the current observability vendor, UI, model, and
application architecture. Store them in an open Parquet-backed lake and expose
snapshot-correct tables through DuckDB and DuckLake rather than making a
proprietary query surface the only path to the data.

### Make retained evidence progressively useful

Flexible storage alone can become expensive to query. VARIANT shredding keeps
stable paths columnar inside Parquet, while tenant-controlled promotion gives
important fields governed names and SQL types. The system can optimize what
becomes important without discarding the full original recording.

### Serve humans and AI agents from the same evidence

Engineers, evaluators, analysts, compliance workflows, and AI agents should
operate on the same traceable records. Standard SQL and stable evidence
anchors make findings inspectable and reproducible across those consumers.

### Build a learning lifecycle, not only an incident workflow

The strategic product lifecycle is:

```text
capture -> investigate -> evaluate -> curate -> regress -> improve
```

The current runtime implements the storage, query, promotion, and score
foundations for this direction. Dataset curation, replay/regression workflows,
and broader governance automation remain product work rather than claims of
current completeness.

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
- promotion manifests that add typed nullable columns and extract their values
  on subsequent ingest, using tenant-scoped PostgreSQL metadata in production
  or a local single-scope SQLite catalog;
- open SQL access through DuckDB and DuckLake.

These are real implementation properties. They are not, by themselves,
evidence of lower total cost or faster queries than another platform.

## Why the architecture supports the thesis

Durable AI recordings have an awkward schema problem. Common fields should be
typed and columnar, but each customer, model, agent, tool, and business process
introduces fields that may become important only later. A single global
physical schema either becomes extremely wide and sparse or forces evolving
evidence into rigid schemas chosen before its value is understood.

Softprobe uses two complementary optimization layers:

1. **VARIANT shredding** retains flexible semi-structured attributes while
   allowing stable subfields to become physical Parquet columns with
   projection and file-pruning benefits.
2. **Tenant-controlled promotion** gives a field a stable name and SQL type
   when that tenant wants an explicit, governed fast path.

This combination can avoid a global union of every tenant's business schema
without reducing the retained recording to only the fields promoted today. It
is particularly promising for selective investigations over long retention,
where a query first finds a small set of sessions or traces and only then reads
large prompts, responses, tool payloads, or other bodies.

## What is and is not differentiated

### Meaningful differentiation

- **Customer-controlled, directly queryable evidence:** customers can retain
  data in open Parquet-backed storage and access snapshot-correct tables
  through DuckDB with the DuckLake extension.
- **AI recordings as durable assets:** the retention model supports reuse for
  evaluation, regression, governance, and improvement rather than only
  short-lived incident diagnosis.
- **Connected application and business context:** the focus extends beyond
  infrastructure telemetry to model interactions, HTTP payloads, business
  identifiers, outcomes, scores, and whole sessions useful to humans and AI
  agents.
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
| ClickHouse / ClickStack | Mature high-rate ingestion, distributed execution, native JSON paths, indexes, projections, replication, and a growing complete observability product | Durable AI evidence on customer-controlled open files, tenant-governed schemas, and simpler embedded/local SQL access |
| Observe | Managed data-lake observability, semantic context graph, accelerated datasets, integrations, explorers, alerting, and AI investigation | AI evidence as a customer-controlled asset, standard SQL, self-hosted or bring-your-own-cloud operation, and less proprietary transformation |
| SigNoz and other ClickHouse products | Complete open-source observability experience backed by a proven analytical engine | AI evidence-lifecycle workflows and per-tenant business schema control |
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

- operate AI applications, agents, or model-powered business workflows;
- regard production traces as future evaluation, regression, audit, or
  improvement assets;
- require long retention of unsampled AI and application evidence;
- need prompts, responses, tool activity, HTTP payloads, scores, and business
  outcomes to remain connected;
- want direct SQL access to customer-controlled storage;
- want the same evidence available to engineers, evaluators, analysts,
  governance workflows, and AI agents;
- accept a less mature visualization, alerting, and integration surface in
  exchange for evidence ownership and long-term reuse.

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
- Promotion creates typed columns for future ingest, using tenant-scoped
  PostgreSQL metadata in production or a local single-scope SQLite catalog.
- Customers can query tenant-bound evidence through DuckDB SQL.
- Durable records can be revisited and queried after their original incident
  window rather than being reduced to short-lived aggregates.
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
- PostgreSQL is the multi-tenant promotion path; SQLite promotion is limited
  to a local single-scope catalog.
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

The decisive workload is not a generic analytical benchmark. It is reuse of
one body of production evidence across its lifecycle:

```text
find an AI outcome in months of production evidence
  -> reconstruct the connected trace, prompts, responses, tools, and outcome
  -> attach or query evaluations
  -> curate the case into a durable regression dataset
  -> revisit it after models, prompts, or agents change
```

If Softprobe can demonstrate that workflow with competitive interactive
latency, substantially fewer bytes read, and lower total retention cost, the
architecture becomes a measurable advantage. Product proof must also show
that evidence identity, lineage, and relevant context survive each transition
without requiring export into a separate closed system.

## Strategic direction

The most defensible roadmap completes the evidence learning lifecycle:

1. preserve complete connected AI recordings with stable evidence identity;
2. attach human, automated, and model-based evaluations without mutating the
   source evidence;
3. curate versioned datasets from production evidence with lineage back to
   the original trace;
4. turn failures and important edge cases into executable regression cases;
5. compare behavior across model, prompt, agent, and tool versions;
6. expose governance, retention, deletion, and audit controls over the same
   evidence;
7. make the lifecycle directly usable by engineers and AI agents.

Under that product lifecycle, a workload-driven physical-design loop keeps the
growing asset economical and responsive:

1. observe tenant query patterns;
2. recommend candidate fields and types;
3. estimate latency, bytes-read, and storage effects before applying;
4. promote, rewrite, or demote fields without losing the original evidence;
5. report the realized cost and latency change.

The combination of a durable evidence lifecycle, business context, lineage,
open SQL access, and adaptive physical design is more difficult to copy than
the storage format alone.

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
