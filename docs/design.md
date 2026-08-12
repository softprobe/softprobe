# Softprobe Runtime Architecture

**Status:** Current
**Storage backend:** DuckLake
**Last verified against:** `src/` on 2026-07-18

## Overview

`softprobe-runtime` is a Rust service that combines:

- OTLP trace, log, and metric ingestion over HTTP
- OTLP trace ingestion over gRPC
- tenant-scoped DuckLake storage and DuckDB queries
- telemetry search and detail APIs
- schema promotion and optional dropdown metadata

DuckLake is the only durable telemetry backend. Apache Iceberg, the
application-level ingest buffer, staged Parquet tier, and application WAL have
been removed. Historical documents for those designs are under
[`legacy/`](legacy/README.md).

## Runtime data flow

```text
OTLP HTTP/gRPC request
        |
        v
authenticate and bind tenant
        |
        v
decode OTLP -> Span / Log / Metric
        |
        v
Arrow RecordBatch -> temporary local Parquet
        |
        v
DuckDB transaction:
  CREATE TABLE IF NEEDED
  INSERT ... SELECT read_parquet(...)
        |
        v
DuckLake
  metadata: PostgreSQL (production) or SQLite (local)
  rows: catalog-inlined or Parquet under data_path
        |
        v
DuckDB query workers ATTACH the same tenant scope
```

Each OTLP request is written through immediately. Upstream OpenTelemetry
collectors own batching. The temporary Parquet file is only an input adapter
between Arrow and DuckLake and is deleted after the transaction; it is not a
staged durability tier.

DuckLake data inlining decides where committed rows live:

- batches at or below `ducklake.data_inlining_row_limit` may stay in the
  metadata catalog;
- larger writes become Parquet files under `ducklake.data_path`.

Both forms are committed DuckLake data and are queried through the same
attached catalog.

## Storage and catalog

The writer in `src/storage/ducklake/` (`writer.rs` plus domain modules
`otlp.rs`, `scores.rs`, `promotion.rs`) is the sole durable writer. It:

1. resolves the tenant's DuckLake scope;
2. applies active telemetry-column promotions;
3. converts records to Arrow using the canonical schemas in
   `src/storage/schema/`;
4. writes a temporary Parquet file;
5. checks out an already-attached DuckDB connection from the scope's writer
   pool;
6. creates the target table if necessary and inserts the rows in one
   transaction;
7. removes the temporary file.

Supported catalog backends:

- `postgres`: production and tenant-scoped deployments;
- `sqlite`: local multi-client development;
- `duckdb`: rejected because DuckLake documents it as single-client only.

SQLite uses `META_JOURNAL_MODE 'WAL'` and a busy timeout. This is SQLite's
catalog journal mode, not the removed Softprobe application WAL.

DuckLake's own conflict retry settings are pinned on writer connections.
The runtime sets `ducklake_max_retry_count=10`,
`ducklake_retry_backoff=1.5`, and `ducklake_retry_wait_ms=100`. Exhausted
ingest writes are surfaced to the HTTP exporter as `503 Service Unavailable`;
Softprobe does not add another hidden write retry loop.

Each catalog scope owns a pool of already-attached writer connections.
`ducklake.writer_pool_size` defaults to `4` and is clamped to `1..=16`.
Writes run on Tokio's blocking pool so PostgreSQL and object-store waits do not
pin async workers.

## Tenant isolation

Authentication resolves a tenant before operational work begins. A
`RuntimeEngine` is then built and cached for that tenant with:

- a tenant-bound DuckLake metadata schema and data path;
- a tenant-bound writer and query engine;
- an optional Postgres dropdown catalog.

With a PostgreSQL catalog, `DuckLakeScopeResolver` stores scope mappings in the
configured registry schema. Operational APIs do not accept arbitrary tenant or
scope parameters after binding.

## Telemetry tables

DuckLake creates tables lazily from Arrow schemas.

### `traces`

Core columns include:

- correlation: `session_id`, `trace_id`, `span_id`, `parent_span_id`
- tenancy/application: `app_id`, `organization_id`, `tenant_id`
- timing/status: `timestamp`, `end_timestamp`, `status_code`,
  `status_message`, `record_date`
- OTLP data: `attributes`, `events`, `span_kind`, `message_type`
- HTTP data: request method/path/headers/body and response
  status/headers/body

Rows are inserted ordered by `record_date`, `app_id`, `session_id`, and
`timestamp`.

### `logs`

Core columns include `session_id`, timestamps, severity, body, attributes,
resource attributes, trace/span correlation, and `record_date`.

### `metrics`

Core columns include metric name, description, unit, type, timestamp, value,
attributes, resource attributes, and `record_date`.

Phase 0 also stores nullable classic histogram / summary fidelity columns on
the same row shape (gauge/sum leave them `NULL`):

- `count`, `sum`
- `bucket_counts`, `explicit_bounds` (classic histogram)
- `quantiles` (summary: list of `{quantile, value}`)
- `aggregation_temporality`
- `exemplars_json`

When OTLP omits histogram `sum` (valid for negative observations), the fidelity
`sum` column is stored as SQL `NULL`. The scalar `value` column still uses
`0.0` in that case for backward SQL compatibility — adapters reconstructing
Prometheus `_sum` must read the fidelity `sum` column, not `value`.

Existing DuckLake `metrics` tables are widened on write with
`ALTER TABLE … ADD COLUMN IF NOT EXISTS` (`ensure_metrics_fidelity_columns`).
If a fidelity name already exists with an incompatible type (e.g. a leftover
promotion column), widen fails loud rather than writing into the wrong type.
Column names and SQL types are owned by `src/metrics_fidelity.rs`.
Exponential / native histograms are not stored; those datapoints are skipped
with a stable `unsupported_feature` log.

### `scores`

Immutable LLM evaluation records are stored separately from spans because an
evaluation commonly arrives after the observed work. A score targets at least
one trace, span, or session and contains one typed numeric, categorical,
boolean, or text value. `score_id` is the tenant-local idempotency key.

### `score_configs`

Append-only score schemas (name + data type + optional numeric bounds /
categorical values). `config_id` is the tenant-local idempotency key. There is
no PATCH; replace a config by inserting a new `config_id`. Human annotation
(Annotate panel → scores) is documented in Softprobe LLM `docs/annotation.md`.

## Schema promotion

Promotion is tenant-scoped and applied through authenticated
`POST /v1/promotions/apply`, not process-global YAML. Active manifests live in
the tenant PostgreSQL metadata schema (`promotion_specs`) in production.
SQLite supports promotion in its configured local single-scope DuckLake
catalog.

- **Telemetry columns:** additive nullable columns on `traces` / `logs` /
  `metrics`. Future ingest extracts declared sources into those columns;
  historical rows stay `NULL`.
- **Business tables:** versioned `<table>_vN` tables plus `<table>_current`
  views with evidence anchors. Apply provisions schema today; automatic OTLP
  row materialization is not wired yet.

`sp.*` attributes are an instrumentation convention only. Softprobe does not
auto-promote them. Canonical contract:
[`promotion.md`](promotion.md).

## Query path

`src/query/duckdb.rs` owns a pool of independent DuckDB worker connections.
Every worker loads `httpfs` and DuckLake, configures object-store access, and
ATTACHes the same DuckLake scope used by its tenant-bound writer.

Public query names remain:

- `union_spans`, `union_logs`, `union_metrics`
- `committed_spans`, `committed_logs`, `committed_metrics`

Because ingest is flush-through, union and committed names resolve to the same
DuckLake tables. Historical `buffer_*`, `staged_*`, and `iceberg_*` aliases are
compatibility spellings only; there are no corresponding runtime tiers.

Query surfaces include:

- tenant-scoped `POST /v1/query/sql` for internal/debug use;
- telemetry search, details, fields, sessions, and traces endpoints;
- `GET /v1/data/ducklake-connection` for clients that query DuckLake locally;
- `make duckdb-shell` for local ad hoc access.

See [`adhoc-duckdb-ducklake.md`](adhoc-duckdb-ducklake.md) for the supported
interactive workflow.

## Maintenance

The scheduler runs when compaction or metadata maintenance is enabled. It
walks the default DuckLake scope and all registered tenant scopes.

For `traces`, `logs`, and `metrics`, it can:

- set the configured target file size;
- call `ducklake_merge_adjacent_files`;
- expire old DuckLake snapshots;
- clean old DuckLake files.

When enabled, the Postgres dropdown catalog is pruned by its active-value
retention. Iceberg manifest rewrite and Iceberg REST catalog maintenance do not
exist in the current path.

## Configuration

The canonical shape is `config.yaml`; defaults and validation live in
`src/config.rs`.

Important DuckLake settings:

- `catalog_type`: `postgres` or `sqlite`
- `metadata_path`: PostgreSQL connection string or SQLite path
- `data_path`: local, `s3://`, or `gs://` data location
- `catalog_alias`
- `metadata_schema`
- `data_inlining_row_limit` (default `10000`)
- `writer_pool_size` (default `4`, clamped to `1..=16`)

Non-secret object-store settings live in the `object_store` section (`region`
and an optional custom `endpoint` for MinIO/R2). Object-store credentials are
never stored in YAML; they are resolved from the environment: `AWS_ACCESS_KEY_ID`
/ `AWS_SECRET_ACCESS_KEY` (with optional `AWS_SESSION_TOKEN`) for `s3://` paths,
and GCS HMAC interoperability credentials `GCS_HMAC_ACCESS_KEY_ID` /
`GCS_HMAC_SECRET` (with `GCP_HMAC_*` aliases) for `gs://` paths.

Config precedence is:

1. supported environment overrides;
2. `CONFIG_FILE` (default `config.yaml`);
3. built-in defaults when the file does not exist.

Supported direct overrides in `src/config.rs` are `PORT`, `S3_REGION`, and
`SOFTPROBE_MAX_HTTP_BODY_BYTES`.

## Network surfaces

- HTTP listens on `SOFTPROBE_LISTEN_ADDR` when set; otherwise it binds
  `0.0.0.0` with `server.port` (default `8090`). The current binary does not
  use `server.host` for its listen address.
- OTLP/gRPC traces listen on `OTEL_GRPC_PORT` (default `4317`), unless
  `SOFTPROBE_GRPC_DISABLE=1`.
- `/v1/*` operational routes require bearer authentication, except tenant
  provisioning which performs its own admin-token validation.
- Auth wiring uses `SOFTPROBE_AUTH_URL` (defaults to a local auth stub URL).

The implemented HTTP routes are exposed by `/openapi.json`; the standalone
ingestion and promotion contract is in
[`ingestion-openapi.yaml`](ingestion-openapi.yaml). Promotion semantics are in
[`promotion.md`](promotion.md).

## Validation

From the repository root:

```bash
make setup
make ci
```

CI on GitHub runs the same Make entry points (`make ci` after
`make setup`; see `.github/workflows/ci.yml` — fmt, lint, `test`, and `test-e2e`;
release packaging is `make release` / `release.yml`). Performance suites are
manual (`make test-perf` / `.github/workflows/performance.yml`).

`make test` is unit/lightweight; `make test-e2e` is isolated MinIO/PostgreSQL
integration. `make duckdb-shell` is the supported manual ATTACH smoke.
