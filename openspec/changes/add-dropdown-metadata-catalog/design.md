## Context

- DuckLake may inline rows into Postgres when `data_inlining_row_limit` is non-zero; those rows never appear as standalone Parquet on object storage. The catalog **must** derive from the same ingest artifact as the lake write (`RecordBatch` slice), **before** `INSERT INTO … SELECT read_parquet(…)`.
- **Multi-tenancy:** Primary key `(tenant_id, entity_type, entity_value)`. Tenant comes from trace batches (`tenant_id` column); mixed tenants in one batch skip upsert (logged).
- **Product:** Hosted-only surface for the HTTP catalog APIs (same bearer middleware as `/v1/*`). No synthetic default tenant for non-hosted paths in this feature.

## Ingest throughput

- Distinct pairs are upserted using **batched multi-row `INSERT … VALUES`** (`dropdown_catalog.upsert_batch_size`, default 500, clamped 1–5000) so fast ingest does not issue one Postgres round-trip per pair.

## Failure behavior

- Catalog upsert failure is **non-fatal** for ingest (logged); lake write continues.
- If lake write fails after catalog upsert, catalog rows may be **orphans** until TTL prune; no automatic compensate-delete in v1.

## Dimensions (v1)

- All non-skipped Utf8 / LargeUtf8 / Int32 columns on the trace Arrow schema except high-cardinality IDs (`trace_id`, `span_id`, `session_id`, `tenant_id`, etc.) and nested blobs (`attributes`, `events`). Configurable `skip_entity_columns` extends the skip set.

## Freshness and cleanup

- **API “active” window:** `last_seen_at` within `active_values_days` (default 7).
- **Maintenance:** optional `DELETE` of rows older than `active_values_days` when `maintenance_prune_enabled` is true.
- **Parquet statistics:** not used for proving value removal (unsound for strings). TTL is the primary correctness lever; optional future reconcile may sample DuckDB metadata where reliable.

## Schema placement

- Table lives in `metadata_schema` from DuckLake config (maps `main` → `public` for quoting).
