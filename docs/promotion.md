# Softprobe Runtime Schema Promotion

**Status:** Current
**Spec version:** `softprobe.promotion.v1`
**Apply API:** authenticated `POST /v1/promotions/apply`
**Last verified against:** `src/promotion.rs`, `src/runtime_api.rs`,
`src/storage/ducklake/mod.rs`, and promotion integration tests on 2026-07-18

This is the canonical contract for Softprobe business attributes and schema
promotion. Other docs link here; do not duplicate the full contract elsewhere.

## What problem promotion solves

OTLP spans store arbitrary attributes in an `attributes` MAP. That always
works:

```sql
WHERE attributes['sp.user.id'] = 'user-123'
```

Promotion lets a tenant declare selected fields as dedicated typed SQL columns
so queries can use:

```sql
WHERE user_id = 'user-123'
```

Promotion is tenant-scoped, additive, and explicit. Softprobe does **not**:

- invent `sp.*` attributes for you;
- auto-promote every `sp.*` key;
- rename `sp.user.id` to `user_id` automatically;
- backfill historical rows when a column is added.

## `sp.*` business attributes (instrumentation convention)

A **business attribute** is application-domain metadata you want to search or
correlate on later: user id, order id, booking reference, workflow name, and
similar identifiers.

The `sp.` prefix means “Softprobe application convention.” It is not an
OpenTelemetry semantic convention and not a runtime-enforced schema.

### Rules

1. Your application must set the attributes explicitly.
2. Softprobe stores them in the telemetry `attributes` MAP as ordinary keys.
3. Naming consistency across services matters more than the prefix itself.
4. Prefer `sp.*` for Softprobe-specific business keys so they do not collide
   with `http.*`, `db.*`, or other OTel conventions.
5. Promotion can source from `sp.*` keys or from any other attribute /
   resource / event / HTTP-body path you declare.

### Example

```javascript
span.setAttribute('sp.user.id', user.id);
span.setAttribute('sp.order.id', order.id);
span.setAttribute('sp.session.id', sessionId);
span.setAttribute('sp.workflow', 'checkout');
```

See [`instrumentation_guide.md`](instrumentation_guide.md) for body capture and
language examples. Keep large HTTP bodies in `http.request` /
`http.response` span events; keep searchable identifiers in attributes.

## Two promotion kinds

| Kind | Purpose | Effect of apply | Effect of later ingest |
|------|---------|-----------------|------------------------|
| `telemetry_columns` | Add nullable columns to `traces`, `logs`, and/or `metrics` | Idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` + activate one telemetry spec (supersede prior) | Extract values into the new columns for **new** rows |
| `business_table` | Create a versioned business table + `*_current` view | Compatibility check, then `CREATE TABLE IF NOT EXISTS` / additive `ADD COLUMN IF NOT EXISTS` / `CREATE OR REPLACE VIEW` + activate one spec per table | Extraction helpers exist; **automatic OTLP ingest materialization is not wired yet** |

Use `telemetry_columns` when you want a first-class filter column on existing
telemetry tables. Use `business_table` when you want a dedicated relational
shape with evidence anchors (`session_id`, `trace_id`, `span_id`, …).

## Apply API

### Request

```http
POST /v1/promotions/apply
Authorization: Bearer <tenant-token>
Content-Type: application/json
```

```json
{
  "manifestYaml": "specVersion: softprobe.promotion.v1\n..."
}
```

`manifestYaml` is a YAML document string. The runtime parses and validates it
before touching DuckLake.

### Success responses

Telemetry columns:

```json
{
  "specVersion": "softprobe.promotion.apply.v1",
  "applied": true,
  "target": {
    "kind": "telemetry_columns",
    "tables": ["traces"]
  },
  "schemaChanges": [
    {
      "table": "traces",
      "action": "add_column",
      "column": "user_id",
      "type": "string",
      "nullable": true
    }
  ]
}
```

Business table:

```json
{
  "specVersion": "softprobe.promotion.apply.v1",
  "applied": true,
  "target": {
    "kind": "business_table",
    "table": "checkout_orders",
    "version": 1
  },
  "schemaChanges": [
    { "action": "create_table", "table": "checkout_orders_v1" },
    {
      "action": "create_or_replace_view",
      "view": "checkout_orders_current",
      "sourceTable": "checkout_orders_v1"
    }
  ]
}
```

### Error shapes

| HTTP | When |
|------|------|
| `401` / `403` | Missing/invalid bearer or unresolved tenant |
| `422` | Invalid YAML / unsupported `specVersion` / validation failure |
| `503` | Tenant DuckLake scope or schema apply/record failed |

Example validation error:

```json
{
  "error": {
    "code": "telemetry_column_not_nullable",
    "message": "telemetry_column_not_nullable at columns[0].nullable: telemetry promoted columns must be nullable"
  }
}
```

### Catalog backends

Promotion apply and ingest-time telemetry extraction work on both backends:

| Backend | Scope model | Spec storage | Apply serialization |
|---------|-------------|--------------|---------------------|
| **PostgreSQL** | Multi-tenant (per-tenant metadata schema via scope registry) | `{tenant_schema}.promotion_specs` | `pg_advisory_xact_lock` across DDL + activate |
| **SQLite** (local/dev) | Single configured catalog scope | `{catalog_alias}.promotion_specs` in the DuckLake catalog | Process-global mutex across DDL + activate |

Both backends serialize the full apply critical section (physical DDL +
activate/deactivate). Physical DDL still runs on DuckLake (outside the Postgres
metadata transaction); the lock/mutex only prevents concurrent applies from
interleaving.

SQLite promotion is intentionally **single-scope**: every tenant id in a local
process shares the configured DuckLake catalog. Multi-tenant isolation still
requires PostgreSQL. Cross-process concurrent apply against the same SQLite
file is out of scope for local/dev (WAL/busy-timeout still protect storage).
DuckLake tables cannot declare `PRIMARY KEY`; uniqueness of `spec_id` is
enforced by the activate path.

Endpoints that need a tenant registry (for example
`/v1/data/ducklake-connection`) still return `503 ducklake_connection_unavailable`
when the Postgres resolver is absent.

Other apply `503` codes:

| `error.code` | Meaning |
|--------------|---------|
| `ducklake_connection_unavailable` | Tenant DuckLake resolver / registry unavailable (Postgres-only endpoints) |
| `ducklake_scope_unavailable` | Authenticated tenant scope could not be resolved |
| `promotion_schema_apply_failed` | DuckLake DDL failed |
| `promotion_spec_record_failed` | Writing `promotion_specs` failed |

## Manifest contract (`softprobe.promotion.v1`)

### Shared fields

| Field | Required | Notes |
|-------|----------|-------|
| `specVersion` | yes | Must be exactly `softprobe.promotion.v1` |
| `target.kind` | yes | `telemetry_columns` or `business_table` |
| `target.tables` | telemetry only | Non-empty list from `traces`, `logs`, `metrics` |
| `target.table` | business only | SQL identifier for the logical business table |
| `target.version` | business only | Integer `> 0` |
| `columns` | yes | At least one column |
| `rowSelector` | business only | Attribute equality match (`attribute.key` + `attribute.equals`) |

### Column fields

| Field | Required | Notes |
|-------|----------|-------|
| `name` | yes | SQL identifier: `[a-z_][a-z0-9_]*` |
| `type` | yes | `string`, `bool`, `int64`, `double`, `decimal`, `timestamp`, `json` |
| `nullable` | yes | Telemetry columns **must** be `true` |
| `source.from` | yes | See source kinds below |

### Source kinds

| `from` | Extra fields | Reads from | Ingest support today |
|--------|--------------|------------|----------------------|
| `attribute` | `key` | Attributes map | `traces`, `logs`, `metrics` |
| `resource_attribute` | `key` | Resource attributes map | `traces`, `logs`, `metrics` |
| `event_attribute` | `event_name`, `key` | First matching named span event attribute | **`traces` only** — logs/metrics pass empty events, so the column stays `NULL` |
| `http_request_body` | `json_path` | Parsed HTTP request body JSON | **`traces` only** |
| `http_response_body` | `json_path` | Parsed HTTP response body JSON | **`traces` only** |

`json_path` must start with `$` at apply time. Extraction supports the root
(`$`) and simple dot paths such as `$.order.id`. Paths with array indexes or
filters (for example `$.items[0].id`) are accepted by validation but resolve
to missing/`NULL` at extract time — they do not raise a type error.

### Types and storage

| Manifest type | Telemetry DuckLake SQL | Business table SQL |
|---------------|------------------------|--------------------|
| `string` | `VARCHAR` | `VARCHAR` |
| `bool` | `BOOLEAN` | `BOOLEAN` |
| `int64` | `BIGINT` | `BIGINT` |
| `double` | `DOUBLE` | `DOUBLE` |
| `decimal` | `DOUBLE` | `DECIMAL(38, 9)` |
| `timestamp` | `TIMESTAMPTZ` | `TIMESTAMPTZ` |
| `json` | `VARCHAR` | `VARCHAR` |

Timestamp values must parse as RFC 3339.

## Telemetry column promotion

### Example manifest

```yaml
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: user_id
    type: string
    nullable: true
    source:
      from: attribute
      key: sp.user.id
  - name: service_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: service.name
```

### Lifecycle

```text
instrument app with attributes (e.g. sp.user.id)
        |
        v
POST /v1/promotions/apply
        |
        +--> validate manifest
        +--> ensure traces/logs/metrics tables exist
        +--> ALTER TABLE ADD COLUMN IF NOT EXISTS (nullable)
        +--> activate this telemetry spec; deactivate other active telemetry specs
        |
        v
later OTLP ingest for that tenant
        |
        +--> load active telemetry manifests for tenant scope
        +--> extract each promoted source
        +--> write dedicated columns on the new rows
        |
        v
query either attributes['sp.user.id'] or user_id
```

### Active-spec lifecycle

- Each tenant has **at most one active** `telemetry_columns` document.
- Re-applying the **same** YAML is idempotent: DDL uses `IF NOT EXISTS`, and
  the existing `promotion_specs` row is upserted back to `active`.
- Applying an **updated** YAML activates the new content-hash `spec_id` and
  marks prior active telemetry specs `inactive` in the same metadata transaction
  (Postgres `BEGIN` / DuckDB `BEGIN TRANSACTION` on SQLite).
- Physical columns are **additive only**: DuckLake never drops a column when
  YAML removes it. Extraction follows the active document only, so removed
  columns stop being populated on new rows while the physical column remains.
- Spec identity uses a hash of the raw YAML text (`telemetry_columns_{hash}`).

### Merging multiple `telemetry_columns` sources before apply

Because a tenant can only have **one active** `telemetry_columns` document,
multiple feature-owned manifests must be merged into a single manifest
client-side before calling apply — applying them one after another would just
supersede the previous one, not union the columns.

`merge_telemetry_columns_manifests` (`src/promotion.rs`) does this:

- rejects manifests that don't target the exact same `target.tables` set
  (`merge_target_tables_mismatch`) — merging would otherwise silently add one
  source's columns to tables it never declared;
- preserves column order (first manifest's columns first) and deduplicates a
  column repeated identically across manifests; a column repeated with a
  *different* type/nullable/source is rejected
  (`merge_conflicting_duplicate_column`);
- re-validates the merged result with `validate_telemetry_column_additive`,
  so collisions with reserved base columns still fail;
- pairs with `telemetry_columns_manifest_to_yaml` to serialize the merged
  manifest back to canonical YAML for `POST /v1/promotions/apply`.

Unit coverage lives in `src/promotion.rs` (`merge_*` tests). Product-specific
column fragments (if any) belong in the owning product repo — not as
domain modules inside thelake.

### Ingest semantics

1. Active manifests are loaded from the tenant metadata schema
   (`promotion_specs` where `status = 'active'` and
   `target_kind = 'telemetry_columns'`). Under normal apply that set has size
   0 or 1. On PostgreSQL this loads from the tenant metadata schema; on SQLite
   it loads from `{catalog_alias}.promotion_specs` in the local DuckLake
   catalog. Other catalog types skip extraction.
2. For each target table, columns from matching manifests are applied.
3. Missing source values become SQL `NULL` in the promoted column.
4. Invalid JSON bodies or type mismatches raise
   `promotion_value_invalid_json` / `promotion_value_type_mismatch` and fail
   the ingest batch (so the exporter can retry after fixing instrumentation or
   the manifest).
5. **Corrupt/invalid active `promotion_specs` rows also fail ingest** for that
   tenant: one unparsable `manifest_json` aborts loading all telemetry
   promotions for the scope before commit.
6. Historical rows written before apply remain `NULL` in the new column. There
   is no automatic backfill.
7. The original attribute keys remain queryable in the `attributes` MAP.

### Reserved names

Promoted telemetry column names must not collide with canonical columns such
as `session_id`, `trace_id`, `span_id`, `attributes`, `events`,
`http_request_body`, `record_date`, and the other base fields defined in
`src/storage/schema/tables.rs` / `src/promotion.rs`.

### Query examples

Without promotion (always available after instrumentation):

```sql
SELECT session_id, trace_id, timestamp, http_request_path
FROM traces
WHERE attributes['sp.user.id'] = 'user-123'
ORDER BY timestamp DESC;
```

With a promoted `user_id` column:

```sql
SELECT session_id, trace_id, timestamp, http_request_path
FROM traces
WHERE user_id = 'user-123'
ORDER BY timestamp DESC;
```

## Business table promotion

### Example manifest

```yaml
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: order_id
    type: string
    nullable: false
    source:
      from: http_response_body
      json_path: $.order.id
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
```

### What apply does

1. Loads any active business-table manifest for the same `target.table` and
   runs `validate_business_table_compatible` (HTTP **422** on failure).
2. Creates physical table `<table>_v<version>` when missing
   (example: `checkout_orders_v1`).
3. Runs idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for each nullable
   column so safe same-version additive updates change the physical table.
4. Creates or replaces view `<table>_current`
   (example: `checkout_orders_current`).
5. Activates this business-table spec and deactivates other active specs for
   the same logical table name.

Every business table includes evidence anchors:

- required: `session_id`, `trace_id`, `span_id`, `source_signal`,
  `source_timestamp`, `promotion_spec_version`
- optional: `event_name`, `event_timestamp`, `service_name`

### Versioning / compatibility

Apply enforces `validate_business_table_compatible` against the prior active
manifest for the same table:

- same version may only add nullable columns;
- adding a required (`nullable: false`) column requires a new table version,
  because existing rows cannot satisfy the constraint;
- dropping a column, changing type, or changing nullability on an existing
  version is rejected with **422**;
- breaking changes must bump `target.version` (higher version is accepted and
  creates a new physical `<table>_vN`).

### Extraction semantics (defined, tested)

`extract_business_promoted_row` implements:

1. Skip the row when `rowSelector.attribute` does not match.
2. Extract each declared column.
3. Omit missing nullable values.
4. Return structured errors for missing/invalid required values
   (`missing_required_value`, `type_mismatch`, JSON errors) so a future ingest
   path can write `promotion_errors` without rejecting the source telemetry
   row.

### Current limitation

As of this document, OTLP ingest writes promoted **telemetry columns** only.
Business-table DDL/apply and extraction helpers are implemented and tested, but
the writer does not yet automatically insert business rows during OTLP ingest.
Do not assume `checkout_orders_current` fills itself from `/v1/traces` until
that ingest path is wired.

## Metadata tables

| Backend | Location | Tables |
|---------|----------|--------|
| PostgreSQL | Each tenant metadata schema | `promotion_specs`, `promotion_errors` |
| SQLite (local) | DuckLake catalog (`softprobe.promotion_specs`) | `promotion_specs` (control table; errors table remains Postgres-oriented for now) |

These are control/diagnostic tables for the promotion system, not telemetry
payload storage.

## Operator checklist

1. For **production multi-tenant** promotion, use a **PostgreSQL** DuckLake
   catalog with tenant scopes. For **local/dev**, SQLite single-scope
   promotion (apply + ingest extraction + query) is supported.
2. Instrument consistent business attributes (`sp.user.id`, …).
3. Verify MAP queries work before promoting anything.
4. Promote only high-value filters you query often.
5. Keep telemetry promoted columns nullable.
6. Prefer `attribute` / `resource_attribute` sources for identifiers. Use
   `event_attribute` / HTTP body JSON paths only on `traces`.
7. After apply, emit new traffic and confirm the promoted column populates.
8. Treat old rows as `NULL` unless you run an explicit one-off backfill yourself.
9. Never leave an invalid active row in `promotion_specs` — it can block
   tenant ingest.
10. For business tables, treat apply as schema provisioning until ingest
    materialization is available, and bump `target.version` yourself for
    breaking schema changes.

## Related docs

- [`instrumentation_guide.md`](instrumentation_guide.md) — how to emit bodies and `sp.*`
- [`design.md`](design.md) — runtime architecture
- [`decision_log.md`](decision_log.md) — current architecture decisions
- [`ingestion-openapi.yaml`](ingestion-openapi.yaml) — HTTP contract including apply
- [`adhoc-duckdb-ducklake.md`](adhoc-duckdb-ducklake.md) — local SQL against DuckLake
