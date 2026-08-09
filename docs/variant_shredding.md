# MAP → VARIANT Shredding Migration

**Status:** Current  
**Breaking change:** yes (physical column type + nested SQL access)

## What changed

Hot telemetry map columns are now DuckLake `VARIANT` columns with Iceberg v3 /
Parquet variant shredding:

| Table | Columns |
|-------|---------|
| `traces` | `attributes` |
| `logs` | `attributes`, `resource_attributes` |
| `metrics` | `attributes`, `resource_attributes` |

Out of scope (still `MAP(VARCHAR, VARCHAR)`):

- `scores.metadata`
- nested `traces.events[].attributes`

## Write path

1. Arrow staging encodes hot attribute maps as **JSON Utf8**.
2. Known numeric keys are typed in JSON for stable shredding:
   - integers: `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`,
     `gen_ai.usage.total_tokens`
   - floats: `sp.cost.total`
3. DuckLake `CREATE TABLE` / `INSERT` cast with
   `SELECT * REPLACE (…::JSON::VARIANT AS …)`.

## Query path (breaking)

VARIANT field extraction returns VARIANT. String filters and `COALESCE` must
cast:

```sql
-- Preferred
WHERE CAST(attributes['sp.user.id'] AS VARCHAR) = 'user-123'

-- COALESCE requires the cast (otherwise it can yield NULL)
SELECT COALESCE(CAST(attributes['sp.observation.type'] AS VARCHAR), 'span')

-- Project for APIs / JSON clients
SELECT CAST(attributes AS JSON) AS attributes FROM traces
```

Runtime SQL compilers (`llm/query`, `telemetry`) already emit
these casts. SoftProbe product adapters (`ThelakeSql`) must use the same
`CAST(col['key'] AS VARCHAR)` form when querying via `/v1/query/sql`.

## Operator migration

Existing DuckLake tables created with `MAP(VARCHAR, VARCHAR)` are **not**
auto-migrated. On write, Softprobe fails fast with a message requiring a table
rebuild when a hot column is not `VARIANT`.

Rebuild options (operator-owned; Softprobe does **not** auto-drop tables):

1. **Dev / local:** recreate the DuckLake metadata/data paths, or use the
   explicit local-only `SPLAKE_RESET_DUCKLAKE=1` bootstrap flag if you accept
   wiping that catalog.
2. **Production:** provision a new DuckLake data path / metadata schema, re-ingest
   (or copy with an explicit offline `INSERT … SELECT …::JSON::VARIANT` rebuild),
   then cut readers over. Do not mix MAP and VARIANT physical types for the same
   logical table name.

To verify shredding after ingest (with inlining disabled):

```sql
SELECT variant_path, shredded_type, min_value, max_value, value_count
FROM __ducklake_metadata_<alias>.ducklake_file_variant_stats
ORDER BY variant_path;
```

Expect paths such as `"sp.observation.type"` (varchar),
`"gen_ai.request.model"` (varchar), and `"sp.cost.total"` (float64).

## Related code

- [`src/storage/schema/variant.rs`](../src/storage/schema/variant.rs)
- [`src/storage/schema/tables.rs`](../src/storage/schema/tables.rs)
- [`src/storage/ducklake/`](../src/storage/ducklake/) (`writer.rs`, `otlp.rs`, `util.rs`)
- [`tests/integration/variant_shredding.rs`](../tests/integration/variant_shredding.rs)
