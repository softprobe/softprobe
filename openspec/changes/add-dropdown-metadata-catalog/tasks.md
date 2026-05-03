## Implementation (tracked in repo root `tasks.md` Phase MD)

- [x] Postgres DDL + index + pool (`DropdownCatalog::ensure_table`).
- [x] Config block `dropdown_catalog` and wire `DropdownCatalog::connect` in ingest pipeline.
- [x] `write_record_batches_internal`: upsert traces before temp parquet / DuckLake transaction.
- [x] Hosted routes + `AppState.dropdown_catalog`.
- [x] Maintenance executor: TTL prune after DuckLake maintenance when enabled.
- [x] Unit tests: pair collection, tenant resolution.
- [x] Contract: `http-control-api.md`, `spec/schemas/catalog-*.schema.json`, `ai-context.md`.
