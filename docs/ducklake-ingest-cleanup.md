# DuckLake ingest cleanup (buffer + hollow tiering)

Status: **in progress** — remove in-memory buffer and Iceberg-era WAL/staged stubs; OTel collector owns batching.

## 1. How often we commit into Postgres

Ingest is flush-through to DuckLake (after this cleanup). Each OTLP request that reaches thelake becomes one DuckLake transaction:

`BEGIN` → temp Parquet → `INSERT … SELECT read_parquet` → `COMMIT`

That **always** updates the Postgres **metadata catalog**. Row bodies land in Postgres only when DuckLake **data inlining** applies (`DATA_INLINING_ROW_LIMIT` on ATTACH). Otherwise data is Parquet under `data_path` with metadata pointers in Postgres.

**Before this cleanup:** commits were gated by `SimpleBuffer` (default 10K spans / 128MB / 60s). **After:** one commit per ingest request; collector batching is the intended coalesce layer.

## 2. Mental model

| Belief | Reality |
|--------|---------|
| Inlining replaced WAL + buffer | WAL/staged were already hollow. Buffer was still the batch gate. Inlining only chooses Postgres rows vs Parquet **after** write. |
| Buffer safe to remove | Yes for the deploy path where the OTel collector batches. |
| Query needs buffer union | Only while unflushed RAM existed. Post-cleanup, committed DuckLake is the sole read tier. |

## 3. Cleanup inventory

### Removed / remove

- `SimpleBuffer` and `span_buffering` config
- Hollow WAL config (`wal_*`, `replay_wal_on_startup`, `optimizer_interval_seconds`)
- `run_optimizer_once`, `list_wal_files`, `list_staged_files` stubs
- Query buffer/staged views (`tm_buf_*`, `tm_stg_*`) and related aliases
- Unused Iceberg-shaped compaction knobs (`min_files_to_compact`, `metadata_min_snapshots_to_keep`, `metadata_rewrite_manifests_enabled`)

### Kept

- DuckLake writer path + `DATA_INLINING_ROW_LIMIT`
- Query rewrite for DuckLake-reserved names (`union_*` / `committed_*` → `tm_*`)
- Compaction: `target_file_size_bytes`, snapshot expire, orphan cleanup

## 4. Quality gates (hard bar)

Both MinIO and DuckLake Postgres are **mandatory** for pre-merge. Redis is also required for tenant OTLP/session e2e suites:

```bash
make setup-local
make check-local
make check-local-postgres
make check-local-redis
make test                   # test-quick + full integration-e2e
```

- Lib-only (`make test-quick` / `cargo test --lib`) is **not** sufficient.
- `make test-local` must fail if MinIO, Postgres, or Redis is down.

### Exit checklist

- [x] `make setup-local` brings up MinIO + ducklake-postgres + Redis
- [x] `make check-local`, `make check-local-postgres`, and `make check-local-redis` all pass
- [x] `make test` fully green (`integration-e2e` included)
- [x] No success-path assertions on empty WAL/staged/optimizer
- [x] Docs state the dual-service gate explicitly
- [x] In-memory buffer + hollow WAL/staged/optimizer removed; ingest is flush-through
- [x] Query refreshes file-backed DuckLake ATTACH safely after writer commits (no DETACH; postgres catalogs rely on metadata visibility)
- [x] Multi-tenant writers route by `tenant_id` unless constructed scope-bound


