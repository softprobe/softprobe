# Test fixtures (reference)

**Strict enforcement:** `tests/integration/storage_contract_validation.rs` (run with `make test-local` / `cargo test --features integration-e2e --test tests`).

**Working DuckDB attach** for local dev: `softprobe-runtime/scripts/duckdb_ducklake_local_init.sql` (used by `make duckdb-shell` / `scripts/interactive_query.sh`). See [`docs/adhoc-duckdb-ducklake.md`](../docs/adhoc-duckdb-ducklake.md).

| File | Notes |
|------|--------|
| `verify_ducklake.sql` | DuckLake SQL samples |
| `legacy_verify_session.sql` | Session UNION template for DuckLake |
| `legacy_verify_e2e.md` | Why old bash/Lakekeeper checks were removed |
