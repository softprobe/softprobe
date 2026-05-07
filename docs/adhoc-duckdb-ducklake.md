# Ad hoc DuckDB queries against DuckLake (optional)

Committed telemetry lives in **DuckLake** (Postgres/SQLite metadata + object-store `data_path`). End-to-end verification is automated by `make test-local` (in `softprobe-runtime`) / repo root **`make test`**, and `tests/integration/storage_contract_validation.rs`.

## Same scope as the runtime (multi-tenant / per-config)

The query worker ATTACHes using **`ducklake.catalog_alias`**, **`ducklake.metadata_schema`**, **`ducklake.data_path`**, and **`ducklake.metadata_path`** from the runtime YAML. **`make duckdb-shell`** does the same: it runs `scripts/duckdb_ducklake_render_init.py` on **`CONFIG_FILE`**.

1. **Default on the host:** `tests/config/duckdb-shell-host.yaml` (localhost Postgres + MinIO, same `metadata_schema` / `data_path` as typical e2e stacks).
2. **Match a running container:**  
   `CONFIG_FILE=../e2e/softprobe-runtime.yaml make duckdb-shell`  
   (uses Docker hostnames — run DuckDB **inside** the network or fix hosts; usually you use the host yaml when the DB is port-forwarded to localhost.)
3. **Another tenant / scope:** use a YAML (or generated config) where `ducklake.metadata_schema` and `ducklake.data_path` are exactly that tenant’s scope — same as the server process for that tenant.

If `CONFIG_FILE` is wrong, you will ATTACH to an empty or wrong schema and see no rows (or errors).

## Interactive CLI

```bash
cd softprobe-runtime
make duckdb-shell
```

The script prints the resolved config path and DuckLake scope from the generated init header, adds **`traces` / `logs` / `metrics` views only when those tables exist**, and runs a **`SELECT 1`** smoke before the REPL.

## Manual ATTACH (reference)

See the emitted SQL from:

```bash
python3 scripts/duckdb_ducklake_render_init.py --config "$CONFIG_FILE" --meta /dev/null
```

(use a real `--meta` path if you need exports for scripting).

Qualified names follow the runtime: **`catalog_alias.metadata_schema.table`** when `metadata_schema` is not `main`, else **`catalog_alias.table`**.

## `union_spans` / `union_logs`

Those names are rewritten by the **runtime** query engine, not by a plain `duckdb` CLI session. For ad hoc work, query **`traces`** / **`logs`** views (after ingest) or the qualified DuckLake tables.
