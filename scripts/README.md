# Scripts

Most workflows are exposed via Makefile targets; scripts are thin helpers.

## Quick Start

```bash
# EC2: sync repo, build `splake` + `perf_stress`, copy client to catalog instance
bash scripts/ec2_sync_build.sh

# EC2 (app instance): start the API server
bash scripts/ec2_start_splake.sh start

# EC2 (catalog instance): run perf client (foreground)
bash scripts/ec2_run_perf_stress.sh --duration 120 --warmup-secs 10 --span-qps 100 --log-qps 100 --metric-qps 100 --query-concurrency 10 --query-interval-ms 500

# EC2 (catalog instance): run perf client (background)
bash scripts/ec2_run_perf_stress.sh --background --duration 86400 --warmup-secs 60 --span-qps 100 --log-qps 100 --metric-qps 100 --query-concurrency 10 --query-interval-ms 500

# Automated verification (ingest + DuckLake + HTTP API)
# From repo root: make test
# From this crate:  make test-all   # needs MinIO for integration-e2e (see Makefile)

# Interactive DuckDB against local DuckLake (Postgres + MinIO; see duckdb_ducklake_local_init.sql)
make duckdb-shell
```

## DuckDB + DuckLake (local)

The runtime stores committed telemetry in **DuckLake** (`ATTACH 'ducklake:postgres:…'`, `data_path` on S3/MinIO). **`make duckdb-shell`** builds that ATTACH from **`CONFIG_FILE`** (default: `tests/config/duckdb-shell-host.yaml` on the host) so `catalog_alias`, `metadata_schema`, and `data_path` match the process you are debugging. Use the **same YAML as the running server**; for another tenant scope, point `CONFIG_FILE` at a config that carries that scope’s `ducklake.*` keys.

- `scripts/duckdb_ducklake_render_init.py` — emits ATTACH + S3 `SET`s from YAML (stdlib only).
- `scripts/duckdb_ducklake_combo.sh` — temp `-init` = rendered ATTACH + `CREATE VIEW` only for existing `traces`/`logs`/`metrics` in that scope.
- `scripts/interactive_query.sh` — used by `make duckdb-shell`; smoke `SELECT 1` before the REPL.
- Legacy static attach: `SOFTPROBE_DUCKDB_INIT=scripts/duckdb_ducklake_local_init.sql` (fixed scope; use CONFIG_FILE instead).

Details and qualified names: [`docs/adhoc-duckdb-ducklake.md`](../docs/adhoc-duckdb-ducklake.md).

## Files

- **telemetrygen_hosted.sh** - Smoke-test OTLP/HTTP ingestion (traces, metrics, logs) against a hosted runtime using [telemetrygen](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen) with `Authorization: Bearer` (set `SOFTPROBE_TOKEN` or `SOFTPROBE_API_KEY`; optional `OTLP_ENDPOINT`, default `runtime.softprobe.dev:443`).
- **duckdb_ducklake_render_init.py** - YAML → DuckDB ATTACH (same shape as runtime)
- **duckdb_ducklake_combo.sh** - Temp `-init` = rendered ATTACH + optional `CREATE VIEW`s for existing tables
- **duckdb_ducklake_local_init.sql** - Legacy static ATTACH (optional `SOFTPROBE_DUCKDB_INIT`)
- **duckdb_ducklake_local_views.sql** - Optional `.read` for all three views when tables exist (adjust qualified names if needed)
- **interactive_query.sh** - Interactive DuckDB session launcher (used by `make duckdb-shell`)
- **demo_session_queries.sh** - Sample session queries against DuckLake (used by `make demo-session`)
- **drop_all_tables.sh** - Reset catalog tables (used by `make drop-tables`)
- **ec2_sync_build.sh** - Sync+build on app EC2 and copy `perf_stress` to catalog EC2
- **ec2_start_splake.sh** - Start/stop `splake` on the app EC2 instance
- **ec2_run_perf_stress.sh** - Run `perf_stress` on the catalog EC2 instance
- **README.md** - This file

## Example Queries

### Count Data
```sql
SELECT COUNT(*) FROM traces;
SELECT COUNT(*) FROM logs;
```

### Recent Data
```sql
SELECT * FROM traces ORDER BY timestamp DESC LIMIT 10;
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 10;
```

### Session Analysis
```sql
-- Sessions with most spans
SELECT session_id, COUNT(*) as span_count
FROM traces
GROUP BY session_id
ORDER BY span_count DESC
LIMIT 10;

-- Sessions with both traces and logs
SELECT
    t.session_id,
    COUNT(DISTINCT t.span_id) as spans,
    COUNT(DISTINCT l.body) as logs
FROM traces t
LEFT JOIN logs l ON t.session_id = l.session_id
GROUP BY t.session_id
HAVING logs > 0
ORDER BY spans + logs DESC;
```

### Specific Session

See [`docs/adhoc-duckdb-ducklake.md`](../docs/adhoc-duckdb-ducklake.md) for qualified names and a `UNION ALL` session example.

## Troubleshooting

### `ATTACH` fails (Postgres)
**Fix**: Start `ducklake-postgres` from this crate’s `docker-compose.yml` and ensure `host=localhost port=5432` matches your init SQL.

### Connection refused to MinIO
**Fix**: Start MinIO and check:
```bash
curl -sf http://localhost:9000/minio/health/live
```
`DATA_PATH` in the `ATTACH` must match the runtime’s DuckLake config (see `test-docker.yaml`).

### Empty tables
**Reasons**:
1. No data ingested yet - send test data
2. Data still in buffer - wait 60s or restart collector
3. Check collector logs for errors

## References

- [DuckDB DuckLake extension](https://duckdb.org/docs/extensions/ducklake)
- [Ad hoc DuckLake queries](../docs/adhoc-duckdb-ducklake.md)
- [Strict storage SQL contracts](../tests/integration/storage_contract_validation.rs) (automated; `make test-local`)
- [Legacy manual SQL fixtures](../tests/fixtures/README.md)
