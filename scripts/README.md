# Scripts

Most workflows are exposed via Makefile targets; scripts are thin helpers.

## Quick Start

```bash
# Automated verification (ingest + DuckLake + HTTP API)
make setup-local && make test

# Performance gate (manual / release)
make test-perf

# Host-first release bits → dist/, then package image (linux/amd64)
make build-release
make package-image   # or: TARGET_PLATFORM=linux/amd64 make build-release && make publish-docker TAG=vX.Y.Z

# Interactive DuckDB against local DuckLake
make duckdb-shell

# Live load stress (not the integration perf gate)
make stress-test BACKEND=local   # or r2 / gcs
```

## DuckDB + DuckLake (local)

The runtime stores committed telemetry in **DuckLake** (`ATTACH 'ducklake:postgres:…'`, `data_path` on S3/MinIO). **`make duckdb-shell`** builds that ATTACH from **`CONFIG_FILE`** so `catalog_alias`, `metadata_schema`, and `data_path` match the process you are debugging.

- `scripts/duckdb_ducklake_render_init.py` — emits ATTACH + S3 `SET`s from YAML
- `scripts/duckdb_ducklake_combo.sh` — temp `-init` for existing tables
- `scripts/interactive_query.sh` — used by `make duckdb-shell`

## Build / CI helpers

- **build-release.sh** — cargo-chef cook (dep cache) + locked release build → `dist/` (snapshots/restores sources blanked by cook)
- **run-isolated-cargo-tests.sh** — `cargo test --no-run` once, then per-test process isolation
- **stress-test.sh** — unified live `perf_stress` driver (`BACKEND=local|r2|gcs`)
- **slo.sh** — phase timings + wall-clock SLO enforcement
- **assert-duckdb-version.sh** — refuse to stage a libduckdb that does not match Cargo.lock

## Other

- **telemetrygen_hosted.sh** — OTLP smoke against a hosted runtime
- **demo_session_queries.sh** / **drop_all_tables.sh** — Make-backed helpers
