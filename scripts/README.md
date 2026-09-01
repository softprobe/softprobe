# thelake/scripts — Make-owned helpers only

Do not add parallel entrypoints. Product compile, gates, and stress are Makefile
targets; these scripts are thin helpers invoked by Make.

## Surviving scripts → Make owner

| Script | Make target |
|--------|-------------|
| `assert-duckdb-version.sh` | `build-release` (stages `dist/`) |
| `run-isolated-cargo-tests.sh` | `test-e2e`, `test-perf` |
| `stress-test.sh` | `stress BACKEND=local\|r2\|gcs` |
| `interactive_query.sh` + `duckdb_ducklake_*` | `duckdb-shell` |
| `interactive_query_ducklake_production.sh` | `duckdb-shell-prod` |
| `demo_session_queries.sh` | `demo-session` |
| `drop_all_tables.sh` | `drop-tables` |
| `generate_telemetry.py` | `generate-telemetry` |
| `grafana-manual-up.sh` / `grafana-manual-down.sh` | `grafana-up` / `grafana-down` |
| `bench-prom-baseline.sh` / `bench-prom-down.sh` | `bench-prom-baseline` / `bench-prom-down` |

## Public Make surface

```text
Build:    build | build-release | package | publish
Test:     test | test-e2e | test-perf
Gates:    ci | release
Infra:    setup | teardown | doctor
Stress:   stress BACKEND=local|r2|gcs
```

Cache: `~/.cache/thelake` (`THELAKE_CACHE_ROOT`). No host cargo-chef. Compile and
publish live only in the Makefile (no parallel release/SLO/publish scripts).
