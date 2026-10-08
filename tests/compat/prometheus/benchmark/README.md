# Softprobe Prom micro-benchmark (Option A)

Local Softprobe-only baseline: host Softprobe + auth-mock + OTel Collector
**hostmetrics** → OTLP, then a curated PromQL suite against `/api/v1`.

This is the fast iteration harness from
[`docs/perf/prometheus-query-findings.md`](../../../../docs/perf/prometheus-query-findings.md).
It is **not** the VictoriaMetrics `prometheus-benchmark` competitor compare (that is Option B later).

## Make targets

```bash
make bench-prom-baseline          # up → warm → measure → write results → down
make bench-prom-baseline LEAVE_UP=1   # leave Softprobe + collector running
make bench-prom-down              # teardown
```

Useful env overrides:

| Variable | Default | Meaning |
|----------|---------|---------|
| `BENCH_WARMUP_SECS` | `20` | Ingest-only warm-up before measuring |
| `BENCH_MEASURE_SECS` | `60` | Wall clock spent issuing queries |
| `BENCH_REPEAT` | `3` | Repeats per query per round |
| `BENCH_LABEL` | `baseline` | Tag in result filenames (e.g. `after-variant-pushdown`) |
| `BENCH_FORCE_PARQUET` | `0` | `1` = disable DuckLake inlining + 1s hostmetrics/tiny batches (small-file stress); maintenance merge runs at end |
| `THELAKE_BENCH_STATE_DIR` | `/tmp/thelake-prom-bench` | Softprobe data/cache/config |
| `SOFTPROBE_LISTEN` | `http://127.0.0.1:8090` | Softprobe base URL |

Do **not** run this while `make grafana-up` owns `:8090` / `:18080`, or while the
OpenTelemetry Demo `otel-collector` container is running (it exports to host
`:8090` and will contaminate results). The harness fails fast unless
`BENCH_ALLOW_FOREIGN_OTLP=1` is set.

## Results

Written under `docs/perf/results/`:

- `<timestamp>-<label>.json` — machine-readable latencies + RSS + data size
- `<timestamp>-<label>.md` — short human summary

Compare labels across fix iterations (`baseline` → `variant-pushdown` → …).

## Queries

[`queries.promql`](queries.promql) — one PromQL expression per line (Softprobe subset).
`{{metric}}` is replaced with the first discovered metric name from
`/api/v1/label/__name__/values`.
