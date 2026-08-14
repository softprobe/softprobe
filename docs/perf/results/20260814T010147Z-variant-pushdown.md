# Prom micro-benchmark — `variant-pushdown`

- UTC: `20260814T010147Z`
- Git: `e23102d`
- Metric under test: `system_cpu_load_average_15m`
- Warm-up / measure: 25s / 30s (repeat=2, rounds=19)
- Requests: 304/304 ok
- Latency overall: p50=37ms p95=42ms max=68ms
- Softprobe RSS: 427772 KiB
- Data dir: 88148 bytes, 12 files (12 parquet)

| Query | n | ok | p50 ms | p95 ms | max ms |
|-------|---|----|--------|--------|--------|
| `avg_over_time(system_cpu_load_average_15m[2m])` | 38 | 38 | 36 | 42 | 47 |
| `max_over_time(system_cpu_load_average_15m[2m])` | 38 | 38 | 36 | 42 | 43 |
| `rate(system_cpu_load_average_15m[1m])` | 38 | 38 | 36 | 42 | 42 |
| `sum by (job) (rate(system_cpu_load_average_15m[1m]))` | 38 | 38 | 37 | 42 | 51 |
| `sum(rate(system_cpu_load_average_15m[1m]))` | 38 | 38 | 36 | 41 | 47 |
| `system_cpu_load_average_15m` | 38 | 38 | 36 | 56 | 68 |
| `{__name__="system_cpu_load_average_15m",job="prom-bench-host"}` | 38 | 38 | 38 | 41 | 42 |
| `{__name__="system_cpu_load_average_15m"}` | 38 | 38 | 36 | 43 | 48 |

Harness: `tests/compat/prometheus/benchmark/` (Option A).

## vs clean `baseline` (`20260814T005449Z`)

| | baseline | variant-pushdown |
|--|----------|------------------|
| overall p50 / p95 / max | 36 / 41 / 93 ms | 37 / 42 / 68 ms |
| RSS | 423544 KiB | 427772 KiB |
| parquet files | 12 | 12 |
| job equality query | (not in suite) | 38/38 ok @ p50=38ms |

**Read:** On this light hostmetrics corpus, `__name__` pushdown already dominated; switching
`job`/`instance` from JSON-extract to VARIANT did not move p50/p95. Max dropped
slightly (noise). Fix is still correct for larger cardinality / shredded attributes
(demo + Grafana). Next A/B should stress label filters or denser ingest.
