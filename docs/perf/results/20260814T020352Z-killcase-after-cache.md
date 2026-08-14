# Prom micro-benchmark — `killcase-after-cache`

- UTC: `20260814T020352Z`
- Git: `1d0a827`
- Metric under test: `bench_http_requests`
- Warm-up / measure: 20s / 30s (repeat=2, rounds=12)
- Requests: 240/240 ok
- Latency overall: p50=27ms p95=268ms max=305ms
- Softprobe RSS: 544608 KiB
- Data dir: 276267 bytes, 42 files
- Parquet: before compact=42 after=1 (force_parquet=False)

| Query | n | ok | p50 ms | p95 ms | max ms |
|-------|---|----|--------|--------|--------|
| `avg_over_time(bench_http_requests[2m])` | 24 | 24 | 29 | 139 | 154 |
| `bench_http_requests` | 24 | 24 | 28 | 289 | 305 |
| `max_over_time(bench_http_requests[2m])` | 24 | 24 | 27 | 290 | 290 |
| `rate(bench_http_requests[1m])` | 24 | 24 | 29 | 281 | 285 |
| `sum by (job) (rate(bench_http_requests[1m]))` | 24 | 24 | 24 | 253 | 275 |
| `sum by (job) (rate(bench_http_requests{job="svc-020"}[1m]))` | 24 | 24 | 24 | 97 | 106 |
| `sum(rate(bench_http_requests[1m]))` | 24 | 24 | 23 | 137 | 141 |
| `{__name__="bench_http_requests",job="svc-020",instance="svc-020-i0"}` | 24 | 24 | 23 | 110 | 121 |
| `{__name__="bench_http_requests",job="svc-020"}` | 24 | 24 | 22 | 103 | 124 |
| `{__name__="bench_http_requests"}` | 24 | 24 | 30 | 138 | 154 |

Harness: `tests/compat/prometheus/benchmark/` (Option A).
