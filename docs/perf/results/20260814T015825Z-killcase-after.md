# Prom micro-benchmark — `killcase-after`

- UTC: `20260814T015825Z`
- Git: `1d0a827`
- Metric under test: `bench_http_requests`
- Warm-up / measure: 20s / 30s (repeat=2, rounds=8)
- Requests: 160/160 ok
- Latency overall: p50=132ms p95=283ms max=319ms
- Softprobe RSS: 513496 KiB
- Data dir: 282855 bytes, 43 files
- Parquet: before compact=43 after=1 (force_parquet=False)

| Query | n | ok | p50 ms | p95 ms | max ms |
|-------|---|----|--------|--------|--------|
| `avg_over_time(bench_http_requests[2m])` | 16 | 16 | 140 | 160 | 160 |
| `bench_http_requests` | 16 | 16 | 157 | 286 | 319 |
| `max_over_time(bench_http_requests[2m])` | 16 | 16 | 144 | 275 | 282 |
| `rate(bench_http_requests[1m])` | 16 | 16 | 158 | 285 | 288 |
| `sum by (job) (rate(bench_http_requests[1m]))` | 16 | 16 | 127 | 145 | 158 |
| `sum by (job) (rate(bench_http_requests{job="svc-020"}[1m]))` | 16 | 16 | 27 | 90 | 94 |
| `sum(rate(bench_http_requests[1m]))` | 16 | 16 | 129 | 155 | 287 |
| `{__name__="bench_http_requests",job="svc-020",instance="svc-020-i0"}` | 16 | 16 | 28 | 97 | 116 |
| `{__name__="bench_http_requests",job="svc-020"}` | 16 | 16 | 32 | 98 | 113 |
| `{__name__="bench_http_requests"}` | 16 | 16 | 139 | 162 | 287 |

Harness: `tests/compat/prometheus/benchmark/` (Option A).
