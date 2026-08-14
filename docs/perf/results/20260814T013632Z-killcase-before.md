# Prom micro-benchmark — `killcase-before`

- UTC: `20260814T013632Z`
- Git: `1d0a827`
- Metric under test: `bench_http_requests`
- Warm-up / measure: 20s / 30s (repeat=2, rounds=7)
- Requests: 140/140 ok
- Latency overall: p50=222ms p95=256ms max=274ms
- Softprobe RSS: 472516 KiB
- Data dir: 282855 bytes, 43 files
- Parquet: before compact=43 after=1 (force_parquet=False)

| Query | n | ok | p50 ms | p95 ms | max ms |
|-------|---|----|--------|--------|--------|
| `avg_over_time(bench_http_requests[2m])` | 14 | 14 | 231 | 247 | 248 |
| `bench_http_requests` | 14 | 14 | 230 | 250 | 253 |
| `max_over_time(bench_http_requests[2m])` | 14 | 14 | 230 | 245 | 259 |
| `rate(bench_http_requests[1m])` | 14 | 14 | 226 | 244 | 265 |
| `sum by (job) (rate(bench_http_requests[1m]))` | 14 | 14 | 220 | 235 | 263 |
| `sum by (job) (rate(bench_http_requests{job="svc-020"}[1m]))` | 14 | 14 | 52 | 55 | 57 |
| `sum(rate(bench_http_requests[1m]))` | 14 | 14 | 216 | 225 | 253 |
| `{__name__="bench_http_requests",job="svc-020",instance="svc-020-i0"}` | 14 | 14 | 50 | 55 | 57 |
| `{__name__="bench_http_requests",job="svc-020"}` | 14 | 14 | 53 | 56 | 57 |
| `{__name__="bench_http_requests"}` | 14 | 14 | 236 | 273 | 274 |

Harness: `tests/compat/prometheus/benchmark/` (Option A).
