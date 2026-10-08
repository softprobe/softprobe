# Prom micro-benchmark — `killcase-lean-scalar`

- UTC: `20260814T043035Z`
- Git: `c41d23e`
- Metric under test: `bench_http_requests`
- Warm-up / measure: 20s / 30s (repeat=2, rounds=11)
- Requests: 220/220 ok
- Latency overall: p50=25ms p95=391ms max=450ms
- Softprobe RSS: 654500 KiB
- Data dir: 474258 bytes, 45 files
- Parquet: before compact=45 after=2 (force_parquet=False)

| Query | n | ok | p50 ms | p95 ms | max ms |
|-------|---|----|--------|--------|--------|
| `avg_over_time(bench_http_requests[2m])` | 22 | 22 | 25 | 186 | 230 |
| `bench_http_requests` | 22 | 22 | 25 | 401 | 450 |
| `max_over_time(bench_http_requests[2m])` | 22 | 22 | 25 | 393 | 400 |
| `rate(bench_http_requests[1m])` | 22 | 22 | 26 | 408 | 415 |
| `sum by (job) (rate(bench_http_requests[1m]))` | 22 | 22 | 21 | 167 | 384 |
| `sum by (job) (rate(bench_http_requests{job="svc-020"}[1m]))` | 22 | 22 | 20 | 114 | 118 |
| `sum(rate(bench_http_requests[1m]))` | 22 | 22 | 20 | 174 | 175 |
| `{__name__="bench_http_requests",job="svc-020",instance="svc-020-i0"}` | 22 | 22 | 21 | 109 | 127 |
| `{__name__="bench_http_requests",job="svc-020"}` | 22 | 22 | 20 | 113 | 127 |
| `{__name__="bench_http_requests"}` | 22 | 22 | 25 | 187 | 401 |

Harness: `tests/compat/prometheus/benchmark/` (Option A).
