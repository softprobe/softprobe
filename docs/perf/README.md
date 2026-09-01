# Softprobe Runtime — performance docs

| Doc | Purpose |
|-----|---------|
| [prometheus-query-findings.md](prometheus-query-findings.md) | Prom/Grafana findings, improvement plan, open competitor benchmark |
| [metrics-timeseries-layout.md](../metrics-timeseries-layout.md) | Proposed DuckLake metrics layout: postings + skinny samples + 5m/1h ladder + collapse; goals and test plan |

## Local micro-benchmark (Option A)

```bash
make bench-prom-baseline
# or: BENCH_LABEL=variant-pushdown LEAVE_UP=1 make bench-prom-baseline
make bench-prom-down
```

Harness: `tests/compat/prometheus/benchmark/`.  
Results land in [`results/`](results/) as `<stamp>-<label>.json` + `.md`.

## Competitor compare (Option B — later)

VictoriaMetrics [prometheus-benchmark](https://github.com/VictoriaMetrics/prometheus-benchmark) via OTLP — see findings doc §5.2.
