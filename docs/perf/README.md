# Softprobe Runtime — performance docs

| Doc | Purpose |
|-----|---------|
| [prometheus-query-findings.md](prometheus-query-findings.md) | Prom/Grafana findings, improvement plan, open competitor benchmark |

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
