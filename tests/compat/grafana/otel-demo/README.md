# OpenTelemetry Demo (Astronomy Shop) — Softprobe Grafana traffic source

Pinned release: **3.0.0** (`ghcr.io/open-telemetry/demo:3.0.0-*`).

`make grafana-up` clones the pin into `~/.cache/thelake/otel-demo/3.0.0` (override
with `THELAKE_CACHE_ROOT`) and starts **minimal + no demo observability stack**:

```text
docker compose -f compose.yaml -f <thelake>/compose.softprobe.yaml
```

Collector extras ([`otelcol-config-extras.yml`](otelcol-config-extras.yml)):

- Export **metrics only** to Softprobe (`host.docker.internal:8090`, Bearer `local-dev-key`)
- Receivers limited to GOLD sources: `otlp`, `prometheus/ad`, `span_metrics`
  (not docker_stats / host_metrics / redis / postgres — those starve ingest)
- Small batches (128–256) so DuckLake can keep up under Grafana refresh
- Traces feed `span_metrics` only; logs stay on `debug`

`grafana-up` waits until Prom series show **non-identical** samples (live scrapes),
not flat lookback lines from a single point.

Requires ~3 GB RAM and Docker. Store UI: http://127.0.0.1:8080
