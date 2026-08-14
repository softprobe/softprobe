# OpenTelemetry Demo (Astronomy Shop) — Softprobe Grafana traffic source

Pinned release: **3.0.0** (`ghcr.io/open-telemetry/demo:3.0.0-*`).

`make grafana-up` clones the pin into `~/.cache/thelake/otel-demo/3.0.0` (override
with `THELAKE_CACHE_ROOT`) and starts **minimal + no demo observability stack**:

```text
docker compose -f compose.yaml -f <thelake>/compose.softprobe.yaml
```

Collector extras ([`otelcol-config-extras.yml`](otelcol-config-extras.yml)) export
traces/metrics/logs to Softprobe at `host.docker.internal:8090` with Bearer
`local-dev-key`.

Requires ~3 GB RAM and Docker. Store UI: http://127.0.0.1:8080
