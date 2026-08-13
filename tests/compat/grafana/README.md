# Grafana compatibility

Pinned Grafana image: `grafana/grafana:11.2.0` (see `docs/compat/references.v0.yaml`).

## Manual stack (team-reproducible)

From the repo root:

```bash
make grafana-up      # Softprobe + Grafana + OpenTelemetry Demo (Astronomy Shop)
# open http://127.0.0.1:3000  (admin / admin)
# Dashboards → Softprobe → Softprobe Prometheus smoke
# Store UI:    http://127.0.0.1:8080
make grafana-down
```

What it starts:

| Piece | Where |
|-------|--------|
| Softprobe runtime | host `:8090` (sqlite DuckLake under `/tmp/thelake-grafana-manual/`) |
| Auth mock | compose `:18080` → Bearer `local-dev-key` → tenant `local-dev-tenant` |
| Grafana | compose `:3000`, Prom datasource → `host.docker.internal:8090` |
| Traffic | Official [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo) **3.0.0** (minimal, no demo o11y stack). Multi-language services + load-generator; Collector extras export OTLP/HTTP to Softprobe. |

Requires Docker and ~3 GB RAM. Demo checkout is cached under
`~/.cache/thelake/otel-demo/3.0.0` (see [`otel-demo/README.md`](otel-demo/README.md)).

Panels target live Astronomy Shop series (spanmetrics / HTTP / RPC when present).
Use Explore if a panel is empty — metric names vary by service SDK.

Correctness vs Prometheus remains `make test-prom-compat`.

Scripts: [`scripts/grafana-manual-up.sh`](../../../scripts/grafana-manual-up.sh),
[`scripts/grafana-manual-down.sh`](../../../scripts/grafana-manual-down.sh).
Compose: [`docker-compose.manual.yml`](docker-compose.manual.yml).

Do **not** use the root `docker-compose.yml` Grafana service (legacy DuckDB plugin / wrong pin).

## Prom-only CI smoke

- Provisioning: [`provisioning/datasources/prometheus.yaml`](provisioning/datasources/prometheus.yaml)
- Dashboard JSON: [`dashboards/softprobe-prom-smoke.json`](dashboards/softprobe-prom-smoke.json)
- Automated HTTP smoke (Grafana-shaped Prom API + Bearer, no Grafana container):
  `tests/integration/grafana_prom_smoke.rs` via `make test` / `make test-grafana-prom-smoke`

Covers discovery, GET↔POST, `rate`, `offset`, `*_over_time`, `sum by`, `topk`, compare,
arith, and a `query_range` timing bound (prefetch regression). It does **not** start Grafana,
the OTel Demo, or Playwright Explore.

**Correctness vs Prometheus** stays on `make test-prom-compat` (mini-diff + curated promqltest).

## Still pending (full Grafana Explore CI)

- Loki / Tempo datasource provisioning and smoke
- Pinned Grafana container + Explore / dashboard JSON assertions in CI
- Cross-signal links and multi-tenant Grafana credentials
