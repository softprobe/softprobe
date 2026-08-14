# Grafana compatibility

Pinned Grafana image: `grafana/grafana:11.2.0` (see `docs/compat/references.v0.yaml`).

## Manual stack (team-reproducible)

From the repo root:

```bash
make grafana-up      # Softprobe + Grafana + OpenTelemetry Demo (Astronomy Shop)
# open http://127.0.0.1:3000  (admin / admin)
# Dashboards → Softprobe → …
# Store UI:    http://127.0.0.1:8080
make grafana-down
```

What it starts:

| Piece | Where |
|-------|--------|
| Softprobe runtime | host `:8090` (sqlite DuckLake under `/tmp/thelake-grafana-manual/`) |
| Auth mock | compose `:18080` → Bearer `local-dev-key` → tenant `local-dev-tenant` |
| Grafana | compose `:3000`, Prom datasource → `host.docker.internal:8090` |
| Traffic | Official [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo) **3.0.0** (minimal, Softprobe BYO OTLP backend) |

Requires Docker and ~3 GB RAM. Demo checkout: `~/.cache/thelake/otel-demo/3.0.0`
(see [`otel-demo/README.md`](otel-demo/README.md)).

### Dashboards (folder Softprobe)

Provisioned from [`dashboards/`](dashboards/). Each maps a supported PromQL family
onto live Astronomy Shop metrics:

| Dashboard | Covers |
|-----------|--------|
| Softprobe · Overview | Load-generator + ad/cart/HTTP/CPU overview |
| Softprobe Prometheus smoke | Bookmark-compatible short smoke set |
| Softprobe · Selectors & matchers | `=`, `=~`, `!=`, `!~`, multi-matcher |
| Softprobe · rate / irate / increase / delta | `rate`, `irate`, `increase`, `delta`, `idelta` |
| Softprobe · Aggregations | `sum\|min\|max\|avg\|count` + `by`/`without`, `topk`/`bottomk` |
| Softprobe · Arithmetic, compare, set ops | `+-*/%^`, compare/`bool`, `and`/`or`/`unless` |
| Softprobe · over_time, math, offset | `*_over_time`, `abs`/`ceil`/`floor`/`round`, `offset` |
| Softprobe · Classic histogram series | `_bucket` / `_sum` / `_count` (no `histogram_quantile`) |

Unsupported on purpose (capability): `@`, subqueries, `on()`/`ignoring()`,
`group_left`/`group_right`, histogram quantiles, full function catalog.

Correctness vs Prometheus remains `make test-prom-compat`.

Scripts: [`scripts/grafana-manual-up.sh`](../../../scripts/grafana-manual-up.sh),
[`scripts/grafana-manual-down.sh`](../../../scripts/grafana-manual-down.sh).
Compose: [`docker-compose.manual.yml`](docker-compose.manual.yml).

Do **not** use the root `docker-compose.yml` Grafana service (legacy DuckDB plugin / wrong pin).

## Prom-only CI smoke

- Provisioning: [`provisioning/datasources/prometheus.yaml`](provisioning/datasources/prometheus.yaml)
- Automated HTTP smoke (Grafana-shaped Prom API + Bearer, no Grafana container):
  `tests/integration/grafana_prom_smoke.rs` via `make test` / `make test-grafana-prom-smoke`

It does **not** start Grafana, the OTel Demo, or Playwright Explore.

**Correctness vs Prometheus** stays on `make test-prom-compat` (mini-diff + curated promqltest).

## Still pending (full Grafana Explore CI)

- Loki / Tempo datasource provisioning and smoke
- Pinned Grafana container + Explore / dashboard JSON assertions in CI
- Cross-signal links and multi-tenant Grafana credentials
