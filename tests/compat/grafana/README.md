# Grafana compatibility (Phase 4)

Pinned Grafana image: `grafana/grafana:11.2.0` (see `docs/compat/references.v0.yaml`).

## Manual stack (team-reproducible)

From the repo root:

```bash
make grafana-up      # build Softprobe, compose Grafana+auth-mock, seed demo metrics
# open http://127.0.0.1:3000  (admin / admin)
# Dashboards → Softprobe → Softprobe Prometheus smoke
make grafana-down
```

What it starts:

| Piece | Where |
|-------|--------|
| Softprobe runtime | host `:8090` (sqlite DuckLake under `/tmp/thelake-grafana-manual/`) |
| Auth mock | compose `:18080` → Bearer `local-dev-key` → tenant `local-dev-tenant` |
| Grafana | compose `:3000`, Prom datasource → `host.docker.internal:8090` |
| Seed | `grafana_seed_otlp` → `http.requests` jobs `checkout` (ramp) + `payments` (sine) |

Each `grafana-up` wipes the disposable DuckLake so only **one** seed is present. Re-running
`grafana-up` while already up does **not** re-seed (overlapping ramps break `rate()`).

### Expected panel shapes

| Panel | Expected |
|-------|----------|
| `http_requests` | checkout ramp up; payments sine |
| `rate(...[5m])` | nearly **flat** ≈ `0.0167/s` on checkout (not a ramp) |
| `avg_over_time` | rising for checkout (smoothed ramp) |
| `sum by (job)` | same shapes as raw |
| `topk(1, …)` | usually checkout (higher) |
| `offset 1m` | ramp shifted earlier |
| `> 40` | filtered series |

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
arith, and a `query_range` timing bound (prefetch regression). It does **not** start Grafana
or run Playwright Explore.

**Correctness vs Prometheus** stays on `make test-prom-compat` (mini-diff + curated promqltest).

## Still pending (full #27)

- Loki / Tempo datasource provisioning and smoke
- Pinned Grafana container + Explore / dashboard JSON assertions in CI
- Cross-signal links and multi-tenant Grafana credentials
