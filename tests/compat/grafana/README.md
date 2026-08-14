# Grafana compatibility

Pinned Grafana image: `grafana/grafana:11.2.0` (see `docs/compat/references.v0.yaml`).

## Manual stack (team-reproducible)

```bash
make grafana-up      # Softprobe + Grafana + OpenTelemetry Demo (Astronomy Shop)
# open http://127.0.0.1:3000  (admin / admin)
# Store UI:    http://127.0.0.1:8080
make grafana-down
```

| Piece | Where |
|-------|--------|
| Softprobe runtime | host `:8090` |
| DuckLake catalog | Postgres 19 (`postgres:19beta3` until stable `:19` tag ships) on `:5434` |
| Parquet data | `/tmp/thelake-grafana-manual/data/` |
| Auth mock | `:18080` → Bearer `local-dev-key` |
| Grafana | `:3000` → Prom datasource Softprobe |
| Traffic | OpenTelemetry Demo **3.0.0** (minimal, Softprobe BYO backend) |

Requires Docker + ~3 GB RAM. Demo cache: `~/.cache/thelake/otel-demo/3.0.0`.

### Dashboard folders

#### Astronomy Shop (service monitoring)

Real SRE-style monitoring against live demo metrics:

| Dashboard | Focus |
|-----------|--------|
| GOLD overview | Shop-wide HTTP/RPC/spanmetrics rates, business KPIs, loadgen, containers |
| Ad (Java) | `demo_ad_*`, HTTP server, JVM |
| Cart (.NET) | cart latency histograms, ASP.NET / .NET runtime |
| Checkout (Go) | RPC/HTTP client, Go memory/goroutines |
| Frontend (Node.js) | HTTP server/client, event loop, V8 |
| Payment (Node.js) | `demo_payment_transactions`, Node runtime |
| Recommendation (Python) | recommendations, CPython GC, process |
| Shipping | shipped items, HTTP RED |
| Currency & Quote | FX conversions, quotes, OTel SDK queues |
| Product Catalog | spanmetrics (sparse app metrics in this build) |
| Load generator (k6) | VUs, iterations, duration histograms, failures |
| Infrastructure | containers, Postgres, nginx, httpcheck |

#### Softprobe PromQL (capability smoke)

Declared PromQL subset coverage (selectors, rate family, aggregations, operators, over_time/math/offset, classic histograms). See [`dashboards/promql/`](dashboards/promql/).

### Notes

- Prefer `rate()` / gauges for charts; avoid `avg_over_time()` on cumulative counters.
- Docker stats use `container_name`, not `job`.
- High-cardinality `sum(rate(k6_http_reqs))` rises as series appear — prefer `k6_iterations` / business counters.
- Correctness vs Prometheus: `make test-prom-compat`.

Scripts: [`scripts/grafana-manual-up.sh`](../../../scripts/grafana-manual-up.sh),
[`scripts/grafana-manual-down.sh`](../../../scripts/grafana-manual-down.sh).
Demo overlay: [`otel-demo/`](otel-demo/).

## Prom-only CI smoke

`tests/integration/grafana_prom_smoke.rs` via `make test-grafana-prom-smoke` (no Grafana/Demo containers).
