# Grafana compatibility

Pinned Grafana image: `grafana/grafana:11.2.0` at the immutable digest in
`docs/compat/references.v0.yaml`. Compose never falls back to a tag-only
Grafana or WireMock image.

## Manual stack (team-reproducible)

```bash
make grafana-up      # Softprobe + Grafana + OpenTelemetry Demo (Astronomy Shop)
# open http://127.0.0.1:3000  (admin / admin)
# Store UI:    http://127.0.0.1:8080
make grafana-down
```

`make grafana-up` validates the checked-in manual compose image pins and passes
the manifest-derived `GRAFANA_COMPOSE_IMAGE=image@sha256:...` to Compose.

What it starts:

| Piece | Where |
|-------|--------|
| Softprobe runtime | host `:8090` |
| DuckLake catalog | Postgres 19 (`postgres:19beta3` until stable `:19` tag ships) on `:5434` |
| Parquet data | `/tmp/thelake-grafana-manual/data/` |
| Auth mock | `:18080` → Bearer `local-dev-key` |
| Grafana | `:3000` → Prom datasource Softprobe |
| Traffic | OpenTelemetry Demo **3.0.0** (minimal, Softprobe BYO backend) |

Requires Docker + ~3 GB RAM. Demo cache: `~/.cache/thelake/otel-demo/3.0.0`.

Collector extras send **metrics only** to Softprobe (OTLP + ad Prometheus + spanmetrics),
in small batches, so Grafana GOLD panels get live non-flat scrapes. Traces/logs stay on
the collector `debug` exporter (spanmetrics still produced). `grafana-up` refuses to
declare ready on lookback-only flat series.

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

#### Softprobe (Loki / Tempo / cross-signal)

| Fixture | Contract coverage |
|---------|-------------------|
| `softprobe-prom-smoke` | Prometheus time-series panels plus the `stat` panel `Checkout request total`. |
| `softprobe-loki-smoke` | Loki log panel using the `service` label variable and a `service_name` label selector plus `checkout` line filter. |
| `softprobe-tempo-smoke` | Tempo TraceQL search panel for the `api` service. |
| `softprobe-cross-signal` | Prometheus `job` and Loki `service` variables plus tenant-A native datasource pivots. |

Cross-signal navigation is concrete and tenant-scoped: Loki datasource derived
fields target `softprobe-tempo-a`/`softprobe-tempo-b`, while Tempo
`tracesToLogsV2` targets `softprobe-loki-a`/`softprobe-loki-b`. The cross-signal
dashboard also provides tenant-A Explore links for the Loki and Tempo native
datasources. These links complement, rather than replace, the provisioning
checks in G7.

Unsupported on purpose (capability): `@`, subqueries, `on()`/`ignoring()`,
`group_left`/`group_right`, histogram quantiles, full function catalog.

### Notes

- Prefer `rate()` / gauges for charts; avoid `avg_over_time()` on cumulative counters.
- Docker stats use `container_name`, not `job`.
- High-cardinality `sum(rate(k6_http_reqs))` rises as series appear — prefer `k6_iterations` / business counters.
- Correctness vs Prometheus: `make test-prom-compat`.

Scripts: [`scripts/grafana-manual-up.sh`](../../../scripts/grafana-manual-up.sh),
[`scripts/grafana-manual-down.sh`](../../../scripts/grafana-manual-down.sh).
Demo overlay: [`otel-demo/`](otel-demo/).

## Prom-only CI smoke

- Provisioning: [`provisioning/datasources/prometheus.yaml`](provisioning/datasources/prometheus.yaml)
- Automated HTTP smoke (Grafana-shaped Prom API + Bearer, no Grafana container):
  `tests/integration/grafana_prom_smoke.rs` via `make test` / `make test-grafana-prom-smoke`

It does **not** start Grafana, the OTel Demo, or Playwright Explore.

**Correctness vs Prometheus** stays on `make test-prom-compat` (mini-diff + curated promqltest).

## CI compose smoke

[`docker-compose.ci.yml`](docker-compose.ci.yml) is the self-contained Grafana
container artifact for the Phase 4 smoke lane. It pins
`grafana/grafana:11.2.0`, mounts [`provisioning/`](provisioning/) and
[`dashboards/`](dashboards/) read-only, and reports readiness through
`GET /api/health` on port `3000`.

The CI compose file receives `GRAFANA_COMPOSE_IMAGE` as the immutable
`image@digest` derived from `docs/compat/references.v0.yaml`; `make
test-grafana-system` validates the tag and digest before starting the stack.
The compose seeder provisions both fixed tenants, sends deterministic OTLP
metrics/logs/traces, and must report all three signals queryable before Grafana
starts.
`scripts/grafana-system-smoke.sh` derives the expected image/tag from the

Set `SOFTPROBE_URL`, `SOFTPROBE_API_KEY`,
`SOFTPROBE_TENANT_A_API_KEY`, `SOFTPROBE_TENANT_B_API_KEY`,
`SOFTPROBE_TENANT_A_ID`, and `SOFTPROBE_TENANT_B_ID` before starting it. The
admin credentials default to `admin`/`admin` for the ephemeral container and
must not be written to artifacts.

From the repository root:

```bash
GRAFANA_COMPOSE_IMAGE="$(make -s grafana-reference-image)" \
  docker compose -f tests/compat/grafana/docker-compose.ci.yml up -d --wait
docker compose -f tests/compat/grafana/docker-compose.ci.yml ps
docker compose -f tests/compat/grafana/docker-compose.ci.yml down --volumes
```

### Exact G-case checklist

Record each case as `PASS`, `FAIL`, or `SKIP`; do not treat an unrun case as a
pass. G-cases validate Grafana wiring and protocol use, not a new query
semantics contract.

| Case | Required check | Pass condition |
|------|----------------|----------------|
| G1 | Start the CI compose service and poll `/api/health`. | Grafana reaches HTTP 200 and reports a healthy database before the timeout. |
| G2 | Inspect provisioned datasource UIDs. | `softprobe-prom`, `softprobe-prom-a`, `softprobe-prom-b`, `softprobe-loki-a`, `softprobe-loki-b`, `softprobe-tempo-a`, and `softprobe-tempo-b` are present. |
| G3 | Inspect the provisioned dashboard folder. | Folder `Softprobe` is present and every JSON file under `dashboards/` is loaded without a provisioning error. |
| G4 | Run the Prometheus Explore smoke for tenants A and B. | Each request uses its tenant bearer key and `X-Scope-OrgID`, returns data, and cannot read the other tenant. |
| G5 | Run the Loki Explore smoke for tenants A and B. | Each request returns the expected tenant-scoped streams with the matching Loki UID and cannot read the other tenant. |
| G6 | Run the Tempo Explore smoke for tenants A and B. | Trace lookup/search returns the expected tenant-scoped trace with the matching Tempo UID and cannot read the other tenant. |
| G7 | Follow both cross-signal links. | Loki `trace_id` derived fields target the matching Tempo tenant UID, and Tempo trace-to-logs targets the matching Loki tenant UID. |
| G8 | Exercise the protocol error/auth boundaries through Grafana. | Missing, invalid, or mismatched tenant credentials fail with the protocol-defined response; Grafana must not silently change it to success. |

The protocol tests are the oracle for G4–G8: Prometheus smoke/differential
tests, the Loki compatibility suite, and the Tempo compatibility suite define
the expected status, response shape, semantics, and tenant isolation. Grafana
smoke results may expose adapter or provisioning regressions, but they must
not waive or redefine a protocol-test failure.

### Skip and artifact rules

The dependency gate has exactly two permitted skips:

1. If `docker info` cannot run, record `SKIP: Docker unavailable` and exit the
   Grafana lane successfully without running G1–G8.
2. If a GNU `timeout` executable is unavailable (or `timeout --version` does
   not identify GNU coreutils), record `SKIP: GNU timeout unavailable` and exit
   the Grafana lane successfully without running G1–G8. BSD `timeout` is not a
   substitute; install GNU coreutils or use the documented skip.

Any compose failure, health timeout, protocol mismatch, dashboard load error,
or test timeout after the dependency gate is a `FAIL`, not a skip. A skip must
include its reason in the job summary; it must never be reported as a green
G-case result.

Write evidence below `target/compat/grafana/` and redact it before upload.
Permitted evidence is compose status, non-secret logs, the health response,
and sanitized datasource/dashboard responses. Remove bearer tokens, API keys,
passwords, cookies, authorization headers, tenant secrets, interpolated
environment values, and credential-bearing URLs. Never upload `.env` files,
raw `docker inspect` output, unredacted compose config, or user artifacts. If
redaction cannot be verified, do not upload the artifact.
