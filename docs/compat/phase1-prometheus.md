# Phase 1 — Prometheus (design)

**Issue:** [#30](https://github.com/softprobe/thelake/issues/30)  
**Branch:** `feat/compat-phase1-prometheus`  
**Status:** Slices A–C implemented (discovery + PromQL subset + mini-diff)

Canonical contracts: [`matrix.md`](matrix.md), [`projections.md`](projections.md), [`capability.v0.yaml`](capability.v0.yaml), [`queryability.md`](queryability.md).

---

## Architecture

```
HTTP /api/v1/*  →  TenantContext (auth only)
                →  DuckLakeMetricsBackend (SQL via QueryEngine)
                →  project_prometheus_labels / classic series expand
                →  promql-parser AST → Softprobe evaluator
                →  Prometheus JSON envelopes
```

Rules:

- Handlers parse params / call backend / encode only — **no raw SQL** in HTTP.
- Tenant only from `Extension<TenantInfo>` / `AppState::engine_for_tenant`.
- SQL reads the rewritten `union_metrics` relation (not bare `metrics`).
- Unsupported AST → `unsupported_feature` via `compat::envelopes`.
- Scan / series / label-value caps fail loud (`limit_exceeded`); invalid matcher regex → `bad_data`.
- **`query_range` / PromQL range eval:** one DuckDB fetch per unique selector
  (window = `[start − lookback|range − offset, end − offset]`), then evaluate
  every `step` in memory. Do **not** issue SQL per step (Grafana refresh was
  O(steps) otherwise). Equality pushdown for `__name__` / `job` / `instance`
  uses typed `metric_name` and VARIANT field access (not JSON extract).

---

## Module layout

```
src/compat/prometheus/   # handlers, params, encode, diff_normalize
src/compat/promql/       # parse (promql-parser), eval
src/compat/backends/ducklake_metrics.rs
src/compat/backends/metrics.rs   # MetricsQueryBackend + matchers
```

Wiring: `api/mod.rs` merges `prometheus_routes()`; Loki/Tempo remain stubs.
Manual Grafana: `make grafana-up` (OpenTelemetry Demo traffic; see `tests/compat/grafana/README.md`).

---

## Declared PromQL subset

See matrix + capability. Supported: selectors/matchers, instant+range, `sum|min|max|avg|count|topk|bottomk` with `by`/`without`, arithmetic/comparison, set ops `and`/`or`/`unless` (default matching ignores `__name__` like Prometheus), `rate`/`irate`/`increase`/`delta`/`idelta`, `*_over_time` (`sum|avg|min|max|count|last`), `abs`/`ceil`/`floor`/`round`, `offset`.

Explicit unsupported (non-exhaustive): `@`, subqueries, `on()`/`ignoring()`, `group_left`/`group_right`, histogram functions, summary `_quantile` expansion, recording rules/alerts, full function catalog.

---

## Classic series expansion

| OTel type | Exposed series |
|-----------|----------------|
| gauge / sum | sanitized `__name__` |
| histogram | `{name}_bucket{le=…}`, `{name}_sum`, `{name}_count` |
| summary | `{name}_sum`, `{name}_count`, base `{name}` — **no** `_quantile` |

---

## Limits

| Limit | Behavior |
|-------|----------|
| `max_query_range_seconds` | `QueryLimits::validate_time_range_ms` (`0` = unlimited; handlers + backend) |
| `max_series` | Hard fail on series / distinct label values over cap |
| scan_cap (`max(max_series*10, 10000)`) | `LIMIT scan_cap+1` over the time window (or full table if unbounded); equality `__name__` / `job` matchers are pushed into SQL (classic `_bucket`/`_sum`/`_count` stripped to base storage name). Remaining matchers apply in-memory after projection. Overrun → `limit_exceeded`. |
| `query_timeout` | Deadline via `TenantContext::remaining()` |
| `max_response_bytes` | Enforced on success envelope encode; overrun → `limit_exceeded` |

---

## Performance (findings + plan)

Storage-feature utilization, small-file/compaction gaps, and an open
competitor-comparable benchmark plan (VictoriaMetrics
`prometheus-benchmark` via OTLP write + Prom query):
[`../perf/prometheus-query-findings.md`](../perf/prometheus-query-findings.md).
Physical layout goals and machine ACs:
[`../metrics-timeseries-layout.md`](../metrics-timeseries-layout.md).

## Mini differential (Slice C)

- Fixtures: `tests/compat/prometheus/diff/`
- Oracle: `prom/prometheus:v2.54.1` (see `references.v0.yaml`)
- Normalize: label order + float tolerance only
- Run: `make test-prom-diff` (Docker required)

## Upstream promqltest (curated)

- Fixtures: `tests/compat/prometheus/promqltest/curated/` (Apache-2.0 excerpts from Prometheus `v2.54.1`)
- Attribution: `tests/compat/prometheus/promqltest/ATTRIBUTION.md`
- Runner loads the same series into lake + pinned Prom, executes supported `eval`s, compares normalized JSON
- Curated set covers range evals, irate/rate/increase (dense + sparse extrapolatedRate), compare/`bool`, by(job), `%`/`^`, literals, selector edges, `*_over_time`, set ops, topk/bottomk, delta/idelta, offset
- Unsupported AST in curated fixtures **fails** (no silent skip)
- Timeline: samples and eval times are shifted from unix-0 to a fixed base so OTLP ingest does not treat timestamp 0 as “now”
- Run: `make test-promqltest` or `make test-prom-compat`

---

## Tests

| Suite | Coverage |
|-------|----------|
| `tests/compat/prometheus/` | Auth / envelope fixtures |
| `tests/integration/prometheus/promqltest.rs` | Lake HTTP API contracts; curated upstream promqltest vs pinned Prometheus (`make test-promqltest`) |
| `tests/integration/prometheus/diff.rs` | Mini-diff vs pinned Prometheus (`#[ignore]` + `make test-prom-diff`) |
| Unit | matcher regex fail-loud, PromQL reject table, evaluator |
