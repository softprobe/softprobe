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

---

## Module layout

```
src/compat/prometheus/   # handlers, params, encode, diff_normalize
src/compat/promql/       # parse (promql-parser), eval
src/compat/backends/ducklake_metrics.rs
src/compat/backends/metrics.rs   # MetricsQueryBackend + matchers
```

Wiring: `api/mod.rs` merges `prometheus_routes()`; Loki/Tempo remain stubs.

---

## Declared PromQL subset

See matrix + capability. Supported: selectors/matchers, instant+range, `sum|min|max|avg|count` with `by`/`without`, arithmetic/comparison, `rate`/`irate`/`increase`.

Explicit unsupported (non-exhaustive): `@`, `offset`, subqueries, set ops, `group_left`/`group_right`, histogram functions, summary `_quantile` expansion, recording rules/alerts.

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
| `max_query_range_seconds` | `QueryLimits::validate_time_range_ms` (handlers + backend) |
| `max_series` | Hard fail on series / distinct label values over cap |
| scan_cap (`max(max_series*10, 10000)`) | `LIMIT scan_cap+1` over the time window (or full table if unbounded); if overrun → `limit_exceeded`. Success means the scan returned every row in scope (matchers applied in-memory after a complete scan). |
| `query_timeout` | Deadline via `TenantContext::remaining()` |
| `max_response_bytes` | Enforced on success envelope encode; overrun → `limit_exceeded` |

---

## Mini differential (Slice C)

- Fixtures: `tests/compat/prometheus/diff/`
- Oracle: `prom/prometheus:v2.54.1` (see `references.v0.yaml`)
- Normalize: label order + float tolerance only
- Run: `make test-prom-diff` (Docker required)

---

## Tests

| Suite | Coverage |
|-------|----------|
| `tests/compat/prometheus/` | Auth / envelope fixtures |
| `tests/integration/prometheus_phase1.rs` | Ingest → labels/series/query; two-tenant isolation; unsupported PromQL |
| `tests/integration/prometheus_diff.rs` | Mini-diff vs pinned Prometheus (`#[ignore]` + make target) |
| Unit | matcher regex fail-loud, PromQL reject table, evaluator |
