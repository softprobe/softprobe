# Verification — Phase 1 Prometheus (#30) + coverage close

**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**PR:** https://github.com/softprobe/thelake/pull/35

## Gates

```text
make check-fmt && make lint && make test   # green
make test-prom-compat                      # green (mini-diff + promqltest)
```

Curated promqltest: **70** evals across 11 fixtures (incl. 3 `eval range`).

## Issue #30 exit criteria (honest)

| Criterion | Status |
|-----------|--------|
| HTTP query / query_range / labels / values / series / metadata | Met |
| PromQL subset (selectors, aggs, arith/compare, rate/irate/increase) | Met + regression |
| Unsupported → explicit error | Met |
| Differential vs pinned Prometheus | Met (`make test-prom-compat`) |
| Tenant isolation | Met (`prometheus_phase1`) |
| Limits/timeouts under test | Met (range limit + deadline wrap) |
| Official promqltest remote HTTP runner | Met (curated; not full 837) |
| Grafana smoke | Deferred (issue lists; not Phase 1 PR scope this slice) |
| Sparse rate extrapolation parity | Documented divergence (`queryability.md`) |
| Stale markers / native hist corpus | Explicit unsupported / deferred |

## PR #35 Codex comments

| Comment | Disposition |
|---------|-------------|
| Drop `__name__` on arith/`bool` | Fixed + unit tests |
| Keep LHS `__name__` on filtering vec–vec compare | Fixed + unit test |
| Metadata keyed by projected Prom name | Fixed; phase1 asserts `http_requests` |
| Time-range i64 overflow | Fixed via i128 + unit test |
| DuckDB await not deadline-bounded | Fixed: `tokio::time::timeout(ctx.remaining(), …)` |

## Coverage gap close (audit top priorities)

Added curated: `range_basic`, `functions_irate_rate`, `operators_compare_bool`, `aggregators_by_job`, `operators_extended`, `literals_scalar`, `selectors_edge`. Runner fail-loud on unsupported AST; scalar normalize for literals.
