# Senior engineer review — Grafana PromQL pack (option C)

**Branch:** `feat/compat-phase1-prometheus`  
**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**PR:** https://github.com/softprobe/thelake/pull/35  
**Initial verdict:** REQUEST_CHANGES  
**After disposition:** APPROVE_WITH_FIXES

## Findings → disposition

| # | Finding | Disposition |
|---|---------|-------------|
| 1 | `round()` half-ties used Rust `.round()` (away from 0); Prom uses `Floor(v/nearest+0.5)` | **Fixed** — Prom formula; unit + `functions_math.test` cover `round(-1.5)→-1` |
| 2 | Parenthesized range args (`rate((m[5m]))`) rejected at parse | **Fixed** — `unwrap_parens` in `validate_call` + parse unit test |
| 3 | `avg_over_time` skipped NaNs (comment claimed Prom parity) | **Fixed** — include NaNs (poison mean); MemBackend unit test; comment notes OTLP drops NaNs |
| 4 | Offset oracle instant-only | **Fixed** — `sum_over_time(...[10m] offset 5m)` in `selectors_offset.test` |
| 5 | DRY allowlists duplicated parse/eval | **Fixed** — `src/compat/promql/funcs.rs` |
| 6 | Docs “default matching” omitted `__name__` ignore | **Fixed** — matrix + phase1 wording |
| 7–9 | topk ties / NaN skip DRY / matrix date | Accepted / minor; date bumped |

## Accepted risks

- No range-boundary extrapolation for `rate`/`increase`/`delta` (documented in `queryability.md`).
- Explicit unsupported: `on`/`ignoring`/`group_*`/`@`/subquery/full catalog.

## Verification (post-disposition)

```text
make check-fmt && make lint && make test   # green
make test-promqltest                       # green — 100 curated oracle evals
```
