# Metrics layout — AC gap board

**SoT:** `docs/metrics-timeseries-layout.md`  
**Coordinator loop:** `.cursor/agents/metrics-layout-implement-loop.md`  
**Updated:** 2026-08-17

**§11 step:** multi-window hist ACs added (H3–H6); prior 49/49 gate is **stale**  
**Ready for verification:** **no** (need new `release_full` 53/53 JSON)  
**Latest JSON:** `docs/perf/results/20260816T084205Z-metrics-layout.json` (49 ids only)  
**Pass count target:** **53/53** (`binary_profile=release`, `fixture_profile=release_full`, `COMPARE_GREPTIME=1`)

## Why the board reopened

Reviewer/coordinator miss: AC-H1/H2 only covered ~30m hist. Grafana `now-3h` classic `_count` diverted to empty `metric_samples_1h`. Grain fix landed; tests/ACs must lock all windows.

| ID | Status | Notes |
|----|--------|-------|
| AC-H3 | wired | harness 3h Prom non-empty |
| AC-H4 | wired | harness 24h Prom non-empty |
| AC-H5 | wired | cargo mid/long SQL + `_sum` |
| AC-H6 | wired | cargo window×type matrix |
| AC-H2 filter | fixed | `hist_selector_always_uses_hist_table` |

## Explicit non-goal (this board)

Gauge/counter 3h → `FiveMin` empty until maintenance is design (§9.1), not the hist empty-grain class.

## Next gate

1. Cargo unit filters for H2/H5/H6 green  
2. Re-run `PERF_SUITE=metrics-layout` `release_full` + `COMPARE_GREPTIME=1`  
3. `validate-metrics-layout-results.py --ready` on new JSON
