# Metrics layout — multi-window / series-type coverage review

**Date:** 2026-08-17  
**Scope:** Classic hist/summary empty panels on Grafana `now-3h`+; gauge/counter grain ladder

## Verdict

**Not yet safe to claim full window coverage for release** until a new `release_full` JSON shows **53/53** including AC-H3..H6. Unit/SQL regression coverage for the hist divert bug is in place and should be treated as **merge-blocking for the grain fix**.

## Findings (severity order)

### Blocker (resolved in code; gate not re-run)

1. **Classic hist/summary >2h diverted to `metric_samples_1h`** → empty Grafana panels.  
   Fix: `select_sample_grain(..., is_histogram=true)` always returns `Hist`.  
   Catch: AC-H3/H4 harness Prom windows + `hist_selector_always_uses_hist_table` + `hist_prom_sql_uses_hist_table_for_mid_and_long_windows` + `window_series_type_grain_matrix`.

### Major (fixed in this change set)

2. **Harness AC-H2 still mapped to deleted cargo filter** `hist_selector_uses_hist_table_short_range` — would silently fail unit batch. Now → `hist_selector_always_uses_hist_table`.
3. **Required AC set was 49 and only exercised ~30m hist** — reviewer/coordinator miss. Now AC-H3..H6 + validator len **53**.
4. **Summary `_sum`/`_count`** covered via same classic-suffix selector + AC-H5 SQL cases (not a separate storage path).

### Minor / documented non-goals

5. **Gauge/counter 3h → `FiveMin`**: empty until maintenance fills `metric_samples_5m` is **by design** (§9.1). Not the same bug class as hist (numeric downsample *can* serve gauges). Product follow-up only if Grafana demo requires live curves without ladder — open non-goal for AC-H\*.
6. **Name collision**: any non-hist metric whose Prom name ends in `_count`/`_sum`/`_bucket` is treated as classic hist grain (`is_classic_hist_selector`). Pre-existing; unlikely for Softprobe GOLD names.
7. **Prior `release_full` JSON (49/49)** is stale vs 53-id gate — do not use `--ready` on old artifacts.

## Evidence map

| AC | Evidence |
|----|----------|
| H1 | Harness Prom 30m + ingest row counts |
| H2 | Cargo `hist_selector_always_uses_hist_table` (+ postings SQL helper) |
| H3 | Harness Prom 3h non-empty |
| H4 | Harness Prom 24h non-empty |
| H5 | Cargo mid/long SQL includes `_sum` |
| H6 | Cargo `window_series_type_grain_matrix` |

## Pass criteria for “safe to claim”

- [ ] `cargo test` grain + postings hist filters green  
- [ ] Validator unittest expects 53  
- [ ] New harness JSON 53/53 + `validate-metrics-layout-results.py --ready`
